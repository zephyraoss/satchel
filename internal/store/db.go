package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SchemaVersion    = "v1"
	DefaultChunkSize = 64 << 10
	RootIno          = 1
)

const schemaV1 = `
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS inodes (
  ino    INTEGER PRIMARY KEY,
  mode   INTEGER NOT NULL,
  uid    INTEGER NOT NULL DEFAULT 0,
  gid    INTEGER NOT NULL DEFAULT 0,
  size   INTEGER NOT NULL DEFAULT 0,
  atime  INTEGER NOT NULL,
  mtime  INTEGER NOT NULL,
  ctime  INTEGER NOT NULL,
  nlink  INTEGER NOT NULL DEFAULT 1,
  target TEXT
);
CREATE TABLE IF NOT EXISTS dentries (
  parent INTEGER NOT NULL REFERENCES inodes(ino),
  name   TEXT NOT NULL,
  ino    INTEGER NOT NULL REFERENCES inodes(ino),
  PRIMARY KEY (parent, name)
);
CREATE INDEX IF NOT EXISTS dentries_by_ino ON dentries(ino);
CREATE TABLE IF NOT EXISTS chunks (
  id   INTEGER PRIMARY KEY,
  ino  INTEGER NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE,
  idx  INTEGER NOT NULL,
  data BLOB NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS chunks_by_ino_idx ON chunks(ino, idx);
CREATE TABLE IF NOT EXISTS xattrs (
  ino   INTEGER NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE,
  name  TEXT NOT NULL,
  value BLOB NOT NULL,
  PRIMARY KEY (ino, name)
);
`

type DB struct {
	*sql.DB
	chunkSize int64
	mu        sync.Mutex
	prepared  map[string]*sql.Stmt
}

type Options struct {
	ChunkSize int64
}

func Open(ctx context.Context, path string) (*DB, error) {
	return OpenWith(ctx, path, Options{})
}

func OpenWith(ctx context.Context, path string, opts Options) (*DB, error) {
	if opts.ChunkSize == 0 {
		opts.ChunkSize = DefaultChunkSize
	}
	sqlDB, err := sql.Open(driverName, driverDSN(path))
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetConnMaxLifetime(0)
	db := &DB{DB: sqlDB, chunkSize: opts.ChunkSize, prepared: map[string]*sql.Stmt{}}
	if err := db.initialize(ctx); err != nil {
		sqlDB.Close()
		return nil, err
	}
	if err := db.prepareHotQueries(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) initialize(ctx context.Context) error {
	if _, err := db.ExecContext(ctx, `PRAGMA page_size = 8192`); err != nil {
		return err
	}
	return db.Do(ctx, func(tx *Tx) error {
		version, err := tx.meta("schema_version")
		if err != nil {
			return err
		}
		if _, err := tx.tx.ExecContext(ctx, schemaV1); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
		switch version {
		case "":
			return tx.bootstrap()
		case "v0":
			return tx.migrateFromV0()
		case SchemaVersion:
			return tx.loadChunkSize()
		default:
			return fmt.Errorf("unsupported schema version %q", version)
		}
	})
}

func (tx *Tx) loadChunkSize() error {
	raw, err := tx.meta("chunk_size")
	if err != nil {
		return err
	}
	size, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || size <= 0 {
		return fmt.Errorf("corrupt chunk_size %q in meta", raw)
	}
	tx.db.chunkSize = size
	return nil
}

func (db *DB) ChunkSize() int64 { return db.chunkSize }

func (tx *Tx) bootstrap() error {
	now := time.Now().UnixNano()
	if _, err := tx.exec(`INSERT OR IGNORE INTO inodes(ino, mode, uid, gid, size, atime, mtime, ctime, nlink) VALUES (?, ?, 0, 0, 0, ?, ?, ?, 2)`,
		RootIno, uint32(dirMode|0o755), now, now, now); err != nil {
		return err
	}
	if err := tx.SetMeta("created_at", time.Now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	if err := tx.SetMeta("chunk_size", strconv.FormatInt(tx.db.chunkSize, 10)); err != nil {
		return err
	}
	return tx.SetMeta("schema_version", SchemaVersion)
}

func (db *DB) Close() error {
	db.mu.Lock()
	for _, stmt := range db.prepared {
		stmt.Close()
	}
	db.prepared = nil
	db.mu.Unlock()
	return db.DB.Close()
}

func (db *DB) prepareHotQueries(ctx context.Context) error {
	for _, query := range hotQueries {
		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			return fmt.Errorf("prepare %q: %w", query, err)
		}
		db.prepared[query] = stmt
	}
	return nil
}

func (db *DB) stmt(query string) *sql.Stmt {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.prepared[query]
}

func (db *DB) Do(ctx context.Context, fn func(tx *Tx) error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = db.once(ctx, fn)
		if !isBusy(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(20*(attempt+1)) * time.Millisecond):
		}
	}
	return err
}

func isBusy(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "SQLITE_BUSY") || strings.Contains(err.Error(), "database is locked"))
}

func (db *DB) once(ctx context.Context, fn func(tx *Tx) error) error {
	sqlTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	tx := &Tx{ctx: ctx, db: db, tx: sqlTx}
	if err := fn(tx); err != nil {
		sqlTx.Rollback()
		return err
	}
	return sqlTx.Commit()
}

func (db *DB) View(ctx context.Context, fn func(tx *Tx) error) error {
	return db.Do(ctx, fn)
}

type Tx struct {
	ctx context.Context
	db  *DB
	tx  *sql.Tx
}

func (tx *Tx) exec(query string, args ...any) (sql.Result, error) {
	if stmt := tx.db.stmt(query); stmt != nil {
		return tx.tx.StmtContext(tx.ctx, stmt).ExecContext(tx.ctx, args...)
	}
	return tx.tx.ExecContext(tx.ctx, query, args...)
}

func (tx *Tx) queryRow(query string, args ...any) *sql.Row {
	if stmt := tx.db.stmt(query); stmt != nil {
		return tx.tx.StmtContext(tx.ctx, stmt).QueryRowContext(tx.ctx, args...)
	}
	return tx.tx.QueryRowContext(tx.ctx, query, args...)
}

func (tx *Tx) query(query string, args ...any) (*sql.Rows, error) {
	if stmt := tx.db.stmt(query); stmt != nil {
		return tx.tx.StmtContext(tx.ctx, stmt).QueryContext(tx.ctx, args...)
	}
	return tx.tx.QueryContext(tx.ctx, query, args...)
}

func (tx *Tx) meta(key string) (string, error) {
	var value string
	err := tx.queryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil && isMissingTable(err) {
		return "", nil
	}
	return value, err
}

func isMissingTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such table")
}

func (tx *Tx) SetMeta(key, value string) error {
	_, err := tx.exec(`INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

func (tx *Tx) Meta(key string) (string, error) {
	return tx.meta(key)
}

func (db *DB) Meta(ctx context.Context, key string) (string, error) {
	var value string
	err := db.View(ctx, func(tx *Tx) error {
		var err error
		value, err = tx.meta(key)
		return err
	})
	return value, err
}
