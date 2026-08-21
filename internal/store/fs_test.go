package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTest(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func createFile(t *testing.T, db *DB, parent uint64, name string) Attr {
	t.Helper()
	var attr Attr
	err := db.Do(context.Background(), func(tx *Tx) error {
		var err error
		attr, err = tx.Create(parent, name, NewInode{Mode: regMode | 0o644})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return attr
}

func TestWriteReadAcrossChunks(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	f := createFile(t, db, RootIno, "big")
	payload := make([]byte, DefaultChunkSize*2+12345)
	rand.Read(payload)

	err := db.Do(ctx, func(tx *Tx) error {
		for off := 0; off < len(payload); off += 100_000 {
			end := min(off+100_000, len(payload))
			if _, err := tx.WriteAt(f.Ino, payload[off:end], int64(off)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload)+500)
	var n int
	err = db.View(ctx, func(tx *Tx) error {
		var err error
		n, err = tx.ReadAt(f.Ino, got, 0)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != len(payload) || !bytes.Equal(got[:n], payload) {
		t.Fatalf("read back mismatch: n=%d", n)
	}

	mid := make([]byte, 1000)
	db.View(ctx, func(tx *Tx) error {
		n, _ = tx.ReadAt(f.Ino, mid, DefaultChunkSize-500)
		return nil
	})
	if n != 1000 || !bytes.Equal(mid, payload[DefaultChunkSize-500:DefaultChunkSize+500]) {
		t.Fatal("chunk-boundary read mismatch")
	}
}

func TestSparseWriteAndTruncate(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	f := createFile(t, db, RootIno, "sparse")
	err := db.Do(ctx, func(tx *Tx) error {
		_, err := tx.WriteAt(f.Ino, []byte("tail"), DefaultChunkSize*3+10)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, DefaultChunkSize*3+14)
	db.View(ctx, func(tx *Tx) error {
		_, err := tx.ReadAt(f.Ino, buf, 0)
		return err
	})
	if !bytes.Equal(buf[:DefaultChunkSize*3+10], make([]byte, DefaultChunkSize*3+10)) || string(buf[DefaultChunkSize*3+10:]) != "tail" {
		t.Fatal("sparse region not zero-filled")
	}

	size := int64(DefaultChunkSize + 7)
	err = db.Do(ctx, func(tx *Tx) error {
		_, err := tx.SetAttr(f.Ino, AttrChange{Size: &size})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	var count int
	db.View(ctx, func(tx *Tx) error {
		return tx.queryRow(`SELECT COUNT(*) FROM chunks WHERE ino = ?`, f.Ino).Scan(&count)
	})
	if count != 0 {
		t.Fatalf("chunks past truncation point remain: %d", count)
	}

	err = db.Do(ctx, func(tx *Tx) error {
		_, err := tx.WriteAt(f.Ino, bytes.Repeat([]byte("x"), 100), 0)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	small := int64(40)
	db.Do(ctx, func(tx *Tx) error {
		_, err := tx.SetAttr(f.Ino, AttrChange{Size: &small})
		return err
	})
	var dataLen int
	db.View(ctx, func(tx *Tx) error {
		return tx.queryRow(`SELECT length(data) FROM chunks WHERE ino = ? AND idx = 0`, f.Ino).Scan(&dataLen)
	})
	if dataLen != 40 {
		t.Fatalf("chunk not trimmed on truncate: len=%d", dataLen)
	}
}

func TestDirectoryOperations(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	err := db.Do(ctx, func(tx *Tx) error {
		dir, err := tx.Create(RootIno, "dir", NewInode{Mode: dirMode | 0o755})
		if err != nil {
			return err
		}
		if _, err := tx.Create(dir.Ino, "f", NewInode{Mode: regMode | 0o644}); err != nil {
			return err
		}
		if err := tx.Rmdir(RootIno, "dir"); !errors.Is(err, ErrNotEmpty) {
			t.Fatalf("rmdir non-empty = %v", err)
		}
		if err := tx.Rename(RootIno, "dir", dir.Ino, "self", RenameOptions{}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("rename into own subtree = %v", err)
		}
		if _, err := tx.Create(RootIno, "dir", NewInode{Mode: dirMode | 0o755}); !errors.Is(err, ErrExists) {
			t.Fatalf("duplicate create = %v", err)
		}
		if err := tx.Rename(RootIno, "dir", RootIno, "renamed", RenameOptions{}); err != nil {
			return err
		}
		if _, err := tx.Lookup(RootIno, "dir"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("old name still resolves: %v", err)
		}
		if _, err := tx.Unlink(dir.Ino, "f", false); err != nil {
			return err
		}
		return tx.Rmdir(RootIno, "renamed")
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOrphanKeptWhileOpenThenCollected(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	f := createFile(t, db, RootIno, "tmp")
	err := db.Do(ctx, func(tx *Tx) error {
		if _, err := tx.WriteAt(f.Ino, []byte("still readable"), 0); err != nil {
			return err
		}
		_, err := tx.Unlink(RootIno, "tmp", true)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	var n int
	db.View(ctx, func(tx *Tx) error {
		n, _ = tx.ReadAt(f.Ino, buf, 0)
		return nil
	})
	if string(buf[:n]) != "still readable" {
		t.Fatalf("orphan unreadable: %q", buf[:n])
	}
	var collected int64
	db.Do(ctx, func(tx *Tx) error {
		collected, _ = tx.CollectOrphans()
		return nil
	})
	if collected != 1 {
		t.Fatalf("collected %d orphans", collected)
	}
	var chunks int
	db.View(ctx, func(tx *Tx) error {
		return tx.queryRow(`SELECT COUNT(*) FROM chunks`).Scan(&chunks)
	})
	if chunks != 0 {
		t.Fatal("orphan chunks left behind")
	}
}

func TestHardlinkNlink(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	f := createFile(t, db, RootIno, "a")
	err := db.Do(ctx, func(tx *Tx) error {
		linked, err := tx.Link(f.Ino, RootIno, "b")
		if err != nil {
			return err
		}
		if linked.Nlink != 2 {
			t.Fatalf("nlink = %d", linked.Nlink)
		}
		after, err := tx.Unlink(RootIno, "a", false)
		if err != nil {
			return err
		}
		if after.Nlink != 1 {
			t.Fatalf("nlink after unlink = %d", after.Nlink)
		}
		_, err = tx.Stat(f.Ino)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestXattrs(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	f := createFile(t, db, RootIno, "x")
	err := db.Do(ctx, func(tx *Tx) error {
		if err := tx.SetXattr(f.Ino, "user.k", []byte("v"), 0); err != nil {
			return err
		}
		if err := tx.SetXattr(f.Ino, "user.k", []byte("v"), 1); !errors.Is(err, ErrExists) {
			t.Fatalf("XATTR_CREATE on existing = %v", err)
		}
		names, err := tx.ListXattr(f.Ino)
		if err != nil || len(names) != 1 || names[0] != "user.k" {
			t.Fatalf("list = %v %v", names, err)
		}
		if err := tx.RemoveXattr(f.Ino, "user.k"); err != nil {
			return err
		}
		if _, err := tx.GetXattr(f.Ino, "user.k"); !errors.Is(err, ErrNoXattr) {
			t.Fatalf("get after remove = %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMigrateFromV0(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "old.db")
	legacy, err := OpenRaw(path)
	if err != nil {
		t.Fatal(err)
	}
	stmts := []string{
		`CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)`,
		`CREATE TABLE files (path TEXT PRIMARY KEY, mode INTEGER NOT NULL, mtime INTEGER NOT NULL, data BLOB)`,
		`INSERT INTO meta VALUES ('schema_version', 'v0')`,
	}
	for _, s := range stmts {
		if _, err := legacy.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	mtime := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	rows := []struct {
		path string
		mode uint32
		data []byte
	}{
		{"dir", 0o755 | 1<<31, nil},
		{"dir/file.txt", 0o640, []byte("legacy content")},
		{"link", 0o777 | 1<<27, []byte("dir/file.txt")},
	}
	for _, r := range rows {
		if _, err := legacy.Exec(`INSERT INTO files VALUES (?, ?, ?, ?)`, r.path, r.mode, mtime.UnixNano(), r.data); err != nil {
			t.Fatal(err)
		}
	}
	legacy.Close()

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entries, err := db.ListFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %+v", entries)
	}
	dst := t.TempDir()
	if err := db.Unpack(ctx, dst); err != nil {
		t.Fatal(err)
	}
	assertFile(t, filepath.Join(dst, "dir", "file.txt"), "legacy content", 0o640)
	version, _ := db.Meta(ctx, "schema_version")
	if version != SchemaVersion {
		t.Fatalf("schema_version = %q", version)
	}
}
