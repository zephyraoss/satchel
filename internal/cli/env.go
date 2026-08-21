package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zephyraoss/satchel/internal/lease"
	"github.com/zephyraoss/satchel/internal/litestream"
	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/store"
)

type Env struct {
	Store      objectstore.Store
	Leases     *lease.Manager
	Litestream litestream.Runner
	WorkDir    string
	Stdout     io.Writer
	Stderr     io.Writer
}

func (e *Env) workspace(volume string) (string, func(), error) {
	dir, err := os.MkdirTemp(e.WorkDir, "satchel-"+volume+"-")
	if err != nil {
		return "", nil, err
	}
	return filepath.Join(dir, volume+".db"), func() { os.RemoveAll(dir) }, nil
}

func (e *Env) exists(ctx context.Context, volume string) (bool, error) {
	keys, err := e.Store.List(ctx, litestream.ReplicaPath(volume)+"/")
	if err != nil {
		return false, err
	}
	return len(keys) > 0, nil
}

func (e *Env) withSnapshot(ctx context.Context, volume string, opts litestream.RestoreOptions, fn func(db *store.DB) error) error {
	ok, err := e.exists(ctx, volume)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("volume %s not found in bucket", volume)
	}
	dbPath, cleanup, err := e.workspace(volume)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := e.Litestream.Restore(ctx, volume, dbPath, opts); err != nil {
		return fmt.Errorf("restore %s: %w", volume, err)
	}
	db, err := store.Open(ctx, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

func (e *Env) withWriteLease(ctx context.Context, volume string, fn func(db *store.DB) error) error {
	l, err := e.Leases.Acquire(ctx, volume)
	if err != nil {
		var held *lease.HeldError
		if errors.As(err, &held) {
			return fmt.Errorf("%w; unmount it there or use 'satchel vol lease break' if that node is dead", held)
		}
		return err
	}
	beatCtx, stopBeat := context.WithCancel(ctx)
	lost := make(chan error, 1)
	go l.Heartbeat(beatCtx, func(cause error) { lost <- cause })
	defer func() {
		stopBeat()
		if relErr := l.Release(context.WithoutCancel(ctx)); relErr != nil {
			fmt.Fprintln(e.Stderr, "warning: release lease:", relErr)
		}
	}()

	dbPath, cleanup, err := e.workspace(volume)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := e.Litestream.Restore(ctx, volume, dbPath, litestream.RestoreOptions{}); err != nil {
		return fmt.Errorf("restore %s: %w", volume, err)
	}
	db, err := store.Open(ctx, dbPath)
	if err != nil {
		return err
	}
	if err := fn(db); err != nil {
		db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	select {
	case cause := <-lost:
		return fmt.Errorf("lease lost during operation, changes discarded: %w", cause)
	default:
	}
	if err := e.Litestream.SyncOnce(ctx, volume, dbPath); err != nil {
		return fmt.Errorf("replicate %s: %w", volume, err)
	}
	return nil
}

func splitPath(p string) (dir, base string) {
	p = strings.Trim(p, "/")
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[:i], p[i+1:]
	}
	return "", p
}

func resolve(tx *store.Tx, p string) (store.Attr, error) {
	p = strings.Trim(p, "/")
	attr, err := tx.Stat(store.RootIno)
	if err != nil || p == "" {
		return attr, err
	}
	for _, part := range strings.Split(p, "/") {
		attr, err = tx.Lookup(attr.Ino, part)
		if err != nil {
			return store.Attr{}, fmt.Errorf("%s: %w", p, err)
		}
	}
	return attr, nil
}

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
