package pack

import (
	"context"
	"fmt"
	"os"

	"github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/store"
)

type Backend struct{}

func New() *Backend { return &Backend{} }

func (*Backend) Name() string { return "pack" }

func (*Backend) Mount(ctx context.Context, dbPath, mountpoint string, opts backend.MountOptions) (backend.Unmounter, error) {
	if err := resetDir(mountpoint); err != nil {
		return nil, err
	}
	db, err := store.Open(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dbPath, err)
	}
	defer db.Close()
	if err := db.Unpack(ctx, mountpoint); err != nil {
		return nil, err
	}
	return &mount{dbPath: dbPath, mountpoint: mountpoint, readOnly: opts.ReadOnly}, nil
}

type mount struct {
	dbPath     string
	mountpoint string
	readOnly   bool
}

func (m *mount) Unmount(ctx context.Context) error {
	if m.readOnly {
		return resetDir(m.mountpoint)
	}
	db, err := store.Open(ctx, m.dbPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", m.dbPath, err)
	}
	if err := db.Pack(ctx, m.mountpoint); err != nil {
		db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	return resetDir(m.mountpoint)
}

func (m *mount) Abandon() error {
	return resetDir(m.mountpoint)
}

func resetDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o755)
}
