package fuse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/store"
)

const DefaultWALLimit = 256 << 20

type Options struct {
	ChunkSize   int64
	WALLimit    int64
	AttrTimeout time.Duration
	Logger      *slog.Logger
	Debug       bool
	AllowOther  bool
}

type Backend struct {
	opts Options
}

func New(opts Options) *Backend {
	if opts.WALLimit == 0 {
		opts.WALLimit = DefaultWALLimit
	}
	if opts.AttrTimeout == 0 {
		opts.AttrTimeout = time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Backend{opts: opts}
}

func (*Backend) Name() string { return "fuse" }

func (b *Backend) Mount(ctx context.Context, dbPath, mountpoint string, mopts backend.MountOptions) (backend.Unmounter, error) {
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return nil, err
	}
	db, err := store.OpenWith(ctx, dbPath, store.Options{ChunkSize: b.opts.ChunkSize})
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dbPath, err)
	}
	var orphans int64
	if !mopts.ReadOnly {
		if err := db.Do(ctx, func(tx *store.Tx) error {
			var err error
			orphans, err = tx.CollectOrphans()
			return err
		}); err != nil {
			db.Close()
			return nil, err
		}
	}
	volume := strings.TrimSuffix(filepath.Base(dbPath), ".db")
	log := b.opts.Logger.With("volume", volume)
	if orphans > 0 {
		log.Info("collected orphaned inodes", "count", orphans)
	}

	vfs := &volumeFS{
		db:       db,
		dbPath:   dbPath,
		walLimit: b.opts.WALLimit,
		volume:   volume,
		log:      log,
		readOnly: mopts.ReadOnly,
	}
	mountFlags := []string{"default_permissions"}
	if mopts.ReadOnly {
		mountFlags = append(mountFlags, "ro")
	}
	root := &node{vfs: vfs, ino: store.RootIno}
	timeout := b.opts.AttrTimeout
	server, err := gofs.Mount(mountpoint, root, &gofs.Options{
		EntryTimeout:    &timeout,
		AttrTimeout:     &timeout,
		NegativeTimeout: &timeout,
		NullPermissions: true,
		RootStableAttr:  &gofs.StableAttr{Ino: store.RootIno, Mode: syscall.S_IFDIR},
		MountOptions: gofuse.MountOptions{
			FsName:      "satchel:" + volume,
			Name:        "satchel",
			AllowOther:  b.opts.AllowOther,
			MaxWrite:    1 << 20,
			DirectMount: true,
			Debug:       b.opts.Debug,
			Options:     mountFlags,
		},
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("fuse mount %s: %w", mountpoint, err)
	}
	if err := server.WaitMount(); err != nil {
		server.Unmount()
		db.Close()
		return nil, err
	}
	log.Info("fuse mounted", "mountpoint", mountpoint, "read_only", mopts.ReadOnly)
	return &mount{server: server, db: db, mountpoint: mountpoint, log: log}, nil
}

type mount struct {
	server     *gofuse.Server
	db         *store.DB
	mountpoint string
	log        *slog.Logger
}

func (m *mount) Unmount(ctx context.Context) error {
	var err error
	for attempt := 0; attempt < 20; attempt++ {
		if err = m.server.Unmount(); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	if err != nil {
		return fmt.Errorf("unmount %s: %w (is a container still using it?)", m.mountpoint, err)
	}
	m.server.Wait()
	if err := m.checkpoint(ctx); err != nil {
		m.log.Warn("final checkpoint failed", "err", err)
	}
	return m.db.Close()
}

func (m *mount) checkpoint(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`)
	return err
}

func (m *mount) Abandon() error {
	if err := m.server.Unmount(); err != nil {
		if detachErr := syscall.Unmount(m.mountpoint, syscall.MNT_DETACH); detachErr != nil && !errors.Is(detachErr, syscall.EINVAL) {
			m.log.Error("lazy unmount failed", "err", detachErr)
		}
	}
	return m.db.Close()
}
