package backend

import "context"

type MountOptions struct {
	ReadOnly bool
}

type Unmounter interface {
	Unmount(ctx context.Context) error
	Abandon() error
}

type Backend interface {
	Name() string
	Mount(ctx context.Context, dbPath, mountpoint string, opts MountOptions) (Unmounter, error)
}
