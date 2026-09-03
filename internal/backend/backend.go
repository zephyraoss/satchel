package backend

import (
	"context"

	"github.com/zephyraoss/satchel/internal/replica"
)

type MountOptions struct {
	ReadOnly   bool
	Filesystem string
}

type Unmounter interface {
	Unmount(ctx context.Context) error
	Abandon() error
}

type Backend interface {
	Format(ctx context.Context, imagePath, filesystem string) error
	Mount(ctx context.Context, device *replica.Device, mountpoint string, opts MountOptions) (Unmounter, error)
}
