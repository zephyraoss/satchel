//go:build linux

package block

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/replica"
)

type Options struct {
	Logger *slog.Logger
}

type Backend struct {
	log *slog.Logger
}

func New(opts Options) *Backend {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Backend{log: opts.Logger}
}

func (*Backend) Format(ctx context.Context, imagePath, filesystem string) error {
	if filesystem != "ext4" {
		return fmt.Errorf("unsupported filesystem %q", filesystem)
	}
	cmd := exec.CommandContext(ctx, "mkfs.ext4", "-q", "-F", "-b", "4096", "-E", "lazy_itable_init=1,lazy_journal_init=1", imagePath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("mkfs.ext4: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

type Mount struct {
	attachment *Attachment
	mountpoint string
	log        *slog.Logger
	mu         sync.Mutex
	closed     bool
}

func (b *Backend) Mount(ctx context.Context, device *replica.Device, mountpoint string, opts backend.MountOptions) (backend.Unmounter, error) {
	if opts.Filesystem == "" {
		opts.Filesystem = "ext4"
	}
	if opts.Filesystem != "ext4" {
		return nil, fmt.Errorf("unsupported filesystem %q", opts.Filesystem)
	}
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return nil, err
	}
	attachment, err := Attach(ctx, device, opts.ReadOnly)
	if err != nil {
		return nil, err
	}
	mountOpts := "noatime,discard"
	if opts.ReadOnly {
		mountOpts = "noatime,ro,noload"
	}
	cmd := exec.CommandContext(ctx, "mount", "-t", opts.Filesystem, "-o", mountOpts, attachment.Path(), mountpoint)
	if output, err := cmd.CombinedOutput(); err != nil {
		attachment.Close()
		return nil, fmt.Errorf("mount %s: %w: %s", attachment.Path(), err, strings.TrimSpace(string(output)))
	}
	b.log.Info("block volume mounted", "device", attachment.Path(), "mountpoint", mountpoint, "read_only", opts.ReadOnly)
	return &Mount{attachment: attachment, mountpoint: mountpoint, log: b.log}, nil
}

func (m *Mount) Unmount(ctx context.Context) error {
	return m.close(ctx, false)
}

func (m *Mount) Abandon() error {
	return m.close(context.Background(), true)
}

func (m *Mount) close(ctx context.Context, detach bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	flags := 0
	if detach {
		flags = syscall.MNT_DETACH
	}
	if err := unmount(ctx, m.mountpoint, flags); err != nil && !detach {
		return err
	}
	var result error
	if err := m.attachment.Close(); err != nil {
		result = err
	}
	if err := os.Remove(m.mountpoint); err != nil && !errors.Is(err, os.ErrNotExist) && result == nil {
		result = err
	}
	m.closed = result == nil
	return result
}

func unmount(ctx context.Context, mountpoint string, flags int) error {
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for {
		err := syscall.Unmount(mountpoint, flags)
		if err == nil || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOENT) {
			return nil
		}
		if !errors.Is(err, syscall.EBUSY) || flags&syscall.MNT_DETACH != 0 {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-deadline.C:
			return err
		case <-time.After(100 * time.Millisecond):
		}
	}
}
