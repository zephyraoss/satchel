package fuse

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/zephyraoss/satchel/internal/metrics"
	"github.com/zephyraoss/satchel/internal/store"
)

type volumeFS struct {
	db       *store.DB
	dbPath   string
	walLimit int64
	volume   string
	log      *slog.Logger
	readOnly bool

	backpressureMu sync.Mutex
	stalledSince   time.Time
}

type node struct {
	gofs.Inode
	vfs *volumeFS
	ino uint64

	mu        sync.Mutex
	openCount int
}

var (
	_ gofs.NodeGetattrer     = (*node)(nil)
	_ gofs.NodeSetattrer     = (*node)(nil)
	_ gofs.NodeLookuper      = (*node)(nil)
	_ gofs.NodeReaddirer     = (*node)(nil)
	_ gofs.NodeMkdirer       = (*node)(nil)
	_ gofs.NodeRmdirer       = (*node)(nil)
	_ gofs.NodeCreater       = (*node)(nil)
	_ gofs.NodeMknoder       = (*node)(nil)
	_ gofs.NodeOpener        = (*node)(nil)
	_ gofs.NodeReader        = (*node)(nil)
	_ gofs.NodeWriter        = (*node)(nil)
	_ gofs.NodeFsyncer       = (*node)(nil)
	_ gofs.NodeFlusher       = (*node)(nil)
	_ gofs.NodeReleaser      = (*node)(nil)
	_ gofs.NodeUnlinker      = (*node)(nil)
	_ gofs.NodeRenamer       = (*node)(nil)
	_ gofs.NodeLinker        = (*node)(nil)
	_ gofs.NodeSymlinker     = (*node)(nil)
	_ gofs.NodeReadlinker    = (*node)(nil)
	_ gofs.NodeStatfser      = (*node)(nil)
	_ gofs.NodeGetxattrer    = (*node)(nil)
	_ gofs.NodeSetxattrer    = (*node)(nil)
	_ gofs.NodeRemovexattrer = (*node)(nil)
	_ gofs.NodeListxattrer   = (*node)(nil)
)

func (n *node) mutate(ctx context.Context, fn func(tx *store.Tx) error) syscall.Errno {
	if n.vfs.readOnly {
		return syscall.EROFS
	}
	return n.do(ctx, fn)
}

func (n *node) do(ctx context.Context, fn func(tx *store.Tx) error) syscall.Errno {
	err := n.vfs.db.Do(context.WithoutCancel(ctx), fn)
	if err != nil {
		errno := store.Errno(err)
		if errno == syscall.EIO {
			n.vfs.log.Error("fuse op failed", "ino", n.ino, "err", err)
		}
		return errno
	}
	return 0
}

func fillAttr(attr store.Attr, out *gofuse.Attr) {
	out.Ino = attr.Ino
	out.Mode = attr.Mode
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Size = uint64(attr.Size)
	out.Blocks = (uint64(attr.Size) + 511) / 512
	out.Blksize = store.DefaultChunkSize
	out.Nlink = attr.Nlink
	out.SetTimes(&attr.Atime, &attr.Mtime, &attr.Ctime)
}

func (n *node) child(ctx context.Context, attr store.Attr) *gofs.Inode {
	return n.NewInode(ctx, &node{vfs: n.vfs, ino: attr.Ino}, gofs.StableAttr{Ino: attr.Ino, Mode: attr.Mode & syscall.S_IFMT})
}

func caller(ctx context.Context) (uid, gid uint32) {
	if c, ok := gofuse.FromContext(ctx); ok {
		return c.Uid, c.Gid
	}
	return 0, 0
}

func (n *node) Getattr(ctx context.Context, _ gofs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	return n.do(ctx, func(tx *store.Tx) error {
		attr, err := tx.Stat(n.ino)
		if err != nil {
			return err
		}
		fillAttr(attr, &out.Attr)
		return nil
	})
}

func (n *node) Setattr(ctx context.Context, _ gofs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	var change store.AttrChange
	if mode, ok := in.GetMode(); ok {
		change.Mode = &mode
	}
	if uid, ok := in.GetUID(); ok {
		change.Uid = &uid
	}
	if gid, ok := in.GetGID(); ok {
		change.Gid = &gid
	}
	if size, ok := in.GetSize(); ok {
		s := int64(size)
		change.Size = &s
	}
	if atime, ok := in.GetATime(); ok {
		change.Atime = &atime
	}
	if mtime, ok := in.GetMTime(); ok {
		change.Mtime = &mtime
	}
	return n.mutate(ctx, func(tx *store.Tx) error {
		attr, err := tx.SetAttr(n.ino, change)
		if err != nil {
			return err
		}
		fillAttr(attr, &out.Attr)
		return nil
	})
}

func (n *node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	var attr store.Attr
	if errno := n.do(ctx, func(tx *store.Tx) error {
		var err error
		attr, err = tx.Lookup(n.ino, name)
		return err
	}); errno != 0 {
		return nil, errno
	}
	fillAttr(attr, &out.Attr)
	return n.child(ctx, attr), 0
}

func (n *node) Readdir(ctx context.Context) (gofs.DirStream, syscall.Errno) {
	var entries []store.DirEntry
	if errno := n.do(ctx, func(tx *store.Tx) error {
		var err error
		entries, err = tx.Readdir(n.ino)
		return err
	}); errno != 0 {
		return nil, errno
	}
	list := make([]gofuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		list = append(list, gofuse.DirEntry{Name: e.Name, Ino: e.Ino, Mode: e.Mode & syscall.S_IFMT})
	}
	return gofs.NewListDirStream(list), 0
}

func (n *node) create(ctx context.Context, name string, mode uint32, target string, out *gofuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	uid, gid := caller(ctx)
	var attr store.Attr
	if errno := n.mutate(ctx, func(tx *store.Tx) error {
		var err error
		attr, err = tx.Create(n.ino, name, store.NewInode{Mode: mode, Uid: uid, Gid: gid, Target: target})
		return err
	}); errno != 0 {
		return nil, errno
	}
	fillAttr(attr, &out.Attr)
	return n.child(ctx, attr), 0
}

func (n *node) Mkdir(ctx context.Context, name string, mode uint32, out *gofuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	return n.create(ctx, name, mode&^syscall.S_IFMT|syscall.S_IFDIR, "", out)
}

func (n *node) Mknod(ctx context.Context, name string, mode uint32, _ uint32, out *gofuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	if mode&syscall.S_IFMT != syscall.S_IFREG && mode&syscall.S_IFMT != 0 {
		return nil, syscall.EPERM
	}
	return n.create(ctx, name, mode&^syscall.S_IFMT|syscall.S_IFREG, "", out)
}

func (n *node) Symlink(ctx context.Context, target, name string, out *gofuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	return n.create(ctx, name, syscall.S_IFLNK|0o777, target, out)
}

func (n *node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*gofs.Inode, gofs.FileHandle, uint32, syscall.Errno) {
	child, errno := n.create(ctx, name, mode&^syscall.S_IFMT|syscall.S_IFREG, "", out)
	if errno != 0 {
		return nil, nil, 0, errno
	}
	return child, child.Operations().(*node).open(), gofuse.FOPEN_KEEP_CACHE, 0
}

type handle struct{}

func (n *node) open() gofs.FileHandle {
	n.mu.Lock()
	n.openCount++
	n.mu.Unlock()
	return &handle{}
}

func (n *node) Open(ctx context.Context, flags uint32) (gofs.FileHandle, uint32, syscall.Errno) {
	return n.open(), gofuse.FOPEN_KEEP_CACHE, 0
}

func (n *node) Release(ctx context.Context, _ gofs.FileHandle) syscall.Errno {
	n.mu.Lock()
	n.openCount--
	last := n.openCount == 0
	n.mu.Unlock()
	if !last || n.vfs.readOnly {
		return 0
	}
	return n.do(ctx, func(tx *store.Tx) error {
		attr, err := tx.Stat(n.ino)
		if err != nil {
			return nil
		}
		if attr.Nlink == 0 {
			return tx.DeleteInode(n.ino)
		}
		return nil
	})
}

func (n *node) isOpen() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.openCount > 0
}

func (n *node) childIsOpen(name string) bool {
	child := n.GetChild(name)
	if child == nil {
		return false
	}
	childNode, ok := child.Operations().(*node)
	return ok && childNode.isOpen()
}

func (n *node) Read(ctx context.Context, _ gofs.FileHandle, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	var read int
	if errno := n.do(ctx, func(tx *store.Tx) error {
		var err error
		read, err = tx.ReadAt(n.ino, dest, off)
		return err
	}); errno != 0 {
		return nil, errno
	}
	return gofuse.ReadResultData(dest[:read]), 0
}

func (n *node) Write(ctx context.Context, _ gofs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	if errno := n.vfs.applyBackpressure(ctx); errno != 0 {
		return 0, errno
	}
	var written int
	if errno := n.mutate(ctx, func(tx *store.Tx) error {
		var err error
		written, err = tx.WriteAt(n.ino, data, off)
		return err
	}); errno != 0 {
		return 0, errno
	}
	return uint32(written), 0
}

func (n *node) Fsync(ctx context.Context, _ gofs.FileHandle, _ uint32) syscall.Errno {
	return 0
}

func (n *node) Flush(ctx context.Context, _ gofs.FileHandle) syscall.Errno {
	return 0
}

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	keep := n.childIsOpen(name)
	return n.mutate(ctx, func(tx *store.Tx) error {
		_, err := tx.Unlink(n.ino, name, keep)
		return err
	})
}

func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.mutate(ctx, func(tx *store.Tx) error {
		return tx.Rmdir(n.ino, name)
	})
}

const (
	renameNoReplace = 1
	renameExchange  = 2
)

func (n *node) Rename(ctx context.Context, name string, newParent gofs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags&renameExchange != 0 {
		return syscall.EINVAL
	}
	dst, ok := newParent.(*node)
	if !ok {
		return syscall.EXDEV
	}
	opts := store.RenameOptions{NoReplace: flags&renameNoReplace != 0, KeepOrphan: dst.childIsOpen(newName)}
	return n.mutate(ctx, func(tx *store.Tx) error {
		return tx.Rename(n.ino, name, dst.ino, newName, opts)
	})
}

func (n *node) Link(ctx context.Context, target gofs.InodeEmbedder, name string, out *gofuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	targetNode, ok := target.(*node)
	if !ok {
		return nil, syscall.EXDEV
	}
	var attr store.Attr
	if errno := n.mutate(ctx, func(tx *store.Tx) error {
		var err error
		attr, err = tx.Link(targetNode.ino, n.ino, name)
		return err
	}); errno != 0 {
		return nil, errno
	}
	fillAttr(attr, &out.Attr)
	return n.child(ctx, attr), 0
}

func (n *node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	var target string
	if errno := n.do(ctx, func(tx *store.Tx) error {
		var err error
		target, err = tx.Readlink(n.ino)
		return err
	}); errno != 0 {
		return nil, errno
	}
	return []byte(target), 0
}

func (n *node) Statfs(ctx context.Context, out *gofuse.StatfsOut) syscall.Errno {
	var stats store.Stats
	if errno := n.do(ctx, func(tx *store.Tx) error {
		var err error
		stats, err = tx.Stats()
		return err
	}); errno != 0 {
		return errno
	}
	const bsize = 4096
	var disk syscall.Statfs_t
	if err := syscall.Statfs(filepath.Dir(n.vfs.dbPath), &disk); err != nil {
		return syscall.EIO
	}
	freeBytes := disk.Bavail * uint64(disk.Bsize)
	used := uint64(stats.Bytes)
	out.Bsize = bsize
	out.Frsize = bsize
	out.Blocks = (used + freeBytes) / bsize
	out.Bfree = freeBytes / bsize
	out.Bavail = out.Bfree
	out.Files = uint64(stats.Inodes)
	out.Ffree = 1 << 31
	out.NameLen = 255
	return 0
}

func (n *node) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	var value []byte
	if errno := n.do(ctx, func(tx *store.Tx) error {
		var err error
		value, err = tx.GetXattr(n.ino, attr)
		return err
	}); errno != 0 {
		return 0, errno
	}
	if len(dest) < len(value) {
		return uint32(len(value)), syscall.ERANGE
	}
	copy(dest, value)
	return uint32(len(value)), 0
}

func (n *node) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return n.mutate(ctx, func(tx *store.Tx) error {
		return tx.SetXattr(n.ino, attr, data, int(flags))
	})
}

func (n *node) Removexattr(ctx context.Context, attr string) syscall.Errno {
	return n.mutate(ctx, func(tx *store.Tx) error {
		return tx.RemoveXattr(n.ino, attr)
	})
}

func (n *node) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	var names []string
	if errno := n.do(ctx, func(tx *store.Tx) error {
		var err error
		names, err = tx.ListXattr(n.ino)
		return err
	}); errno != 0 {
		return 0, errno
	}
	var packed []byte
	for _, name := range names {
		packed = append(packed, name...)
		packed = append(packed, 0)
	}
	if len(dest) < len(packed) {
		return uint32(len(packed)), syscall.ERANGE
	}
	copy(dest, packed)
	return uint32(len(packed)), 0
}

func (v *volumeFS) walSize() int64 {
	return v.db.WALState().PendingBytes()
}

func (v *volumeFS) applyBackpressure(ctx context.Context) syscall.Errno {
	size := v.walSize()
	metrics.WALBytes.WithLabelValues(v.volume).Set(float64(size))
	if size <= v.walLimit {
		v.clearStall()
		return 0
	}
	v.noteStall(size)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return syscall.EINTR
		case <-ticker.C:
		}
		size = v.walSize()
		metrics.WALBytes.WithLabelValues(v.volume).Set(float64(size))
		if size <= v.walLimit {
			v.clearStall()
			return 0
		}
	}
}

func (v *volumeFS) noteStall(size int64) {
	v.backpressureMu.Lock()
	defer v.backpressureMu.Unlock()
	if v.stalledSince.IsZero() {
		v.stalledSince = time.Now()
		metrics.BackpressureEvents.WithLabelValues(v.volume).Inc()
		v.log.Warn("WAL over limit; delaying writes until litestream catches up", "wal_bytes", size, "limit", v.walLimit)
	}
}

func (v *volumeFS) clearStall() {
	v.backpressureMu.Lock()
	defer v.backpressureMu.Unlock()
	if !v.stalledSince.IsZero() {
		v.log.Info("WAL back under limit; resuming writes", "stalled_for", time.Since(v.stalledSince))
		v.stalledSince = time.Time{}
	}
}
