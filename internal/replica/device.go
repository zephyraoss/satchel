package replica

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

const DefaultBlockSize = 4096

var ErrDeviceClosed = errors.New("block device is closed")

// Generation is an immutable view of the blocks changed during one sync
// interval. Blocks contains complete blocks, even when the original write was
// smaller, so a later write cannot change a generation while it is uploading.
type Generation struct {
	Blocks map[uint64][]byte
	bytes  int64
}

func (g *Generation) Empty() bool { return g == nil || len(g.Blocks) == 0 }

func (g *Generation) Bytes() int64 {
	if g == nil {
		return 0
	}
	if g.bytes != 0 {
		return g.bytes
	}
	var n int64
	for _, block := range g.Blocks {
		n += int64(len(block))
	}
	return n
}

// Device is the local backing image and its in-memory change journal.
//
// The journal is intentionally not durable. A machine failure may lose writes
// that have not reached S3, as controlled by the sync interval. The image is
// also disposable and is rebuilt from S3 whenever a volume mounts.
type Device struct {
	mu             sync.Mutex
	ready          *sync.Cond
	file           *os.File
	size           int64
	blockSize      int64
	maxDirty       int64
	dirty          int64
	active         map[uint64][]byte
	onBackpressure func()
	onDirty        func()
	onDirtyChange  func(int64)
	dirtyThreshold int64
	dirtyNotified  bool
	onFlush        func() error
	onRemoteFlush  func(*Generation) <-chan error
	skipLocalFlush bool
	localDirty     bool
	blockPool      sync.Pool
	hydrator       *LazyImage
	closed         bool
	remoteFlush    atomic.Bool
}

func OpenDevice(path string, size, maxDirty int64) (*Device, error) {
	if size <= 0 || size%DefaultBlockSize != 0 {
		return nil, fmt.Errorf("image size must be a positive multiple of %d", DefaultBlockSize)
	}
	if maxDirty <= 0 {
		maxDirty = 256 << 20
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, err
	}
	d := &Device{
		file:      f,
		size:      size,
		blockSize: DefaultBlockSize,
		maxDirty:  maxDirty,
		active:    make(map[uint64][]byte),
	}
	d.ready = sync.NewCond(&d.mu)
	return d, nil
}

func (d *Device) Size() int64 { return d.size }

// SetBackpressureHandler sets a function called once each time an I/O request
// must wait for unpublished generations to fall below the dirty byte limit.
func (d *Device) SetBackpressureHandler(handler func()) {
	d.mu.Lock()
	d.onBackpressure = handler
	d.mu.Unlock()
}

// SetDirtyHandler sets a nonblocking callback that runs once when unpublished
// data crosses threshold. It becomes eligible again after replication releases
// enough data to fall below the threshold.
func (d *Device) SetDirtyHandler(threshold int64, handler func()) {
	d.mu.Lock()
	d.dirtyThreshold = threshold
	d.onDirty = handler
	d.dirtyNotified = threshold > 0 && d.dirty >= threshold
	d.mu.Unlock()
}

func (d *Device) SetDirtyObserver(observer func(bytes int64)) {
	d.mu.Lock()
	d.onDirtyChange = observer
	d.mu.Unlock()
}

func (d *Device) observeDirtyLocked() {
	if d.onDirtyChange != nil {
		d.onDirtyChange(d.dirty)
	}
}

// SetFlushHandler sets a function called after the local image reaches stable
// storage.
func (d *Device) SetFlushHandler(handler func() error) {
	d.mu.Lock()
	d.onFlush = handler
	d.onRemoteFlush = nil
	d.skipLocalFlush = false
	d.mu.Unlock()
	d.remoteFlush.Store(false)
}

// SetAsyncFlush makes filesystem flushes ordering barriers without syncing the
// disposable local image. The in-memory generation journal remains the source
// for background replication, so syncing the image would not improve the
// recovery point after an unclean remount.
func (d *Device) SetAsyncFlush() {
	d.mu.Lock()
	d.onFlush = nil
	d.onRemoteFlush = nil
	d.skipLocalFlush = true
	d.mu.Unlock()
	d.remoteFlush.Store(false)
}

// SetRemoteFlushHandler sets a function whose successful completion makes all
// prior writes durable without relying on the disposable local image. Satchel
// uses it when the handler publishes those writes to remote storage.
func (d *Device) SetRemoteFlushHandler(handler func(*Generation) <-chan error) {
	d.mu.Lock()
	d.onFlush = nil
	d.onRemoteFlush = handler
	d.skipLocalFlush = true
	d.mu.Unlock()
	d.remoteFlush.Store(true)
}

// RemoteFlushEnabled reports whether filesystem flushes wait for remote
// publication. The NBD server uses it to overlap that network wait with later
// local I/O while keeping the cheaper serial path for async durability.
func (d *Device) RemoteFlushEnabled() bool { return d.remoteFlush.Load() }

// BeginRemoteFlush seals the current write generation and queues it while
// holding the device lock. Writes that start after this method returns belong
// to a later flush boundary and may proceed while the caller waits for result.
func (d *Device) BeginRemoteFlush() (<-chan error, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrDeviceClosed
	}
	if d.onRemoteFlush == nil {
		return nil, errors.New("remote flush is not configured")
	}
	return d.onRemoteFlush(d.sealLocked()), nil
}

func (d *Device) SetLazyImage(image *LazyImage) {
	d.mu.Lock()
	d.hydrator = image
	d.mu.Unlock()
}

func (d *Device) ReadAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0, ErrDeviceClosed
	}
	if off < 0 || off >= d.size {
		return 0, io.EOF
	}
	if int64(len(p)) > d.size-off {
		p = p[:d.size-off]
	}
	if d.hydrator != nil {
		if err := d.hydrator.Hydrate(off, int64(len(p))); err != nil {
			return 0, err
		}
	}
	return d.file.ReadAt(p, off)
}

func (d *Device) WriteAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0, ErrDeviceClosed
	}
	if off < 0 || int64(len(p)) > d.size-off {
		return 0, io.ErrShortWrite
	}
	first, last := d.blockRange(off, int64(len(p)))
	newBlocks := d.newBlockCount(first, last)
	reported := false
	for !d.closed && d.dirty > 0 && d.dirty+newBlocks*d.blockSize > d.maxDirty {
		if !reported && d.onBackpressure != nil {
			d.onBackpressure()
			reported = true
		}
		d.ready.Wait()
		newBlocks = d.newBlockCount(first, last)
	}
	if d.closed {
		return 0, ErrDeviceClosed
	}
	if d.hydrator != nil {
		if err := d.hydrator.PrepareWrite(off, int64(len(p))); err != nil {
			return 0, err
		}
	}
	n, err := d.file.WriteAt(p, off)
	if n > 0 {
		d.localDirty = true
		actualFirst, actualLast := d.blockRange(off, int64(n))
		if captureErr := d.captureWrite(p[:n], off, actualFirst, actualLast); captureErr != nil {
			return n, captureErr
		}
		if d.hydrator != nil {
			d.hydrator.CommitWrite(off, int64(n))
		}
		d.observeDirtyLocked()
		d.notifyDirtyLocked()
	}
	if err != nil {
		return n, err
	}
	return n, nil
}

func (d *Device) Trim(off, length int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDeviceClosed
	}
	if off < 0 || length < 0 || length > d.size-off {
		return fmt.Errorf("trim outside device")
	}
	first, last := d.blockRange(off, length)
	newBlocks := d.newBlockCount(first, last)
	reported := false
	for !d.closed && d.dirty > 0 && d.dirty+newBlocks*d.blockSize > d.maxDirty {
		if !reported && d.onBackpressure != nil {
			d.onBackpressure()
			reported = true
		}
		d.ready.Wait()
		newBlocks = d.newBlockCount(first, last)
	}
	if d.closed {
		return ErrDeviceClosed
	}
	if d.hydrator != nil {
		if err := d.hydrator.PrepareWrite(off, length); err != nil {
			return err
		}
	}
	if err := unix.Fallocate(int(d.file.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, off, length); err != nil {
		zero := make([]byte, min(length, 1<<20))
		for written := int64(0); written < length; {
			n := min(int64(len(zero)), length-written)
			if _, writeErr := d.file.WriteAt(zero[:n], off+written); writeErr != nil {
				return writeErr
			}
			written += n
		}
	}
	if err := d.capture(first, last); err != nil {
		return err
	}
	if d.hydrator != nil {
		d.hydrator.CommitWrite(off, length)
	}
	if length > 0 {
		d.localDirty = true
	}
	d.observeDirtyLocked()
	d.notifyDirtyLocked()
	return nil
}

func (d *Device) Flush() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrDeviceClosed
	}
	handler := d.onFlush
	skipLocal := d.skipLocalFlush
	var err error
	if !skipLocal && d.localDirty {
		err = unix.Fdatasync(int(d.file.Fd()))
		if err == nil {
			d.localDirty = false
		}
	}
	remoteHandler := d.onRemoteFlush
	var remoteResult <-chan error
	if err == nil && remoteHandler != nil {
		remoteResult = remoteHandler(d.sealLocked())
	}
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if remoteResult != nil {
		return <-remoteResult
	}
	if handler != nil {
		return handler()
	}
	return nil
}

func (d *Device) newBlock() []byte {
	if value := d.blockPool.Get(); value != nil {
		return value.([]byte)
	}
	return make([]byte, d.blockSize)
}

func (d *Device) blockRange(off, length int64) (uint64, uint64) {
	first := uint64(off / d.blockSize)
	if length == 0 {
		return first, first
	}
	return first, uint64((off+length-1)/d.blockSize) + 1
}

func (d *Device) newBlockCount(first, last uint64) int64 {
	var count int64
	for block := first; block < last; block++ {
		if _, ok := d.active[block]; !ok {
			count++
		}
	}
	return count
}

func (d *Device) notifyDirtyLocked() {
	if d.onDirty == nil || d.dirtyThreshold <= 0 || d.dirtyNotified || d.dirty < d.dirtyThreshold {
		return
	}
	d.dirtyNotified = true
	d.onDirty()
}

func (d *Device) capture(first, last uint64) error {
	for block := first; block < last; block++ {
		buf, exists := d.active[block]
		if !exists {
			buf = d.newBlock()
			d.active[block] = buf
			d.dirty += d.blockSize
		}
		if _, err := d.file.ReadAt(buf, int64(block)*d.blockSize); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
	}
	return nil
}

func (d *Device) captureWrite(data []byte, off int64, first, last uint64) error {
	writeEnd := off + int64(len(data))
	for block := first; block < last; block++ {
		blockOff := int64(block) * d.blockSize
		start := max(off, blockOff)
		end := min(writeEnd, blockOff+d.blockSize)
		buf, exists := d.active[block]
		if !exists {
			buf = d.newBlock()
			d.active[block] = buf
			d.dirty += d.blockSize
			if start != blockOff || end != blockOff+d.blockSize {
				if _, err := d.file.ReadAt(buf, blockOff); err != nil && !errors.Is(err, io.EOF) {
					return err
				}
			}
		}
		copy(buf[start-blockOff:end-blockOff], data[start-off:end-off])
	}
	return nil
}

// MarkAllocated records every non-zero block currently present in the sparse
// image. It is used once after mkfs creates a new filesystem.
func (d *Device) MarkAllocated() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDeviceClosed
	}
	for off := int64(0); off < d.size; {
		data, err := unix.Seek(int(d.file.Fd()), off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		}
		if err != nil {
			return d.markByScan()
		}
		hole, err := unix.Seek(int(d.file.Fd()), data, unix.SEEK_HOLE)
		if errors.Is(err, unix.ENXIO) {
			hole = d.size
		} else if err != nil {
			return d.markByScan()
		}
		first, last := d.blockRange(data, hole-data)
		if err := d.captureNonZero(first, last); err != nil {
			return err
		}
		off = hole
	}
	return nil
}

func (d *Device) markByScan() error {
	return d.captureNonZero(0, uint64(d.size/d.blockSize))
}

func (d *Device) captureNonZero(first, last uint64) error {
	for block := first; block < last; block++ {
		buf := make([]byte, d.blockSize)
		if _, err := d.file.ReadAt(buf, int64(block)*d.blockSize); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if isZero(buf) {
			continue
		}
		d.active[block] = buf
		d.dirty += d.blockSize
	}
	return nil
}

func (d *Device) Seal() *Generation {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sealLocked()
}

func (d *Device) sealLocked() *Generation {
	if len(d.active) == 0 {
		return nil
	}
	g := &Generation{Blocks: d.active, bytes: int64(len(d.active)) * d.blockSize}
	d.active = make(map[uint64][]byte)
	return g
}

func (d *Device) Release(g *Generation) {
	if g == nil {
		return
	}
	bytes := g.Bytes()
	d.mu.Lock()
	d.dirty -= bytes
	if d.dirty < 0 {
		d.dirty = 0
	}
	if d.dirty < d.dirtyThreshold {
		d.dirtyNotified = false
	}
	d.observeDirtyLocked()
	d.ready.Broadcast()
	d.mu.Unlock()
	for _, block := range g.Blocks {
		d.blockPool.Put(block)
	}
}

func (d *Device) DirtyBytes() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dirty
}

func (d *Device) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.dirty = 0
	d.observeDirtyLocked()
	d.ready.Broadcast()
	err := d.file.Close()
	if d.hydrator != nil {
		err = errors.Join(err, d.hydrator.Close())
	}
	d.mu.Unlock()
	return err
}

func sortedBlocks(g *Generation) []uint64 {
	blocks := make([]uint64, 0, len(g.Blocks))
	for block := range g.Blocks {
		blocks = append(blocks, block)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	return blocks
}
