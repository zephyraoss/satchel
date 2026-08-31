package replica

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"golang.org/x/sys/unix"
)

const DefaultBlockSize = 4096

var ErrDeviceClosed = errors.New("block device is closed")

// Generation is an immutable view of the blocks changed during one sync
// interval. Blocks contains complete blocks, even when the original write was
// smaller, so a later write cannot change a generation while it is uploading.
type Generation struct {
	Blocks map[uint64][]byte
}

func (g *Generation) Empty() bool { return g == nil || len(g.Blocks) == 0 }

func (g *Generation) Bytes() int64 {
	if g == nil {
		return 0
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
	onFlush        func() error
	closed         bool
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

// SetFlushHandler sets a function called after the local image reaches stable
// storage. Satchel uses it to publish all dirty blocks before acknowledging a
// filesystem flush.
func (d *Device) SetFlushHandler(handler func() error) {
	d.mu.Lock()
	d.onFlush = handler
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
	n, err := d.file.WriteAt(p, off)
	if err != nil {
		return n, err
	}
	if err := d.capture(first, last); err != nil {
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
	return d.capture(first, last)
}

func (d *Device) Flush() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrDeviceClosed
	}
	err := d.file.Sync()
	handler := d.onFlush
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if handler != nil {
		return handler()
	}
	return nil
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

func (d *Device) capture(first, last uint64) error {
	for block := first; block < last; block++ {
		buf, exists := d.active[block]
		if !exists {
			buf = make([]byte, d.blockSize)
			d.active[block] = buf
			d.dirty += d.blockSize
		}
		if _, err := d.file.ReadAt(buf, int64(block)*d.blockSize); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
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

// Checkpoint returns compressed segments containing every non-zero block in
// the image. The caller must stop filesystem I/O before calling it.
func (d *Device) Checkpoint(ctx context.Context, blocksPerSegment int, emit func(Segment) error) error {
	if blocksPerSegment <= 0 {
		blocksPerSegment = 4096
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrDeviceClosed
	}
	d.mu.Unlock()
	batch := &Generation{Blocks: make(map[uint64][]byte, blocksPerSegment)}
	flush := func() error {
		if batch.Empty() {
			return nil
		}
		segment, err := EncodeSegment(batch)
		if err != nil {
			return err
		}
		batch = &Generation{Blocks: make(map[uint64][]byte, blocksPerSegment)}
		return emit(segment)
	}
	zero := make([]byte, d.blockSize)
	for off := int64(0); off < d.size; {
		data, dataErr := unix.Seek(int(d.file.Fd()), off, unix.SEEK_DATA)
		if errors.Is(dataErr, unix.ENXIO) {
			break
		}
		if dataErr != nil {
			data = off
		}
		hole, holeErr := unix.Seek(int(d.file.Fd()), data, unix.SEEK_HOLE)
		if errors.Is(holeErr, unix.ENXIO) || dataErr != nil {
			hole = d.size
		} else if holeErr != nil {
			return holeErr
		}
		first, last := d.blockRange(data, hole-data)
		for block := first; block < last; block++ {
			if block%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			buf := make([]byte, d.blockSize)
			if _, err := d.file.ReadAt(buf, int64(block)*d.blockSize); err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			if bytes.Equal(buf, zero) {
				continue
			}
			batch.Blocks[block] = buf
			if len(batch.Blocks) >= blocksPerSegment {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		off = hole
	}
	if err := flush(); err != nil {
		return err
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
		if bytes.Equal(buf, make([]byte, d.blockSize)) {
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
	if len(d.active) == 0 {
		return nil
	}
	g := &Generation{Blocks: d.active}
	d.active = make(map[uint64][]byte)
	return g
}

func (d *Device) Release(g *Generation) {
	if g == nil {
		return
	}
	d.mu.Lock()
	d.dirty -= g.Bytes()
	if d.dirty < 0 {
		d.dirty = 0
	}
	d.ready.Broadcast()
	d.mu.Unlock()
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
	d.ready.Broadcast()
	err := d.file.Close()
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
