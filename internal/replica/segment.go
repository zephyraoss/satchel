package replica

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const segmentMagic = "SATSEG01"

var segmentWriterPool = sync.Pool{New: func() any {
	writer, err := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
	if err != nil {
		panic(err)
	}
	return writer
}}

type Segment struct {
	Data        []byte   `json:"-"`
	SHA256      string   `json:"sha256"`
	Bytes       int64    `json:"bytes"`
	Blocks      int64    `json:"blocks"`
	Extents     []Extent `json:"extents"`
	ZeroExtents []Extent `json:"zero_extents,omitempty"`
}

type Extent struct {
	Start  uint64 `json:"start"`
	Blocks uint64 `json:"blocks"`
}

func (e Extent) end() uint64 { return e.Start + e.Blocks }

const DefaultSegmentBlocks = 1024

func EncodeSegments(g *Generation, blocksPerSegment int) ([]Segment, error) {
	if g.Empty() {
		return nil, errors.New("cannot encode an empty generation")
	}
	if blocksPerSegment <= 0 {
		blocksPerSegment = DefaultSegmentBlocks
	}
	blocks := sortedBlocks(g)
	count := (len(blocks) + blocksPerSegment - 1) / blocksPerSegment
	if count == 1 {
		segment, err := EncodeSegment(g)
		if err != nil {
			return nil, err
		}
		return []Segment{segment}, nil
	}
	segments := make([]Segment, count)
	jobs := make(chan int)
	var firstErr error
	var errOnce sync.Once
	var group sync.WaitGroup
	workers := min(4, count)
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for index := range jobs {
				start := index * blocksPerSegment
				end := min(len(blocks), start+blocksPerSegment)
				segment, err := encodeSegmentBlocks(g, blocks[start:end])
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					continue
				}
				segments[index] = segment
			}
		}()
	}
	for index := range count {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return segments, nil
}

func EncodeSegment(g *Generation) (Segment, error) {
	if g.Empty() {
		return Segment{}, errors.New("cannot encode an empty generation")
	}
	return encodeSegmentBlocks(g, sortedBlocks(g))
}

func encodeSegmentBlocks(g *Generation, blocks []uint64) (Segment, error) {
	runs := make([][]uint64, 0)
	for _, block := range blocks {
		if len(g.Blocks[block]) != DefaultBlockSize {
			return Segment{}, fmt.Errorf("block %d has length %d, want %d", block, len(g.Blocks[block]), DefaultBlockSize)
		}
		if len(runs) == 0 || block != runs[len(runs)-1][len(runs[len(runs)-1])-1]+1 || len(runs[len(runs)-1]) >= 256 {
			runs = append(runs, []uint64{block})
		} else {
			runs[len(runs)-1] = append(runs[len(runs)-1], block)
		}
	}

	var compressed bytes.Buffer
	zw := segmentWriterPool.Get().(*gzip.Writer)
	zw.Reset(&compressed)
	closed := false
	defer func() {
		if !closed {
			_ = zw.Close()
		}
		zw.Reset(io.Discard)
		segmentWriterPool.Put(zw)
	}()
	closeWithError := func(cause error) (Segment, error) {
		_ = zw.Close()
		closed = true
		return Segment{}, cause
	}
	if _, err := zw.Write([]byte(segmentMagic)); err != nil {
		return closeWithError(err)
	}
	if err := binary.Write(zw, binary.BigEndian, uint32(DefaultBlockSize)); err != nil {
		return closeWithError(err)
	}
	if err := binary.Write(zw, binary.BigEndian, uint32(len(runs))); err != nil {
		return closeWithError(err)
	}
	extents := make([]Extent, 0, len(runs))
	for _, run := range runs {
		if err := binary.Write(zw, binary.BigEndian, run[0]); err != nil {
			return closeWithError(err)
		}
		length := uint32(len(run) * DefaultBlockSize)
		if err := binary.Write(zw, binary.BigEndian, length); err != nil {
			return closeWithError(err)
		}
		extents = append(extents, Extent{Start: run[0], Blocks: uint64(len(run))})
		for _, block := range run {
			if _, err := zw.Write(g.Blocks[block]); err != nil {
				return closeWithError(err)
			}
		}
	}
	if err := zw.Close(); err != nil {
		return Segment{}, err
	}
	closed = true
	data := compressed.Bytes()
	hash := sha256.Sum256(data)
	return Segment{
		Data:        data,
		SHA256:      hex.EncodeToString(hash[:]),
		Bytes:       int64(len(data)),
		Blocks:      int64(len(blocks)),
		Extents:     extents,
		ZeroExtents: zeroExtents(g, blocks),
	}, nil
}

func zeroExtents(g *Generation, blocks []uint64) []Extent {
	zeroBlocks := make([]uint64, 0)
	for _, block := range blocks {
		if isZero(g.Blocks[block]) {
			zeroBlocks = append(zeroBlocks, block)
		}
	}
	return blocksToExtents(zeroBlocks)
}

func blocksToExtents(blocks []uint64) []Extent {
	if len(blocks) == 0 {
		return nil
	}
	extents := make([]Extent, 0)
	for _, block := range blocks {
		if len(extents) == 0 || extents[len(extents)-1].end() != block {
			extents = append(extents, Extent{Start: block, Blocks: 1})
		} else {
			extents[len(extents)-1].Blocks++
		}
	}
	return extents
}

type decodedRun struct {
	extent Extent
	data   []byte
}

func decodeSegment(segment Segment) ([]decodedRun, error) {
	if segment.Bytes != 0 && segment.Bytes != int64(len(segment.Data)) {
		return nil, errors.New("segment length mismatch")
	}
	digest := sha256.Sum256(segment.Data)
	if hex.EncodeToString(digest[:]) != segment.SHA256 {
		return nil, errors.New("segment checksum mismatch")
	}
	zr, err := gzip.NewReader(bytes.NewReader(segment.Data))
	if err != nil {
		return nil, fmt.Errorf("open segment: %w", err)
	}
	defer zr.Close()
	header := make([]byte, len(segmentMagic))
	if _, err := io.ReadFull(zr, header); err != nil {
		return nil, err
	}
	if string(header) != segmentMagic {
		return nil, errors.New("invalid segment magic")
	}
	var blockSize, runCount uint32
	if err := binary.Read(zr, binary.BigEndian, &blockSize); err != nil {
		return nil, err
	}
	if blockSize != DefaultBlockSize {
		return nil, fmt.Errorf("unsupported segment block size %d", blockSize)
	}
	if err := binary.Read(zr, binary.BigEndian, &runCount); err != nil {
		return nil, err
	}
	if segment.Blocks <= 0 || int64(runCount) > segment.Blocks {
		return nil, errors.New("segment has an invalid run count")
	}
	runs := make([]decodedRun, 0, runCount)
	var blocks int64
	var lastEnd uint64
	for i := uint32(0); i < runCount; i++ {
		var block uint64
		var length uint32
		if err := binary.Read(zr, binary.BigEndian, &block); err != nil {
			return nil, err
		}
		if err := binary.Read(zr, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		if length == 0 || length%blockSize != 0 {
			return nil, errors.New("segment contains an invalid extent")
		}
		extent := Extent{Start: block, Blocks: uint64(length / blockSize)}
		if int64(extent.Blocks) > segment.Blocks-blocks {
			return nil, errors.New("segment block count exceeds its manifest")
		}
		if extent.end() < extent.Start || i > 0 && extent.Start < lastEnd {
			return nil, errors.New("segment contains overlapping extents")
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(zr, data); err != nil {
			return nil, err
		}
		runs = append(runs, decodedRun{extent: extent, data: data})
		blocks += int64(extent.Blocks)
		lastEnd = extent.end()
	}
	var extra [1]byte
	if n, err := zr.Read(extra[:]); n != 0 || err != io.EOF {
		return nil, errors.New("segment has trailing or unreadable data")
	}
	if segment.Blocks != 0 && segment.Blocks != blocks {
		return nil, errors.New("segment block count mismatch")
	}
	return runs, nil
}

func ApplySegment(path string, size int64, segment Segment) error {
	runs, err := decodeSegment(segment)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return applyRuns(f, size, runs, nil)
}

func applyRuns(f *os.File, size int64, runs []decodedRun, selected []Extent) error {
	if selected == nil {
		selected = make([]Extent, len(runs))
		for i, run := range runs {
			selected[i] = run.extent
		}
	}
	for _, run := range runs {
		for _, overlap := range intersectExtent(run.extent, selected) {
			offset := int64(overlap.Start) * DefaultBlockSize
			length := int64(overlap.Blocks) * DefaultBlockSize
			if offset < 0 || length > size-offset {
				return errors.New("segment contains an extent outside the image")
			}
			start := int((overlap.Start - run.extent.Start) * DefaultBlockSize)
			data := run.data[start : start+int(length)]
			if isZero(data) {
				if err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, offset, length); err != nil {
					if _, err := f.WriteAt(data, offset); err != nil {
						return err
					}
				}
			} else if _, err := f.WriteAt(data, offset); err != nil {
				return err
			}
		}
	}
	return nil
}

func intersectExtent(target Extent, extents []Extent) []Extent {
	var result []Extent
	for _, extent := range extents {
		if extent.end() <= target.Start {
			continue
		}
		if extent.Start >= target.end() {
			break
		}
		start := max(target.Start, extent.Start)
		end := min(target.end(), extent.end())
		if start < end {
			result = append(result, Extent{Start: start, Blocks: end - start})
		}
	}
	return result
}

func isZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}
