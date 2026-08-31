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

	"golang.org/x/sys/unix"
)

const segmentMagic = "SATSEG01"

type Segment struct {
	Data   []byte `json:"-"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Blocks int64  `json:"blocks"`
}

func EncodeSegment(g *Generation) (Segment, error) {
	if g.Empty() {
		return Segment{}, errors.New("cannot encode an empty generation")
	}
	var raw bytes.Buffer
	if _, err := raw.WriteString(segmentMagic); err != nil {
		return Segment{}, err
	}
	if err := binary.Write(&raw, binary.BigEndian, uint32(DefaultBlockSize)); err != nil {
		return Segment{}, err
	}
	blocks := sortedBlocks(g)
	runs := make([][]uint64, 0)
	for _, block := range blocks {
		if len(runs) == 0 || block != runs[len(runs)-1][len(runs[len(runs)-1])-1]+1 || len(runs[len(runs)-1]) >= 256 {
			runs = append(runs, []uint64{block})
		} else {
			runs[len(runs)-1] = append(runs[len(runs)-1], block)
		}
	}
	if err := binary.Write(&raw, binary.BigEndian, uint32(len(runs))); err != nil {
		return Segment{}, err
	}
	for _, run := range runs {
		if err := binary.Write(&raw, binary.BigEndian, run[0]); err != nil {
			return Segment{}, err
		}
		length := uint32(len(run) * DefaultBlockSize)
		if err := binary.Write(&raw, binary.BigEndian, length); err != nil {
			return Segment{}, err
		}
		for _, block := range run {
			if _, err := raw.Write(g.Blocks[block]); err != nil {
				return Segment{}, err
			}
		}
	}

	var compressed bytes.Buffer
	zw, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return Segment{}, err
	}
	if _, err := zw.Write(raw.Bytes()); err != nil {
		return Segment{}, err
	}
	if err := zw.Close(); err != nil {
		return Segment{}, err
	}
	data := compressed.Bytes()
	hash := sha256.Sum256(data)
	return Segment{
		Data:   data,
		SHA256: hex.EncodeToString(hash[:]),
		Bytes:  int64(len(data)),
		Blocks: int64(len(blocks)),
	}, nil
}

func ApplySegment(path string, size int64, segment Segment) error {
	if segment.Bytes != 0 && segment.Bytes != int64(len(segment.Data)) {
		return errors.New("segment length mismatch")
	}
	hash := sha256.Sum256(segment.Data)
	if hex.EncodeToString(hash[:]) != segment.SHA256 {
		return errors.New("segment checksum mismatch")
	}
	zr, err := gzip.NewReader(bytes.NewReader(segment.Data))
	if err != nil {
		return fmt.Errorf("open segment: %w", err)
	}
	defer zr.Close()
	header := make([]byte, len(segmentMagic))
	if _, err := io.ReadFull(zr, header); err != nil {
		return err
	}
	if string(header) != segmentMagic {
		return errors.New("invalid segment magic")
	}
	var blockSize, runs uint32
	if err := binary.Read(zr, binary.BigEndian, &blockSize); err != nil {
		return err
	}
	if blockSize != DefaultBlockSize {
		return fmt.Errorf("unsupported segment block size %d", blockSize)
	}
	if err := binary.Read(zr, binary.BigEndian, &runs); err != nil {
		return err
	}
	if uint64(runs) > uint64(size/DefaultBlockSize) {
		return errors.New("segment has too many extents")
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	var blocks int64
	for i := uint32(0); i < runs; i++ {
		var block uint64
		var length uint32
		if err := binary.Read(zr, binary.BigEndian, &block); err != nil {
			return err
		}
		if err := binary.Read(zr, binary.BigEndian, &length); err != nil {
			return err
		}
		offset := int64(block) * int64(blockSize)
		if length == 0 || length%blockSize != 0 || offset < 0 || int64(length) > size-offset {
			return errors.New("segment contains an invalid extent")
		}
		blocks += int64(length / blockSize)
		data := make([]byte, length)
		if _, err := io.ReadFull(zr, data); err != nil {
			return err
		}
		if isZero(data) {
			if err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, offset, int64(length)); err != nil {
				if _, err := f.WriteAt(data, offset); err != nil {
					return err
				}
			}
		} else if _, err := f.WriteAt(data, offset); err != nil {
			return err
		}
	}
	var extra [1]byte
	if n, err := zr.Read(extra[:]); n != 0 || err != io.EOF {
		return errors.New("segment has trailing or unreadable data")
	}
	if segment.Blocks != 0 && segment.Blocks != blocks {
		return errors.New("segment block count mismatch")
	}
	return nil
}

func isZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}
