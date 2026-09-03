package replica

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func craftedSegment(t *testing.T, block uint64, length uint32) Segment {
	t.Helper()
	var raw bytes.Buffer
	zw := gzip.NewWriter(&raw)
	zw.Write([]byte(segmentMagic))
	binary.Write(zw, binary.BigEndian, uint32(DefaultBlockSize))
	binary.Write(zw, binary.BigEndian, uint32(1))
	binary.Write(zw, binary.BigEndian, block)
	binary.Write(zw, binary.BigEndian, length)
	zw.Write(bytes.Repeat([]byte{'x'}, int(min(length, DefaultBlockSize))))
	zw.Close()
	digest := sha256.Sum256(raw.Bytes())
	return Segment{Data: raw.Bytes(), SHA256: hex.EncodeToString(digest[:]), Bytes: int64(raw.Len()), Blocks: int64(length / DefaultBlockSize)}
}

func TestDecodeSegmentRejectsOversizedRunBeforeAllocating(t *testing.T) {
	segment := craftedSegment(t, 0, 1<<30)
	if _, err := decodeSegment(segment); err == nil {
		t.Fatal("run larger than the format maximum was accepted")
	}
}

func TestApplySegmentRejectsBlockNumberThatOverflowsOffset(t *testing.T) {
	segment := craftedSegment(t, math.MaxUint64/DefaultBlockSize-1, DefaultBlockSize)
	if _, err := decodeSegment(segment); err == nil {
		t.Fatal("block number beyond any image was accepted")
	}
	near := craftedSegment(t, 1<<52, DefaultBlockSize)
	path := filepath.Join(t.TempDir(), "image")
	if err := os.WriteFile(path, make([]byte, 4*DefaultBlockSize), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ApplySegment(path, 4*DefaultBlockSize, near); err == nil {
		t.Fatal("extent outside the image was applied")
	}
	data, _ := os.ReadFile(path)
	if !isZero(data) {
		t.Fatal("rejected segment modified the image")
	}
}

func TestEncodeSegmentsToleratesHugeSegmentSize(t *testing.T) {
	g := &Generation{Blocks: map[uint64][]byte{0: make([]byte, DefaultBlockSize)}}
	segments, err := EncodeSegments(g, math.MaxInt)
	if err != nil || len(segments) != 1 {
		t.Fatalf("segments=%d err=%v", len(segments), err)
	}
}
