package store

import (
	"encoding/binary"
	"io"
	"os"
)

const walIndexPrefix = 100

type WALState struct {
	PageSize int64
	Frames   int64
	Backfill int64
}

func (s WALState) LiveBytes() int64 {
	if s.Frames == 0 {
		return 0
	}
	return 32 + s.Frames*(s.PageSize+24)
}

func (s WALState) PendingBytes() int64 {
	pending := s.Frames - s.Backfill
	if pending <= 0 {
		return 0
	}
	return pending * (s.PageSize + 24)
}

func (db *DB) WALState() WALState {
	f := db.walIndexFile()
	if f == nil {
		return WALState{}
	}
	return readWALState(f)
}

func (db *DB) walIndexFile() *os.File {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.walIndex == nil {
		f, err := os.Open(db.path + "-shm")
		if err != nil {
			return nil
		}
		db.walIndex = f
	}
	return db.walIndex
}

func ReadWALState(dbPath string) WALState {
	f, err := os.Open(dbPath + "-shm")
	if err != nil {
		return WALState{}
	}
	defer f.Close()
	return readWALState(f)
}

func readWALState(f io.ReaderAt) WALState {
	var header [walIndexPrefix]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		return WALState{}
	}
	if header[12] == 0 {
		return WALState{}
	}
	pageSize := int64(binary.NativeEndian.Uint16(header[14:16]))
	if pageSize == 1 {
		pageSize = 65536
	}
	return WALState{
		PageSize: pageSize,
		Frames:   int64(binary.NativeEndian.Uint32(header[16:20])),
		Backfill: int64(binary.NativeEndian.Uint32(header[96:100])),
	}
}

func LiveWALBytes(dbPath string) int64 {
	return ReadWALState(dbPath).LiveBytes()
}

func PendingWALBytes(dbPath string) int64 {
	return ReadWALState(dbPath).PendingBytes()
}
