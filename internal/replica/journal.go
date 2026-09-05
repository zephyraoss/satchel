package replica

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	journalFormat       = "satchel-local-journal-v1"
	journalHeaderMagic  = "SATJNL01"
	journalRecordMagic  = "SATGEN01"
	journalHeaderPrefix = 12
	journalRecordHeader = 60
	maxJournalMetadata  = 64 << 10
	journalCompactBytes = 64 << 20
)

var ErrJournalConflict = errors.New("local journal conflicts with remote history")

type JournalMetadata struct {
	Format         string `json:"format"`
	VolumeID       string `json:"volume_id"`
	VolumeName     string `json:"volume_name"`
	Holder         string `json:"holder"`
	Size           int64  `json:"size"`
	BlockSize      int64  `json:"block_size"`
	WriterEpoch    uint64 `json:"writer_epoch"`
	BaseGeneration uint64 `json:"base_generation"`
	BaseManifest   string `json:"base_manifest,omitempty"`
}

type journalWaiters struct {
	acknowledged chan error
	ready        chan error
}

// Journal is a durable, append-only record of generations acknowledged before
// they reach remote storage. The sparse image remains disposable.
type Journal struct {
	mu           sync.Mutex
	syncMu       sync.Mutex
	path         string
	file         *os.File
	metadata     JournalMetadata
	entries      []*Generation
	nextSeq      uint64
	pendingSync  []journalWaiters
	pendingData  int
	inflight     []journalWaiters
	syncRunning  bool
	syncWG       sync.WaitGroup
	closed       bool
	broken       error
	syncFile     func(*os.File) error
	diskBytes    int64
	reclaimBytes int64
	compactAfter int64
}

func syncJournalFile(file *os.File) error {
	return unix.Fdatasync(int(file.Fd()))
}

func journalMetadata(state State) (JournalMetadata, error) {
	if state.Lease == nil {
		return JournalMetadata{}, errors.New("cannot create local journal without a lease")
	}
	return JournalMetadata{
		Format: journalFormat, VolumeID: state.ID, VolumeName: state.Name, Holder: state.Lease.Holder,
		Size: state.Size, BlockSize: state.BlockSize, WriterEpoch: state.Epoch,
		BaseGeneration: state.Generation, BaseManifest: state.Manifest,
	}, nil
}

func CreateJournal(path string, state State) (*Journal, error) {
	metadata, err := journalMetadata(state)
	if err != nil {
		return nil, err
	}
	journal := &Journal{
		path: path, metadata: metadata, nextSeq: 1, syncFile: syncJournalFile,
		compactAfter: journalCompactBytes,
	}
	if err := journal.rewriteLocked(); err != nil {
		return nil, err
	}
	return journal, nil
}

func OpenJournal(path string) (*Journal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, err
	}
	metadata, entries, validBytes, err := readJournal(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	diskBytes := stat.Size()
	if validBytes < diskBytes {
		if err := file.Truncate(validBytes); err != nil {
			file.Close()
			return nil, fmt.Errorf("discard torn journal tail: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, fmt.Errorf("sync repaired journal: %w", err)
		}
		diskBytes = validBytes
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, err
	}
	nextSeq := uint64(1)
	if len(entries) > 0 {
		nextSeq = entries[len(entries)-1].journalSeq + 1
	}
	return &Journal{
		path: path, file: file, metadata: metadata, entries: entries, nextSeq: nextSeq,
		syncFile: syncJournalFile, diskBytes: diskBytes, compactAfter: journalCompactBytes,
	}, nil
}

func (j *Journal) ValidateRecovery(state State) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	metadata := j.metadata
	if metadata.Format != journalFormat || metadata.VolumeID != state.ID || metadata.VolumeName != state.Name ||
		metadata.Size != state.Size || metadata.BlockSize != state.BlockSize {
		return fmt.Errorf("%w: journal identity does not match volume %s", ErrJournalConflict, state.Name)
	}
	if state.Generation < metadata.BaseGeneration {
		return fmt.Errorf("%w: remote generation %d precedes journal base %d", ErrJournalConflict, state.Generation, metadata.BaseGeneration)
	}
	if state.Epoch == metadata.WriterEpoch {
		if state.Generation == metadata.BaseGeneration && state.Manifest != metadata.BaseManifest {
			return fmt.Errorf("%w: remote manifest changed at generation %d", ErrJournalConflict, state.Generation)
		}
		return nil
	}
	sameHolder := state.Lease != nil && state.Lease.Holder == metadata.Holder
	unchangedHead := state.Generation == metadata.BaseGeneration && state.Manifest == metadata.BaseManifest
	// A normal restart advances the lease epoch once. A larger jump means
	// another claim occurred after this journal's writer stopped.
	resumedPublishedPrefix := state.Epoch == metadata.WriterEpoch+1 && state.Generation > metadata.BaseGeneration
	if sameHolder && (unchangedHead || resumedPublishedPrefix) {
		return nil
	}
	return fmt.Errorf("%w: journal belongs to epoch %d but remote is at epoch %d", ErrJournalConflict, metadata.WriterEpoch, state.Epoch)
}

func (j *Journal) Entries() []*Generation {
	j.mu.Lock()
	defer j.mu.Unlock()
	entries := make([]*Generation, len(j.entries))
	copy(entries, j.entries)
	return entries
}

func (j *Journal) Rebase(state State) error {
	metadata, err := journalMetadata(state)
	if err != nil {
		return err
	}
	j.syncMu.Lock()
	defer j.syncMu.Unlock()
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return errors.New("local journal is closed")
	}
	if j.broken != nil {
		return j.broken
	}
	j.metadata = metadata
	return j.rewriteLocked()
}

// Enqueue appends a generation before returning. The two result channels both
// receive the outcome of the same stable-storage flush. One is returned to the
// filesystem and the other orders remote publication after local durability.
func (j *Journal) Enqueue(generation *Generation) (<-chan error, <-chan error) {
	acknowledged := make(chan error, 1)
	ready := make(chan error, 1)
	complete := func(err error) {
		acknowledged <- err
		ready <- err
	}
	if generation.Empty() {
		j.mu.Lock()
		err := j.broken
		if j.closed && err == nil {
			err = errors.New("local journal is closed")
		}
		if err == nil && j.syncRunning {
			waiter := journalWaiters{acknowledged: acknowledged, ready: ready}
			if j.pendingData == 0 {
				j.inflight = append(j.inflight, waiter)
			} else {
				j.pendingSync = append(j.pendingSync, waiter)
			}
			j.mu.Unlock()
			return acknowledged, ready
		}
		j.mu.Unlock()
		complete(err)
		return acknowledged, ready
	}

	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		complete(errors.New("local journal is closed"))
		return acknowledged, ready
	}
	if j.broken != nil {
		err := j.broken
		j.mu.Unlock()
		complete(err)
		return acknowledged, ready
	}
	sequence := j.nextSeq
	record, err := encodeJournalRecord(generation, sequence)
	if err != nil {
		j.mu.Unlock()
		complete(err)
		return acknowledged, ready
	}
	if err := writeAll(j.file, record); err != nil {
		j.broken = fmt.Errorf("append local journal: %w", err)
		err := j.broken
		j.mu.Unlock()
		complete(err)
		return acknowledged, ready
	}
	j.nextSeq++
	j.entries = append(j.entries, generation)
	j.diskBytes += int64(len(record))
	j.pendingSync = append(j.pendingSync, journalWaiters{acknowledged: acknowledged, ready: ready})
	j.pendingData++
	if !j.syncRunning {
		j.syncRunning = true
		j.syncWG.Add(1)
		go j.syncPending()
	}
	j.mu.Unlock()
	return acknowledged, ready
}

func (j *Journal) syncPending() {
	defer j.syncWG.Done()
	for {
		j.syncMu.Lock()
		j.mu.Lock()
		if len(j.pendingSync) == 0 {
			j.syncRunning = false
			j.mu.Unlock()
			j.syncMu.Unlock()
			return
		}
		waiters := j.pendingSync
		j.pendingSync = nil
		j.pendingData = 0
		file := j.file
		syncFile := j.syncFile
		j.mu.Unlock()

		err := syncFile(file)

		j.mu.Lock()
		waiters = append(waiters, j.inflight...)
		j.inflight = nil
		if err != nil {
			j.broken = fmt.Errorf("sync local journal: %w", err)
			err = j.broken
			waiters = append(waiters, j.pendingSync...)
			j.pendingSync = nil
			j.pendingData = 0
			j.syncRunning = false
		} else if len(j.pendingSync) == 0 {
			j.syncRunning = false
		}
		more := j.syncRunning
		j.mu.Unlock()
		j.syncMu.Unlock()

		for _, waiter := range waiters {
			waiter.acknowledged <- err
			waiter.ready <- err
		}
		if !more {
			return
		}
	}
}

func (j *Journal) Acknowledge(sequence uint64, state State) error {
	if sequence == 0 {
		return nil
	}
	metadata, err := journalMetadata(state)
	if err != nil {
		return err
	}
	j.syncMu.Lock()
	defer j.syncMu.Unlock()
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return errors.New("local journal is closed")
	}
	if j.broken != nil {
		return j.broken
	}
	remaining := j.entries[:0]
	for _, entry := range j.entries {
		if entry.journalSeq > sequence {
			remaining = append(remaining, entry)
		} else {
			j.reclaimBytes += journalRecordSize(entry)
		}
	}
	j.entries = remaining
	j.metadata = metadata
	if j.reclaimBytes < j.compactAfter || j.reclaimBytes*2 < j.diskBytes {
		return nil
	}
	if err := j.rewriteLocked(); err != nil {
		j.broken = fmt.Errorf("compact local journal: %w", err)
		return j.broken
	}
	return nil
}

func (j *Journal) Close(remove bool) error {
	j.syncMu.Lock()
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		j.syncMu.Unlock()
		return nil
	}
	j.closed = true
	j.mu.Unlock()
	j.syncMu.Unlock()
	j.syncWG.Wait()

	j.syncMu.Lock()
	defer j.syncMu.Unlock()
	j.mu.Lock()
	defer j.mu.Unlock()
	var err error
	if j.file != nil {
		err = j.file.Close()
	}
	if remove {
		removeErr := os.Remove(j.path)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		err = errors.Join(err, removeErr)
		if syncErr := syncDirectory(filepath.Dir(j.path)); syncErr != nil {
			err = errors.Join(err, syncErr)
		}
	}
	return err
}

func (j *Journal) rewriteLocked() error {
	dir := filepath.Dir(j.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(dir, ".journal-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if err := writeJournalHeader(temporary, j.metadata); err != nil {
		return err
	}
	for _, entry := range j.entries {
		record, err := encodeJournalRecord(entry, entry.journalSeq)
		if err != nil {
			return err
		}
		if err := writeAll(temporary, record); err != nil {
			return err
		}
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if j.file != nil {
		if err := j.file.Close(); err != nil {
			return err
		}
		j.file = nil
	}
	if err := os.Rename(temporaryPath, j.path); err != nil {
		j.file, _ = os.OpenFile(j.path, os.O_RDWR|os.O_APPEND, 0)
		return err
	}
	keep = true
	file, err := os.OpenFile(j.path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	j.file = file
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	j.diskBytes = stat.Size()
	j.reclaimBytes = 0
	if err := syncDirectory(dir); err != nil {
		return err
	}
	return nil
}

func journalRecordSize(generation *Generation) int64 {
	return journalRecordHeader + int64(len(generation.Blocks))*(8+DefaultBlockSize)
}

func writeJournalHeader(writer io.Writer, metadata JournalMetadata) error {
	body, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if len(body) == 0 || len(body) > maxJournalMetadata {
		return fmt.Errorf("local journal metadata is %d bytes", len(body))
	}
	prefix := make([]byte, journalHeaderPrefix)
	copy(prefix, journalHeaderMagic)
	binary.BigEndian.PutUint32(prefix[8:12], uint32(len(body)))
	digest := sha256.Sum256(body)
	return writeAll(writer, append(append(prefix, body...), digest[:]...))
}

func encodeJournalRecord(generation *Generation, sequence uint64) ([]byte, error) {
	if generation.Empty() {
		return nil, errors.New("cannot journal an empty generation")
	}
	if len(generation.Blocks) > int(^uint32(0)) {
		return nil, errors.New("journal generation has too many blocks")
	}
	blocks := sortedBlocks(generation)
	payloadBytes := int64(len(blocks)) * (8 + DefaultBlockSize)
	if payloadBytes > int64(int(^uint(0)>>1))-journalRecordHeader {
		return nil, errors.New("journal generation is too large")
	}
	record := make([]byte, journalRecordHeader+int(payloadBytes))
	copy(record, journalRecordMagic)
	binary.BigEndian.PutUint64(record[8:16], sequence)
	binary.BigEndian.PutUint32(record[16:20], uint32(len(blocks)))
	binary.BigEndian.PutUint64(record[20:28], uint64(payloadBytes))
	offset := journalRecordHeader
	for _, block := range blocks {
		data := generation.Blocks[block]
		if len(data) != DefaultBlockSize {
			return nil, fmt.Errorf("journal block %d has %d bytes", block, len(data))
		}
		binary.BigEndian.PutUint64(record[offset:offset+8], block)
		copy(record[offset+8:offset+8+DefaultBlockSize], data)
		offset += 8 + DefaultBlockSize
	}
	hash := sha256.New()
	_, _ = hash.Write(record[8:28])
	_, _ = hash.Write(record[journalRecordHeader:])
	copy(record[28:60], hash.Sum(nil))
	generation.journalSeq = sequence
	return record, nil
}

func readJournal(file *os.File) (JournalMetadata, []*Generation, int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return JournalMetadata{}, nil, 0, err
	}
	prefix := make([]byte, journalHeaderPrefix)
	if _, err := io.ReadFull(file, prefix); err != nil {
		return JournalMetadata{}, nil, 0, fmt.Errorf("read local journal header: %w", err)
	}
	if string(prefix[:8]) != journalHeaderMagic {
		return JournalMetadata{}, nil, 0, errors.New("local journal has invalid header magic")
	}
	metadataBytes := int(binary.BigEndian.Uint32(prefix[8:12]))
	if metadataBytes <= 0 || metadataBytes > maxJournalMetadata {
		return JournalMetadata{}, nil, 0, fmt.Errorf("local journal metadata length %d is invalid", metadataBytes)
	}
	body := make([]byte, metadataBytes)
	if _, err := io.ReadFull(file, body); err != nil {
		return JournalMetadata{}, nil, 0, fmt.Errorf("read local journal metadata: %w", err)
	}
	digest := make([]byte, sha256.Size)
	if _, err := io.ReadFull(file, digest); err != nil {
		return JournalMetadata{}, nil, 0, fmt.Errorf("read local journal metadata checksum: %w", err)
	}
	wantDigest := sha256.Sum256(body)
	if !bytes.Equal(digest, wantDigest[:]) {
		return JournalMetadata{}, nil, 0, errors.New("local journal metadata checksum does not match")
	}
	var metadata JournalMetadata
	if err := json.Unmarshal(body, &metadata); err != nil {
		return JournalMetadata{}, nil, 0, fmt.Errorf("decode local journal metadata: %w", err)
	}
	if metadata.Format != journalFormat || metadata.VolumeID == "" || metadata.VolumeName == "" ||
		metadata.Size <= 0 || metadata.BlockSize != DefaultBlockSize {
		return JournalMetadata{}, nil, 0, errors.New("local journal metadata is invalid")
	}
	offset := int64(journalHeaderPrefix + metadataBytes + sha256.Size)
	entries := make([]*Generation, 0)
	lastSequence := uint64(0)
	for {
		recordOffset := offset
		header := make([]byte, journalRecordHeader)
		n, err := io.ReadFull(file, header)
		offset += int64(n)
		if errors.Is(err, io.EOF) {
			return metadata, entries, recordOffset, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return metadata, entries, recordOffset, nil
		}
		if err != nil {
			return JournalMetadata{}, nil, 0, err
		}
		if string(header[:8]) != journalRecordMagic {
			return JournalMetadata{}, nil, 0, fmt.Errorf("local journal record at byte %d has invalid magic", recordOffset)
		}
		sequence := binary.BigEndian.Uint64(header[8:16])
		blockCount := uint64(binary.BigEndian.Uint32(header[16:20]))
		payloadBytes := binary.BigEndian.Uint64(header[20:28])
		expectedBytes := blockCount * uint64(8+DefaultBlockSize)
		if sequence <= lastSequence || blockCount == 0 || blockCount > uint64(metadata.Size/metadata.BlockSize) || payloadBytes != expectedBytes {
			return JournalMetadata{}, nil, 0, fmt.Errorf("local journal record at byte %d has invalid dimensions", recordOffset)
		}
		generation := &Generation{Blocks: make(map[uint64][]byte, blockCount), bytes: int64(blockCount) * metadata.BlockSize, journalSeq: sequence}
		hash := sha256.New()
		_, _ = hash.Write(header[8:28])
		for range blockCount {
			blockHeader := make([]byte, 8)
			n, err := io.ReadFull(file, blockHeader)
			offset += int64(n)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return metadata, entries, recordOffset, nil
			}
			if err != nil {
				return JournalMetadata{}, nil, 0, err
			}
			block := binary.BigEndian.Uint64(blockHeader)
			if block >= uint64(metadata.Size/metadata.BlockSize) {
				return JournalMetadata{}, nil, 0, fmt.Errorf("local journal block %d is outside the volume", block)
			}
			if _, exists := generation.Blocks[block]; exists {
				return JournalMetadata{}, nil, 0, fmt.Errorf("local journal repeats block %d in one generation", block)
			}
			data := make([]byte, metadata.BlockSize)
			n, err = io.ReadFull(file, data)
			offset += int64(n)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return metadata, entries, recordOffset, nil
			}
			if err != nil {
				return JournalMetadata{}, nil, 0, err
			}
			_, _ = hash.Write(blockHeader)
			_, _ = hash.Write(data)
			generation.Blocks[block] = data
		}
		if !bytes.Equal(header[28:60], hash.Sum(nil)) {
			return JournalMetadata{}, nil, 0, fmt.Errorf("local journal record %d checksum does not match", sequence)
		}
		entries = append(entries, generation)
		lastSequence = sequence
	}
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
