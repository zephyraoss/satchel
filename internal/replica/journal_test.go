package replica

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func journalTestState() State {
	return State{
		Format: Format, ID: "volume-id", Name: "data", Size: 8 * DefaultBlockSize,
		BlockSize: DefaultBlockSize, Generation: 4, Manifest: "manifest-4", Epoch: 7,
		Lease: &LeaseRecord{Holder: "node-a", Epoch: 7, ExpiresAt: time.Now().Add(time.Minute)},
	}
}

func TestJournalRecoversCommittedGenerationAndDiscardsTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.journal")
	state := journalTestState()
	journal, err := CreateJournal(path, state)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{'j'}, DefaultBlockSize)
	generation := &Generation{Blocks: map[uint64][]byte{3: want}}
	acknowledged, ready := journal.Enqueue(generation)
	if err := <-acknowledged; err != nil {
		t.Fatal(err)
	}
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(false); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte(journalRecordMagic + "torn")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close(false)
	if err := recovered.ValidateRecovery(state); err != nil {
		t.Fatal(err)
	}
	entries := recovered.Entries()
	if len(entries) != 1 || !bytes.Equal(entries[0].Blocks[3], want) {
		t.Fatalf("recovered entries = %+v", entries)
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	wantSize := int64(journalHeaderPrefix + len(mustJSON(t, recovered.metadata)) + sha256Size + journalRecordHeader + 8 + DefaultBlockSize)
	if stat.Size() != wantSize {
		t.Fatalf("repaired journal size = %d, want %d", stat.Size(), wantSize)
	}
}

const sha256Size = 32

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestJournalAcknowledgementCompactsPublishedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.journal")
	state := journalTestState()
	journal, err := CreateJournal(path, state)
	if err != nil {
		t.Fatal(err)
	}
	journal.compactAfter = 1
	first := &Generation{Blocks: map[uint64][]byte{1: bytes.Repeat([]byte{'a'}, DefaultBlockSize)}}
	second := &Generation{Blocks: map[uint64][]byte{2: bytes.Repeat([]byte{'b'}, DefaultBlockSize)}}
	third := &Generation{Blocks: map[uint64][]byte{3: bytes.Repeat([]byte{'c'}, DefaultBlockSize)}}
	for _, generation := range []*Generation{first, second, third} {
		acknowledged, ready := journal.Enqueue(generation)
		if err := <-acknowledged; err != nil {
			t.Fatal(err)
		}
		if err := <-ready; err != nil {
			t.Fatal(err)
		}
	}
	state.Generation++
	state.Manifest = "manifest-5"
	if err := journal.Acknowledge(second.journalSeq, state); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(false); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(false)
	entries := reopened.Entries()
	if len(entries) != 1 || entries[0].journalSeq != third.journalSeq || entries[0].Blocks[3][0] != 'c' {
		t.Fatalf("entries after acknowledgement = %+v", entries)
	}
	if reopened.metadata.BaseGeneration != state.Generation || reopened.metadata.BaseManifest != state.Manifest {
		t.Fatalf("journal base = %+v", reopened.metadata)
	}
}

func TestJournalDefersSmallCompactionAndReplaysPublishedRecordsSafely(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.journal")
	state := journalTestState()
	journal, err := CreateJournal(path, state)
	if err != nil {
		t.Fatal(err)
	}
	generation := &Generation{Blocks: map[uint64][]byte{1: bytes.Repeat([]byte{'a'}, DefaultBlockSize)}}
	acknowledged, ready := journal.Enqueue(generation)
	if err := <-acknowledged; err != nil {
		t.Fatal(err)
	}
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	state.Generation++
	state.Manifest = "manifest-5"
	if err := journal.Acknowledge(generation.journalSeq, state); err != nil {
		t.Fatal(err)
	}
	if entries := journal.Entries(); len(entries) != 0 {
		t.Fatalf("acknowledged entries retained in memory: %d", len(entries))
	}
	if err := journal.Close(false); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(false)
	if err := reopened.ValidateRecovery(state); err != nil {
		t.Fatalf("deferred compaction made recovery unsafe: %v", err)
	}
	if entries := reopened.Entries(); len(entries) != 1 || entries[0].journalSeq != generation.journalSeq {
		t.Fatalf("deferred on-disk entries = %+v", entries)
	}
}

func TestJournalPipelinesConcurrentFlushes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.journal")
	journal, err := CreateJournal(path, journalTestState())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close(false)

	started := make(chan struct{})
	release := make(chan struct{})
	var syncs atomic.Int64
	journal.syncFile = func(file *os.File) error {
		if syncs.Add(1) == 1 {
			close(started)
			<-release
		}
		return syncJournalFile(file)
	}

	firstAck, firstReady := journal.Enqueue(&Generation{Blocks: map[uint64][]byte{
		0: bytes.Repeat([]byte{'a'}, DefaultBlockSize),
	}})
	<-started

	type results struct {
		acknowledged <-chan error
		ready        <-chan error
	}
	queued := make(chan results, 2)
	for block := uint64(1); block <= 2; block++ {
		block := block
		go func() {
			acknowledged, ready := journal.Enqueue(&Generation{Blocks: map[uint64][]byte{
				block: bytes.Repeat([]byte{byte('a' + block)}, DefaultBlockSize),
			}})
			queued <- results{acknowledged: acknowledged, ready: ready}
		}()
	}
	dequeue := func() results {
		t.Helper()
		select {
		case result := <-queued:
			return result
		case <-time.After(time.Second):
			close(release)
			t.Fatal("journal append blocked behind an in-flight sync")
			return results{}
		}
	}
	second := dequeue()
	third := dequeue()
	emptyAck, emptyReady := journal.Enqueue(nil)
	close(release)

	for _, result := range []<-chan error{firstAck, firstReady, second.acknowledged, second.ready, third.acknowledged, third.ready, emptyAck, emptyReady} {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	if got := syncs.Load(); got != 2 {
		t.Fatalf("journal syncs = %d, want 2 batches", got)
	}
}

func TestJournalRejectsCorruptCommittedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.journal")
	journal, err := CreateJournal(path, journalTestState())
	if err != nil {
		t.Fatal(err)
	}
	generation := &Generation{Blocks: map[uint64][]byte{1: bytes.Repeat([]byte{'c'}, DefaultBlockSize)}}
	acknowledged, ready := journal.Enqueue(generation)
	if err := <-acknowledged; err != nil {
		t.Fatal(err)
	}
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(false); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(-1, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{'x'}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(path); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("open corrupt journal error = %v", err)
	}
}

func TestJournalRejectsRecoveryAfterAnotherWriterAdvances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.journal")
	state := journalTestState()
	journal, err := CreateJournal(path, state)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close(false)

	takenOver := state
	takenOver.Epoch++
	takenOver.Generation++
	takenOver.Manifest = "other-writer"
	takenOver.Lease = &LeaseRecord{Holder: "node-b", Epoch: takenOver.Epoch, ExpiresAt: time.Now().Add(time.Minute)}
	if err := journal.ValidateRecovery(takenOver); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("recovery error = %v, want journal conflict", err)
	}

	resumed := state
	resumed.Epoch++
	resumed.Lease = &LeaseRecord{Holder: "node-a", Epoch: resumed.Epoch, ExpiresAt: time.Now().Add(time.Minute)}
	if err := journal.ValidateRecovery(resumed); err != nil {
		t.Fatalf("same-node claim with unchanged remote head was rejected: %v", err)
	}

	resumed.Generation++
	resumed.Manifest = "manifest-5"
	if err := journal.ValidateRecovery(resumed); err != nil {
		t.Fatalf("same-node claim rejected its own published prefix: %v", err)
	}

	resumed.Epoch++
	resumed.Lease.Epoch = resumed.Epoch
	if err := journal.ValidateRecovery(resumed); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("claim after an intervening writer error = %v, want journal conflict", err)
	}
}

func TestDeviceReplaysJournalOverLazyImage(t *testing.T) {
	device, err := OpenDevice(filepath.Join(t.TempDir(), "image"), 4*DefaultBlockSize, 4*DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	want := bytes.Repeat([]byte{'r'}, DefaultBlockSize)
	generation := &Generation{Blocks: map[uint64][]byte{2: want}, bytes: DefaultBlockSize, journalSeq: 1}
	if err := device.Replay([]*Generation{generation}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, DefaultBlockSize)
	if _, err := device.ReadAt(got, 2*DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) || device.DirtyBytes() != DefaultBlockSize {
		t.Fatal("journal replay did not restore and account for the block")
	}
}

func BenchmarkJournalFlush4K(b *testing.B) {
	state := journalTestState()
	journal, err := CreateJournal(filepath.Join(b.TempDir(), "data.journal"), state)
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close(false)
	block := bytes.Repeat([]byte{'b'}, DefaultBlockSize)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		generation := &Generation{Blocks: map[uint64][]byte{uint64(index % 8): block}}
		acknowledged, ready := journal.Enqueue(generation)
		if err := <-acknowledged; err != nil {
			b.Fatal(err)
		}
		if err := <-ready; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJournalFlush4KParallel(b *testing.B) {
	state := journalTestState()
	journal, err := CreateJournal(filepath.Join(b.TempDir(), "data.journal"), state)
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close(false)
	var block atomic.Uint64
	var syncs atomic.Int64
	journal.syncFile = func(file *os.File) error {
		syncs.Add(1)
		return syncJournalFile(file)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			generation := &Generation{Blocks: map[uint64][]byte{
				block.Add(1) % 8: bytes.Repeat([]byte{'b'}, DefaultBlockSize),
			}}
			acknowledged, ready := journal.Enqueue(generation)
			if err := <-acknowledged; err != nil {
				b.Error(err)
				return
			}
			if err := <-ready; err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(syncs.Load())/float64(b.N), "syncs/op")
}
