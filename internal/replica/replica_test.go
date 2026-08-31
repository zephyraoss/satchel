package replica

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

func TestDeviceSealsImmutableGenerations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "image")
	d, err := OpenDevice(path, 4*DefaultBlockSize, 16*DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.WriteAt([]byte("first"), 0); err != nil {
		t.Fatal(err)
	}
	first := d.Seal()
	if _, err := d.WriteAt([]byte("later"), 0); err != nil {
		t.Fatal(err)
	}
	if got := string(first.Blocks[0][:5]); got != "first" {
		t.Fatalf("sealed block changed to %q", got)
	}
	if got, want := d.DirtyBytes(), int64(2*DefaultBlockSize); got != want {
		t.Fatalf("dirty bytes = %d, want %d", got, want)
	}
	d.Release(first)
	if got, want := d.DirtyBytes(), int64(DefaultBlockSize); got != want {
		t.Fatalf("dirty bytes after release = %d, want %d", got, want)
	}
}

func TestDeviceReportsBackpressure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "image")
	device, err := OpenDevice(path, 4*DefaultBlockSize, DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()

	blocked := make(chan struct{}, 1)
	device.SetBackpressureHandler(func() { blocked <- struct{}{} })
	if _, err := device.WriteAt([]byte("first"), 0); err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := device.WriteAt([]byte("second"), DefaultBlockSize)
		writeDone <- err
	}()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("second write did not report backpressure")
	}

	generation := device.Seal()
	device.Release(generation)
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second write did not resume after releasing the generation")
	}
}

func TestSegmentRoundTripAndChecksum(t *testing.T) {
	g := &Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte{1}, DefaultBlockSize),
		2: bytes.Repeat([]byte{2}, DefaultBlockSize),
		7: bytes.Repeat([]byte{7}, DefaultBlockSize),
	}}
	segment, err := EncodeSegment(g)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "image")
	if err := os.WriteFile(path, make([]byte, 10*DefaultBlockSize), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ApplySegment(path, 10*DefaultBlockSize, segment); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for block, want := range g.Blocks {
		off := int(block) * DefaultBlockSize
		if !bytes.Equal(data[off:off+DefaultBlockSize], want) {
			t.Fatalf("block %d did not round trip", block)
		}
	}
	segment.Data[len(segment.Data)/2] ^= 1
	if err := ApplySegment(path, 10*DefaultBlockSize, segment); err == nil {
		t.Fatal("corrupt segment was accepted")
	}
}

func TestPublishRestoreAndFenceStaleWriter(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	remote := &Remote{Store: store, TTL: 30 * time.Second, Now: func() time.Time { return now }}
	first, created, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize, Filesystem: "ext4"})
	if err != nil || !created {
		t.Fatalf("first acquire: created=%v err=%v", created, err)
	}
	segA, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{1: bytes.Repeat([]byte("a"), DefaultBlockSize)}})
	if err := first.Publish(ctx, segA); err != nil {
		t.Fatal(err)
	}

	now = now.Add(time.Minute)
	second, created, err := remote.Acquire(ctx, "data", "node-b", CreateOptions{})
	if err != nil || created {
		t.Fatalf("takeover: created=%v err=%v", created, err)
	}
	if got := second.State().Lease.Epoch; got != 2 {
		t.Fatalf("lease epoch = %d, want 2", got)
	}
	stale, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{2: bytes.Repeat([]byte("x"), DefaultBlockSize)}})
	if err := first.Publish(ctx, stale); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale publish error = %v", err)
	}
	segB, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{2: bytes.Repeat([]byte("b"), DefaultBlockSize)}})
	if err := second.Publish(ctx, segB); err != nil {
		t.Fatal(err)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	state, err := remote.Restore(ctx, "data", restored)
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 2 {
		t.Fatalf("generation = %d, want 2", state.Generation)
	}
	data, err := os.ReadFile(restored)
	if err != nil {
		t.Fatal(err)
	}
	if got := data[DefaultBlockSize]; got != 'a' {
		t.Fatalf("block 1 starts with %q", got)
	}
	if got := data[2*DefaultBlockSize]; got != 'b' {
		t.Fatalf("block 2 starts with %q", got)
	}
}

func TestReleasePreservesLeaseEpoch(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	next, _, err := remote.Acquire(ctx, "data", "node-b", CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := next.State().Lease.Epoch; got != 2 {
		t.Fatalf("lease epoch = %d, want 2", got)
	}
}

func TestBreakCannotClearAReplacementLease(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	remote := &Remote{Store: store, TTL: time.Minute, Now: func() time.Time { return now }}
	first, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	firstToken := first.State().Lease.Token
	now = now.Add(2 * time.Minute)
	second, _, err := remote.Acquire(ctx, "data", "node-b", CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Break(ctx, "data", firstToken); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("break stale lease error = %v", err)
	}
	state, _, err := remote.Inspect(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if state.Lease == nil || state.Lease.Token != second.State().Lease.Token {
		t.Fatal("replacement lease was cleared")
	}
}

type blockingRenewStore struct {
	objectstore.Store
	started chan struct{}
}

func (s *blockingRenewStore) PutIfMatch(ctx context.Context, _ string, _ []byte, _ string) (string, error) {
	s.started <- struct{}{}
	<-ctx.Done()
	return "", ctx.Err()
}

func TestCancelHeartbeatDoesNotReportLeaseLoss(t *testing.T) {
	store := &blockingRenewStore{Store: objectstore.NewMemory(), started: make(chan struct{}, 1)}
	remote := &Remote{Store: store, TTL: 30 * time.Millisecond}
	lease, _, err := remote.Acquire(context.Background(), "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lost := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		lease.Heartbeat(ctx, func(err error) { lost <- err })
		close(done)
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not start a renewal")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not stop after cancellation")
	}
	select {
	case err := <-lost:
		t.Fatalf("cancellation reported lease loss: %v", err)
	default:
	}
}

func TestExistingVolumeGeometryCannotChange(t *testing.T) {
	ctx := context.Background()
	remote := &Remote{Store: objectstore.NewMemory()}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize, Filesystem: "ext4"})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := remote.Acquire(ctx, "data", "node-b", CreateOptions{Size: 9 * DefaultBlockSize, Filesystem: "ext4"}); err == nil {
		t.Fatal("changed volume size was accepted")
	}
}

func TestDeleteMarksStateBeforeRemovingObjects(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := remote.Delete(ctx, "data"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := remote.Inspect(ctx, "data"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("inspect after delete = %v", err)
	}
}

func TestCheckpointEndsRestoreChain(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	remote := &Remote{Store: store, Now: func() time.Time { return now }}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{1: bytes.Repeat([]byte("a"), DefaultBlockSize)}})
	if err := lease.Publish(ctx, first); err != nil {
		t.Fatal(err)
	}
	firstTime := now
	now = now.Add(time.Minute)
	second, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{2: bytes.Repeat([]byte("b"), DefaultBlockSize)}})
	if err := lease.Publish(ctx, second); err != nil {
		t.Fatal(err)
	}
	secondManifest := lease.State().Manifest
	now = now.Add(time.Minute)

	checkpoint, err := lease.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	full, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte("a"), DefaultBlockSize),
		2: bytes.Repeat([]byte("b"), DefaultBlockSize),
	}})
	if err := checkpoint.Add(ctx, full); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	state := lease.State()
	if state.Generation != 3 || state.Checkpoint != 3 {
		t.Fatalf("state after checkpoint = %+v", state)
	}
	manifestObject, err := store.Get(ctx, state.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestObject.Data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Kind != "checkpoint" || manifest.Parent != secondManifest {
		t.Fatalf("checkpoint manifest = %+v", manifest)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	if err := remote.RestoreState(ctx, state, restored); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(restored)
	if err != nil {
		t.Fatal(err)
	}
	if data[DefaultBlockSize] != 'a' || data[2*DefaultBlockSize] != 'b' {
		t.Fatal("checkpoint did not restore the current image")
	}

	for name, opts := range map[string]RestoreOptions{
		"generation": {Generation: 1},
		"timestamp":  {Timestamp: firstTime},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "historic")
			historic, err := remote.RestoreWithOptions(ctx, "data", path, opts)
			if err != nil {
				t.Fatal(err)
			}
			if historic.Generation != 1 {
				t.Fatalf("restored generation = %d, want 1", historic.Generation)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if data[DefaultBlockSize] != 'a' || data[2*DefaultBlockSize] != 0 {
				t.Fatal("historic restore did not select generation one")
			}
		})
	}
}

func TestGarbageCollectionRetainsHistoryThenSweeps(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	remote := &Remote{Store: store, Now: func() time.Time { return now }}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	publish := func(block uint64, value byte) {
		t.Helper()
		segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{block: bytes.Repeat([]byte{value}, DefaultBlockSize)}})
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Publish(ctx, segment); err != nil {
			t.Fatal(err)
		}
	}
	publish(1, 'a')
	firstManifest := lease.State().Manifest
	now = now.Add(time.Minute)
	publish(2, 'b')

	checkpoint, err := lease.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	full, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte{'a'}, DefaultBlockSize),
		2: bytes.Repeat([]byte{'b'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoint.Add(ctx, full); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := checkpoint.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	now = now.Add(8 * 24 * time.Hour)
	publish(3, 'c')
	gcOpts := GCOptions{HistoryRetention: 7 * 24 * time.Hour, GracePeriod: time.Hour}
	result, err := lease.CollectGarbage(ctx, gcOpts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Marked == 0 || result.Deleted != 0 {
		t.Fatalf("first collection = %+v", result)
	}
	if _, err := store.Get(ctx, firstManifest); err != nil {
		t.Fatalf("history was removed before the grace period: %v", err)
	}

	now = now.Add(2 * time.Hour)
	result, err = lease.CollectGarbage(ctx, gcOpts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted == 0 {
		t.Fatalf("second collection = %+v", result)
	}
	if _, err := store.Get(ctx, firstManifest); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("old manifest still exists: %v", err)
	}
	if _, err := remote.RestoreWithOptions(ctx, "data", filepath.Join(t.TempDir(), "old"), RestoreOptions{Generation: 1}); err == nil {
		t.Fatal("pruned generation was still restorable")
	}
	latestPath := filepath.Join(t.TempDir(), "latest")
	if _, err := remote.Restore(ctx, "data", latestPath); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(latestPath)
	if err != nil {
		t.Fatal(err)
	}
	if data[DefaultBlockSize] != 'a' || data[2*DefaultBlockSize] != 'b' || data[3*DefaultBlockSize] != 'c' {
		t.Fatal("garbage collection damaged the current generation")
	}
}
