package replica

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

func incompressibleBlock(t testing.TB, marker byte) []byte {
	t.Helper()
	block := make([]byte, DefaultBlockSize)
	if _, err := rand.Read(block); err != nil {
		t.Fatal(err)
	}
	block[0] = marker
	block[10] = marker
	block[11] = marker
	return block
}

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

func TestDeviceNotifiesWhenDirtyThresholdIsCrossed(t *testing.T) {
	device, err := OpenDevice(filepath.Join(t.TempDir(), "image"), 8*DefaultBlockSize, 8*DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	notifications := 0
	device.SetDirtyHandler(2*DefaultBlockSize, func() { notifications++ })
	if _, err := device.WriteAt([]byte("first"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := device.WriteAt([]byte("second"), DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if _, err := device.WriteAt([]byte("third"), 2*DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if notifications != 1 {
		t.Fatalf("dirty notifications = %d, want 1", notifications)
	}
	generation := device.Seal()
	device.Release(generation)
	if _, err := device.WriteAt([]byte("again"), 3*DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if _, err := device.WriteAt([]byte("threshold"), 4*DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if notifications != 2 {
		t.Fatalf("dirty notifications after release = %d, want 2", notifications)
	}
}

func TestRemoteFlushUsesRemoteDurabilityInsteadOfLocalImageSync(t *testing.T) {
	device, err := OpenDevice(filepath.Join(t.TempDir(), "image"), 4*DefaultBlockSize, 4*DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	if err := device.file.Close(); err != nil {
		t.Fatal(err)
	}
	called := false
	device.SetRemoteFlushHandler(func(*Generation) <-chan error {
		called = true
		result := make(chan error, 1)
		result <- nil
		return result
	})
	if err := device.Flush(); err != nil {
		t.Fatalf("remote flush used the closed local image: %v", err)
	}
	if !called {
		t.Fatal("remote durability handler was not called")
	}
}

func TestAsyncFlushDoesNotSyncDisposableImage(t *testing.T) {
	device, err := OpenDevice(filepath.Join(t.TempDir(), "image"), 4*DefaultBlockSize, 4*DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := device.WriteAt([]byte("unpublished"), 0); err != nil {
		t.Fatal(err)
	}
	device.SetAsyncFlush()
	if err := device.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := device.Flush(); err != nil {
		t.Fatalf("async flush used the closed disposable image: %v", err)
	}
}

func TestLocalFlushSkipsSyncWithoutNewWrites(t *testing.T) {
	device, err := OpenDevice(filepath.Join(t.TempDir(), "image"), 4*DefaultBlockSize, 4*DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	if err := device.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := device.Flush(); err != nil {
		t.Fatalf("clean flush touched the closed local image: %v", err)
	}
}

type concurrentUploadStore struct {
	objectstore.Store
	segmentStarted  chan struct{}
	manifestStarted chan struct{}
	releaseSegment  chan struct{}
	releaseManifest chan struct{}
	segmentOnce     sync.Once
	manifestOnce    sync.Once
}

type blockingStateStore struct {
	objectstore.Store
	stateStarted chan struct{}
	releaseState chan struct{}
	stateOnce    sync.Once
}

type blockingBundleStore struct {
	objectstore.Store
	bundleStarted chan struct{}
	releaseBundle chan struct{}
	bundleOnce    sync.Once
}

func (s *blockingBundleStore) PutIfAbsent(ctx context.Context, key string, data []byte) (string, error) {
	if strings.Contains(key, "/manifest-bundles/") {
		s.bundleOnce.Do(func() { close(s.bundleStarted) })
		select {
		case <-s.releaseBundle:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.Store.PutIfAbsent(ctx, key, data)
}

func (s *blockingStateStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if strings.HasSuffix(key, "/state.json") {
		s.stateOnce.Do(func() { close(s.stateStarted) })
		select {
		case <-s.releaseState:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.Store.PutIfMatch(ctx, key, data, etag)
}

func (s *concurrentUploadStore) PutIfAbsent(ctx context.Context, key string, data []byte) (string, error) {
	if strings.Contains(key, "/segments/") {
		s.segmentOnce.Do(func() { close(s.segmentStarted) })
		select {
		case <-s.releaseSegment:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if strings.Contains(key, "/manifests/") {
		s.manifestOnce.Do(func() { close(s.manifestStarted) })
		select {
		case <-s.releaseManifest:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.Store.PutIfAbsent(ctx, key, data)
}

func TestPublishUploadsLargeManifestAlongsideSegments(t *testing.T) {
	store := &concurrentUploadStore{
		Store: objectstore.NewMemory(), segmentStarted: make(chan struct{}), manifestStarted: make(chan struct{}),
		releaseSegment: make(chan struct{}), releaseManifest: make(chan struct{}),
	}
	remote := &Remote{Store: store}
	const extentCount = 12_000
	lease, _, err := remote.Acquire(context.Background(), "parallel", "node-a", CreateOptions{Size: 2 * extentCount * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte{'x'}, inlineSegmentLimit+1)
	digest := sha256.Sum256(body)
	extents := make([]Extent, extentCount)
	for index := range extents {
		extents[index] = Extent{Start: uint64(index * 2), Blocks: 1}
	}
	segment := Segment{
		Data: body, SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(body)),
		Blocks: extentCount, Extents: extents,
	}
	done := make(chan error, 1)
	go func() { done <- lease.Publish(context.Background(), segment) }()
	select {
	case <-store.segmentStarted:
	case <-time.After(time.Second):
		t.Fatal("segment upload did not start")
	}
	select {
	case <-store.manifestStarted:
	case <-time.After(time.Second):
		t.Fatal("manifest upload waited for the segment upload")
	}
	close(store.releaseManifest)
	close(store.releaseSegment)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type failingManifestStore struct {
	objectstore.Store
	mu           sync.Mutex
	manifestPuts int
}

func (s *failingManifestStore) PutIfAbsent(ctx context.Context, key string, data []byte) (string, error) {
	if strings.Contains(key, "/manifests/") {
		s.mu.Lock()
		s.manifestPuts++
		s.mu.Unlock()
		return "", errors.New("injected manifest archive failure")
	}
	return s.Store.PutIfAbsent(ctx, key, data)
}

func (s *failingManifestStore) manifestPutCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.manifestPuts
}

func TestInlineManifestMakesArchiveOptionalForRemoteFlush(t *testing.T) {
	ctx := context.Background()
	store := &failingManifestStore{Store: objectstore.NewMemory()}
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "inline-head", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		2: bytes.Repeat([]byte{'i'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatalf("publish with unavailable manifest archive: %v", err)
	}

	state, _, err := remote.Inspect(ctx, "inline-head")
	if err != nil {
		t.Fatal(err)
	}
	if len(state.InlineManifests) != 1 {
		t.Fatalf("inline manifests = %d, want 1", len(state.InlineManifests))
	}
	if got := store.manifestPutCount(); got != 0 {
		t.Fatalf("small publish made %d manifest PUTs, want 0", got)
	}
	if _, err := store.Get(ctx, state.Manifest); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("archived manifest lookup error = %v, want not found", err)
	}

	path := filepath.Join(t.TempDir(), "restored")
	if err := (&Remote{Store: store}).RestoreState(ctx, state, path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if data[2*DefaultBlockSize] != 'i' {
		t.Fatal("restore did not read the manifest embedded in state.json")
	}
}

func TestLargeManifestRequiresImmutableArchive(t *testing.T) {
	ctx := context.Background()
	store := &failingManifestStore{Store: objectstore.NewMemory()}
	remote := &Remote{Store: store}
	const extentCount = 12_000
	lease, _, err := remote.Acquire(ctx, "large-head", "node-a", CreateOptions{Size: 2 * extentCount * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("encoded segment placeholder")
	digest := sha256.Sum256(body)
	extents := make([]Extent, extentCount)
	for index := range extents {
		extents[index] = Extent{Start: uint64(index * 2), Blocks: 1}
	}
	segment := Segment{
		Data: body, SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(body)),
		Blocks: extentCount, Extents: extents,
	}
	if err := lease.Publish(ctx, segment); err == nil {
		t.Fatal("large manifest publish succeeded without its immutable archive")
	}
	if got := store.manifestPutCount(); got != 1 {
		t.Fatalf("large publish made %d manifest PUTs, want 1", got)
	}
	if state := lease.State(); state.Generation != 0 || state.Manifest != "" {
		t.Fatalf("failed large publish advanced state: %+v", state)
	}
}

func TestInlineManifestLimitRollsHistoryIntoBundle(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}
	const extentCount = 100
	lease, _, err := remote.Acquire(ctx, "bounded-head", "node-a", CreateOptions{Size: 2 * extentCount * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("encoded segment placeholder")
	digest := sha256.Sum256(body)
	extents := make([]Extent, extentCount)
	for index := range extents {
		extents[index] = Extent{Start: uint64(index * 2), Blocks: 1}
	}
	segment := Segment{
		Data: body, SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(body)),
		Blocks: extentCount, Extents: extents,
	}
	for range 64 {
		if err := lease.Publish(ctx, segment); err != nil {
			t.Fatal(err)
		}
	}
	state := lease.State()
	if state.ManifestBundle == nil {
		t.Fatalf("manifest bundle is missing after %d generations", state.Generation)
	}
	bundle, err := remote.readManifestBundle(ctx, state, *state.ManifestBundle)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Parent == nil {
		t.Fatal("manifest bundle chain did not retain its parent")
	}
	if got := inlineManifestBytes(state.InlineManifests); got > inlineManifestStateLimit {
		t.Fatalf("inline manifest bytes = %d, limit %d", got, inlineManifestStateLimit)
	}
	history, err := remote.loadHistory(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != int(state.Generation) {
		t.Fatalf("history entries = %d, want %d", len(history), state.Generation)
	}
}

func TestReleaseWaitsForBackgroundManifestBundle(t *testing.T) {
	ctx := context.Background()
	store := &blockingBundleStore{
		Store: objectstore.NewMemory(), bundleStarted: make(chan struct{}), releaseBundle: make(chan struct{}),
	}
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "release-bundle", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		1: incompressibleBlock(t, 'b'),
	}})
	if err != nil {
		t.Fatal(err)
	}

	bundleStarted := false
	for range 16 {
		if err := lease.Publish(ctx, segment); err != nil {
			t.Fatal(err)
		}
		select {
		case <-store.bundleStarted:
			bundleStarted = true
		default:
		}
		if bundleStarted {
			break
		}
	}
	if !bundleStarted {
		t.Fatal("background manifest bundle upload did not start")
	}

	released := make(chan error, 1)
	go func() { released <- lease.Release(ctx) }()
	select {
	case err := <-released:
		t.Fatalf("release returned before bundle upload completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.releaseBundle)
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	state, _, err := remote.Inspect(ctx, "release-bundle")
	if err != nil {
		t.Fatal(err)
	}
	if state.Lease != nil {
		t.Fatal("release left the lease active")
	}
	if state.ManifestBundle == nil {
		t.Fatal("release did not install the completed manifest bundle")
	}
}

func TestManifestBundleAllowsExternalGenerationGap(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}
	const extentCount = 12_000
	lease, _, err := remote.Acquire(ctx, "bundle-gap", "node-a", CreateOptions{Size: 2 * extentCount * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	small, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte{'s'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, small); err != nil {
		t.Fatal(err)
	}
	body := []byte("encoded segment placeholder")
	digest := sha256.Sum256(body)
	extents := make([]Extent, extentCount)
	for index := range extents {
		extents[index] = Extent{Start: uint64(index * 2), Blocks: 1}
	}
	large := Segment{
		Data: body, SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(body)),
		Blocks: extentCount, Extents: extents,
	}
	if err := lease.Publish(ctx, large); err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, small); err != nil {
		t.Fatal(err)
	}
	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	state := lease.State()
	history, err := remote.loadHistory(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 4 {
		t.Fatalf("history entries = %d, want 4", len(history))
	}
}

func TestCheckpointBundlesInlineManifests(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "bundled-head", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	first, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte{'a'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, first); err != nil {
		t.Fatal(err)
	}
	second, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		2: bytes.Repeat([]byte{'b'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, second); err != nil {
		t.Fatal(err)
	}
	if len(lease.State().InlineManifests) != 2 {
		t.Fatal("delta manifests were not retained in state")
	}
	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	state := lease.State()
	if len(state.InlineManifests) != 0 {
		t.Fatalf("checkpoint retained %d inline manifests", len(state.InlineManifests))
	}
	if state.ManifestBundle == nil {
		t.Fatal("checkpoint did not publish a manifest bundle")
	}
	path := filepath.Join(t.TempDir(), "restored")
	if err := remote.RestoreState(ctx, state, path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if data[DefaultBlockSize] != 'a' || data[2*DefaultBlockSize] != 'b' {
		t.Fatal("restore did not read inline segments from the manifest bundle")
	}
}

func TestSyncerNotifyPublishesBeforeInterval(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "urgent", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	device, err := OpenDevice(filepath.Join(t.TempDir(), "image"), 8*DefaultBlockSize, 8*DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	syncer := StartSyncer(device, lease, time.Hour, 0, nil, nil)
	if _, err := device.WriteAt([]byte("urgent"), 0); err != nil {
		t.Fatal(err)
	}
	syncer.Notify()
	deadline := time.Now().Add(time.Second)
	for lease.State().Generation == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := lease.State().Generation; got != 1 {
		t.Fatalf("generation after urgent notification = %d, want 1", got)
	}
	if err := syncer.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSyncerGroupsQueuedFlushGenerations(t *testing.T) {
	ctx := context.Background()
	store := &blockingStateStore{
		Store: objectstore.NewMemory(), stateStarted: make(chan struct{}), releaseState: make(chan struct{}),
	}
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "grouped", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	device, err := OpenDevice(filepath.Join(t.TempDir(), "image"), 8*DefaultBlockSize, 8*DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	syncer := StartSyncer(device, lease, time.Hour, 0, nil, nil)
	device.SetRemoteFlushHandler(syncer.EnqueueGeneration)

	results := make([]<-chan error, 0, 3)
	for block, value := range []byte{'a', 'b', 'c'} {
		if _, err := device.WriteAt(bytes.Repeat([]byte{value}, DefaultBlockSize), int64(block*DefaultBlockSize)); err != nil {
			t.Fatal(err)
		}
		result, err := device.BeginRemoteFlush()
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, result)
		if block == 0 {
			select {
			case <-store.stateStarted:
			case <-time.After(time.Second):
				t.Fatal("first flush did not start publishing")
			}
		}
	}
	close(store.releaseState)
	for _, result := range results {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	if got := lease.State().Generation; got != 2 {
		t.Fatalf("published generations = %d, want 2", got)
	}
	if err := syncer.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	if err := remote.RestoreState(ctx, lease.State(), restored); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(restored)
	if err != nil {
		t.Fatal(err)
	}
	for block, want := range []byte{'a', 'b', 'c'} {
		if got := data[block*DefaultBlockSize]; got != want {
			t.Fatalf("restored block %d = %q, want %q", block, got, want)
		}
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

func TestSegmentMetadataAllowsZeroRunAcrossEncodedExtents(t *testing.T) {
	generation := &Generation{Blocks: make(map[uint64][]byte, 300)}
	for block := range uint64(300) {
		generation.Blocks[block] = make([]byte, DefaultBlockSize)
	}
	segment, err := EncodeSegment(generation)
	if err != nil {
		t.Fatal(err)
	}
	if len(segment.Extents) != 2 || len(segment.ZeroExtents) != 1 {
		t.Fatalf("segment extents = %v zero extents = %v", segment.Extents, segment.ZeroExtents)
	}
	remote := &Remote{Store: objectstore.NewMemory()}
	lease, _, err := remote.Acquire(context.Background(), "zeros", "node-a", CreateOptions{Size: 512 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(context.Background(), segment); err != nil {
		t.Fatal(err)
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

func TestPublishRenewsLease(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	remote := &Remote{Store: objectstore.NewMemory(), TTL: 30 * time.Second, Now: func() time.Time { return now }}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(20 * time.Second)
	segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{0: bytes.Repeat([]byte("x"), DefaultBlockSize)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatal(err)
	}
	want := now.Add(30 * time.Second)
	if got := lease.State().Lease.ExpiresAt; !got.Equal(want) {
		t.Fatalf("lease expiry after publish = %s, want %s", got, want)
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

	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	state := lease.State()
	if state.Generation != 3 || state.Checkpoint != 3 {
		t.Fatalf("state after checkpoint = %+v", state)
	}
	checkpoint, err := remote.readManifest(ctx, state, state.Manifest, 3)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Kind != "checkpoint" || checkpoint.Parent != secondManifest {
		t.Fatalf("checkpoint manifest = %+v", checkpoint)
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

type countingStore struct {
	objectstore.Store
	mu          sync.Mutex
	gets        map[string]int
	failSegment bool
}

func (s *countingStore) Get(ctx context.Context, key string) (objectstore.Object, error) {
	s.mu.Lock()
	if s.gets == nil {
		s.gets = make(map[string]int)
	}
	s.gets[key]++
	fail := strings.Contains(key, "/segments/") && s.failSegment
	if fail {
		s.failSegment = false
	}
	s.mu.Unlock()
	if fail {
		return objectstore.Object{}, errors.New("temporary segment read failure")
	}
	return s.Store.Get(ctx, key)
}

func (s *countingStore) reset() {
	s.mu.Lock()
	s.gets = make(map[string]int)
	s.mu.Unlock()
}

func (s *countingStore) failNextSegment() {
	s.mu.Lock()
	s.failSegment = true
	s.mu.Unlock()
}

func (s *countingStore) segmentGets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int
	for key, count := range s.gets {
		if strings.Contains(key, "/segments/") {
			total += count
		}
	}
	return total
}

func (s *countingStore) manifestGets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int
	for key, count := range s.gets {
		if strings.Contains(key, "/manifests/") {
			total += count
		}
	}
	return total
}

func TestMetadataCheckpointReusesSegments(t *testing.T) {
	ctx := context.Background()
	store := &countingStore{Store: objectstore.NewMemory()}
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	first, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte{'a'}, DefaultBlockSize),
		2: bytes.Repeat([]byte{'b'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, first); err != nil {
		t.Fatal(err)
	}
	second, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte{'c'}, DefaultBlockSize),
		2: make([]byte, DefaultBlockSize),
		3: bytes.Repeat([]byte{'d'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, second); err != nil {
		t.Fatal(err)
	}
	before, err := store.List(ctx, VolumePrefix("data")+"segments/")
	if err != nil {
		t.Fatal(err)
	}
	store.reset()
	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if got := store.manifestGets(); got != 0 {
		t.Fatalf("cached checkpoint fetched %d manifests", got)
	}
	after, err := store.List(ctx, VolumePrefix("data")+"segments/")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("checkpoint uploaded segments: before=%d after=%d", len(before), len(after))
	}
	state := lease.State()
	if state.Generation != 3 || state.Checkpoint != 3 {
		t.Fatalf("state after checkpoint = %+v", state)
	}
	path := filepath.Join(t.TempDir(), "restored")
	if err := remote.RestoreState(ctx, state, path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if data[DefaultBlockSize] != 'c' || data[2*DefaultBlockSize] != 0 || data[3*DefaultBlockSize] != 'd' {
		t.Fatal("metadata checkpoint restored incorrect data")
	}
}

func TestSmallSegmentIsStoredInManifest(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "inline", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		2: bytes.Repeat([]byte{'i'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatal(err)
	}
	segmentKeys, err := store.List(ctx, VolumePrefix("inline")+"segments/")
	if err != nil {
		t.Fatal(err)
	}
	if len(segmentKeys) != 0 {
		t.Fatalf("small generation uploaded %d segment objects", len(segmentKeys))
	}
	deltaKey := lease.State().Manifest
	delta, err := remote.readManifest(ctx, lease.State(), deltaKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Segments) != 1 || len(delta.Segments[0].InlineData) == 0 {
		t.Fatal("small segment was not embedded in its manifest")
	}
	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := remote.readManifest(ctx, lease.State(), lease.State().Manifest, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoint.Segments) != 1 || checkpoint.Segments[0].SourceManifest != deltaKey || len(checkpoint.Segments[0].InlineData) != 0 {
		t.Fatalf("checkpoint reference = %+v", checkpoint.Segments)
	}
	path := filepath.Join(t.TempDir(), "restored")
	if err := remote.RestoreState(ctx, lease.State(), path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if data[2*DefaultBlockSize] != 'i' {
		t.Fatal("inline segment did not survive checkpoint restore")
	}
}

func TestLeaseLazyRestoreSeedsCheckpointPlan(t *testing.T) {
	ctx := context.Background()
	store := &countingStore{Store: objectstore.NewMemory()}
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte{'a'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}

	restoredLease, _, err := remote.Acquire(ctx, "data", "node-b", CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lazy, err := restoredLease.PrepareLazyRestore(ctx, filepath.Join(t.TempDir(), "lazy"))
	if err != nil {
		t.Fatal(err)
	}
	defer lazy.Close()
	store.reset()
	if err := restoredLease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if got := store.manifestGets(); got != 0 {
		t.Fatalf("checkpoint after lazy restore fetched %d manifests", got)
	}
}

func TestLazyRestoreFetchesOnReadAndPreservesOverwrites(t *testing.T) {
	ctx := context.Background()
	store := &countingStore{Store: objectstore.NewMemory()}
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 32 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	blocks := make(map[uint64][]byte, 20)
	for block := uint64(8); block < 28; block++ {
		blocks[block] = incompressibleBlock(t, byte(block))
	}
	blocks[1] = incompressibleBlock(t, 'a')
	blocks[3] = incompressibleBlock(t, 'b')
	blocks[7] = incompressibleBlock(t, 'x')
	segment, err := EncodeSegment(&Generation{Blocks: blocks})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "lazy")
	lazy, err := remote.PrepareLazyRestore(ctx, lease.State(), path)
	if err != nil {
		t.Fatal(err)
	}
	device, err := OpenDevice(path, 32*DefaultBlockSize, 32*DefaultBlockSize)
	if err != nil {
		lazy.Close()
		t.Fatal(err)
	}
	device.SetLazyImage(lazy)
	defer device.Close()
	store.reset()

	local := bytes.Repeat([]byte{'x'}, DefaultBlockSize)
	if _, err := device.WriteAt(local, DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if got := store.segmentGets(); got != 0 {
		t.Fatalf("full-block overwrite fetched %d segments", got)
	}
	read := make([]byte, DefaultBlockSize)
	store.failNextSegment()
	if _, err := device.ReadAt(read, 3*DefaultBlockSize); err == nil {
		t.Fatal("temporary segment failure was not returned")
	}
	if _, err := device.ReadAt(read, 3*DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if read[0] != 'b' {
		t.Fatalf("lazy block starts with %q", read[0])
	}
	if got := store.segmentGets(); got != 2 {
		t.Fatalf("lazy read attempts = %d, want 2", got)
	}
	if _, err := device.ReadAt(read, DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read, local) {
		t.Fatal("segment hydration overwrote a local write")
	}
	if _, err := device.ReadAt(read, 2*DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if !isZero(read) || store.segmentGets() != 2 {
		t.Fatalf("sparse block zero=%v segment GETs=%d, want true and 2", isZero(read), store.segmentGets())
	}
	generation := device.Seal()
	segments, err := EncodeSegments(generation, DefaultSegmentBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, segments...); err != nil {
		t.Fatal(err)
	}
	device.Release(generation)
	materialized := filepath.Join(t.TempDir(), "materialized")
	if err := remote.RestoreState(ctx, lease.State(), materialized); err != nil {
		t.Fatal(err)
	}
	materializedData, err := os.ReadFile(materialized)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(materializedData[DefaultBlockSize:2*DefaultBlockSize], local) || materializedData[3*DefaultBlockSize] != 'b' {
		t.Fatal("lazy mount changes did not publish correctly")
	}

	secondPath := filepath.Join(t.TempDir(), "lazy-partial")
	secondLazy, err := remote.PrepareLazyRestore(ctx, lease.State(), secondPath)
	if err != nil {
		t.Fatal(err)
	}
	secondDevice, err := OpenDevice(secondPath, 32*DefaultBlockSize, 32*DefaultBlockSize)
	if err != nil {
		secondLazy.Close()
		t.Fatal(err)
	}
	secondDevice.SetLazyImage(secondLazy)
	defer secondDevice.Close()
	store.reset()
	if _, err := secondDevice.WriteAt([]byte{'z'}, 3*DefaultBlockSize+10); err != nil {
		t.Fatal(err)
	}
	if got := store.segmentGets(); got != 1 {
		t.Fatalf("partial-block overwrite fetched %d segments, want 1", got)
	}
	if _, err := secondDevice.ReadAt(read, 3*DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if read[0] != 'b' || read[10] != 'z' || read[11] != 'b' {
		t.Fatal("partial-block overwrite did not preserve remote bytes")
	}
}

func TestGarbageCollectionRetainsHistoryThenSweeps(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	remote := &Remote{Store: store, Now: func() time.Time { return now }}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 32 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	publish := func(block uint64, value byte) {
		t.Helper()
		blocks := make(map[uint64][]byte, 21)
		for current := uint64(8); current < 28; current++ {
			blocks[current] = incompressibleBlock(t, value)
		}
		blocks[block] = bytes.Repeat([]byte{value}, DefaultBlockSize)
		segment, err := EncodeSegment(&Generation{Blocks: blocks})
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Publish(ctx, segment); err != nil {
			t.Fatal(err)
		}
	}
	publish(1, 'a')
	now = now.Add(time.Minute)
	publish(2, 'b')

	now = now.Add(time.Minute)
	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	checkpointState := lease.State()
	if checkpointState.ManifestBundle == nil {
		t.Fatal("checkpoint did not publish a manifest bundle")
	}
	firstBundle := checkpointState.ManifestBundle.Key

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
	if _, err := store.Get(ctx, firstBundle); err != nil {
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
	if _, err := store.Get(ctx, firstBundle); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("old manifest bundle still exists: %v", err)
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

func TestGarbageCollectionRetainsBundleUsedByCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	remote := &Remote{Store: store, Now: func() time.Time { return now }}
	lease, _, err := remote.Acquire(ctx, "inline-gc", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte{'a'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatal(err)
	}
	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	state := lease.State()
	if state.ManifestBundle == nil {
		t.Fatal("checkpoint did not bundle its inline source")
	}
	bundleKey := state.ManifestBundle.Key

	now = now.Add(8 * 24 * time.Hour)
	later, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
		2: bytes.Repeat([]byte{'b'}, DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, later); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := lease.CollectGarbage(ctx, GCOptions{HistoryRetention: 0, GracePeriod: 0}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Get(ctx, bundleKey); err != nil {
		t.Fatalf("garbage collection removed a live source bundle: %v", err)
	}
	path := filepath.Join(t.TempDir(), "restored")
	if err := remote.RestoreState(ctx, lease.State(), path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if data[DefaultBlockSize] != 'a' {
		t.Fatal("restore lost data sourced from a retained manifest bundle")
	}
}

type flakyStateStore struct {
	objectstore.Store
	mu        sync.Mutex
	failsLeft int
	failErr   error
	attempts  int
}

func (s *flakyStateStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if strings.HasSuffix(key, "/state.json") {
		s.mu.Lock()
		s.attempts++
		if s.failsLeft > 0 {
			s.failsLeft--
			err := s.failErr
			s.mu.Unlock()
			return "", err
		}
		s.mu.Unlock()
	}
	return s.Store.PutIfMatch(ctx, key, data, etag)
}

func (s *flakyStateStore) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func newFlushingSyncer(t *testing.T, store objectstore.Store, onLost func(error)) (*Device, *Syncer) {
	t.Helper()
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(context.Background(), "stall", "node-a", CreateOptions{Size: 16 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	device, err := OpenDevice(filepath.Join(t.TempDir(), "image"), 16*DefaultBlockSize, 16*DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	syncer := StartSyncer(device, lease, time.Hour, 0, nil, onLost)
	device.SetRemoteFlushHandler(syncer.EnqueueGeneration)
	return device, syncer
}

func TestRemoteFlushStallsThroughTransientPublishFailures(t *testing.T) {
	store := &flakyStateStore{Store: objectstore.NewMemory(), failsLeft: 3, failErr: errors.New("dial tcp 127.0.0.1:9000: connect: connection refused")}
	fenced := make(chan error, 1)
	device, syncer := newFlushingSyncer(t, store, func(err error) { fenced <- err })
	defer syncer.Abandon()
	defer device.Close()

	if _, err := device.WriteAt(incompressibleBlock(t, 'a'), 0); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- device.Flush() }()

	select {
	case err := <-done:
		t.Fatalf("flush returned during transient S3 outage instead of stalling: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("flush failed after S3 recovered: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush never completed after transient failures cleared")
	}

	select {
	case err := <-fenced:
		t.Fatalf("volume fenced on transient failure: %v", err)
	default:
	}
	if got := store.attemptCount(); got < 4 {
		t.Fatalf("expected the publish to be retried, saw %d state attempts", got)
	}
}

func TestRemoteFlushFailsImmediatelyOnLeaseTakeover(t *testing.T) {
	store := &flakyStateStore{Store: objectstore.NewMemory(), failsLeft: 1, failErr: objectstore.ErrPreconditionFailed}
	fenced := make(chan error, 1)
	device, syncer := newFlushingSyncer(t, store, func(err error) { fenced <- err })
	defer syncer.Abandon()
	defer device.Close()

	if _, err := device.WriteAt(incompressibleBlock(t, 'b'), 0); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- device.Flush() }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("flush error = %v, want ErrLeaseLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not fail promptly on lease takeover")
	}

	select {
	case err := <-fenced:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("fence cause = %v, want ErrLeaseLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lease takeover did not fence the volume")
	}
	if got := store.attemptCount(); got != 1 {
		t.Fatalf("takeover should not retry, saw %d state attempts", got)
	}
}

type lostAckStateStore struct {
	objectstore.Store
	mu          sync.Mutex
	dropsLeft   int
	getFailures int
}

func (s *lostAckStateStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (string, error) {
	newETag, err := s.Store.PutIfMatch(ctx, key, data, etag)
	if err != nil || !strings.HasSuffix(key, "/state.json") {
		return newETag, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dropsLeft > 0 {
		s.dropsLeft--
		s.getFailures++
		return "", errors.New("write state: connection reset while awaiting response")
	}
	return newETag, nil
}

func (s *lostAckStateStore) Get(ctx context.Context, key string) (objectstore.Object, error) {
	if strings.HasSuffix(key, "/state.json") {
		s.mu.Lock()
		if s.getFailures > 0 {
			s.getFailures--
			s.mu.Unlock()
			return objectstore.Object{}, errors.New("read state: connection reset")
		}
		s.mu.Unlock()
	}
	return s.Store.Get(ctx, key)
}

func TestPublishRetryAdoptsCommitAfterLostAck(t *testing.T) {
	ctx := context.Background()
	store := &lostAckStateStore{Store: objectstore.NewMemory(), dropsLeft: 1}
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	remote := &Remote{Store: store, TTL: 30 * time.Second, Now: func() time.Time { return now }}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	first := incompressibleBlock(t, 'a')
	segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{1: first}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, segment); err == nil {
		t.Fatal("first publish attempt should surface the lost ack")
	}
	now = now.Add(5 * time.Second)
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatalf("retried publish error = %v, want success", err)
	}
	if got := lease.State().Generation; got != 1 {
		t.Fatalf("lease generation = %d, want 1", got)
	}
	state, _, err := remote.Inspect(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 1 {
		t.Fatalf("remote generation = %d, want exactly one publish", state.Generation)
	}
	second := incompressibleBlock(t, 'b')
	next, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{2: second}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, next); err != nil {
		t.Fatalf("publish after adopted commit error = %v", err)
	}
	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatalf("checkpoint after adopted commit error = %v", err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if _, err := remote.Restore(ctx, "data", restored); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(restored)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data[DefaultBlockSize:2*DefaultBlockSize], first) {
		t.Fatal("block 1 did not round-trip through the adopted commit")
	}
	if !bytes.Equal(data[2*DefaultBlockSize:3*DefaultBlockSize], second) {
		t.Fatal("block 2 did not round-trip after the adopted commit")
	}
}

func TestPublishRetryAfterLostAckDetectsTakeover(t *testing.T) {
	ctx := context.Background()
	store := &lostAckStateStore{Store: objectstore.NewMemory(), dropsLeft: 1}
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	remote := &Remote{Store: store, TTL: 30 * time.Second, Now: func() time.Time { return now }}
	first, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{1: incompressibleBlock(t, 'a')}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Publish(ctx, segment); err == nil {
		t.Fatal("first publish attempt should surface the lost ack")
	}
	now = now.Add(time.Minute)
	second, created, err := remote.Acquire(ctx, "data", "node-b", CreateOptions{})
	if err != nil || created {
		t.Fatalf("takeover: created=%v err=%v", created, err)
	}
	if err := first.Publish(ctx, segment); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("retried publish after takeover = %v, want ErrLeaseLost", err)
	}
	state, _, err := remote.Inspect(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if state.Lease == nil || state.Lease.Token != second.State().Lease.Token {
		t.Fatal("takeover lease was clobbered by the fenced retry")
	}
}

func TestRemoteFlushRecoversFromLostPublishAck(t *testing.T) {
	store := &lostAckStateStore{Store: objectstore.NewMemory(), dropsLeft: 1}
	fenced := make(chan error, 1)
	device, syncer := newFlushingSyncer(t, store, func(err error) { fenced <- err })
	defer syncer.Abandon()
	defer device.Close()

	if _, err := device.WriteAt(incompressibleBlock(t, 'c'), 0); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- device.Flush() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("flush after lost ack = %v, want success", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush never completed after the lost ack")
	}

	select {
	case err := <-fenced:
		t.Fatalf("volume fenced on a lost ack: %v", err)
	default:
	}
	if got := syncer.lease.State().Generation; got != 1 {
		t.Fatalf("generation = %d, want exactly one publish", got)
	}
}

func largeExternalSegment(t *testing.T, extentCount int) Segment {
	t.Helper()
	body := []byte("encoded segment placeholder")
	digest := sha256.Sum256(body)
	extents := make([]Extent, extentCount)
	for index := range extents {
		extents[index] = Extent{Start: uint64(index * 2), Blocks: 1}
	}
	return Segment{
		Data: body, SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(body)),
		Blocks: int64(extentCount), Extents: extents,
	}
}

func tornGCVolume(t *testing.T) (*Remote, *objectstore.Memory, *Lease, string, string) {
	t.Helper()
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}
	const extentCount = 12_000
	lease, _, err := remote.Acquire(ctx, "torn-gc", "node-a", CreateOptions{Size: 2 * extentCount * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	segment := largeExternalSegment(t, extentCount)
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatal(err)
	}
	expired := lease.State().Manifest
	if !strings.Contains(expired, "/manifests/") {
		t.Fatalf("generation one manifest is not external: %q", expired)
	}
	for range 2 {
		if err := lease.Publish(ctx, segment); err != nil {
			t.Fatal(err)
		}
	}
	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	checkpoint := lease.State().Manifest
	if !strings.Contains(checkpoint, "/manifests/") {
		t.Fatalf("checkpoint manifest is not external: %q", checkpoint)
	}
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatal(err)
	}
	return remote, store, lease, expired, checkpoint
}

func TestHistoryToleratesManifestsRemovedByInterruptedGC(t *testing.T) {
	ctx := context.Background()
	remote, store, lease, expired, _ := tornGCVolume(t)
	if err := store.Delete(ctx, expired); err != nil {
		t.Fatal(err)
	}
	history, err := remote.loadHistory(ctx, lease.State())
	if err != nil {
		t.Fatalf("history walk failed on a gap behind the checkpoint: %v", err)
	}
	if len(history) != 4 {
		t.Fatalf("history entries = %d, want 4 (walk truncated at the interrupted-sweep gap)", len(history))
	}
	if history[len(history)-1].manifest.Kind == "checkpoint" {
		t.Fatal("the gap must sit below surviving deltas, not directly under the checkpoint")
	}
	if _, err := lease.CollectGarbage(ctx, GCOptions{}); err != nil {
		t.Fatalf("garbage collection stayed wedged after an interrupted sweep: %v", err)
	}
}

func TestHistoryFailsOnMissingManifestAboveNewestCheckpoint(t *testing.T) {
	ctx := context.Background()
	remote, store, lease, _, checkpoint := tornGCVolume(t)
	if err := store.Delete(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.loadHistory(ctx, lease.State()); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("missing restorable-chain manifest must fail the walk, got: %v", err)
	}
}
