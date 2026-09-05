package plugin

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/go-plugins-helpers/volume"

	"github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/replica"
)

type fakeBackend struct {
	device *replica.Device
}

func (b *fakeBackend) Format(_ context.Context, imagePath, _ string) error {
	file, err := os.OpenFile(imagePath, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteAt([]byte("formatted"), 0)
	return err
}

func (b *fakeBackend) Mount(_ context.Context, device *replica.Device, mountpoint string, _ backend.MountOptions) (backend.Unmounter, error) {
	b.device = device
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return nil, err
	}
	return fakeUnmount{}, nil
}

type fakeUnmount struct{}

func (fakeUnmount) Unmount(context.Context) error { return nil }
func (fakeUnmount) Abandon() error                { return nil }

type blockingManifestStore struct {
	objectstore.Store
	block   atomic.Bool
	started chan struct{}
	release chan struct{}
}

type countingListStore struct {
	objectstore.Store
	lists atomic.Int64
}

type getHookStore struct {
	objectstore.Store
	mu   sync.Mutex
	hook func() error
}

func (s *countingListStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.lists.Add(1)
	return s.Store.List(ctx, prefix)
}

func (s *getHookStore) Get(ctx context.Context, key string) (objectstore.Object, error) {
	object, err := s.Store.Get(ctx, key)
	if err != nil || key != replica.StateKey("data") {
		return object, err
	}
	s.mu.Lock()
	hook := s.hook
	s.hook = nil
	s.mu.Unlock()
	if hook != nil {
		if err := hook(); err != nil {
			return objectstore.Object{}, err
		}
	}
	return object, nil
}

func (s *blockingManifestStore) PutIfAbsent(ctx context.Context, key string, data []byte) (string, error) {
	checkpoint := strings.Contains(key, "/manifests/") && bytes.Contains(data, []byte(`"kind":"checkpoint"`))
	if checkpoint && s.block.CompareAndSwap(true, false) {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.Store.PutIfAbsent(ctx, key, data)
}

func newTestDriver(t *testing.T, store objectstore.Store, node string) (*Driver, *fakeBackend) {
	t.Helper()
	return newTestDriverAt(t, store, node, t.TempDir(), nil)
}

func newTestDriverAt(t *testing.T, store objectstore.Store, node, stateDir string, now func() time.Time) (*Driver, *fakeBackend) {
	t.Helper()
	be := &fakeBackend{}
	driver, err := New(
		Config{NodeID: node, StateDir: stateDir, MaxDirty: 1 << 20, CheckpointInterval: 1},
		&replica.Remote{Store: store, TTL: time.Minute, Now: now}, be,
	)
	if err != nil {
		t.Fatal(err)
	}
	return driver, be
}

func TestDriverUsesConfiguredJournalDirectory(t *testing.T) {
	stateDir := t.TempDir()
	journalDir := filepath.Join(t.TempDir(), "fast-journals")
	driver, err := New(
		Config{NodeID: "node-a", StateDir: stateDir, JournalDir: journalDir},
		&replica.Remote{Store: objectstore.NewMemory()},
		&fakeBackend{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := driver.journalPath("data"), filepath.Join(journalDir, "data.journal"); got != want {
		t.Fatalf("journal path = %q, want %q", got, want)
	}
	if info, err := os.Stat(journalDir); err != nil || !info.IsDir() {
		t.Fatalf("configured journal directory was not created: %v", err)
	}
}

func crashTestMount(t *testing.T, driver *Driver, name string) {
	t.Helper()
	state := driver.volumes[name]
	state.mu.Lock()
	defer state.mu.Unlock()
	mount := state.mounts[name]
	mount.syncer.Abandon()
	mount.stopBeat()
	_ = mount.unmounter.Abandon()
	_ = mount.device.Close()
	if err := mount.journal.Close(false); err != nil {
		t.Fatal(err)
	}
	mount.active = false
}

func TestLocalJournalRecoversAfterSatchelCrash(t *testing.T) {
	store := objectstore.NewMemory()
	stateDir := t.TempDir()
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	driver, backend := newTestDriverAt(t, store, "node-a", stateDir, clock)
	options := map[string]string{"size": "64MiB", "durability": "local", "sync_interval": "1h"}
	if err := driver.Create(&volume.CreateRequest{Name: "data", Options: options}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{'d'}, replica.DefaultBlockSize)
	if _, err := backend.device.WriteAt(want, 4*replica.DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if err := backend.device.Flush(); err != nil {
		t.Fatal(err)
	}
	before, _, err := driver.remote.Inspect(context.Background(), "data")
	if err != nil {
		t.Fatal(err)
	}

	crashTestMount(t, driver, "data")
	now = now.Add(2 * time.Minute)

	restarted, restartedBackend := newTestDriverAt(t, store, "node-a", stateDir, clock)
	if _, err := restarted.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := restartedBackend.device.ReadAt(got, 4*replica.DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("restarted mount did not replay the acknowledged local generation")
	}
	if err := restarted.Unmount(&volume.UnmountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	after, _, err := restarted.remote.Inspect(context.Background(), "data")
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation <= before.Generation {
		t.Fatalf("recovered generation was not published: before=%d after=%d", before.Generation, after.Generation)
	}
	if _, err := os.Stat(restarted.journalPath("data")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("clean unmount retained local journal: %v", err)
	}
}

func TestLocalJournalRefusesReplayAfterAnotherWriterAdvances(t *testing.T) {
	store := objectstore.NewMemory()
	stateDir := t.TempDir()
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	driver, backend := newTestDriverAt(t, store, "node-a", stateDir, clock)
	options := map[string]string{"size": "64MiB", "durability": "local", "sync_interval": "1h"}
	if err := driver.Create(&volume.CreateRequest{Name: "data", Options: options}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.device.WriteAt(bytes.Repeat([]byte{'a'}, replica.DefaultBlockSize), 0); err != nil {
		t.Fatal(err)
	}
	if err := backend.device.Flush(); err != nil {
		t.Fatal(err)
	}
	crashTestMount(t, driver, "data")
	now = now.Add(2 * time.Minute)

	remote := &replica.Remote{Store: store, TTL: time.Minute, Now: clock}
	other, _, err := remote.Acquire(context.Background(), "data", "node-b", replica.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	segment, err := replica.EncodeSegment(&replica.Generation{Blocks: map[uint64][]byte{
		1: bytes.Repeat([]byte{'b'}, replica.DefaultBlockSize),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Publish(context.Background(), segment); err != nil {
		t.Fatal(err)
	}
	if err := other.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	restarted, _ := newTestDriverAt(t, store, "node-a", stateDir, clock)
	if _, err := restarted.Mount(&volume.MountRequest{Name: "data", ID: "container"}); !errors.Is(err, replica.ErrJournalConflict) {
		t.Fatalf("mount error = %v, want journal conflict", err)
	}
	if _, err := os.Stat(restarted.journalPath("data")); err != nil {
		t.Fatalf("conflicting journal was not preserved: %v", err)
	}
}

func TestLocalJournalRechecksRemoteHistoryAfterAcquiringLease(t *testing.T) {
	store := objectstore.NewMemory()
	stateDir := t.TempDir()
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	driver, backend := newTestDriverAt(t, store, "node-a", stateDir, clock)
	options := map[string]string{"size": "64MiB", "durability": "local", "sync_interval": "1h"}
	if err := driver.Create(&volume.CreateRequest{Name: "data", Options: options}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.device.WriteAt(bytes.Repeat([]byte{'a'}, replica.DefaultBlockSize), 0); err != nil {
		t.Fatal(err)
	}
	if err := backend.device.Flush(); err != nil {
		t.Fatal(err)
	}
	crashTestMount(t, driver, "data")
	now = now.Add(2 * time.Minute)

	hooked := &getHookStore{Store: store}
	hooked.hook = func() error {
		otherRemote := &replica.Remote{Store: store, TTL: time.Minute, Now: clock}
		other, _, err := otherRemote.Acquire(context.Background(), "data", "node-b", replica.CreateOptions{})
		if err != nil {
			return err
		}
		segment, err := replica.EncodeSegment(&replica.Generation{Blocks: map[uint64][]byte{
			1: bytes.Repeat([]byte{'b'}, replica.DefaultBlockSize),
		}})
		if err != nil {
			return err
		}
		if err := other.Publish(context.Background(), segment); err != nil {
			return err
		}
		return other.Release(context.Background())
	}

	restarted, _ := newTestDriverAt(t, hooked, "node-a", stateDir, clock)
	if _, err := restarted.Mount(&volume.MountRequest{Name: "data", ID: "container"}); !errors.Is(err, replica.ErrJournalConflict) {
		t.Fatalf("mount error = %v, want journal conflict", err)
	}
	if _, err := os.Stat(restarted.journalPath("data")); err != nil {
		t.Fatalf("conflicting journal was not preserved: %v", err)
	}
}

func TestVolumeReplicatesBlockChanges(t *testing.T) {
	store := objectstore.NewMemory()
	driver, be := newTestDriver(t, store, "node-a")
	if err := driver.Create(&volume.CreateRequest{Name: "data", Options: map[string]string{"size": "64MiB", "sync_interval": "1h"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("payload"), 512)
	if _, err := be.device.WriteAt(want, 4*replica.DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if err := driver.Unmount(&volume.UnmountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}

	restored := filepath.Join(t.TempDir(), "image")
	remote := &replica.Remote{Store: store}
	state, err := remote.Restore(context.Background(), "data", restored)
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 3 || state.Checkpoint != 3 {
		t.Fatalf("state after format, data, and checkpoint generations = %+v", state)
	}
	file, err := os.Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got := make([]byte, len(want))
	if _, err := file.ReadAt(got, 4*replica.DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("restored block data differs")
	}
}

func TestFilesystemFlushPublishesRemoteGeneration(t *testing.T) {
	store := &blockingManifestStore{
		Store: objectstore.NewMemory(), started: make(chan struct{}), release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseCheckpoint := func() { releaseOnce.Do(func() { close(store.release) }) }
	t.Cleanup(releaseCheckpoint)
	driver, backend := newTestDriver(t, store, "node-a")
	if err := driver.Create(&volume.CreateRequest{Name: "data", Options: map[string]string{"size": "64MiB", "durability": "remote"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	before, _, err := driver.remote.Inspect(context.Background(), "data")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.device.WriteAt([]byte("committed"), 4*replica.DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	store.block.Store(true)
	if err := backend.device.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not start")
	}
	after, _, err := driver.remote.Inspect(context.Background(), "data")
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation <= before.Generation {
		t.Fatalf("state after flush = %+v, want a published generation", after)
	}
	releaseCheckpoint()
	if err := driver.Unmount(&volume.UnmountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
}

func TestLocalDurabilityDoesNotBlockFlushOnS3(t *testing.T) {
	store := objectstore.NewMemory()
	driver, backend := newTestDriver(t, store, "node-a")
	options := map[string]string{"size": "64MiB", "durability": "local", "sync_interval": "1h"}
	if err := driver.Create(&volume.CreateRequest{Name: "data", Options: options}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	before, _, err := driver.remote.Inspect(context.Background(), "data")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.device.WriteAt([]byte("not-yet-remote"), 4*replica.DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if err := backend.device.Flush(); err != nil {
		t.Fatal(err)
	}
	after, _, err := driver.remote.Inspect(context.Background(), "data")
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation != before.Generation {
		t.Fatalf("local flush advanced remote generation from %d to %d", before.Generation, after.Generation)
	}
	if _, err := os.Stat(driver.journalPath("data")); err != nil {
		t.Fatalf("local flush did not create a durable journal: %v", err)
	}
	if err := driver.Unmount(&volume.UnmountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
}

func TestUnmountDoesNotRunGarbageCollection(t *testing.T) {
	store := &countingListStore{Store: objectstore.NewMemory()}
	driver, _ := newTestDriver(t, store, "node-a")
	if err := driver.Create(&volume.CreateRequest{Name: "data", Options: map[string]string{"size": "64MiB", "durability": "remote"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	store.lists.Store(0)
	if err := driver.Unmount(&volume.UnmountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	if got := store.lists.Load(); got != 0 {
		t.Fatalf("unmount listed object history %d times", got)
	}
}

func TestSecondNodeCannotMountHeldVolume(t *testing.T) {
	store := objectstore.NewMemory()
	first, _ := newTestDriver(t, store, "node-a")
	second, _ := newTestDriver(t, store, "node-b")
	request := &volume.CreateRequest{Name: "data", Options: map[string]string{"size": "64MiB"}}
	if err := first.Create(request); err != nil {
		t.Fatal(err)
	}
	if err := second.Create(request); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Mount(&volume.MountRequest{Name: "data", ID: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Mount(&volume.MountRequest{Name: "data", ID: "b"}); err == nil {
		t.Fatal("second node mounted a held volume")
	} else {
		var held *replica.HeldError
		if !errors.As(err, &held) {
			t.Fatalf("mount error = %v", err)
		}
	}
	if err := first.Unmount(&volume.UnmountRequest{Name: "data", ID: "a"}); err != nil {
		t.Fatal(err)
	}
}

type failStateStore struct {
	objectstore.Store
	fail atomic.Bool
}

func TestTransientRemoteFlushFailureStallsThenSucceeds(t *testing.T) {
	store := &failStateStore{Store: objectstore.NewMemory()}
	driver, backend := newTestDriver(t, store, "node-a")
	if err := driver.Create(&volume.CreateRequest{Name: "data", Options: map[string]string{"size": "64MiB", "durability": "remote"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.device.WriteAt([]byte("committed"), 4*replica.DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	store.fail.Store(true)
	if err := backend.device.Flush(); err != nil {
		t.Fatalf("flush should stall through a transient outage and succeed, got: %v", err)
	}
	if store.fail.Load() {
		t.Fatal("flush did not exercise the injected transient failure")
	}

	path := filepath.Join(t.TempDir(), "restored")
	if _, err := driver.remote.Restore(context.Background(), "data", path); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got := make([]byte, len("committed"))
	if _, err := file.ReadAt(got, 4*replica.DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if string(got) != "committed" {
		t.Fatalf("restored data = %q", got)
	}
	if err := driver.Unmount(&volume.UnmountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
}

func (s *failStateStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if strings.HasSuffix(key, "/state.json") && s.fail.CompareAndSwap(true, false) {
		return "", errors.New("injected state update failure")
	}
	return s.Store.PutIfMatch(ctx, key, data, etag)
}

func TestInitialPublishFailureRollsBackMount(t *testing.T) {
	store := &failStateStore{Store: objectstore.NewMemory()}
	store.fail.Store(true)
	driver, _ := newTestDriver(t, store, "node-a")
	if err := driver.Create(&volume.CreateRequest{Name: "data", Options: map[string]string{"size": "64MiB"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err == nil {
		t.Fatal("mount succeeded despite the state update failure")
	}

	state := driver.volumes["data"]
	state.mu.Lock()
	mount := state.mounts["data"]
	if mount.active || mount.device != nil || mount.unmounter != nil || mount.stopBeat != nil {
		state.mu.Unlock()
		t.Fatalf("failed mount was left active: %+v", mount)
	}
	state.mu.Unlock()

	remoteState, _, err := driver.remote.Inspect(context.Background(), "data")
	if err != nil {
		t.Fatal(err)
	}
	if remoteState.Lease != nil {
		t.Fatal("failed mount retained its lease")
	}
	if _, err := driver.Mount(&volume.MountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatalf("retry mount: %v", err)
	}
	if err := driver.Unmount(&volume.UnmountRequest{Name: "data", ID: "container"}); err != nil {
		t.Fatal(err)
	}
}

func TestParseVolumeOptions(t *testing.T) {
	defaults, err := ParseVolumeOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !defaults.LocalDurability() {
		t.Fatal("local durability is not the default")
	}
	opts, err := ParseVolumeOptions(map[string]string{
		"mode": "ro", "scope": "replica", "durability": "remote", "size": "2GiB", "filesystem": "ext4", "sync_interval": "2s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.ReadOnly() || !opts.PerReplica() || !opts.RemoteDurability() || opts.Size != 2<<30 || opts.SyncInterval != 2*time.Second {
		t.Fatalf("unexpected options: %+v", opts)
	}
	for _, raw := range []map[string]string{
		{"mode": "rwx"}, {"scope": "node"}, {"size": "1MiB"}, {"size": "68157441"},
		{"filesystem": "xfs"}, {"durability": "async"}, {"durability": "maybe"}, {"class": "fuse"}, {"bogus": "1"}, {"mode": "ro", "seed": "/x"},
	} {
		if _, err := ParseVolumeOptions(raw); err == nil {
			t.Fatalf("accepted invalid options %v", raw)
		}
	}
}
