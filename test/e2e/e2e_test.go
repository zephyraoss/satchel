//go:build linux

package e2e

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/docker/go-plugins-helpers/volume"

	"github.com/zephyraoss/satchel/internal/backend/block"
	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/plugin"
	"github.com/zephyraoss/satchel/internal/replica"
)

func e2eEnvOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func e2eHeadMode(t *testing.T) replica.HeadMode {
	mode, err := replica.ParseHeadMode(os.Getenv("SATCHEL_E2E_S3_HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	return mode
}

func e2eRemote(t *testing.T, store objectstore.Store, ttl time.Duration) *replica.Remote {
	remote := &replica.Remote{Store: store, Head: e2eHeadMode(t), TTL: ttl}
	if err := remote.VerifyBackend(context.Background()); err != nil {
		t.Fatal(err)
	}
	return remote
}

func e2eS3Config(endpoint string) objectstore.S3Config {
	return objectstore.S3Config{
		Endpoint:       endpoint,
		Region:         e2eEnvOr("SATCHEL_E2E_S3_REGION", "us-east-1"),
		Bucket:         e2eEnvOr("SATCHEL_E2E_S3_BUCKET", "satchel"),
		AccessKeyID:    e2eEnvOr("SATCHEL_E2E_S3_ACCESS_KEY", "minioadmin"),
		SecretKey:      e2eEnvOr("SATCHEL_E2E_S3_SECRET_KEY", "minioadmin"),
		ForcePathStyle: true,
	}
}

func TestVolumeMovesBetweenNodes(t *testing.T) {
	endpoint := os.Getenv("SATCHEL_E2E_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set SATCHEL_E2E_S3_ENDPOINT to run")
	}
	if os.Geteuid() != 0 {
		t.Skip("NBD mount test requires root")
	}
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skip("mkfs.ext4 is not installed")
	}
	if _, err := os.Stat("/dev/nbd0"); err != nil {
		t.Skip("load the nbd kernel module before running")
	}
	cfg := e2eS3Config(endpoint)
	store := objectstore.NewS3(cfg)
	name := "e2e-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = store.DeletePrefix(context.Background(), replica.VolumePrefix(name)) })

	node := func(id string) *plugin.Driver {
		driver, err := plugin.New(
			plugin.Config{NodeID: id, StateDir: filepath.Join(t.TempDir(), id), CheckpointInterval: 1},
			e2eRemote(t, store, 10*time.Second),
			block.New(block.Options{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return driver
	}
	first := node("node-a")
	if err := first.Create(&volume.CreateRequest{Name: name, Options: map[string]string{"size": "64MiB", "sync_interval": "1h"}}); err != nil {
		t.Fatal(err)
	}
	mounted, err := first.Mount(&volume.MountRequest{Name: name, ID: "writer-a"})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(mounted.Mountpoint, "hello"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("from node a")); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	durable, _, err := e2eRemote(t, store, 0).Inspect(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if durable.Generation < 2 {
		t.Fatalf("fsync did not publish a remote generation: %+v", durable)
	}
	if err := first.Unmount(&volume.UnmountRequest{Name: name, ID: "writer-a"}); err != nil {
		t.Fatal(err)
	}
	state, _, err := e2eRemote(t, store, 0).Inspect(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if state.Checkpoint == 0 {
		t.Fatal("clean unmount did not publish a checkpoint")
	}

	second := node("node-b")
	mounted, err = second.Mount(&volume.MountRequest{Name: name, ID: "writer-b"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(mounted.Mountpoint, "hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "from node a" {
		t.Fatalf("restored data = %q", data)
	}
	if err := second.Unmount(&volume.UnmountRequest{Name: name, ID: "writer-b"}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	reader := node("node-c")
	if err := reader.Create(&volume.CreateRequest{Name: name, Options: map[string]string{"size": "64MiB", "mode": "ro"}}); err != nil {
		t.Fatal(err)
	}
	mounted, err = reader.Mount(&volume.MountRequest{Name: name, ID: "reader"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mounted.Mountpoint, "forbidden"), []byte("x"), 0o644); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("read-only write error = %v", err)
	}
	if err := reader.Unmount(&volume.UnmountRequest{Name: name, ID: "reader"}); err != nil {
		t.Fatal(err)
	}
}

func writeThenSync(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func TestConcurrentWriterIsFencedAfterTakeover(t *testing.T) {
	endpoint := os.Getenv("SATCHEL_E2E_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set SATCHEL_E2E_S3_ENDPOINT to run")
	}
	if os.Geteuid() != 0 {
		t.Skip("NBD mount test requires root")
	}
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skip("mkfs.ext4 is not installed")
	}
	if _, err := os.Stat("/dev/nbd0"); err != nil {
		t.Skip("load the nbd kernel module before running")
	}
	cfg := e2eS3Config(endpoint)
	store := objectstore.NewS3(cfg)
	ctx := context.Background()
	name := "fence-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = store.DeletePrefix(ctx, replica.VolumePrefix(name)) })

	// A long TTL keeps A's heartbeat from racing the deterministic lease break.
	node := func(id string) *plugin.Driver {
		driver, err := plugin.New(
			plugin.Config{NodeID: id, StateDir: filepath.Join(t.TempDir(), id), CheckpointInterval: 1, SyncInterval: time.Hour},
			e2eRemote(t, store, 5*time.Minute),
			block.New(block.Options{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return driver
	}
	inspect := e2eRemote(t, store, 0)

	nodeA := node("node-a")
	if err := nodeA.Create(&volume.CreateRequest{Name: name, Options: map[string]string{"size": "64MiB"}}); err != nil {
		t.Fatal(err)
	}
	mountA, err := nodeA.Mount(&volume.MountRequest{Name: name, ID: "writer-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nodeA.Unmount(&volume.UnmountRequest{Name: name, ID: "writer-a"}) }()

	if err := writeThenSync(filepath.Join(mountA.Mountpoint, "key"), []byte("from-a-1")); err != nil {
		t.Fatal(err)
	}
	held, _, err := inspect.Inspect(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if held.Generation < 2 || held.Lease == nil {
		t.Fatalf("node A did not publish under a held lease: %+v", held)
	}

	// Wait out node A's trailing async checkpoint so the conditional break does
	// not race a concurrent state update; operators are likewise told to
	// quiesce the holder before breaking its lease.
	deadline := time.Now().Add(30 * time.Second)
	for held.Checkpoint < held.Generation {
		if time.Now().After(deadline) {
			t.Fatalf("node A never quiesced: %+v", held)
		}
		time.Sleep(200 * time.Millisecond)
		if held, _, err = inspect.Inspect(ctx, name); err != nil {
			t.Fatal(err)
		}
	}

	// Operator fences node A and hands the volume to node B.
	if err := inspect.Break(ctx, name, held.Lease.Token); err != nil {
		t.Fatalf("break lease: %v", err)
	}
	broken, _, err := inspect.Inspect(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	nodeB := node("node-b")
	mountB, err := nodeB.Mount(&volume.MountRequest{Name: name, ID: "writer-b"})
	if err != nil {
		t.Fatalf("node B takeover mount: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(mountB.Mountpoint, "key")); err != nil || string(data) != "from-a-1" {
		t.Fatalf("takeover restore = %q, err=%v", data, err)
	}
	afterTakeover, _, err := inspect.Inspect(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if afterTakeover.Lease == nil || afterTakeover.Lease.Holder != "node-b" || afterTakeover.Lease.Epoch != broken.Epoch+1 {
		t.Fatalf("takeover lease = %+v, want node-b at epoch %d", afterTakeover.Lease, broken.Epoch+1)
	}

	// Node A is now a superseded zombie holding a live NBD mount. Forcing a
	// durable write makes it attempt a conditional publish, which fails because
	// node B bumped the lease epoch. The fsync itself may or may not surface the
	// error synchronously depending on fence timing, so it is best-effort; the
	// guarantees are that A becomes fenced and its write never reaches S3.
	if err := writeThenSync(filepath.Join(mountA.Mountpoint, "shared"), []byte("from-a-2")); err != nil {
		t.Logf("zombie write on the superseded node failed as expected: %v", err)
	}
	fenced := false
	for range 50 {
		if _, err := nodeA.Mount(&volume.MountRequest{Name: name, ID: "writer-a"}); err != nil && strings.Contains(err.Error(), "fenced") {
			fenced = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !fenced {
		t.Fatal("superseded node A was not fenced after takeover")
	}

	// Node B is the sole legitimate writer.
	if err := writeThenSync(filepath.Join(mountB.Mountpoint, "shared"), []byte("from-b")); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.Unmount(&volume.UnmountRequest{Name: name, ID: "writer-b"}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// The durable state is the real arbiter: only node B's writes survived, and
	// the superseded node's post-takeover write is absent.
	restored := filepath.Join(t.TempDir(), "restored")
	if _, err := inspect.Restore(ctx, name, restored); err != nil {
		t.Fatal(err)
	}
	image, err := os.Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	if err := mountAndCheck(t, image.Name(), map[string]string{
		"key":    "from-a-1",
		"shared": "from-b",
	}); err != nil {
		t.Fatal(err)
	}
}

var errPartitioned = errors.New("injected network partition")

type partitionableStore struct {
	objectstore.Store
	partitioned atomic.Bool
}

func (s *partitionableStore) Get(ctx context.Context, key string) (objectstore.Object, error) {
	if s.partitioned.Load() {
		return objectstore.Object{}, errPartitioned
	}
	return s.Store.Get(ctx, key)
}

func (s *partitionableStore) Put(ctx context.Context, key string, data []byte) (string, error) {
	if s.partitioned.Load() {
		return "", errPartitioned
	}
	return s.Store.Put(ctx, key, data)
}

func (s *partitionableStore) PutIfAbsent(ctx context.Context, key string, data []byte) (string, error) {
	if s.partitioned.Load() {
		return "", errPartitioned
	}
	return s.Store.PutIfAbsent(ctx, key, data)
}

func (s *partitionableStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if s.partitioned.Load() {
		return "", errPartitioned
	}
	return s.Store.PutIfMatch(ctx, key, data, etag)
}

func (s *partitionableStore) Delete(ctx context.Context, key string) error {
	if s.partitioned.Load() {
		return errPartitioned
	}
	return s.Store.Delete(ctx, key)
}

func (s *partitionableStore) List(ctx context.Context, prefix string) ([]string, error) {
	if s.partitioned.Load() {
		return nil, errPartitioned
	}
	return s.Store.List(ctx, prefix)
}

func (s *partitionableStore) DeletePrefix(ctx context.Context, prefix string) error {
	if s.partitioned.Load() {
		return errPartitioned
	}
	return s.Store.DeletePrefix(ctx, prefix)
}

func TestPartitionedWriterSelfFencesAndYields(t *testing.T) {
	endpoint := os.Getenv("SATCHEL_E2E_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set SATCHEL_E2E_S3_ENDPOINT to run")
	}
	if os.Geteuid() != 0 {
		t.Skip("NBD mount test requires root")
	}
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skip("mkfs.ext4 is not installed")
	}
	if _, err := os.Stat("/dev/nbd0"); err != nil {
		t.Skip("load the nbd kernel module before running")
	}
	cfg := e2eS3Config(endpoint)
	rawStore := objectstore.NewS3(cfg)
	ctx := context.Background()
	name := "partition-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = rawStore.DeletePrefix(ctx, replica.VolumePrefix(name)) })

	// A short TTL bounds the test: A must self-fence and B must be able to
	// acquire within real-clock TTL expiry, no operator intervention.
	const ttl = 4 * time.Second
	node := func(id string, store objectstore.Store) *plugin.Driver {
		driver, err := plugin.New(
			plugin.Config{NodeID: id, StateDir: filepath.Join(t.TempDir(), id), CheckpointInterval: 1, SyncInterval: time.Hour},
			e2eRemote(t, store, ttl),
			block.New(block.Options{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return driver
	}
	inspect := e2eRemote(t, rawStore, 0)

	partitioned := &partitionableStore{Store: rawStore}
	nodeA := node("node-a", partitioned)
	if err := nodeA.Create(&volume.CreateRequest{Name: name, Options: map[string]string{"size": "64MiB"}}); err != nil {
		t.Fatal(err)
	}
	mountA, err := nodeA.Mount(&volume.MountRequest{Name: name, ID: "writer-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nodeA.Unmount(&volume.UnmountRequest{Name: name, ID: "writer-a"}) }()

	if err := writeThenSync(filepath.Join(mountA.Mountpoint, "key"), []byte("from-a-1")); err != nil {
		t.Fatal(err)
	}
	held, _, err := inspect.Inspect(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if held.Generation < 2 || held.Lease == nil {
		t.Fatalf("node A did not publish under a held lease: %+v", held)
	}

	// Partition A from S3. A durable write now stalls in publish retries until
	// the lease heartbeat gives up and fences, which fails the write with an
	// error instead of acknowledging it.
	partitioned.partitioned.Store(true)
	stalledWrite := make(chan error, 1)
	go func() {
		stalledWrite <- writeThenSync(filepath.Join(mountA.Mountpoint, "shared"), []byte("from-a-2"))
	}()

	fenced := false
	fenceDeadline := time.Now().Add(5 * ttl)
	for time.Now().Before(fenceDeadline) {
		if _, err := nodeA.Mount(&volume.MountRequest{Name: name, ID: "writer-a"}); err != nil && strings.Contains(err.Error(), "fenced") {
			fenced = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !fenced {
		t.Fatal("partitioned node A did not self-fence after its lease TTL expired")
	}
	select {
	case err := <-stalledWrite:
		if err == nil {
			t.Fatal("durable write on the partitioned node was acknowledged")
		}
		t.Logf("partitioned write failed after fencing: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("partitioned write did not unblock after fencing")
	}

	nodeB := node("node-b", rawStore)
	var mountB *volume.MountResponse
	takeoverDeadline := time.Now().Add(4 * ttl)
	for time.Now().Before(takeoverDeadline) {
		response, err := nodeB.Mount(&volume.MountRequest{Name: name, ID: "writer-b"})
		if err == nil {
			mountB = response
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if mountB == nil {
		t.Fatal("node B could not take over after node A's lease expired")
	}
	defer func() { _ = nodeB.Unmount(&volume.UnmountRequest{Name: name, ID: "writer-b"}) }()
	if data, err := os.ReadFile(filepath.Join(mountB.Mountpoint, "key")); err != nil || string(data) != "from-a-1" {
		t.Fatalf("takeover restore = %q, err=%v", data, err)
	}
	if err := writeThenSync(filepath.Join(mountB.Mountpoint, "shared"), []byte("from-b")); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.Unmount(&volume.UnmountRequest{Name: name, ID: "writer-b"}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	if _, err := inspect.Restore(ctx, name, restored); err != nil {
		t.Fatal(err)
	}
	if err := mountAndCheck(t, restored, map[string]string{
		"key":    "from-a-1",
		"shared": "from-b",
	}); err != nil {
		t.Fatal(err)
	}
}

func mountAndCheck(t *testing.T, image string, want map[string]string) error {
	t.Helper()
	dir := t.TempDir()
	if out, err := exec.Command("mount", "-o", "ro,loop", image, dir).CombinedOutput(); err != nil {
		return errors.New("mount restored image: " + err.Error() + ": " + string(out))
	}
	defer func() {
		if out, err := exec.Command("umount", dir).CombinedOutput(); err != nil {
			t.Errorf("umount %s: %v: %s", dir, err, out)
		}
	}()
	for file, contents := range want {
		got, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			return err
		}
		if string(got) != contents {
			return errors.New("restored " + file + " = " + string(got) + ", want " + contents)
		}
	}
	return nil
}

func TestSimultaneousMountRaceAdmitsExactlyOneWriter(t *testing.T) {
	endpoint := os.Getenv("SATCHEL_E2E_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set SATCHEL_E2E_S3_ENDPOINT to run")
	}
	if os.Geteuid() != 0 {
		t.Skip("NBD mount test requires root")
	}
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skip("mkfs.ext4 is not installed")
	}
	if _, err := os.Stat("/dev/nbd0"); err != nil {
		t.Skip("load the nbd kernel module before running")
	}
	cfg := e2eS3Config(endpoint)
	store := objectstore.NewS3(cfg)
	ctx := context.Background()
	name := "race-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = store.DeletePrefix(ctx, replica.VolumePrefix(name)) })

	node := func(id string) *plugin.Driver {
		driver, err := plugin.New(
			plugin.Config{NodeID: id, StateDir: filepath.Join(t.TempDir(), id), CheckpointInterval: 1, SyncInterval: time.Hour},
			e2eRemote(t, store, 10*time.Second),
			block.New(block.Options{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return driver
	}

	raceMounts := func(drivers map[string]*plugin.Driver) (string, *volume.MountResponse) {
		t.Helper()
		type outcome struct {
			id       string
			response *volume.MountResponse
			err      error
		}
		results := make(chan outcome, len(drivers))
		gate := make(chan struct{})
		for id, driver := range drivers {
			go func() {
				<-gate
				response, err := driver.Mount(&volume.MountRequest{Name: name, ID: id})
				results <- outcome{id: id, response: response, err: err}
			}()
		}
		close(gate)
		winner := ""
		var mounted *volume.MountResponse
		for range drivers {
			result := <-results
			if result.err == nil {
				if winner != "" {
					t.Fatalf("both racing nodes mounted the same volume: %s and %s", winner, result.id)
				}
				winner, mounted = result.id, result.response
				continue
			}
			t.Logf("loser %s rejected: %v", result.id, result.err)
			if !strings.Contains(result.err.Error(), "is held by") && !errors.Is(result.err, replica.ErrLeaseLost) {
				t.Fatalf("racer %s failed for a reason other than lease contention: %v", result.id, result.err)
			}
		}
		if winner == "" {
			t.Fatal("no racing node acquired the volume")
		}
		return winner, mounted
	}

	nodeA, nodeB := node("node-a"), node("node-b")
	for id, driver := range map[string]*plugin.Driver{"node-a": nodeA, "node-b": nodeB} {
		if err := driver.Create(&volume.CreateRequest{Name: name, Options: map[string]string{"size": "64MiB"}}); err != nil {
			t.Fatalf("create on %s: %v", id, err)
		}
	}
	drivers := map[string]*plugin.Driver{"node-a": nodeA, "node-b": nodeB}
	winner, mounted := raceMounts(drivers)
	t.Logf("creation race winner: %s", winner)

	loser := "node-a"
	if winner == "node-a" {
		loser = "node-b"
	}
	if _, err := drivers[loser].Mount(&volume.MountRequest{Name: name, ID: loser}); err == nil {
		t.Fatal("loser mounted while the winner holds the lease")
	} else if !strings.Contains(err.Error(), "is held by") {
		t.Fatalf("loser retry error = %v, want a held-lease rejection", err)
	}
	if err := writeThenSync(filepath.Join(mounted.Mountpoint, "owner"), []byte(winner)); err != nil {
		t.Fatal(err)
	}
	if err := drivers[winner].Unmount(&volume.UnmountRequest{Name: name, ID: winner}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	nodeC := node("node-c")
	takeoverDrivers := map[string]*plugin.Driver{loser: drivers[loser], "node-c": nodeC}
	second, mounted := raceMounts(takeoverDrivers)
	t.Logf("takeover race winner: %s", second)
	data, err := os.ReadFile(filepath.Join(mounted.Mountpoint, "owner"))
	if err != nil || string(data) != winner {
		t.Fatalf("takeover winner read %q, err=%v, want %q", data, err, winner)
	}
	if err := takeoverDrivers[second].Unmount(&volume.UnmountRequest{Name: name, ID: second}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
