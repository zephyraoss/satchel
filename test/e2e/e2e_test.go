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
	cfg := objectstore.S3Config{
		Endpoint: endpoint, Region: "us-east-1", Bucket: "satchel",
		AccessKeyID: "minioadmin", SecretKey: "minioadmin", ForcePathStyle: true,
	}
	store := objectstore.NewS3(cfg)
	if err := objectstore.VerifyConditionalWrites(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	name := "e2e-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = store.DeletePrefix(context.Background(), replica.VolumePrefix(name)) })

	node := func(id string) *plugin.Driver {
		driver, err := plugin.New(
			plugin.Config{NodeID: id, StateDir: filepath.Join(t.TempDir(), id), CheckpointInterval: 1},
			&replica.Remote{Store: store, TTL: 10 * time.Second},
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
	durable, _, err := (&replica.Remote{Store: store}).Inspect(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if durable.Generation < 2 {
		t.Fatalf("fsync did not publish a remote generation: %+v", durable)
	}
	if err := first.Unmount(&volume.UnmountRequest{Name: name, ID: "writer-a"}); err != nil {
		t.Fatal(err)
	}
	state, _, err := (&replica.Remote{Store: store}).Inspect(context.Background(), name)
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
	cfg := objectstore.S3Config{
		Endpoint: endpoint, Region: "us-east-1", Bucket: "satchel",
		AccessKeyID: "minioadmin", SecretKey: "minioadmin", ForcePathStyle: true,
	}
	store := objectstore.NewS3(cfg)
	if err := objectstore.VerifyConditionalWrites(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	name := "fence-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = store.DeletePrefix(ctx, replica.VolumePrefix(name)) })

	// A long TTL keeps A's heartbeat from racing the deterministic lease break.
	node := func(id string) *plugin.Driver {
		driver, err := plugin.New(
			plugin.Config{NodeID: id, StateDir: filepath.Join(t.TempDir(), id), CheckpointInterval: 1, SyncInterval: time.Hour},
			&replica.Remote{Store: store, TTL: 5 * time.Minute},
			block.New(block.Options{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return driver
	}
	inspect := &replica.Remote{Store: store}

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

	// Operator fences node A and hands the volume to node B.
	if err := inspect.Break(ctx, name, held.Lease.Token); err != nil {
		t.Fatalf("break lease: %v", err)
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
	if afterTakeover.Lease == nil || afterTakeover.Lease.Holder != "node-b" || afterTakeover.Lease.Epoch != held.Lease.Epoch+1 {
		t.Fatalf("takeover lease = %+v, want node-b at epoch %d", afterTakeover.Lease, held.Lease.Epoch+1)
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
	cfg := objectstore.S3Config{
		Endpoint: endpoint, Region: "us-east-1", Bucket: "satchel",
		AccessKeyID: "minioadmin", SecretKey: "minioadmin", ForcePathStyle: true,
	}
	rawStore := objectstore.NewS3(cfg)
	if err := objectstore.VerifyConditionalWrites(context.Background(), rawStore); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	name := "partition-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = rawStore.DeletePrefix(ctx, replica.VolumePrefix(name)) })

	// A short TTL bounds the test: A must self-fence and B must be able to
	// acquire within real-clock TTL expiry, no operator intervention.
	const ttl = 4 * time.Second
	node := func(id string, store objectstore.Store) *plugin.Driver {
		driver, err := plugin.New(
			plugin.Config{NodeID: id, StateDir: filepath.Join(t.TempDir(), id), CheckpointInterval: 1, SyncInterval: time.Hour},
			&replica.Remote{Store: store, TTL: ttl},
			block.New(block.Options{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return driver
	}
	inspect := &replica.Remote{Store: rawStore}

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
	defer exec.Command("umount", dir).Run()
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
