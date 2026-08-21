package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-plugins-helpers/volume"
	"github.com/google/uuid"

	"github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/backend/fuse"
	"github.com/zephyraoss/satchel/internal/backend/pack"
	"github.com/zephyraoss/satchel/internal/cli"
	"github.com/zephyraoss/satchel/internal/lease"
	"github.com/zephyraoss/satchel/internal/litestream"
	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/plugin"
)

type env struct {
	s3    objectstore.S3Config
	store *objectstore.S3
}

func setup(t *testing.T) *env {
	t.Helper()
	endpoint := os.Getenv("SATCHEL_E2E_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set SATCHEL_E2E_S3_ENDPOINT (and SATCHEL_E2E_S3_BUCKET/ACCESS_KEY/SECRET_KEY) to run against MinIO")
	}
	if _, err := exec.LookPath("litestream"); err != nil {
		t.Skip("litestream binary not on PATH")
	}
	cfg := objectstore.S3Config{
		Endpoint:       endpoint,
		Bucket:         envOr("SATCHEL_E2E_S3_BUCKET", "satchel"),
		AccessKeyID:    envOr("SATCHEL_E2E_S3_ACCESS_KEY", "minioadmin"),
		SecretKey:      envOr("SATCHEL_E2E_S3_SECRET_KEY", "minioadmin"),
		ForcePathStyle: true,
	}
	store := objectstore.NewS3(cfg)
	if err := objectstore.VerifyConditionalWrites(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	return &env{s3: cfg, store: store}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func (e *env) node(t *testing.T, id string) *plugin.Driver {
	return e.nodeWith(t, id, pickBackend(t))
}

func pickBackend(t *testing.T) backend.Backend {
	t.Helper()
	if os.Getenv("SATCHEL_E2E_BACKEND") == "pack" {
		return pack.New()
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Log("no /dev/fuse; using pack backend")
		return pack.New()
	}
	return fuse.New(fuse.Options{})
}

func (e *env) nodeWith(t *testing.T, id string, be backend.Backend) *plugin.Driver {
	t.Helper()
	stateDir := t.TempDir()
	d, err := plugin.New(
		plugin.Config{NodeID: id, StateDir: stateDir},
		e.store,
		&lease.Manager{Store: e.store, NodeID: id, TTL: 3 * time.Second},
		litestream.New(litestream.Config{ConfigDir: filepath.Join(stateDir, "litestream"), S3: e.s3, SyncInterval: 500 * time.Millisecond}),
		be,
	)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func (e *env) volumeName(t *testing.T) string {
	name := "e2e-" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	t.Cleanup(func() {
		ctx := context.Background()
		e.store.DeletePrefix(ctx, litestream.ReplicaPath(name)+"/")
		e.store.Delete(ctx, lease.Key(name))
	})
	return name
}

func TestDefinitionOfDone(t *testing.T) {
	e := setup(t)
	nodeA, nodeB := e.node(t, "node-a"), e.node(t, "node-b")
	vol := e.volumeName(t)

	if err := nodeA.Create(&volume.CreateRequest{Name: vol}); err != nil {
		t.Fatal(err)
	}
	mountA, err := nodeA.Mount(&volume.MountRequest{Name: vol, ID: "deploy-1"})
	if err != nil {
		t.Fatal(err)
	}
	writeTree(t, mountA.Mountpoint, map[string]string{
		"config.yml":        "listen: :8080\n",
		"uploads/one.bin":   strings.Repeat("\xde\xad", 4096),
		"uploads/two.txt":   "second",
		"nested/deep/x.txt": "deep",
	})
	if err := nodeA.Unmount(&volume.UnmountRequest{Name: vol, ID: "deploy-1"}); err != nil {
		t.Fatal(err)
	}

	mountB, err := nodeB.Mount(&volume.MountRequest{Name: vol, ID: "deploy-2"})
	if err != nil {
		t.Fatalf("redeploy to node B: %v", err)
	}
	assertTree(t, mountB.Mountpoint, map[string]string{
		"config.yml":        "listen: :8080\n",
		"uploads/one.bin":   strings.Repeat("\xde\xad", 4096),
		"uploads/two.txt":   "second",
		"nested/deep/x.txt": "deep",
	})

	_, err = nodeA.Mount(&volume.MountRequest{Name: vol, ID: "deploy-3"})
	want := fmt.Sprintf("volume %s is held by node node-b", vol)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("concurrent mount on node A: err=%v, want contains %q", err, want)
	}

	writeTree(t, mountB.Mountpoint, map[string]string{"uploads/three.txt": "written on b"})
	if err := os.Remove(filepath.Join(mountB.Mountpoint, "uploads/two.txt")); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.Unmount(&volume.UnmountRequest{Name: vol, ID: "deploy-2"}); err != nil {
		t.Fatal(err)
	}

	mountA, err = nodeA.Mount(&volume.MountRequest{Name: vol, ID: "deploy-4"})
	if err != nil {
		t.Fatalf("move back to node A: %v", err)
	}
	assertTree(t, mountA.Mountpoint, map[string]string{
		"config.yml":        "listen: :8080\n",
		"uploads/three.txt": "written on b",
	})
	if _, err := os.Stat(filepath.Join(mountA.Mountpoint, "uploads/two.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted file resurrected: %v", err)
	}
	if err := nodeA.Unmount(&volume.UnmountRequest{Name: vol, ID: "deploy-4"}); err != nil {
		t.Fatal(err)
	}

	keys, err := e.store.List(context.Background(), litestream.ReplicaPath(vol)+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) < 3 {
		t.Fatalf("expected at least 3 ltx files in a single lineage, got %v", keys)
	}
	if err := nodeA.Remove(&volume.RemoveRequest{Name: vol}); err != nil {
		t.Fatal(err)
	}
	keys, _ = e.store.List(context.Background(), litestream.ReplicaPath(vol)+"/")
	if len(keys) != 0 {
		t.Fatalf("remove left objects behind: %v", keys)
	}
}

func TestLiveReplicationWhileMounted(t *testing.T) {
	e := setup(t)
	if _, ok := pickBackend(t).(*fuse.Backend); !ok {
		t.Skip("live replication needs the fuse backend")
	}
	nodeA := e.node(t, "node-a")
	vol := e.volumeName(t)
	if err := nodeA.Create(&volume.CreateRequest{Name: vol}); err != nil {
		t.Fatal(err)
	}
	mountA, err := nodeA.Mount(&volume.MountRequest{Name: vol, ID: "m1"})
	if err != nil {
		t.Fatal(err)
	}
	prefix := litestream.ReplicaPath(vol) + "/"
	waitForObjects := func(min int) []string {
		deadline := time.Now().Add(15 * time.Second)
		for {
			keys, err := e.store.List(context.Background(), prefix)
			if err != nil {
				t.Fatal(err)
			}
			if len(keys) >= min {
				return keys
			}
			if time.Now().After(deadline) {
				t.Fatalf("expected >= %d objects under %s, got %v", min, prefix, keys)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	initial := len(waitForObjects(1))
	writeTree(t, mountA.Mountpoint, map[string]string{"live.txt": "written while mounted"})
	waitForObjects(initial + 1)

	nodeB := e.node(t, "node-b")
	if err := nodeB.Create(&volume.CreateRequest{Name: vol}); err != nil {
		t.Fatal(err)
	}
	if err := (&lease.Manager{Store: e.store}).Break(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		get, _ := nodeA.Get(&volume.GetRequest{Name: vol})
		if _, fenced := get.Volume.Status["fenced"]; fenced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node A did not fence")
		}
		time.Sleep(100 * time.Millisecond)
	}
	mountB, err := nodeB.Mount(&volume.MountRequest{Name: vol, ID: "m2"})
	if err != nil {
		t.Fatal(err)
	}
	assertTree(t, mountB.Mountpoint, map[string]string{"live.txt": "written while mounted"})
	if err := nodeB.Unmount(&volume.UnmountRequest{Name: vol, ID: "m2"}); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.Unmount(&volume.UnmountRequest{Name: vol, ID: "m1"}); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseTakeoverAfterCrash(t *testing.T) {
	e := setup(t)
	nodeA, nodeB := e.node(t, "node-a"), e.node(t, "node-b")
	vol := e.volumeName(t)
	for _, node := range []*plugin.Driver{nodeA, nodeB} {
		if err := node.Create(&volume.CreateRequest{Name: vol}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := nodeA.Mount(&volume.MountRequest{Name: vol, ID: "m1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeB.Mount(&volume.MountRequest{Name: vol, ID: "m2"}); err == nil {
		t.Fatal("expected mount on B to fail while A heartbeats")
	}
	if err := (&lease.Manager{Store: e.store}).Break(context.Background(), vol); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		get, err := nodeA.Get(&volume.GetRequest{Name: vol})
		if err != nil {
			t.Fatal(err)
		}
		if _, fenced := get.Volume.Status["fenced"]; fenced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node A did not fence itself after losing the lease")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := nodeB.Mount(&volume.MountRequest{Name: vol, ID: "m2"}); err != nil {
		t.Fatalf("node B mount after lease break: %v", err)
	}
	if err := nodeB.Unmount(&volume.UnmountRequest{Name: vol, ID: "m2"}); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.Unmount(&volume.UnmountRequest{Name: vol, ID: "m1"}); err != nil {
		t.Fatal(err)
	}
}

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func assertTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, want := range files {
		got, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if string(got) != want {
			t.Fatalf("%s = %q, want %q", rel, truncate(got), truncate([]byte(want)))
		}
	}
}

func truncate(b []byte) string {
	if len(b) > 40 {
		return string(b[:40]) + "..."
	}
	return string(b)
}

func TestReadOnlyReplicasAndCLI(t *testing.T) {
	e := setup(t)
	writer := e.node(t, "node-a")
	vol := e.volumeName(t)
	if err := writer.Create(&volume.CreateRequest{Name: vol}); err != nil {
		t.Fatal(err)
	}
	m, err := writer.Mount(&volume.MountRequest{Name: vol, ID: "w"})
	if err != nil {
		t.Fatal(err)
	}
	writeTree(t, m.Mountpoint, map[string]string{"config.yml": "v1"})
	if err := writer.Unmount(&volume.UnmountRequest{Name: vol, ID: "w"}); err != nil {
		t.Fatal(err)
	}

	env := &cli.Env{
		Store:      e.store,
		Leases:     &lease.Manager{Store: e.store, NodeID: "cli@test", TTL: 3 * time.Second},
		Litestream: litestream.New(litestream.Config{ConfigDir: filepath.Join(t.TempDir(), "ls"), S3: e.s3}),
		WorkDir:    t.TempDir(),
		Stdout:     &bytes.Buffer{},
		Stderr:     &bytes.Buffer{},
	}
	if err := env.Put(context.Background(), vol, "config.yml", strings.NewReader("v2 from cli"), 0o644); err != nil {
		t.Fatal(err)
	}

	readers := []*plugin.Driver{e.node(t, "node-b"), e.node(t, "node-c")}
	for i, r := range readers {
		if err := r.Create(&volume.CreateRequest{Name: vol, Options: map[string]string{"mode": "ro"}}); err != nil {
			t.Fatal(err)
		}
		rm, err := r.Mount(&volume.MountRequest{Name: vol, ID: fmt.Sprintf("r%d", i)})
		if err != nil {
			t.Fatalf("ro mount %d: %v", i, err)
		}
		assertTree(t, rm.Mountpoint, map[string]string{"config.yml": "v2 from cli"})
		if err := os.WriteFile(filepath.Join(rm.Mountpoint, "nope"), []byte("x"), 0o644); err == nil {
			t.Fatal("write on read-only mount succeeded")
		}
	}
	m, err = writer.Mount(&volume.MountRequest{Name: vol, ID: "w2"})
	if err != nil {
		t.Fatalf("writer blocked while read-only replicas are mounted: %v", err)
	}
	assertTree(t, m.Mountpoint, map[string]string{"config.yml": "v2 from cli"})
	var out bytes.Buffer
	if err := env.Cat(context.Background(), vol, "config.yml", &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "v2 from cli" {
		t.Fatalf("cli cat = %q", out.String())
	}
	if err := env.Put(context.Background(), vol, "config.yml", strings.NewReader("v3"), 0o644); err == nil || !strings.Contains(err.Error(), "held by node node-a") {
		t.Fatalf("cli put while mounted = %v", err)
	}
	for i, r := range readers {
		if err := r.Unmount(&volume.UnmountRequest{Name: vol, ID: fmt.Sprintf("r%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Unmount(&volume.UnmountRequest{Name: vol, ID: "w2"}); err != nil {
		t.Fatal(err)
	}
}
