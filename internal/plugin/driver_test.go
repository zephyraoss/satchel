package plugin

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-plugins-helpers/volume"

	"github.com/zephyraoss/satchel/internal/backend/pack"
	"github.com/zephyraoss/satchel/internal/lease"
	"github.com/zephyraoss/satchel/internal/litestream/litestreamtest"
	"github.com/zephyraoss/satchel/internal/objectstore"
)

func (m *memFetcher) Fetch(ctx context.Context, _ string, key string) (io.ReadCloser, error) {
	obj, err := m.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(obj.Data)), nil
}

type memFetcher struct{ *objectstore.Memory }

type cluster struct {
	store *memFetcher
	ls    *litestreamtest.Fake
}

func newCluster() *cluster {
	store := objectstore.NewMemory()
	return &cluster{store: &memFetcher{store}, ls: litestreamtest.New(store)}
}

func (c *cluster) node(t *testing.T, id string) *Driver {
	t.Helper()
	d, err := New(Config{NodeID: id, StateDir: t.TempDir()}, c.store, &lease.Manager{Store: c.store, NodeID: id, TTL: time.Second}, c.ls, pack.New())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestRedeployRoundTripPreservesFiles(t *testing.T) {
	c := newCluster()
	a, b := c.node(t, "node-a"), c.node(t, "node-b")

	if err := a.Create(&volume.CreateRequest{Name: "data"}); err != nil {
		t.Fatal(err)
	}
	resp, err := a.Mount(&volume.MountRequest{Name: "data", ID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(resp.Mountpoint, "hello.txt"), []byte("from node a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := a.Unmount(&volume.UnmountRequest{Name: "data", ID: "c1"}); err != nil {
		t.Fatal(err)
	}

	resp, err = b.Mount(&volume.MountRequest{Name: "data", ID: "c2"})
	if err != nil {
		t.Fatalf("mount on node b (volume never Created there): %v", err)
	}
	got, err := os.ReadFile(filepath.Join(resp.Mountpoint, "hello.txt"))
	if err != nil || string(got) != "from node a" {
		t.Fatalf("content on node b = %q, err=%v", got, err)
	}
	if err := os.WriteFile(filepath.Join(resp.Mountpoint, "more.txt"), []byte("from node b"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.Unmount(&volume.UnmountRequest{Name: "data", ID: "c2"}); err != nil {
		t.Fatal(err)
	}

	resp, err = a.Mount(&volume.MountRequest{Name: "data", ID: "c3"})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Unmount(&volume.UnmountRequest{Name: "data", ID: "c3"})
	for file, want := range map[string]string{"hello.txt": "from node a", "more.txt": "from node b"} {
		got, err := os.ReadFile(filepath.Join(resp.Mountpoint, file))
		if err != nil || string(got) != want {
			t.Fatalf("%s back on node a = %q, err=%v", file, got, err)
		}
	}
}

func TestSecondNodeMountFailsWhileHeld(t *testing.T) {
	c := newCluster()
	a, b := c.node(t, "node-a"), c.node(t, "node-b")
	if err := a.Create(&volume.CreateRequest{Name: "data"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Create(&volume.CreateRequest{Name: "data"}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Mount(&volume.MountRequest{Name: "data", ID: "c1"}); err != nil {
		t.Fatal(err)
	}
	_, err := b.Mount(&volume.MountRequest{Name: "data", ID: "c2"})
	if err == nil || !strings.Contains(err.Error(), "volume data is held by node node-a") {
		t.Fatalf("second mount err = %v", err)
	}
	if err := b.Remove(&volume.RemoveRequest{Name: "data"}); err == nil {
		t.Fatal("remove while held on another node should fail")
	}
	if err := a.Unmount(&volume.UnmountRequest{Name: "data", ID: "c1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Mount(&volume.MountRequest{Name: "data", ID: "c2"}); err != nil {
		t.Fatalf("mount after release: %v", err)
	}
}

func TestMountRefcountOnSameNode(t *testing.T) {
	c := newCluster()
	a := c.node(t, "node-a")
	if err := a.Create(&volume.CreateRequest{Name: "data"}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Mount(&volume.MountRequest{Name: "data", ID: "c1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Mount(&volume.MountRequest{Name: "data", ID: "c2"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Unmount(&volume.UnmountRequest{Name: "data", ID: "c1"}); err != nil {
		t.Fatal(err)
	}
	if rec, _ := (&lease.Manager{Store: c.store}).Inspect(context.Background(), "data"); rec == nil {
		t.Fatal("lease released while a mount is still active")
	}
	if err := a.Unmount(&volume.UnmountRequest{Name: "data", ID: "c2"}); err != nil {
		t.Fatal(err)
	}
	if rec, _ := (&lease.Manager{Store: c.store}).Inspect(context.Background(), "data"); rec != nil {
		t.Fatal("lease not released after last unmount")
	}
}

func TestLostLeaseFencesVolume(t *testing.T) {
	c := newCluster()
	a := c.node(t, "node-a")
	if err := a.Create(&volume.CreateRequest{Name: "data"}); err != nil {
		t.Fatal(err)
	}
	resp, err := a.Mount(&volume.MountRequest{Name: "data", ID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(resp.Mountpoint, "doomed.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := c.store.Delete(context.Background(), lease.Key("data")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		get, _ := a.Get(&volume.GetRequest{Name: "data"})
		if _, fenced := get.Volume.Status["fenced"]; fenced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("volume was not fenced after lease loss")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := a.Mount(&volume.MountRequest{Name: "data", ID: "c9"}); err == nil {
		t.Fatal("mount on fenced volume should fail")
	}
	if err := a.Unmount(&volume.UnmountRequest{Name: "data", ID: "c1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.Get(context.Background(), c.ls.Key("data")); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("fenced node replicated anyway: err=%v", err)
	}
	if _, err := a.Mount(&volume.MountRequest{Name: "data", ID: "c2"}); err != nil {
		t.Fatalf("remount after fence cleared: %v", err)
	}
}

func TestRemoveDeletesRemote(t *testing.T) {
	c := newCluster()
	a := c.node(t, "node-a")
	if err := a.Create(&volume.CreateRequest{Name: "data"}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Mount(&volume.MountRequest{Name: "data", ID: "c1"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Unmount(&volume.UnmountRequest{Name: "data", ID: "c1"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Remove(&volume.RemoveRequest{Name: "data"}); err != nil {
		t.Fatal(err)
	}
	keys, _ := c.store.List(context.Background(), "")
	if len(keys) != 0 {
		t.Fatalf("remote objects left behind: %v", keys)
	}
	if _, err := a.Get(&volume.GetRequest{Name: "data"}); err == nil {
		t.Fatal("volume still exists after remove")
	}
}
