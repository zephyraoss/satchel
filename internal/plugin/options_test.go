package plugin

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/docker/go-plugins-helpers/volume"

	"github.com/zephyraoss/satchel/internal/backend/fuse"
	"github.com/zephyraoss/satchel/internal/backend/pack"
	"github.com/zephyraoss/satchel/internal/lease"
	"github.com/zephyraoss/satchel/internal/seed"
)

func TestParseVolumeOptions(t *testing.T) {
	opts, err := ParseVolumeOptions(map[string]string{"mode": "ro", "scope": "replica", "sync_interval": "2s"})
	if err != nil || !opts.ReadOnly() || !opts.PerReplica() || opts.SyncInterval != 2*time.Second {
		t.Fatalf("opts=%+v err=%v", opts, err)
	}
	for _, bad := range []map[string]string{
		{"mode": "rwx"}, {"scope": "node"}, {"sync_interval": "soon"}, {"class": "block"}, {"bogus": "1"}, {"mode": "ro", "seed": "/x"},
	} {
		if _, err := ParseVolumeOptions(bad); err == nil {
			t.Fatalf("expected error for %v", bad)
		}
	}
}

func TestReadOnlyMountSkipsLeaseAndReplication(t *testing.T) {
	c := newCluster()
	writer, reader := c.node(t, "node-a"), c.node(t, "node-b")
	if err := writer.Create(&volume.CreateRequest{Name: "cfg"}); err != nil {
		t.Fatal(err)
	}
	m, err := writer.Mount(&volume.MountRequest{Name: "cfg", ID: "w"})
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(m.Mountpoint, "app.yml"), []byte("v1"), 0o644)
	if err := writer.Unmount(&volume.UnmountRequest{Name: "cfg", ID: "w"}); err != nil {
		t.Fatal(err)
	}

	if err := reader.Create(&volume.CreateRequest{Name: "cfg", Options: map[string]string{"mode": "ro"}}); err != nil {
		t.Fatal(err)
	}
	rm, err := reader.Mount(&volume.MountRequest{Name: "cfg", ID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(rm.Mountpoint, "app.yml")); string(got) != "v1" {
		t.Fatalf("ro content = %q", got)
	}
	if rec, _ := (&lease.Manager{Store: c.store}).Inspect(context.Background(), "cfg"); rec != nil {
		t.Fatal("read-only mount took the lease")
	}
	if _, err := writer.Mount(&volume.MountRequest{Name: "cfg", ID: "w2"}); err != nil {
		t.Fatalf("writer blocked by a read-only mount: %v", err)
	}
	os.WriteFile(filepath.Join(rm.Mountpoint, "app.yml"), []byte("tampered"), 0o644)
	if err := reader.Unmount(&volume.UnmountRequest{Name: "cfg", ID: "r1"}); err != nil {
		t.Fatal(err)
	}
	writer.Unmount(&volume.UnmountRequest{Name: "cfg", ID: "w2"})
	obj, err := c.store.Get(context.Background(), c.ls.Key("cfg"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(obj.Data, []byte("tampered")) {
		t.Fatal("read-only node replicated its local changes")
	}
}

func TestReadOnlyFuseMountRejectsWrites(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse")
	}
	c := newCluster()
	d, err := New(Config{NodeID: "n", StateDir: t.TempDir()}, c.store, &lease.Manager{Store: c.store, NodeID: "n"}, c.ls, fuse.New(fuse.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Create(&volume.CreateRequest{Name: "ro", Options: map[string]string{"mode": "ro"}}); err != nil {
		t.Fatal(err)
	}
	m, err := d.Mount(&volume.MountRequest{Name: "ro", ID: "x"})
	if err != nil {
		t.Skipf("fuse unavailable: %v", err)
	}
	defer d.Unmount(&volume.UnmountRequest{Name: "ro", ID: "x"})
	err = os.WriteFile(filepath.Join(m.Mountpoint, "nope"), []byte("x"), 0o644)
	if pe, ok := err.(*os.PathError); !ok || pe.Err != syscall.EROFS {
		t.Fatalf("write on ro mount = %v, want EROFS", err)
	}
}

func TestReplicaScopeGivesEachMountItsOwnVolume(t *testing.T) {
	c := newCluster()
	a := c.node(t, "node-a")
	if err := a.Create(&volume.CreateRequest{Name: "scratch", Options: map[string]string{"scope": "replica"}}); err != nil {
		t.Fatal(err)
	}
	m1, err := a.Mount(&volume.MountRequest{Name: "scratch", ID: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	m2, err := a.Mount(&volume.MountRequest{Name: "scratch", ID: "container-2"})
	if err != nil {
		t.Fatalf("second replica refused: %v", err)
	}
	if m1.Mountpoint == m2.Mountpoint {
		t.Fatal("replicas share a mountpoint")
	}
	os.WriteFile(filepath.Join(m1.Mountpoint, "who"), []byte("one"), 0o644)
	os.WriteFile(filepath.Join(m2.Mountpoint, "who"), []byte("two"), 0o644)
	for _, id := range []string{"container-1", "container-2"} {
		if err := a.Unmount(&volume.UnmountRequest{Name: "scratch", ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	b := c.node(t, "node-b")
	b.Create(&volume.CreateRequest{Name: "scratch", Options: map[string]string{"scope": "replica"}})
	m, err := b.Mount(&volume.MountRequest{Name: "scratch", ID: "container-2"})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(m.Mountpoint, "who")); string(got) != "two" {
		t.Fatalf("replica volume did not follow its mount id: %q", got)
	}
	b.Unmount(&volume.UnmountRequest{Name: "scratch", ID: "container-2"})
	keys, _ := c.store.List(context.Background(), "vols/scratch.r-")
	if len(keys) != 2 {
		t.Fatalf("expected two replica volumes in bucket, got %v", keys)
	}
}

func TestSeedFromDirectoryAndTarball(t *testing.T) {
	c := newCluster()
	srcDir := t.TempDir()
	os.MkdirAll(filepath.Join(srcDir, "conf.d"), 0o755)
	os.WriteFile(filepath.Join(srcDir, "conf.d", "main.conf"), []byte("seeded"), 0o600)

	var tarball bytes.Buffer
	gz := gzip.NewWriter(&tarball)
	tw := tar.NewWriter(gz)
	tw.WriteHeader(&tar.Header{Name: "data/", Typeflag: tar.TypeDir, Mode: 0o755})
	tw.WriteHeader(&tar.Header{Name: "data/hello.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: 5})
	tw.Write([]byte("world"))
	tw.WriteHeader(&tar.Header{Name: "data/link", Typeflag: tar.TypeSymlink, Linkname: "hello.txt", Mode: 0o777})
	tw.Close()
	gz.Close()
	c.store.PutIfAbsent(context.Background(), "seeds/app.tgz", tarball.Bytes())

	d, err := New(Config{NodeID: "n", StateDir: t.TempDir(), Seeder: &seed.Seeder{Fetch: c.store.Fetch}}, c.store, &lease.Manager{Store: c.store, NodeID: "n"}, c.ls, pack.New())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Create(&volume.CreateRequest{Name: "fromdir", Options: map[string]string{"seed": srcDir}}); err != nil {
		t.Fatal(err)
	}
	m, err := d.Mount(&volume.MountRequest{Name: "fromdir", ID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(m.Mountpoint, "conf.d", "main.conf")); string(got) != "seeded" {
		t.Fatalf("dir seed = %q", got)
	}
	os.WriteFile(filepath.Join(m.Mountpoint, "conf.d", "main.conf"), []byte("edited"), 0o600)
	d.Unmount(&volume.UnmountRequest{Name: "fromdir", ID: "1"})
	m, _ = d.Mount(&volume.MountRequest{Name: "fromdir", ID: "2"})
	if got, _ := os.ReadFile(filepath.Join(m.Mountpoint, "conf.d", "main.conf")); string(got) != "edited" {
		t.Fatalf("seed re-applied over existing data: %q", got)
	}
	d.Unmount(&volume.UnmountRequest{Name: "fromdir", ID: "2"})

	if err := d.Create(&volume.CreateRequest{Name: "fromtar", Options: map[string]string{"seed": "s3://whatever/seeds/app.tgz"}}); err != nil {
		t.Fatal(err)
	}
	m, err = d.Mount(&volume.MountRequest{Name: "fromtar", ID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Unmount(&volume.UnmountRequest{Name: "fromtar", ID: "1"})
	if got, _ := os.ReadFile(filepath.Join(m.Mountpoint, "data", "hello.txt")); string(got) != "world" {
		t.Fatalf("tar seed = %q", got)
	}
	if target, _ := os.Readlink(filepath.Join(m.Mountpoint, "data", "link")); target != "hello.txt" {
		t.Fatalf("tar symlink = %q", target)
	}
}
