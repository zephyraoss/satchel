package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-plugins-helpers/volume"

	"github.com/zephyraoss/satchel/internal/backend/pack"
	"github.com/zephyraoss/satchel/internal/lease"
	"github.com/zephyraoss/satchel/internal/litestream"
	"github.com/zephyraoss/satchel/internal/litestream/litestreamtest"
	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/plugin"
)

type harness struct {
	store  *objectstore.Memory
	ls     *litestreamtest.Fake
	env    *Env
	stdout *bytes.Buffer
	node   *plugin.Driver
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	store := objectstore.NewMemory()
	ls := litestreamtest.New(store)
	stdout := &bytes.Buffer{}
	env := &Env{
		Store:      store,
		Leases:     &lease.Manager{Store: store, NodeID: "cli@laptop", TTL: time.Second},
		Litestream: ls,
		WorkDir:    t.TempDir(),
		Stdout:     stdout,
		Stderr:     &bytes.Buffer{},
	}
	node, err := plugin.New(plugin.Config{NodeID: "node-a", StateDir: t.TempDir()}, store, &lease.Manager{Store: store, NodeID: "node-a", TTL: time.Second}, ls, pack.New())
	if err != nil {
		t.Fatal(err)
	}
	return &harness{store: store, ls: ls, env: env, stdout: stdout, node: node}
}

func (h *harness) seedVolume(t *testing.T, name string, files map[string]string) {
	t.Helper()
	if err := h.node.Create(&volume.CreateRequest{Name: name}); err != nil {
		t.Fatal(err)
	}
	m, err := h.node.Mount(&volume.MountRequest{Name: name, ID: "seed"})
	if err != nil {
		t.Fatal(err)
	}
	for rel, content := range files {
		os.MkdirAll(filepath.Dir(filepath.Join(m.Mountpoint, rel)), 0o755)
		os.WriteFile(filepath.Join(m.Mountpoint, rel), []byte(content), 0o644)
	}
	if err := h.node.Unmount(&volume.UnmountRequest{Name: name, ID: "seed"}); err != nil {
		t.Fatal(err)
	}
}

func TestLsCatLsFiles(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.seedVolume(t, "app", map[string]string{"config.yml": "port: 80\n", "uploads/a.txt": "A"})
	h.seedVolume(t, "other", map[string]string{"x": "y"})

	if err := h.env.ls(ctx); err != nil {
		t.Fatal(err)
	}
	out := h.stdout.String()
	if !strings.Contains(out, "app") || !strings.Contains(out, "other") {
		t.Fatalf("ls output:\n%s", out)
	}
	h.stdout.Reset()
	if err := h.env.Cat(ctx, "app", "config.yml", h.stdout); err != nil {
		t.Fatal(err)
	}
	if h.stdout.String() != "port: 80\n" {
		t.Fatalf("cat = %q", h.stdout.String())
	}
	h.stdout.Reset()
	if err := h.env.lsFiles(ctx, "app", "", true); err != nil {
		t.Fatal(err)
	}
	out = h.stdout.String()
	if !strings.Contains(out, "uploads/") || !strings.Contains(out, "uploads/a.txt") || !strings.Contains(out, "config.yml") {
		t.Fatalf("ls-files output:\n%s", out)
	}
	if err := h.env.Cat(ctx, "missing", "x", h.stdout); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("cat on missing volume = %v", err)
	}
}

func TestPutRoundTripsThroughNode(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.seedVolume(t, "app", map[string]string{"keep": "me"})
	if err := h.env.Put(ctx, "app", "new/dir/file.txt", strings.NewReader("from cli"), 0o600); err != nil {
		t.Fatal(err)
	}
	if rec, _ := h.env.Leases.Inspect(ctx, "app"); rec != nil {
		t.Fatal("cli left the lease held")
	}
	m, err := h.node.Mount(&volume.MountRequest{Name: "app", ID: "after"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.node.Unmount(&volume.UnmountRequest{Name: "app", ID: "after"})
	got, _ := os.ReadFile(filepath.Join(m.Mountpoint, "new/dir/file.txt"))
	if string(got) != "from cli" {
		t.Fatalf("node sees %q", got)
	}
	info, _ := os.Stat(filepath.Join(m.Mountpoint, "new/dir/file.txt"))
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("perm = %o", info.Mode().Perm())
	}
	if got, _ := os.ReadFile(filepath.Join(m.Mountpoint, "keep")); string(got) != "me" {
		t.Fatal("put clobbered other files")
	}
}

func TestWritesRefusedWhileMounted(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.seedVolume(t, "app", nil)
	if _, err := h.node.Mount(&volume.MountRequest{Name: "app", ID: "live"}); err != nil {
		t.Fatal(err)
	}
	defer h.node.Unmount(&volume.UnmountRequest{Name: "app", ID: "live"})
	err := h.env.Put(ctx, "app", "f", strings.NewReader("x"), 0o644)
	if err == nil || !strings.Contains(err.Error(), "held by node node-a") {
		t.Fatalf("put while mounted = %v", err)
	}
	if err := h.env.sql(ctx, "app", "SELECT COUNT(*) FROM inodes", false); err != nil {
		t.Fatalf("read-only sql while mounted should work: %v", err)
	}
	if err := h.env.sql(ctx, "app", "DELETE FROM inodes", true); err == nil {
		t.Fatal("sql --write while mounted should fail")
	}
}

func TestSQLAndRestore(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.seedVolume(t, "app", map[string]string{"a": "1", "b/c": "2"})
	if err := h.env.sql(ctx, "app", "SELECT name FROM dentries ORDER BY name", false); err != nil {
		t.Fatal(err)
	}
	if out := h.stdout.String(); !strings.Contains(out, "name\na\nb\nc\n") {
		t.Fatalf("sql output:\n%s", out)
	}
	dir := filepath.Join(t.TempDir(), "out")
	if err := h.env.restore(ctx, "app", dir, litestream.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "b", "c")); string(got) != "2" {
		t.Fatalf("restored b/c = %q", got)
	}
	if err := h.env.restore(ctx, "app", dir, litestream.RestoreOptions{}); err == nil {
		t.Fatal("restore into non-empty dir should fail")
	}
}

func TestLeaseStatusAndBreak(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.seedVolume(t, "app", nil)
	if _, err := h.node.Mount(&volume.MountRequest{Name: "app", ID: "live"}); err != nil {
		t.Fatal(err)
	}
	if err := h.env.leaseStatus(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(h.stdout.String(), "held by node-a") {
		t.Fatalf("status = %q", h.stdout.String())
	}
	if err := h.env.leaseBreak(ctx, "app", true); err != nil {
		t.Fatal(err)
	}
	if rec, _ := h.env.Leases.Inspect(ctx, "app"); rec != nil {
		t.Fatal("lease still present after break")
	}
	h.stdout.Reset()
	if err := h.env.leaseStatus(ctx, "app"); err != nil || !strings.Contains(h.stdout.String(), "not held") {
		t.Fatalf("status after break = %q %v", h.stdout.String(), err)
	}
}
