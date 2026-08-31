//go:build linux

package e2e

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
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
