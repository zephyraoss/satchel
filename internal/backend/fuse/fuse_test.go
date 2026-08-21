package fuse

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/store"
)

func mountTest(t *testing.T) (string, string) {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vol.db")
	mountpoint := filepath.Join(dir, "mnt")
	um, err := New(Options{}).Mount(context.Background(), dbPath, mountpoint, backend.MountOptions{})
	if err != nil {
		t.Skipf("fuse mount unavailable: %v", err)
	}
	t.Cleanup(func() {
		if err := um.Unmount(context.Background()); err != nil {
			t.Errorf("unmount: %v", err)
		}
	})
	return mountpoint, dbPath
}

var _ backend.Backend = (*Backend)(nil)

func TestBasicFileOperations(t *testing.T) {
	mnt, _ := mountTest(t)
	path := filepath.Join(mnt, "hello.txt")
	if err := os.WriteFile(path, []byte("hello world"), 0o640); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "hello world" {
		t.Fatalf("read = %q, %v", got, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o640 || info.Size() != 11 {
		t.Fatalf("stat = %v %v", info, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("HELLO"), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(8); err != nil {
		t.Fatal(err)
	}
	f.Close()
	got, _ = os.ReadFile(path)
	if string(got) != "HELLO wo" {
		t.Fatalf("after overwrite+truncate = %q", got)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	when := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
	info, _ = os.Stat(path)
	if info.Mode().Perm() != 0o600 || !info.ModTime().Equal(when) {
		t.Fatalf("chmod/chtimes: %v %v", info.Mode(), info.ModTime())
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat after remove = %v", err)
	}
}

func TestLargeFileRoundTrip(t *testing.T) {
	mnt, _ := mountTest(t)
	payload := make([]byte, 3*store.DefaultChunkSize+777)
	rand.Read(payload)
	path := filepath.Join(mnt, "big.bin")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("large file mismatch: len=%d err=%v", len(got), err)
	}
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	defer f.Close()
	f.WriteAt([]byte("patch"), store.DefaultChunkSize-2)
	buf := make([]byte, 9)
	f.ReadAt(buf, store.DefaultChunkSize-4)
	want := append(append([]byte{}, payload[store.DefaultChunkSize-4:store.DefaultChunkSize-2]...), "patch"...)
	want = append(want, payload[store.DefaultChunkSize+3:store.DefaultChunkSize+5]...)
	if !bytes.Equal(buf, want) {
		t.Fatalf("boundary patch = %q want %q", buf, want)
	}
}

func TestDirectoriesRenameSymlinkHardlink(t *testing.T) {
	mnt, _ := mountTest(t)
	if err := os.MkdirAll(filepath.Join(mnt, "a/b/c"), 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(mnt, "a/b/c/f"), []byte("x"), 0o644)
	if err := os.Rename(filepath.Join(mnt, "a/b"), filepath.Join(mnt, "moved")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(filepath.Join(mnt, "moved/c/f")); err != nil {
		t.Fatalf("after dir rename: %v", err)
	}
	if err := os.Remove(filepath.Join(mnt, "moved")); err == nil {
		t.Fatal("rmdir of non-empty dir succeeded")
	}
	if err := os.Symlink("moved/c/f", filepath.Join(mnt, "lnk")); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(mnt, "lnk")); string(got) != "x" {
		t.Fatalf("through symlink = %q", got)
	}
	if err := os.Link(filepath.Join(mnt, "moved/c/f"), filepath.Join(mnt, "hard")); err != nil {
		t.Fatal(err)
	}
	var st syscall.Stat_t
	syscall.Stat(filepath.Join(mnt, "hard"), &st)
	if st.Nlink != 2 {
		t.Fatalf("nlink = %d", st.Nlink)
	}
	os.WriteFile(filepath.Join(mnt, "hard"), []byte("shared"), 0o644)
	if got, _ := os.ReadFile(filepath.Join(mnt, "moved/c/f")); string(got) != "shared" {
		t.Fatalf("hardlink not shared: %q", got)
	}
	entries, _ := os.ReadDir(mnt)
	names := []string{}
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 4 {
		t.Fatalf("readdir = %v", names)
	}
	if err := os.RemoveAll(filepath.Join(mnt, "a")); err != nil {
		t.Fatal(err)
	}
}

func TestUnlinkWhileOpen(t *testing.T) {
	mnt, dbPath := mountTest(t)
	path := filepath.Join(mnt, "tmp")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("scratch data")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	n, err := f.ReadAt(buf, 0)
	if err != nil && n == 0 {
		t.Fatalf("read after unlink: %v", err)
	}
	if string(buf[:n]) != "scratch data" {
		t.Fatalf("read after unlink = %q", buf[:n])
	}
	f.Close()
	time.Sleep(100 * time.Millisecond)

	db, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var inodes int
	db.View(context.Background(), func(tx *store.Tx) error {
		s, err := tx.Stats()
		inodes = int(s.Inodes)
		return err
	})
	if inodes != 1 {
		t.Fatalf("orphan not collected after close: %d inodes", inodes)
	}
}

func TestXattrsThroughKernel(t *testing.T) {
	mnt, _ := mountTest(t)
	path := filepath.Join(mnt, "x")
	os.WriteFile(path, nil, 0o644)
	if err := syscall.Setxattr(path, "user.key", []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := syscall.Getxattr(path, "user.key", buf)
	if err != nil || string(buf[:n]) != "value" {
		t.Fatalf("getxattr = %q %v", buf[:n], err)
	}
	n, err = syscall.Listxattr(path, buf)
	if err != nil || string(buf[:n]) != "user.key\x00" {
		t.Fatalf("listxattr = %q %v", buf[:n], err)
	}
	if err := syscall.Removexattr(path, "user.key"); err != nil {
		t.Fatal(err)
	}
	if _, err := syscall.Getxattr(path, "user.key", buf); !errors.Is(err, syscall.ENODATA) {
		t.Fatalf("after remove = %v", err)
	}
}

func TestStatfs(t *testing.T) {
	mnt, _ := mountTest(t)
	var st syscall.Statfs_t
	if err := syscall.Statfs(mnt, &st); err != nil {
		t.Fatal(err)
	}
	if st.Bsize != 4096 || st.Bavail == 0 {
		t.Fatalf("statfs = %+v", st)
	}
}

func TestDataSurvivesRemount(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vol.db")
	mnt := filepath.Join(dir, "mnt")
	ctx := context.Background()
	um, err := New(Options{}).Mount(ctx, dbPath, mnt, backend.MountOptions{})
	if err != nil {
		t.Skipf("fuse mount unavailable: %v", err)
	}
	os.MkdirAll(filepath.Join(mnt, "d"), 0o755)
	os.WriteFile(filepath.Join(mnt, "d/persist"), []byte("kept"), 0o644)
	if err := um.Unmount(ctx); err != nil {
		t.Fatal(err)
	}
	um, err = New(Options{}).Mount(ctx, dbPath, mnt, backend.MountOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer um.Unmount(ctx)
	if got, _ := os.ReadFile(filepath.Join(mnt, "d/persist")); string(got) != "kept" {
		t.Fatalf("after remount = %q", got)
	}
}

func TestBackpressureStallsAndResumes(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vol.db")
	mnt := filepath.Join(dir, "mnt")
	ctx := context.Background()
	um, err := New(Options{WALLimit: 3 << 20}).Mount(ctx, dbPath, mnt, backend.MountOptions{})
	if err != nil {
		t.Skipf("fuse mount unavailable: %v", err)
	}
	defer um.Unmount(ctx)

	holder, err := store.OpenRaw(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	holder.SetMaxOpenConns(1)
	reader, err := holder.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader.QueryRow(`SELECT COUNT(*) FROM inodes`).Scan(new(int))

	payload := make([]byte, 1<<20)
	f, err := os.Create(filepath.Join(mnt, "fill"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i := 0; i < 3; i++ {
		if _, err := f.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if pending := store.PendingWALBytes(dbPath); pending < 3<<20 {
		t.Fatalf("pending WAL only %d bytes; reader lock did not pin the WAL", pending)
	}

	done := make(chan error, 1)
	go func() {
		_, err := f.Write(payload)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("write completed despite WAL over limit (err=%v)", err)
	case <-time.After(500 * time.Millisecond):
	}

	reader.Rollback()
	if _, err := holder.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write after checkpoint: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("write did not resume after checkpoint")
	}
}
