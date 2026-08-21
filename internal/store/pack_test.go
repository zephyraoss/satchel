package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPackUnpackRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "a.txt"), "hello", 0o644)
	mustWrite(t, filepath.Join(src, "sub", "deep", "b.bin"), "\x00\x01\x02", 0o600)
	if err := os.MkdirAll(filepath.Join(src, "empty"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}

	db, err := Open(ctx, filepath.Join(t.TempDir(), "vol.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Pack(ctx, src); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	if err := db.Unpack(ctx, dst); err != nil {
		t.Fatal(err)
	}
	assertFile(t, filepath.Join(dst, "a.txt"), "hello", 0o644)
	assertFile(t, filepath.Join(dst, "sub", "deep", "b.bin"), "\x00\x01\x02", 0o600)
	if info, err := os.Stat(filepath.Join(dst, "empty")); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("empty dir: info=%v err=%v", info, err)
	}
	if target, err := os.Readlink(filepath.Join(dst, "link")); err != nil || target != "a.txt" {
		t.Fatalf("symlink: target=%q err=%v", target, err)
	}
}

func TestPackReplacesPreviousContents(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "vol.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	first := t.TempDir()
	mustWrite(t, filepath.Join(first, "old.txt"), "x", 0o644)
	if err := db.Pack(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := t.TempDir()
	mustWrite(t, filepath.Join(second, "new.txt"), "y", 0o644)
	if err := db.Pack(ctx, second); err != nil {
		t.Fatal(err)
	}
	entries, err := db.ListFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Path != "new.txt" {
		t.Fatalf("entries = %+v", entries)
	}
}

func mustWrite(t *testing.T, path, content string, perm os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), perm); err != nil {
		t.Fatal(err)
	}
}

func assertFile(t *testing.T, path, want string, perm os.FileMode) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != perm {
		t.Fatalf("%s perm = %o, want %o", path, info.Mode().Perm(), perm)
	}
}
