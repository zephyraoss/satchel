package seed

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestApplyDirectory(t *testing.T) {
	source := t.TempDir()
	if err := os.Mkdir(filepath.Join(source, "config"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "config", "app.yml"), []byte("port: 80\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	count, err := (&Seeder{}).Apply(context.Background(), destination, source)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("entry count = %d, want 2", count)
	}
	data, err := os.ReadFile(filepath.Join(destination, "config", "app.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "port: 80\n" {
		t.Fatalf("seeded data = %q", data)
	}
}

func TestArchiveCannotEscapeDestination(t *testing.T) {
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	if err := tw.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	if _, err := importArchive(context.Background(), destination, &data, false); err == nil {
		t.Fatal("archive traversal was accepted")
	}
}

func TestArchiveCannotTraverseSymlinkParent(t *testing.T) {
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	entries := []struct {
		header tar.Header
		data   string
	}{
		{header: tar.Header{Name: "link", Linkname: "..", Typeflag: tar.TypeSymlink, Mode: 0o777}},
		{header: tar.Header{Name: "link/escape", Typeflag: tar.TypeReg, Mode: 0o600, Size: 1}, data: "x"},
	}
	for _, entry := range entries {
		if err := tw.WriteHeader(&entry.header); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(entry.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := importArchive(context.Background(), t.TempDir(), &data, false); err == nil {
		t.Fatal("archive symlink traversal was accepted")
	}
}

func TestApplyFollowsSymlinkedSourceDirectory(t *testing.T) {
	real := t.TempDir()
	if err := os.WriteFile(filepath.Join(real, "file"), []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	count, err := (&Seeder{}).Apply(context.Background(), destination, link)
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if _, err := os.Stat(filepath.Join(destination, "file")); err != nil {
		t.Fatal("symlinked source directory was not copied")
	}
}

func TestArchiveCannotWriteThroughSymlinkLeaf(t *testing.T) {
	outside := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(outside, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	if err := tw.WriteHeader(&tar.Header{Name: "leaf", Linkname: outside, Typeflag: tar.TypeSymlink, Mode: 0o777}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "leaf", Mode: 0o600, Size: 5, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("pwned")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := importArchive(context.Background(), t.TempDir(), &data, false); err == nil {
		t.Fatal("write through a symlink leaf was accepted")
	}
	got, _ := os.ReadFile(outside)
	if string(got) != "original" {
		t.Fatalf("file outside the destination was overwritten: %q", got)
	}
}

func TestArchiveRootEntryAndDirectoryTimesArePreserved(t *testing.T) {
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	for _, header := range []tar.Header{
		{Name: "./", Typeflag: tar.TypeDir, Mode: 0o700, ModTime: stamp},
		{Name: "dir/", Typeflag: tar.TypeDir, Mode: 0o755, ModTime: stamp},
		{Name: "dir/file", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1, ModTime: time.Now()},
	} {
		if err := tw.WriteHeader(&header); err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeReg {
			tw.Write([]byte("x"))
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	before, _ := os.Stat(destination)
	if _, err := importArchive(context.Background(), destination, &data, false); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(destination)
	if !after.ModTime().Equal(before.ModTime()) && after.ModTime().Equal(stamp) {
		t.Fatal("archive root entry rewrote the destination's own metadata")
	}
	dir, _ := os.Stat(filepath.Join(destination, "dir"))
	if !dir.ModTime().Equal(stamp) {
		t.Fatalf("directory mtime = %s, want %s", dir.ModTime(), stamp)
	}
}
