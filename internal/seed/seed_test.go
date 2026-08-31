package seed

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
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
