package fuse

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/zephyraoss/satchel/internal/backend"
)

func benchChunkSize() int64 {
	if v, err := strconv.ParseInt(os.Getenv("SATCHEL_BENCH_CHUNK"), 10, 64); err == nil {
		return v
	}
	return 0
}

func benchMount(b *testing.B) string {
	b.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		b.Skip("no /dev/fuse")
	}
	dir := b.TempDir()
	mnt := filepath.Join(dir, "mnt")
	um, err := New(Options{Logger: slog.New(slog.DiscardHandler), ChunkSize: benchChunkSize()}).Mount(context.Background(), filepath.Join(dir, "vol.db"), mnt, backend.MountOptions{})
	if err != nil {
		b.Skipf("fuse mount unavailable: %v", err)
	}
	b.Cleanup(func() { um.Unmount(context.Background()) })
	return mnt
}

func BenchmarkSmallFileCreate4K(b *testing.B) {
	mnt := benchMount(b)
	payload := make([]byte, 4096)
	rand.Read(payload)
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := os.WriteFile(filepath.Join(mnt, fmt.Sprintf("f%06d", i)), payload, 0o644); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSmallFileRead4K(b *testing.B) {
	mnt := benchMount(b)
	payload := make([]byte, 4096)
	rand.Read(payload)
	const files = 1000
	for i := 0; i < files; i++ {
		os.WriteFile(filepath.Join(mnt, fmt.Sprintf("f%06d", i)), payload, 0o644)
	}
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := os.ReadFile(filepath.Join(mnt, fmt.Sprintf("f%06d", i%files))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSequentialWrite1M(b *testing.B) {
	mnt := benchMount(b)
	payload := make([]byte, 1<<20)
	rand.Read(payload)
	f, err := os.Create(filepath.Join(mnt, "seq"))
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSequentialRead1M(b *testing.B) {
	mnt := benchMount(b)
	payload := make([]byte, 1<<20)
	rand.Read(payload)
	path := filepath.Join(mnt, "seq")
	f, _ := os.Create(path)
	const size = 64
	for i := 0; i < size; i++ {
		f.Write(payload)
	}
	f.Close()
	f, _ = os.Open(path)
	defer f.Close()
	buf := make([]byte, 1<<20)
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.ReadAt(buf, int64(i%size)<<20); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRandomWrite4K(b *testing.B) {
	mnt := benchMount(b)
	path := filepath.Join(mnt, "rand")
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	const size = 64 << 20
	f.Truncate(size)
	block := make([]byte, 4096)
	rand.Read(block)
	offsets := make([]int64, 4096)
	for i := range offsets {
		var r [8]byte
		rand.Read(r[:])
		offsets[i] = int64(uint64(r[0])<<8|uint64(r[1])) % (size / 4096) * 4096
	}
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.WriteAt(block, offsets[i%len(offsets)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStat(b *testing.B) {
	mnt := benchMount(b)
	path := filepath.Join(mnt, "s")
	os.WriteFile(path, []byte("x"), 0o644)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := os.Lstat(path); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRandomRead4KUncached(b *testing.B) {
	mnt := benchMount(b)
	path := filepath.Join(mnt, "rr")
	payload := make([]byte, 1<<20)
	rand.Read(payload)
	f, _ := os.Create(path)
	const size = 64 << 20
	for i := 0; i < size>>20; i++ {
		f.Write(payload)
	}
	f.Close()
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECT, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 4096)
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(i*7919%(size/4096)) * 4096
		if _, err := f.ReadAt(buf, off); err != nil {
			b.Fatal(err)
		}
	}
}
