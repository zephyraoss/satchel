package replica

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

const (
	benchmarkGenerations         = 128
	benchmarkBlocksPerGeneration = 20
)

func benchmarkHistory(b *testing.B) (*Remote, *countingStore, *Lease) {
	b.Helper()
	ctx := context.Background()
	store := &countingStore{Store: objectstore.NewMemory()}
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "bench", "node-a", CreateOptions{
		Size:       benchmarkBlocksPerGeneration * benchmarkGenerations * DefaultBlockSize,
		Filesystem: "ext4",
	})
	if err != nil {
		b.Fatal(err)
	}
	for generation := range benchmarkGenerations {
		firstBlock := uint64(benchmarkBlocksPerGeneration * generation)
		value := byte(generation%251 + 1)
		blocks := make(map[uint64][]byte, benchmarkBlocksPerGeneration)
		for offset := range benchmarkBlocksPerGeneration {
			blocks[firstBlock+uint64(offset)] = incompressibleBlock(b, value)
		}
		segment, err := EncodeSegment(&Generation{Blocks: blocks})
		if err != nil {
			b.Fatal(err)
		}
		if len(segment.Data) <= inlineSegmentLimit {
			b.Fatalf("benchmark segment has %d bytes, want more than inline limit %d", len(segment.Data), inlineSegmentLimit)
		}
		if err := lease.Publish(ctx, segment); err != nil {
			b.Fatal(err)
		}
	}
	return remote, store, lease
}

func BenchmarkMetadataCheckpoint128(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		_, store, lease := benchmarkHistory(b)
		store.reset()
		b.StartTimer()
		if err := lease.PublishCheckpoint(ctx); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if gets := store.segmentGets(); gets != 0 {
			b.Fatalf("checkpoint fetched %d segment bodies", gets)
		}
		if gets := store.manifestGets(); gets != 0 {
			b.Fatalf("checkpoint fetched %d manifests", gets)
		}
	}
	b.ReportMetric(0, "segment_gets/op")
	b.ReportMetric(0, "manifest_gets/op")
}

func BenchmarkRestorePreparation128(b *testing.B) {
	ctx := context.Background()
	remote, store, lease := benchmarkHistory(b)
	if err := lease.PublishCheckpoint(ctx); err != nil {
		b.Fatal(err)
	}
	state := lease.State()

	b.Run("lazy", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "lazy.img")
		b.ReportAllocs()
		store.reset()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			lazy, err := remote.PrepareLazyRestore(ctx, state, path)
			if err != nil {
				b.Fatal(err)
			}
			if err := lazy.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if gets := store.segmentGets(); gets != 0 {
			b.Fatalf("lazy preparation fetched %d segment bodies", gets)
		}
		b.ReportMetric(0, "segment_gets/op")
	})

	b.Run("materialized", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "materialized.img")
		b.ReportAllocs()
		store.reset()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			if err := remote.RestoreState(ctx, state, path); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		gets := store.segmentGets()
		if want := benchmarkGenerations * b.N; gets != want {
			b.Fatalf("materialized restore fetched %d segment bodies, want %d", gets, want)
		}
		b.ReportMetric(float64(gets)/float64(b.N), "segment_gets/op")
	})
}
