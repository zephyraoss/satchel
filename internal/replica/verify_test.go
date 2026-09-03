package replica

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

func buildVerifiableVolume(t *testing.T) (*Remote, *objectstore.Memory, *Lease, string) {
	t.Helper()
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "verify-me", "node-a", CreateOptions{Size: 4096 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	small := func(block uint64, marker byte) Segment {
		segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{
			block: bytes.Repeat([]byte{marker}, DefaultBlockSize),
		}})
		if err != nil {
			t.Fatal(err)
		}
		return segment
	}
	external := func(start uint64) Segment {
		blocks := make(map[uint64][]byte)
		for i := uint64(0); i < 20; i++ {
			blocks[start+i] = incompressibleBlock(t, byte(i))
		}
		segment, err := EncodeSegment(&Generation{Blocks: blocks})
		if err != nil {
			t.Fatal(err)
		}
		return segment
	}
	for i := uint64(0); i < 40; i++ {
		if err := lease.Publish(ctx, small(i, byte('a'+i%20))); err != nil {
			t.Fatal(err)
		}
	}
	if err := lease.Publish(ctx, external(100)); err != nil {
		t.Fatal(err)
	}
	if err := lease.PublishCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	late := external(300)
	lateKey := segmentReference(lease.State(), late).Key
	if lateKey == "" {
		t.Fatal("late segment did not stay external")
	}
	if err := lease.Publish(ctx, late); err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 3; i++ {
		if err := lease.Publish(ctx, small(500+i, 'z')); err != nil {
			t.Fatal(err)
		}
	}
	return remote, store, lease, lateKey
}

func TestVerifyMetadataHealthyVolume(t *testing.T) {
	ctx := context.Background()
	remote, _, lease, _ := buildVerifiableVolume(t)
	state := lease.State()
	if state.ManifestBundle == nil {
		t.Fatal("volume did not roll a manifest bundle")
	}
	report, err := remote.VerifyMetadata(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Problems) != 0 {
		t.Fatalf("healthy volume reported problems: %v", report.Problems)
	}
	if report.Manifests != int(state.Generation) {
		t.Fatalf("manifests = %d, want %d", report.Manifests, state.Generation)
	}
	if report.TruncatedBelowCheckpoint {
		t.Fatal("complete history reported as truncated")
	}
	if report.Bundles < 1 {
		t.Fatalf("bundles = %d, want at least 1", report.Bundles)
	}
	if report.ExternalObjects != 2 {
		t.Fatalf("external objects = %d, want 2", report.ExternalObjects)
	}
	if report.RefsChecked < report.Manifests {
		t.Fatalf("refs checked = %d, want at least one per manifest", report.RefsChecked)
	}
	if report.BytesFetched == 0 {
		t.Fatal("external existence checks fetched no bytes")
	}
}

func TestVerifyMetadataReportsMissingExternalSegment(t *testing.T) {
	ctx := context.Background()
	remote, store, lease, lateKey := buildVerifiableVolume(t)
	if err := store.Delete(ctx, lateKey); err != nil {
		t.Fatal(err)
	}
	report, err := remote.VerifyMetadata(ctx, lease.State())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Problems) != 1 {
		t.Fatalf("problems = %v, want exactly the deleted segment", report.Problems)
	}
	if !strings.Contains(report.Problems[0], lateKey) {
		t.Fatalf("problem %q does not name the missing segment %s", report.Problems[0], lateKey)
	}
}

func TestVerifyMetadataReportsUnresolvableSourceManifest(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	remote := &Remote{Store: store}

	state := State{
		Format: Format, ID: "vol-id", Name: "dangling-source", Size: 64 * DefaultBlockSize,
		BlockSize: DefaultBlockSize, Filesystem: "ext4", Epoch: 1,
	}
	segment, err := EncodeSegment(&Generation{Blocks: map[uint64][]byte{3: bytes.Repeat([]byte{'d'}, DefaultBlockSize)}})
	if err != nil {
		t.Fatal(err)
	}
	inlineRef := segmentReference(state, segment)
	if len(inlineRef.InlineData) == 0 {
		t.Fatal("segment did not inline")
	}
	source := Manifest{
		Format: Format, VolumeID: state.ID, Generation: 1, Epoch: 1, Kind: "delta",
		Segments: []ObjectRef{inlineRef}, CreatedAt: time.Now().UTC(),
	}
	preparedSource, err := prepareManifest(state, source)
	if err != nil {
		t.Fatal(err)
	}

	staleRef := inlineRef
	staleRef.InlineData = nil
	staleRef.SourceManifest = preparedSource.key
	staleRef.SourceIndex = 0
	stateAtCheckpoint := state
	stateAtCheckpoint.Generation = 1
	checkpoint := Manifest{
		Format: Format, VolumeID: state.ID, Generation: 2, Epoch: 1, Kind: "checkpoint",
		Parent: preparedSource.key, Segments: []ObjectRef{staleRef}, CreatedAt: time.Now().UTC(),
	}
	preparedCheckpoint, err := prepareManifest(stateAtCheckpoint, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutIfAbsent(ctx, preparedCheckpoint.key, preparedCheckpoint.body); err != nil {
		t.Fatal(err)
	}

	state.Generation = 2
	state.Checkpoint = 2
	state.Manifest = preparedCheckpoint.key

	report, err := remote.VerifyMetadata(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Problems) != 1 {
		t.Fatalf("problems = %v, want exactly the dangling source", report.Problems)
	}
	if !strings.Contains(report.Problems[0], preparedSource.key) {
		t.Fatalf("problem %q does not name the unstored source manifest %s", report.Problems[0], preparedSource.key)
	}
	if !report.TruncatedBelowCheckpoint {
		t.Fatal("walk past the missing parent below the checkpoint must report truncation")
	}
}

func TestVerifyMetadataDeepDetectsCorruptSegment(t *testing.T) {
	ctx := context.Background()
	remote, store, lease, lateKey := buildVerifiableVolume(t)
	obj, err := store.Get(ctx, lateKey)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), obj.Data...)
	corrupt[len(corrupt)/2] ^= 0xff
	if _, err := store.PutIfMatch(ctx, lateKey, corrupt, obj.ETag); err != nil {
		t.Fatal(err)
	}
	state := lease.State()

	shallow, err := remote.VerifyMetadata(ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(shallow.Problems) != 0 {
		t.Fatalf("shallow verify flagged content it never hashed: %v", shallow.Problems)
	}

	deep, err := remote.VerifyMetadataWithOptions(ctx, state, VerifyOptions{Deep: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(deep.Problems) != 1 {
		t.Fatalf("deep problems = %v, want exactly the corrupted segment", deep.Problems)
	}
	if !strings.Contains(deep.Problems[0], lateKey) {
		t.Fatalf("problem %q does not name the corrupted segment %s", deep.Problems[0], lateKey)
	}
}
