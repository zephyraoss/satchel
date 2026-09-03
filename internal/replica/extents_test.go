package replica

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestUnionExtentsMergesSortedAndUnsortedSets(t *testing.T) {
	left := []Extent{{Start: 2, Blocks: 2}, {Start: 10, Blocks: 2}}
	right := []Extent{{Start: 12, Blocks: 2}, {Start: 0, Blocks: 2}, {Start: 7, Blocks: 2}, {Start: 3, Blocks: 5}}
	want := []Extent{{Start: 0, Blocks: 9}, {Start: 10, Blocks: 4}}
	if got := unionExtents(left, right); !reflect.DeepEqual(got, want) {
		t.Fatalf("unionExtents() = %v, want %v", got, want)
	}
}

func TestSubtractExtentsStartsAtFirstPossibleOverlap(t *testing.T) {
	extents := []Extent{{Start: 100, Blocks: 10}, {Start: 120, Blocks: 10}}
	removed := []Extent{{Start: 0, Blocks: 50}, {Start: 103, Blocks: 4}, {Start: 125, Blocks: 20}}
	want := []Extent{{Start: 100, Blocks: 3}, {Start: 107, Blocks: 3}, {Start: 120, Blocks: 5}}
	if got := subtractExtents(extents, removed); !reflect.DeepEqual(got, want) {
		t.Fatalf("subtractExtents() = %v, want %v", got, want)
	}
}

func TestExtentSetOperationsMatchBlockSets(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for iteration := 0; iteration < 1_000; iteration++ {
		leftInput := randomExtents(rng, 64)
		rightInput := randomExtents(rng, 64)
		left := normalizeExtents(leftInput)
		right := normalizeExtents(rightInput)

		assertExtentBlocks(t, unionExtents(left, right), extentBlocks(leftInput, rightInput))

		wantDifference := extentBlocks(leftInput)
		for block := range extentBlocks(rightInput) {
			delete(wantDifference, block)
		}
		assertExtentBlocks(t, subtractExtents(left, right), wantDifference)
	}
}

func randomExtents(rng *rand.Rand, maxBlock uint64) []Extent {
	extents := make([]Extent, rng.Intn(12))
	for index := range extents {
		start := uint64(rng.Intn(int(maxBlock)))
		extents[index] = Extent{Start: start, Blocks: uint64(rng.Intn(int(maxBlock-start)) + 1)}
	}
	return extents
}

func extentBlocks(groups ...[]Extent) map[uint64]struct{} {
	blocks := make(map[uint64]struct{})
	for _, extents := range groups {
		for _, extent := range extents {
			for block := extent.Start; block < extent.end(); block++ {
				blocks[block] = struct{}{}
			}
		}
	}
	return blocks
}

func assertExtentBlocks(t *testing.T, extents []Extent, want map[uint64]struct{}) {
	t.Helper()
	if _, err := validateExtents(extents, 64); err != nil {
		t.Fatalf("invalid result %v: %v", extents, err)
	}
	if got := extentBlocks(extents); !reflect.DeepEqual(got, want) {
		t.Fatalf("extent blocks = %v, want %v", got, want)
	}
}
