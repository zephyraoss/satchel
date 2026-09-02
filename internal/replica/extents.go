package replica

import (
	"errors"
	"fmt"
	"sort"
)

func validateExtents(extents []Extent, maxBlocks uint64) (uint64, error) {
	var total uint64
	var previousEnd uint64
	for i, extent := range extents {
		end := extent.end()
		if extent.Blocks == 0 || end < extent.Start || end > maxBlocks {
			return 0, fmt.Errorf("invalid block extent at index %d", i)
		}
		if i > 0 && extent.Start < previousEnd {
			return 0, errors.New("block extents overlap or are not sorted")
		}
		if total > ^uint64(0)-extent.Blocks {
			return 0, errors.New("block extent count overflows")
		}
		total += extent.Blocks
		previousEnd = end
	}
	return total, nil
}

func extentsContain(outer, inner []Extent) bool {
	outerIndex := 0
	for _, candidate := range inner {
		cursor := candidate.Start
		for cursor < candidate.end() {
			for outerIndex < len(outer) && outer[outerIndex].end() <= cursor {
				outerIndex++
			}
			if outerIndex == len(outer) || outer[outerIndex].Start > cursor {
				return false
			}
			cursor = min(candidate.end(), outer[outerIndex].end())
		}
	}
	return true
}

func subtractExtents(extents, removed []Extent) []Extent {
	if len(extents) == 0 || len(removed) == 0 {
		return append([]Extent(nil), extents...)
	}
	result := make([]Extent, 0, len(extents))
	removedIndex := sort.Search(len(removed), func(i int) bool {
		return removed[i].end() > extents[0].Start
	})
	for _, extent := range extents {
		cursor := extent.Start
		end := extent.end()
		for removedIndex < len(removed) && removed[removedIndex].end() <= cursor {
			removedIndex++
		}
		for i := removedIndex; i < len(removed) && removed[i].Start < end; i++ {
			if removed[i].Start > cursor {
				result = append(result, Extent{Start: cursor, Blocks: min(end, removed[i].Start) - cursor})
			}
			if removed[i].end() > cursor {
				cursor = removed[i].end()
			}
			if cursor >= end {
				break
			}
		}
		if cursor < end {
			result = append(result, Extent{Start: cursor, Blocks: end - cursor})
		}
	}
	return result
}

// unionExtents merges an already normalized left set with an arbitrary right
// set and returns sorted, disjoint extents.
func unionExtents(left, right []Extent) []Extent {
	if len(left) == 0 && len(right) == 0 {
		return nil
	}
	right = normalizeExtents(right)
	result := make([]Extent, 0, len(left)+len(right))
	appendExtent := func(extent Extent) {
		if extent.Blocks == 0 {
			return
		}
		if len(result) == 0 || result[len(result)-1].end() < extent.Start {
			result = append(result, extent)
			return
		}
		if extent.end() > result[len(result)-1].end() {
			result[len(result)-1].Blocks = extent.end() - result[len(result)-1].Start
		}
	}
	leftIndex, rightIndex := 0, 0
	for leftIndex < len(left) || rightIndex < len(right) {
		if rightIndex == len(right) || leftIndex < len(left) && left[leftIndex].Start <= right[rightIndex].Start {
			appendExtent(left[leftIndex])
			leftIndex++
		} else {
			appendExtent(right[rightIndex])
			rightIndex++
		}
	}
	return result
}

func normalizeExtents(extents []Extent) []Extent {
	if len(extents) == 0 {
		return nil
	}
	result := append([]Extent(nil), extents...)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Start == result[j].Start {
			return result[i].Blocks < result[j].Blocks
		}
		return result[i].Start < result[j].Start
	})
	write := 0
	for _, extent := range result {
		if extent.Blocks == 0 {
			continue
		}
		if write == 0 || result[write-1].end() < extent.Start {
			result[write] = extent
			write++
			continue
		}
		if extent.end() > result[write-1].end() {
			result[write-1].Blocks = extent.end() - result[write-1].Start
		}
	}
	return result[:write]
}

func byteRangeExtents(off, length, size int64) []Extent {
	if length <= 0 || off < 0 || off >= size {
		return nil
	}
	if length > size-off {
		length = size - off
	}
	start := uint64(off / DefaultBlockSize)
	end := uint64((off+length-1)/DefaultBlockSize) + 1
	return []Extent{{Start: start, Blocks: end - start}}
}
