package replica

import (
	"context"
	"os"
	"sort"
	"sync"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

// LazyImage leaves the local sparse image empty and hydrates immutable S3
// segments when NBD first reads their blocks. A fetched segment populates all
// still-relevant blocks it owns, which makes nearby reads local afterward.
type LazyImage struct {
	mu         sync.Mutex
	store      objectstore.Store
	manifests  *manifestSourceLoader
	file       *os.File
	size       int64
	refs       []ObjectRef
	sources    []lazyExtent
	loaded     []bool
	overridden []Extent
	ctx        context.Context
	cancel     context.CancelFunc
	closed     bool
}

type lazyExtent struct {
	Extent
	ref int
}

func (r *Remote) PrepareLazyRestore(ctx context.Context, state State, path string) (*LazyImage, error) {
	if err := validateState(state.Name, state); err != nil {
		return nil, err
	}
	refs, err := r.restorePlan(ctx, state)
	if err != nil {
		return nil, err
	}
	f, err := createSparseImage(path, state.Size)
	if err != nil {
		return nil, err
	}
	readCtx, cancel := context.WithCancel(context.Background())
	lazy := &LazyImage{
		store: r.Store, manifests: newManifestSourceLoader(r.Store, state), file: f, size: state.Size, refs: refs,
		loaded: make([]bool, len(refs)), ctx: readCtx, cancel: cancel,
	}
	for refIndex, ref := range refs {
		for _, extent := range ref.Extents {
			lazy.sources = append(lazy.sources, lazyExtent{Extent: extent, ref: refIndex})
		}
	}
	sort.Slice(lazy.sources, func(i, j int) bool { return lazy.sources[i].Start < lazy.sources[j].Start })
	return lazy, nil
}

// PrepareLazyRestore builds a lazy image from the lease's current head and
// keeps the same extent plan for later metadata-only checkpoints.
func (l *Lease) PrepareLazyRestore(ctx context.Context, path string) (*LazyImage, error) {
	state := l.State()
	image, err := l.remote.PrepareLazyRestore(ctx, state, path)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.state.Generation == state.Generation && l.state.Manifest == state.Manifest {
		l.plan = checkpointReadyRefs(image.refs)
		l.planKnown = true
	}
	l.mu.Unlock()
	return image, nil
}

func (l *LazyImage) Hydrate(off, length int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.hydrateLocked(byteRangeExtents(off, length, l.size))
}

func (l *LazyImage) PrepareWrite(off, length int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrDeviceClosed
	}
	if length <= 0 {
		return nil
	}
	end := off + length
	var boundaries []Extent
	if off%DefaultBlockSize != 0 {
		boundaries = append(boundaries, Extent{Start: uint64(off / DefaultBlockSize), Blocks: 1})
	}
	if end%DefaultBlockSize != 0 {
		last := uint64((end - 1) / DefaultBlockSize)
		if len(boundaries) == 0 || boundaries[0].Start != last {
			boundaries = append(boundaries, Extent{Start: last, Blocks: 1})
		}
	}
	return l.hydrateLocked(boundaries)
}

func (l *LazyImage) CommitWrite(off, length int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.overridden = unionExtents(l.overridden, byteRangeExtents(off, length, l.size))
	}
}

func (l *LazyImage) hydrateLocked(requested []Extent) error {
	if l.closed {
		return ErrDeviceClosed
	}
	if len(requested) == 0 {
		return nil
	}
	wanted := make(map[int]struct{})
	first := sort.Search(len(l.sources), func(i int) bool { return l.sources[i].end() > requested[0].Start })
	for index := first; index < len(l.sources); index++ {
		source := l.sources[index]
		if source.Start >= requested[len(requested)-1].end() {
			break
		}
		if l.loaded[source.ref] || len(intersectExtent(source.Extent, requested)) == 0 {
			continue
		}
		visible := subtractExtents(intersectExtent(source.Extent, requested), l.overridden)
		if len(visible) > 0 {
			wanted[source.ref] = struct{}{}
		}
	}
	indexes := make([]int, 0, len(wanted))
	for index := range wanted {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		selected := subtractExtents(l.refs[index].Extents, l.overridden)
		if len(selected) == 0 {
			l.loaded[index] = true
			continue
		}
		ref := l.refs[index]
		ref.Extents = selected
		if err := applyObjectRef(l.ctx, l.store, l.manifests, l.file, l.size, ref); err != nil {
			return err
		}
		l.loaded[index] = true
	}
	return nil
}

func (l *LazyImage) Close() error {
	l.cancel()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.file.Close()
}
