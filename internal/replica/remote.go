package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

const Format = "satchel-block-v1"

var validVolumeName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

type LeaseRecord struct {
	Holder    string    `json:"holder"`
	Token     string    `json:"token"`
	Epoch     uint64    `json:"epoch"`
	ExpiresAt time.Time `json:"expires_at"`
	MountedAt time.Time `json:"mounted_at"`
}

type State struct {
	Format          string                     `json:"format"`
	ID              string                     `json:"id"`
	Name            string                     `json:"name"`
	Size            int64                      `json:"size"`
	BlockSize       int64                      `json:"block_size"`
	Filesystem      string                     `json:"filesystem"`
	Generation      uint64                     `json:"generation"`
	Manifest        string                     `json:"manifest,omitempty"`
	InlineManifests map[string]json.RawMessage `json:"inline_manifests,omitempty"`
	ManifestBundle  *ManifestBundleRef         `json:"manifest_bundle,omitempty"`
	Checkpoint      uint64                     `json:"checkpoint_generation,omitempty"`
	Epoch           uint64                     `json:"epoch"`
	Deleting        bool                       `json:"deleting,omitempty"`
	Lease           *LeaseRecord               `json:"lease,omitempty"`
}

type Manifest struct {
	Format     string      `json:"format"`
	VolumeID   string      `json:"volume_id"`
	Generation uint64      `json:"generation"`
	Epoch      uint64      `json:"epoch"`
	Kind       string      `json:"kind"`
	Parent     string      `json:"parent,omitempty"`
	Segments   []ObjectRef `json:"segments"`
	CreatedAt  time.Time   `json:"created_at"`
}

type ObjectRef struct {
	Key            string   `json:"key,omitempty"`
	SourceManifest string   `json:"source_manifest,omitempty"`
	SourceBundle   string   `json:"source_bundle,omitempty"`
	SourceIndex    uint32   `json:"source_index,omitempty"`
	InlineData     []byte   `json:"inline_data,omitempty"`
	SHA256         string   `json:"sha256"`
	Bytes          int64    `json:"bytes"`
	Blocks         int64    `json:"blocks"`
	Extents        []Extent `json:"extents"`
	ZeroExtents    []Extent `json:"zero_extents,omitempty"`
}

const (
	inlineSegmentLimit       = 64 << 10
	inlineManifestLimit      = 64 << 10
	inlineManifestStateLimit = 64 << 10
	manifestBundleTarget     = 16 << 10
)

type ManifestBundleRef struct {
	Key             string `json:"key"`
	FirstGeneration uint64 `json:"first_generation"`
	LastGeneration  uint64 `json:"last_generation"`
}

type manifestBundle struct {
	Format          string             `json:"format"`
	VolumeID        string             `json:"volume_id"`
	FirstGeneration uint64             `json:"first_generation"`
	LastGeneration  uint64             `json:"last_generation"`
	Parent          *ManifestBundleRef `json:"parent,omitempty"`
	Manifests       []bundledManifest  `json:"manifests"`
}

type bundledManifest struct {
	Key  string          `json:"key"`
	Body json.RawMessage `json:"body"`
}

type RestoreOptions struct {
	Generation uint64
	Timestamp  time.Time
}

type GCOptions struct {
	HistoryRetention time.Duration
	GracePeriod      time.Duration
}

type GCResult struct {
	Marked  int
	Deleted int
}

type gcRecord struct {
	Format    string    `json:"format"`
	VolumeID  string    `json:"volume_id"`
	CreatedAt time.Time `json:"created_at"`
	Objects   []string  `json:"objects"`
}

type CreateOptions struct {
	Size       int64
	Filesystem string
}

type Remote struct {
	Store             objectstore.Store
	Head              HeadMode
	Settle            time.Duration
	TTL               time.Duration
	Now               func() time.Time
	Observe           func(stage string, duration time.Duration)
	ObserveGeneration func(inputBytes, storedBytes int64, segments int)

	headOnce   sync.Once
	volumeHead volumeHead
}

func (r *Remote) observeGeneration(generation *Generation, segments []Segment) {
	if r.ObserveGeneration == nil {
		return
	}
	var stored int64
	for _, segment := range segments {
		stored += segment.Bytes
	}
	r.ObserveGeneration(generation.Bytes(), stored, len(segments))
}

func (r *Remote) observe(stage string, started time.Time) {
	if r.Observe != nil {
		r.Observe(stage, time.Since(started))
	}
}

func StateKey(name string) string     { return "volumes/" + name + "/state.json" }
func VolumePrefix(name string) string { return "volumes/" + name + "/" }

func (r *Remote) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func (r *Remote) ttl() time.Duration {
	if r.TTL > 0 {
		return r.TTL
	}
	return 30 * time.Second
}

func (r *Remote) Inspect(ctx context.Context, name string) (State, string, error) {
	body, cursor, err := r.head().read(ctx, name)
	if err != nil {
		return State{}, "", err
	}
	var state State
	if err := json.Unmarshal(body, &state); err != nil {
		return State{}, "", fmt.Errorf("decode volume state: %w", err)
	}
	if err := validateState(name, state); err != nil {
		return State{}, "", err
	}
	return state, cursor, nil
}

func (r *Remote) readHeadBody(ctx context.Context, name string) ([]byte, string, error) {
	return r.head().read(ctx, name)
}

func validateState(name string, state State) error {
	if state.Format != Format {
		return fmt.Errorf("volume %s uses unsupported format %q", name, state.Format)
	}
	if state.Name != name || state.ID == "" {
		return fmt.Errorf("volume %s has corrupt identity", name)
	}
	if state.Size <= 0 || state.Size%DefaultBlockSize != 0 || state.BlockSize != DefaultBlockSize {
		return fmt.Errorf("volume %s has invalid geometry", name)
	}
	if state.Checkpoint > state.Generation {
		return fmt.Errorf("volume %s has an invalid checkpoint generation", name)
	}
	if (state.Generation == 0) != (state.Manifest == "") {
		return fmt.Errorf("volume %s has inconsistent generation metadata", name)
	}
	inlineBytes := 0
	for key, body := range state.InlineManifests {
		inlineBytes += len(body)
		if inlineBytes > inlineManifestStateLimit {
			return fmt.Errorf("volume %s has too much inline manifest data", name)
		}
		if _, err := decodeManifest(state, key, body, 0); err != nil {
			return err
		}
	}
	if bundle := state.ManifestBundle; bundle != nil {
		if !validManifestBundleRef(state.Name, *bundle) {
			return fmt.Errorf("volume %s has an invalid manifest bundle", name)
		}
	}
	return nil
}

func validManifestBundleRef(name string, ref ManifestBundleRef) bool {
	prefix := VolumePrefix(name) + "manifest-bundles/"
	return strings.HasPrefix(ref.Key, prefix) && strings.HasSuffix(ref.Key, ".json") &&
		ref.FirstGeneration > 0 && ref.LastGeneration >= ref.FirstGeneration
}

type HeldError struct {
	Volume    string
	Holder    string
	ExpiresAt time.Time
}

func (e *HeldError) Error() string {
	return fmt.Sprintf("volume %s is held by node %s until %s", e.Volume, e.Holder, e.ExpiresAt.Format(time.RFC3339))
}

var ErrLeaseLost = errors.New("volume lease lost")

func (r *Remote) Acquire(ctx context.Context, name, holder string, opts CreateOptions) (*Lease, bool, error) {
	if !validVolumeName.MatchString(name) {
		return nil, false, fmt.Errorf("invalid volume name %q", name)
	}
	now := r.now()
	state, etag, err := r.Inspect(ctx, name)
	created := false
	if errors.Is(err, objectstore.ErrNotFound) {
		if opts.Size <= 0 || opts.Size%DefaultBlockSize != 0 {
			return nil, false, fmt.Errorf("new volume size must be a positive multiple of %d", DefaultBlockSize)
		}
		if opts.Filesystem == "" {
			opts.Filesystem = "ext4"
		}
		state = State{
			Format: Format, ID: uuid.NewString(), Name: name, Size: opts.Size,
			BlockSize: DefaultBlockSize, Filesystem: opts.Filesystem,
		}
		created = true
	} else if err != nil {
		return nil, false, err
	} else if state.Deleting {
		return nil, false, fmt.Errorf("volume %s is being deleted", name)
	} else if state.Lease != nil && state.Lease.ExpiresAt.After(now) {
		return nil, false, &HeldError{Volume: name, Holder: state.Lease.Holder, ExpiresAt: state.Lease.ExpiresAt}
	}
	if !created {
		if opts.Size > 0 && opts.Size != state.Size {
			return nil, false, fmt.Errorf("volume %s has size %d, requested %d", name, state.Size, opts.Size)
		}
		if opts.Filesystem != "" && opts.Filesystem != state.Filesystem {
			return nil, false, fmt.Errorf("volume %s uses %s, requested %s", name, state.Filesystem, opts.Filesystem)
		}
	}
	epoch := state.Epoch + 1
	state.Epoch = epoch
	state.Lease = &LeaseRecord{
		Holder: holder, Token: uuid.NewString(), Epoch: epoch,
		ExpiresAt: now.Add(r.ttl()), MountedAt: now,
	}
	body, err := json.Marshal(state)
	if err != nil {
		return nil, false, err
	}
	if created {
		etag = ""
	}
	etag, err = r.head().commit(ctx, name, body, etag, state)
	if err != nil {
		currentBody, currentCursor, getErr := r.readHeadBody(ctx, name)
		if getErr == nil && bytes.Equal(currentBody, body) {
			etag, err = currentCursor, nil
		}
	}
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return nil, false, fmt.Errorf("acquire %s: %w", name, ErrLeaseLost)
	}
	if err != nil {
		return nil, false, err
	}
	lease := &Lease{
		remote: r, state: state, etag: etag,
		planKnown: state.Generation == 0,
	}
	lease.startManifestBundleArchive()
	return lease, created, nil
}

type Lease struct {
	mu        sync.Mutex
	remote    *Remote
	state     State
	etag      string
	lost      bool
	plan      []ObjectRef
	planKnown bool

	pending         *pendingCommit
	adoptedManifest *Manifest

	bundleInFlight string
	bundleDone     chan struct{}
	readyBundle    *preparedManifestBundle
}

type pendingCommit struct {
	body     []byte
	manifest *Manifest
	commit   func(etag string)
}

func (l *Lease) Heartbeat(ctx context.Context, onLost func(error)) {
	interval := l.remote.ttl() / 3
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		state := l.State()
		if state.Lease != nil && state.Lease.ExpiresAt.After(l.remote.now().Add(2*interval)) {
			continue
		}
		renewCtx, cancel := context.WithTimeout(ctx, interval)
		err := l.Renew(renewCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			state := l.State()
			if errors.Is(err, ErrLeaseLost) || state.Lease == nil || !l.remote.now().Before(state.Lease.ExpiresAt) {
				if onLost != nil {
					onLost(fmt.Errorf("renew lease: %w", err))
				}
				return
			}
		}
	}
}

func (l *Lease) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	return cloneState(l.state)
}

func (l *Lease) update(ctx context.Context, mutate func(*State) error) error {
	if err := l.resolvePending(ctx); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost || l.state.Lease == nil {
		return ErrLeaseLost
	}
	candidate := cloneState(l.state)
	bundle := l.readyManifestBundleLocked()
	if bundle != nil {
		applyManifestBundle(&candidate, bundle)
	}
	if err := mutate(&candidate); err != nil {
		return err
	}
	body, err := json.Marshal(candidate)
	if err != nil {
		return err
	}
	etag, err := l.commitState(ctx, body, candidate)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		l.lost = true
		return ErrLeaseLost
	}
	if err != nil {
		l.pending = &pendingCommit{body: body, commit: func(etag string) {
			l.applyCommitLocked(candidate, etag, bundle)
		}}
		return err
	}
	l.applyCommitLocked(candidate, etag, bundle)
	return nil
}

func (l *Lease) resolvePending(ctx context.Context) error {
	l.mu.Lock()
	pending := l.pending
	if pending == nil || l.lost || l.state.Lease == nil {
		l.mu.Unlock()
		return nil
	}
	name := l.state.Name
	l.mu.Unlock()
	currentBody, currentCursor, err := l.remote.readHeadBody(ctx, name)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pending != pending {
		return nil
	}
	l.pending = nil
	if bytes.Equal(currentBody, pending.body) {
		pending.commit(currentCursor)
		if pending.manifest != nil {
			l.adoptedManifest = pending.manifest
		}
	}
	return nil
}

func (l *Lease) applyCommitLocked(candidate State, etag string, bundle *preparedManifestBundle) {
	l.state = candidate
	l.etag = etag
	if bundle != nil {
		l.plan = attachBundleToRefs(l.plan, bundle)
		if l.readyBundle != nil && l.readyBundle.ref.Key == bundle.ref.Key {
			l.readyBundle = nil
		}
	}
}

func (l *Lease) applyManifestCommitLocked(candidate State, etag string, prepared preparedManifest, bundle *preparedManifestBundle) {
	l.applyCommitLocked(candidate, etag, bundle)
	l.adoptedManifest = nil
	switch prepared.manifest.Kind {
	case "delta":
		if l.planKnown {
			l.plan = applyDeltaToPlan(l.plan, checkpointRefs(prepared))
		}
	case "checkpoint":
		l.plan = cloneObjectRefs(prepared.manifest.Segments)
		l.planKnown = true
	}
}

func sameLogicalManifest(a, b Manifest) bool {
	a.CreatedAt = time.Time{}
	b.CreatedAt = time.Time{}
	return reflect.DeepEqual(a, b)
}

func (l *Lease) commitState(ctx context.Context, body []byte, candidate State) (string, error) {
	etag, err := l.remote.head().commit(ctx, l.state.Name, body, l.etag, candidate)
	if err == nil {
		return etag, nil
	}
	currentBody, currentCursor, getErr := l.remote.readHeadBody(ctx, l.state.Name)
	if getErr == nil && bytes.Equal(currentBody, body) {
		return currentCursor, nil
	}
	return "", err
}

func (l *Lease) renewLocked(ctx context.Context) {
	if l.state.Lease == nil || l.state.Lease.ExpiresAt.After(l.remote.now().Add(l.remote.ttl()/2)) {
		return
	}
	candidate := cloneState(l.state)
	candidate.Lease.ExpiresAt = l.remote.now().Add(l.remote.ttl())
	body, err := json.Marshal(candidate)
	if err != nil {
		return
	}
	etag, err := l.commitState(ctx, body, candidate)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		l.lost = true
		return
	}
	if err != nil {
		return
	}
	l.applyCommitLocked(candidate, etag, nil)
}

func (l *Lease) Renew(ctx context.Context) error {
	return l.update(ctx, func(state *State) error {
		state.Lease.ExpiresAt = l.remote.now().Add(l.remote.ttl())
		return nil
	})
}

func (l *Lease) Publish(ctx context.Context, segments ...Segment) error {
	if len(segments) == 0 {
		return errors.New("cannot publish an empty generation")
	}
	for index, segment := range segments {
		digest := sha256.Sum256(segment.Data)
		if len(segment.Data) == 0 || len(segment.Extents) == 0 ||
			segment.SHA256 != hex.EncodeToString(digest[:]) || segment.Bytes != int64(len(segment.Data)) {
			return fmt.Errorf("segment %d metadata does not match its data", index)
		}
	}
	if err := l.resolvePending(ctx); err != nil {
		return err
	}
	l.mu.Lock()
	if l.lost || l.state.Lease == nil {
		l.mu.Unlock()
		return ErrLeaseLost
	}
	if l.consumeAdoptedPublishLocked(segments) {
		l.renewLocked(ctx)
		l.mu.Unlock()
		return nil
	}
	snapshot := cloneState(l.state)
	l.mu.Unlock()
	maxBlocks := uint64(snapshot.Size / snapshot.BlockSize)
	var generationExtents []Extent
	var generationBlocks uint64
	for index, segment := range segments {
		blocks, err := validateExtents(segment.Extents, maxBlocks)
		if err != nil || blocks != uint64(segment.Blocks) {
			return fmt.Errorf("segment %d has invalid extents", index)
		}
		if _, err := validateExtents(segment.ZeroExtents, maxBlocks); err != nil || !extentsContain(segment.Extents, segment.ZeroExtents) {
			return fmt.Errorf("segment %d has invalid zero extents", index)
		}
		if generationBlocks > ^uint64(0)-blocks {
			return errors.New("segment block count overflows")
		}
		generationBlocks += blocks
		generationExtents = append(generationExtents, segment.Extents...)
	}
	coveredBlocks, err := validateExtents(unionExtents(nil, generationExtents), maxBlocks)
	if err != nil || coveredBlocks != generationBlocks {
		return errors.New("segments overlap within generation")
	}

	refs := make([]ObjectRef, len(segments))
	for index, segment := range segments {
		refs[index] = segmentReference(snapshot, segment)
	}
	manifest := Manifest{
		Format: Format, VolumeID: snapshot.ID, Generation: snapshot.Generation + 1,
		Epoch: snapshot.Epoch, Kind: "delta", Parent: snapshot.Manifest,
		Segments: refs, CreatedAt: l.remote.now(),
	}
	prepared, err := prepareManifest(snapshot, manifest)
	if err != nil {
		return err
	}
	return l.publishPrepared(ctx, snapshot, prepared, segments, nil, false)
}

func (l *Lease) consumeAdoptedPublishLocked(segments []Segment) bool {
	adopted := l.adoptedManifest
	if adopted == nil || adopted.Kind != "delta" ||
		adopted.Generation != l.state.Generation || adopted.Epoch != l.state.Epoch ||
		len(adopted.Segments) != len(segments) {
		return false
	}
	for index, segment := range segments {
		if !sameSegmentReference(adopted.Segments[index], segment) {
			return false
		}
	}
	l.adoptedManifest = nil
	return true
}

func sameSegmentReference(ref ObjectRef, segment Segment) bool {
	return ref.SHA256 == segment.SHA256 && ref.Bytes == segment.Bytes && ref.Blocks == segment.Blocks &&
		reflect.DeepEqual(ref.Extents, segment.Extents) && reflect.DeepEqual(ref.ZeroExtents, segment.ZeroExtents)
}

func (l *Lease) uploadSegments(ctx context.Context, snapshot State, segments []Segment) ([]ObjectRef, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	refs := make([]ObjectRef, len(segments))
	jobs := make(chan int)
	var firstErr error
	var errOnce sync.Once
	worker := func() {
		for index := range jobs {
			ref, err := l.uploadSegment(ctx, snapshot, segments[index])
			if err != nil {
				errOnce.Do(func() {
					firstErr = err
					cancel()
				})
				continue
			}
			refs[index] = ref
		}
	}
	workers := min(4, len(segments))
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			worker()
		}()
	}
sendLoop:
	for index := range segments {
		select {
		case jobs <- index:
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	group.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return refs, nil
}

func segmentKey(name string, epoch, generation uint64, digest string) string {
	return fmt.Sprintf("%ssegments/e%d-g%d-%s.seg.gz", VolumePrefix(name), epoch, generation, digest)
}

func (l *Lease) uploadSegment(ctx context.Context, snapshot State, segment Segment) (ObjectRef, error) {
	ref := segmentReference(snapshot, segment)
	if len(ref.InlineData) != 0 {
		return ref, nil
	}
	if _, err := l.remote.Store.PutIfAbsent(ctx, ref.Key, segment.Data); err != nil {
		if !errors.Is(err, objectstore.ErrPreconditionFailed) {
			return ObjectRef{}, fmt.Errorf("upload segment: %w", err)
		}
		existing, getErr := l.remote.Store.Get(ctx, ref.Key)
		if getErr != nil || !bytes.Equal(existing.Data, segment.Data) {
			return ObjectRef{}, fmt.Errorf("segment object %s exists with different content", ref.Key)
		}
	}
	return ref, nil
}

func segmentReference(snapshot State, segment Segment) ObjectRef {
	ref := ObjectRef{
		Key:    segmentKey(snapshot.Name, snapshot.Epoch, snapshot.Generation+1, segment.SHA256),
		SHA256: segment.SHA256, Bytes: segment.Bytes, Blocks: segment.Blocks,
		Extents: append([]Extent(nil), segment.Extents...), ZeroExtents: append([]Extent(nil), segment.ZeroExtents...),
	}
	if len(segment.Data) <= inlineSegmentLimit {
		ref.Key = ""
		ref.InlineData = segment.Data
	}
	return ref
}

func (l *Lease) PublishCheckpoint(ctx context.Context) error {
	l.mu.Lock()
	if l.lost || l.state.Lease == nil {
		l.mu.Unlock()
		return ErrLeaseLost
	}
	snapshot := cloneState(l.state)
	refs := cloneObjectRefs(l.plan)
	planKnown := l.planKnown
	l.mu.Unlock()

	if !planKnown {
		var err error
		refs, err = l.remote.restorePlan(ctx, snapshot)
		if err != nil {
			return err
		}
	}
	bundle, err := prepareManifestBundle(snapshot)
	if err != nil {
		return err
	}
	if bundle != nil {
		refs = attachBundleToRefs(refs, bundle)
	}
	refs = checkpointReadyRefs(refs)
	manifest := Manifest{
		Format: Format, VolumeID: snapshot.ID, Generation: snapshot.Generation + 1,
		Epoch: snapshot.Epoch, Kind: "checkpoint", Parent: snapshot.Manifest,
		Segments: refs, CreatedAt: l.remote.now(),
	}
	prepared, err := prepareManifest(snapshot, manifest)
	if err != nil {
		return err
	}
	return l.publishPrepared(ctx, snapshot, prepared, nil, bundle, true)
}

type preparedManifestBundle struct {
	ref      ManifestBundleRef
	body     []byte
	keys     map[string]struct{}
	uploaded bool
}

func (l *Lease) publishPrepared(
	ctx context.Context,
	snapshot State,
	prepared preparedManifest,
	segments []Segment,
	bundle *preparedManifestBundle,
	archiveCurrent bool,
) error {
	if bundle == nil && !archiveCurrent {
		bundle = l.readyManifestBundle(snapshot)
	}
	if len(prepared.body) > inlineManifestLimit {
		archiveCurrent = true
	}
	embed := !archiveCurrent && len(prepared.body) <= inlineManifestLimit &&
		inlineManifestBytesAfterBundle(snapshot.InlineManifests, bundle)+len(prepared.body) <= inlineManifestStateLimit
	if !embed && bundle == nil && !archiveCurrent {
		var err error
		bundle, err = prepareManifestBundle(snapshot)
		if err != nil {
			return err
		}
		if len(prepared.body) <= inlineManifestLimit {
			embed = true
		} else {
			archiveCurrent = true
		}
	}
	// Every published manifest body must be durable somewhere: embedded in
	// state.json, archived as its own object, or already part of the attached
	// bundle. A ready background bundle that leaves too little inline room used
	// to fall through with neither, committing a manifest key with no body.
	if !embed && !archiveCurrent {
		archiveCurrent = true
	}

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var firstErr error
	var errOnce sync.Once
	recordError := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}
	var uploads sync.WaitGroup
	if len(segments) != 0 {
		uploads.Add(1)
		go func() {
			defer uploads.Done()
			started := time.Now()
			_, err := l.uploadSegments(uploadCtx, snapshot, segments)
			l.remote.observe("segments", started)
			recordError(err)
		}()
	}
	if bundle != nil && !bundle.uploaded {
		uploads.Add(1)
		go func() {
			defer uploads.Done()
			started := time.Now()
			err := l.uploadManifestBundle(uploadCtx, *bundle)
			l.remote.observe("manifest_bundle", started)
			recordError(err)
		}()
	}
	if archiveCurrent {
		uploads.Add(1)
		go func() {
			defer uploads.Done()
			started := time.Now()
			err := l.uploadManifest(uploadCtx, prepared)
			l.remote.observe("manifest", started)
			recordError(err)
		}()
	}
	uploads.Wait()
	if firstErr != nil {
		return firstErr
	}

	started := time.Now()
	err := l.commitManifest(ctx, snapshot, prepared, embed, bundle)
	l.remote.observe("head", started)
	if err == nil {
		l.startManifestBundleArchive()
	}
	return err
}

func inlineManifestBytes(manifests map[string]json.RawMessage) int {
	var total int
	for _, body := range manifests {
		total += len(body)
	}
	return total
}

func inlineManifestBytesAfterBundle(manifests map[string]json.RawMessage, bundle *preparedManifestBundle) int {
	var total int
	for key, body := range manifests {
		if bundle != nil {
			if _, ok := bundle.keys[key]; ok {
				continue
			}
		}
		total += len(body)
	}
	return total
}

func prepareManifestBundle(snapshot State) (*preparedManifestBundle, error) {
	if len(snapshot.InlineManifests) == 0 {
		return nil, nil
	}
	type entryWithGeneration struct {
		entry      bundledManifest
		generation uint64
	}
	entries := make([]entryWithGeneration, 0, len(snapshot.InlineManifests))
	for key, body := range snapshot.InlineManifests {
		manifest, err := decodeManifest(snapshot, key, body, 0)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entryWithGeneration{
			entry: bundledManifest{Key: key, Body: append(json.RawMessage(nil), body...)}, generation: manifest.Generation,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].generation < entries[j].generation })
	bundle := manifestBundle{
		Format: Format, VolumeID: snapshot.ID,
		FirstGeneration: entries[0].generation, LastGeneration: entries[len(entries)-1].generation,
		Manifests: make([]bundledManifest, len(entries)),
	}
	if snapshot.ManifestBundle != nil {
		parent := *snapshot.ManifestBundle
		bundle.Parent = &parent
	}
	keys := make(map[string]struct{}, len(entries))
	for index, entry := range entries {
		bundle.Manifests[index] = entry.entry
		keys[entry.entry.Key] = struct{}{}
	}
	body, err := json.Marshal(bundle)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(body)
	key := VolumePrefix(snapshot.Name) + "manifest-bundles/" + hex.EncodeToString(hash[:]) + ".json"
	return &preparedManifestBundle{
		ref:  ManifestBundleRef{Key: key, FirstGeneration: bundle.FirstGeneration, LastGeneration: bundle.LastGeneration},
		body: body, keys: keys,
	}, nil
}

func attachBundleToRefs(refs []ObjectRef, bundle *preparedManifestBundle) []ObjectRef {
	result := cloneObjectRefs(refs)
	for index := range result {
		if _, ok := bundle.keys[result[index].SourceManifest]; ok {
			result[index].SourceBundle = bundle.ref.Key
		}
	}
	return result
}

func (l *Lease) uploadManifestBundle(ctx context.Context, bundle preparedManifestBundle) error {
	if _, err := l.remote.Store.PutIfAbsent(ctx, bundle.ref.Key, bundle.body); err != nil {
		if !errors.Is(err, objectstore.ErrPreconditionFailed) {
			return fmt.Errorf("upload manifest bundle: %w", err)
		}
		existing, getErr := l.remote.Store.Get(ctx, bundle.ref.Key)
		if getErr != nil || !bytes.Equal(existing.Data, bundle.body) {
			return fmt.Errorf("manifest bundle %s exists with different content", bundle.ref.Key)
		}
	}
	return nil
}

func (l *Lease) readyManifestBundle(snapshot State) *preparedManifestBundle {
	l.mu.Lock()
	defer l.mu.Unlock()
	bundle := l.readyManifestBundleLocked()
	if bundle == nil || !manifestBundleApplies(snapshot, bundle) {
		return nil
	}
	return bundle
}

func (l *Lease) readyManifestBundleLocked() *preparedManifestBundle {
	if l.readyBundle == nil || !manifestBundleApplies(l.state, l.readyBundle) {
		l.readyBundle = nil
		return nil
	}
	return l.readyBundle
}

func manifestBundleApplies(state State, bundle *preparedManifestBundle) bool {
	if bundle == nil {
		return false
	}
	currentParent := ""
	if state.ManifestBundle != nil {
		currentParent = state.ManifestBundle.Key
	}
	bundleParent := ""
	var decoded manifestBundle
	if err := json.Unmarshal(bundle.body, &decoded); err != nil {
		return false
	}
	if decoded.Parent != nil {
		bundleParent = decoded.Parent.Key
	}
	if currentParent != bundleParent {
		return false
	}
	for key := range bundle.keys {
		if _, ok := state.InlineManifests[key]; !ok {
			return false
		}
	}
	return true
}

func applyManifestBundle(state *State, bundle *preparedManifestBundle) {
	for key := range bundle.keys {
		delete(state.InlineManifests, key)
	}
	if len(state.InlineManifests) == 0 {
		state.InlineManifests = nil
	}
	bundleRef := bundle.ref
	state.ManifestBundle = &bundleRef
}

func (l *Lease) startManifestBundleArchive() {
	l.mu.Lock()
	if l.lost || l.state.Lease == nil || l.bundleInFlight != "" || l.readyBundle != nil ||
		inlineManifestBytes(l.state.InlineManifests) < manifestBundleTarget {
		l.mu.Unlock()
		return
	}
	snapshot := cloneState(l.state)
	bundle, err := prepareManifestBundle(snapshot)
	if err != nil || bundle == nil {
		l.mu.Unlock()
		return
	}
	l.bundleInFlight = bundle.ref.Key
	done := make(chan struct{})
	l.bundleDone = done
	l.mu.Unlock()

	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), l.remote.ttl())
		started := time.Now()
		err := l.uploadManifestBundle(ctx, *bundle)
		l.remote.observe("manifest_bundle_background", started)
		cancel()

		l.mu.Lock()
		defer l.mu.Unlock()
		if l.bundleInFlight == bundle.ref.Key {
			l.bundleInFlight = ""
			l.bundleDone = nil
		}
		if err == nil && manifestBundleApplies(l.state, bundle) {
			bundle.uploaded = true
			l.readyBundle = bundle
		}
	}()
}

type preparedManifest struct {
	manifest Manifest
	body     []byte
	key      string
}

func prepareManifest(snapshot State, manifest Manifest) (preparedManifest, error) {
	manifestBody, err := json.Marshal(manifest)
	if err != nil {
		return preparedManifest{}, err
	}
	hash := sha256.Sum256(manifestBody)
	manifestKey := VolumePrefix(snapshot.Name) + "manifests/" + hex.EncodeToString(hash[:]) + ".json"
	return preparedManifest{manifest: manifest, body: manifestBody, key: manifestKey}, nil
}

func (l *Lease) uploadManifest(ctx context.Context, prepared preparedManifest) error {
	if _, err := l.remote.Store.PutIfAbsent(ctx, prepared.key, prepared.body); err != nil {
		if !errors.Is(err, objectstore.ErrPreconditionFailed) {
			return fmt.Errorf("upload manifest: %w", err)
		}
		existing, getErr := l.remote.Store.Get(ctx, prepared.key)
		if getErr != nil || !bytes.Equal(existing.Data, prepared.body) {
			return fmt.Errorf("manifest object %s exists with different content", prepared.key)
		}
	}
	return nil
}

func (l *Lease) commitManifest(
	ctx context.Context,
	snapshot State,
	prepared preparedManifest,
	embed bool,
	bundle *preparedManifestBundle,
) error {
	if err := l.resolvePending(ctx); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost || l.state.Lease == nil {
		return ErrLeaseLost
	}
	if adopted := l.adoptedManifest; adopted != nil && sameLogicalManifest(*adopted, prepared.manifest) {
		l.adoptedManifest = nil
		l.renewLocked(ctx)
		return nil
	}
	if l.state.Generation != snapshot.Generation || l.state.Manifest != snapshot.Manifest || l.state.Epoch != snapshot.Epoch {
		return errors.New("volume head changed during publish")
	}
	candidate := cloneState(l.state)
	if bundle != nil {
		if !manifestBundleApplies(candidate, bundle) {
			return errors.New("manifest bundle no longer matches volume state")
		}
		applyManifestBundle(&candidate, bundle)
	}
	if candidate.Lease != nil {
		lease := *candidate.Lease
		lease.ExpiresAt = l.remote.now().Add(l.remote.ttl())
		candidate.Lease = &lease
	}
	candidate.Generation = prepared.manifest.Generation
	candidate.Manifest = prepared.key
	if embed {
		if candidate.InlineManifests == nil {
			candidate.InlineManifests = make(map[string]json.RawMessage)
		}
		candidate.InlineManifests[prepared.key] = append(json.RawMessage(nil), prepared.body...)
	}
	if prepared.manifest.Kind == "checkpoint" {
		candidate.Checkpoint = prepared.manifest.Generation
	}
	stateBody, err := json.Marshal(candidate)
	if err != nil {
		return err
	}
	etag, err := l.commitState(ctx, stateBody, candidate)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		l.lost = true
		return ErrLeaseLost
	}
	if err != nil {
		manifest := prepared.manifest
		l.pending = &pendingCommit{body: stateBody, manifest: &manifest, commit: func(etag string) {
			l.applyManifestCommitLocked(candidate, etag, prepared, bundle)
		}}
		return fmt.Errorf("publish volume head: %w", err)
	}
	l.applyManifestCommitLocked(candidate, etag, prepared, bundle)
	return nil
}

func checkpointRefs(prepared preparedManifest) []ObjectRef {
	refs := cloneObjectRefs(prepared.manifest.Segments)
	for index := range refs {
		if len(refs[index].InlineData) == 0 {
			continue
		}
		refs[index].InlineData = nil
		refs[index].SourceManifest = prepared.key
		refs[index].SourceIndex = uint32(index)
	}
	return refs
}

func checkpointReadyRefs(refs []ObjectRef) []ObjectRef {
	result := cloneObjectRefs(refs)
	for index := range result {
		if result[index].SourceManifest != "" {
			result[index].InlineData = nil
		}
	}
	return result
}

func applyDeltaToPlan(current, delta []ObjectRef) []ObjectRef {
	result := make([]ObjectRef, 0, len(delta)+len(current))
	var covered []Extent
	for _, original := range delta {
		visible := subtractExtents(original.Extents, original.ZeroExtents)
		if len(visible) > 0 {
			ref := original
			ref.Extents = visible
			ref.ZeroExtents = nil
			result = append(result, ref)
		}
		covered = unionExtents(covered, original.Extents)
	}
	for _, original := range current {
		visible := subtractExtents(original.Extents, covered)
		if len(visible) == 0 {
			continue
		}
		ref := original
		ref.Extents = visible
		ref.ZeroExtents = nil
		result = append(result, ref)
	}
	return result
}

func cloneObjectRefs(refs []ObjectRef) []ObjectRef {
	cloned := make([]ObjectRef, len(refs))
	for index, original := range refs {
		cloned[index] = original
		cloned[index].Extents = append([]Extent(nil), original.Extents...)
		cloned[index].ZeroExtents = append([]Extent(nil), original.ZeroExtents...)
		cloned[index].InlineData = append([]byte(nil), original.InlineData...)
	}
	return cloned
}

func cloneState(state State) State {
	cloned := state
	if state.Lease != nil {
		lease := *state.Lease
		cloned.Lease = &lease
	}
	if state.InlineManifests != nil {
		cloned.InlineManifests = make(map[string]json.RawMessage, len(state.InlineManifests))
		for key, body := range state.InlineManifests {
			cloned.InlineManifests[key] = append(json.RawMessage(nil), body...)
		}
	}
	if state.ManifestBundle != nil {
		bundle := *state.ManifestBundle
		cloned.ManifestBundle = &bundle
	}
	return cloned
}

func (l *Lease) Release(ctx context.Context) error {
	if err := l.waitForManifestBundle(ctx); err != nil {
		return err
	}
	return l.update(ctx, func(state *State) error {
		state.Lease = nil
		return nil
	})
}

func (l *Lease) waitForManifestBundle(ctx context.Context) error {
	l.mu.Lock()
	done := l.bundleDone
	l.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Remote) Break(ctx context.Context, name, expectedToken string) error {
	state, etag, err := r.Inspect(ctx, name)
	if err != nil {
		return err
	}
	if state.Lease == nil {
		return nil
	}
	if expectedToken != "" && state.Lease.Token != expectedToken {
		return fmt.Errorf("lease holder changed while awaiting confirmation: %w", ErrLeaseLost)
	}
	state.Lease = nil
	state.Epoch++
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = r.head().commit(ctx, name, body, etag, state)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return fmt.Errorf("lease changed while breaking it: %w", ErrLeaseLost)
	}
	return err
}

func (r *Remote) Restore(ctx context.Context, name, path string) (State, error) {
	return r.RestoreWithOptions(ctx, name, path, RestoreOptions{})
}

func (r *Remote) RestoreWithOptions(ctx context.Context, name, path string, opts RestoreOptions) (State, error) {
	state, _, err := r.Inspect(ctx, name)
	if err != nil {
		return State{}, err
	}
	if opts.Generation != 0 && !opts.Timestamp.IsZero() {
		return State{}, errors.New("generation and timestamp are mutually exclusive")
	}
	if opts.Generation != 0 || !opts.Timestamp.IsZero() {
		entry, err := r.selectHistory(ctx, state, opts)
		if err != nil {
			return State{}, err
		}
		state.Generation = entry.manifest.Generation
		state.Manifest = entry.key
		if state.Checkpoint > state.Generation {
			state.Checkpoint = 0
		}
	}
	return state, r.RestoreState(ctx, state, path)
}

type manifestEntry struct {
	key      string
	manifest Manifest
	bundle   string
}

func (r *Remote) selectHistory(ctx context.Context, state State, opts RestoreOptions) (manifestEntry, error) {
	history, err := r.loadHistory(ctx, state)
	if err != nil {
		return manifestEntry{}, err
	}
	for _, entry := range history {
		switch {
		case opts.Generation != 0 && entry.manifest.Generation == opts.Generation:
			return entry, nil
		case !opts.Timestamp.IsZero() && !entry.manifest.CreatedAt.After(opts.Timestamp):
			return entry, nil
		}
	}
	if opts.Generation != 0 {
		return manifestEntry{}, fmt.Errorf("generation %d is not retained for volume %s", opts.Generation, state.Name)
	}
	return manifestEntry{}, fmt.Errorf("no generation at or before %s is retained for volume %s", opts.Timestamp.Format(time.RFC3339), state.Name)
}

func (r *Remote) loadHistory(ctx context.Context, state State) ([]manifestEntry, error) {
	if state.Generation == 0 {
		return nil, nil
	}
	var history []manifestEntry
	bundles := make(map[string]manifestBundle)
	key := state.Manifest
	expectedGeneration := state.Generation
	behindCheckpoint := false
	for key != "" {
		manifest, bundle, err := r.readManifestWithSourceCache(ctx, state, key, expectedGeneration, bundles)
		// History behind the newest traversed checkpoint is point-in-time
		// metadata only; a missing object there means garbage collection
		// truncated it (possibly mid-sweep), not that restorable data is gone.
		if errors.Is(err, objectstore.ErrNotFound) && behindCheckpoint {
			break
		}
		if err != nil {
			return nil, err
		}
		history = append(history, manifestEntry{key: key, manifest: manifest, bundle: bundle})
		if manifest.Generation == 1 {
			if manifest.Parent != "" {
				return nil, fmt.Errorf("generation one manifest %s has a parent", key)
			}
			break
		}
		if manifest.Kind == "checkpoint" {
			behindCheckpoint = true
		}
		key = manifest.Parent
		expectedGeneration--
		if key == "" && manifest.Kind != "checkpoint" {
			return nil, errors.New("manifest history ended before generation one")
		}
		if len(history) > 1_000_000 {
			return nil, errors.New("manifest history is too long")
		}
	}
	return history, nil
}

func (r *Remote) readManifest(ctx context.Context, state State, key string, expectedGeneration uint64) (Manifest, error) {
	manifest, _, err := r.readManifestWithSource(ctx, state, key, expectedGeneration)
	return manifest, err
}

func (r *Remote) readManifestWithSource(
	ctx context.Context,
	state State,
	key string,
	expectedGeneration uint64,
) (Manifest, string, error) {
	return r.readManifestWithSourceCache(ctx, state, key, expectedGeneration, make(map[string]manifestBundle))
}

func (r *Remote) readManifestWithSourceCache(
	ctx context.Context,
	state State,
	key string,
	expectedGeneration uint64,
	bundles map[string]manifestBundle,
) (Manifest, string, error) {
	body, ok := state.InlineManifests[key]
	if ok {
		manifest, err := decodeManifest(state, key, body, expectedGeneration)
		return manifest, "", err
	}
	bundleRef := state.ManifestBundle
	for bundleRef != nil && expectedGeneration != 0 && expectedGeneration <= bundleRef.LastGeneration {
		bundle, exists := bundles[bundleRef.Key]
		if !exists {
			var err error
			bundle, err = r.readManifestBundle(ctx, state, *bundleRef)
			if err != nil {
				return Manifest{}, "", err
			}
			bundles[bundleRef.Key] = bundle
		}
		if expectedGeneration >= bundleRef.FirstGeneration {
			for _, entry := range bundle.Manifests {
				if entry.Key == key {
					manifest, err := decodeManifest(state, key, entry.Body, expectedGeneration)
					return manifest, bundleRef.Key, err
				}
			}
			break
		}
		bundleRef = bundle.Parent
	}
	obj, err := r.Store.Get(ctx, key)
	if err != nil {
		return Manifest{}, "", fmt.Errorf("read manifest %s: %w", key, err)
	}
	manifest, err := decodeManifest(state, key, obj.Data, expectedGeneration)
	return manifest, "", err
}

func (r *Remote) readManifestBundle(ctx context.Context, state State, ref ManifestBundleRef) (manifestBundle, error) {
	obj, err := r.Store.Get(ctx, ref.Key)
	if err != nil {
		return manifestBundle{}, fmt.Errorf("read manifest bundle %s: %w", ref.Key, err)
	}
	hash := sha256.Sum256(obj.Data)
	wantKey := VolumePrefix(state.Name) + "manifest-bundles/" + hex.EncodeToString(hash[:]) + ".json"
	if ref.Key != wantKey {
		return manifestBundle{}, fmt.Errorf("manifest bundle %s checksum mismatch", ref.Key)
	}
	var bundle manifestBundle
	if err := json.Unmarshal(obj.Data, &bundle); err != nil {
		return manifestBundle{}, fmt.Errorf("decode manifest bundle %s: %w", ref.Key, err)
	}
	if bundle.Format != Format || bundle.VolumeID != state.ID || bundle.FirstGeneration != ref.FirstGeneration ||
		bundle.LastGeneration != ref.LastGeneration || len(bundle.Manifests) == 0 {
		return manifestBundle{}, fmt.Errorf("invalid manifest bundle %s", ref.Key)
	}
	if bundle.Parent != nil {
		if !validManifestBundleRef(state.Name, *bundle.Parent) || bundle.Parent.LastGeneration >= bundle.FirstGeneration {
			return manifestBundle{}, fmt.Errorf("manifest bundle %s has an invalid parent", ref.Key)
		}
	}
	var first, previous uint64
	for index, entry := range bundle.Manifests {
		manifest, err := decodeManifest(state, entry.Key, entry.Body, 0)
		if err != nil {
			return manifestBundle{}, fmt.Errorf("validate manifest bundle %s: %w", ref.Key, err)
		}
		if manifest.Generation < bundle.FirstGeneration || manifest.Generation > bundle.LastGeneration ||
			index > 0 && manifest.Generation <= previous {
			return manifestBundle{}, fmt.Errorf("manifest bundle %s has unordered generations", ref.Key)
		}
		if index == 0 {
			first = manifest.Generation
		}
		previous = manifest.Generation
	}
	if first != bundle.FirstGeneration || previous != bundle.LastGeneration {
		return manifestBundle{}, fmt.Errorf("manifest bundle %s has an invalid generation range", ref.Key)
	}
	return bundle, nil
}

func decodeManifest(state State, key string, body []byte, expectedGeneration uint64) (Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest %s: %w", key, err)
	}
	hash := sha256.Sum256(body)
	wantKey := VolumePrefix(state.Name) + "manifests/" + hex.EncodeToString(hash[:]) + ".json"
	if key != wantKey {
		return Manifest{}, fmt.Errorf("manifest %s checksum mismatch", key)
	}
	if expectedGeneration == 0 {
		expectedGeneration = manifest.Generation
	}
	if err := validateManifest(state, key, manifest, expectedGeneration); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateManifest(state State, label string, manifest Manifest, expectedGeneration uint64) error {
	if manifest.Format != Format || manifest.VolumeID != state.ID || manifest.Epoch == 0 {
		return fmt.Errorf("manifest %s does not belong to volume %s", label, state.Name)
	}
	if manifest.Generation != expectedGeneration || expectedGeneration == 0 {
		return fmt.Errorf("manifest %s breaks the generation chain", label)
	}
	switch manifest.Kind {
	case "delta":
		if len(manifest.Segments) == 0 {
			return fmt.Errorf("delta manifest %s has no segments", label)
		}
	case "checkpoint":
	default:
		return fmt.Errorf("manifest %s has unknown kind %q", label, manifest.Kind)
	}
	maxBlocks := uint64(state.Size / state.BlockSize)
	var manifestExtents []Extent
	var manifestBlocks uint64
	for index, ref := range manifest.Segments {
		if ref.Bytes <= 0 || ref.Blocks <= 0 || uint64(ref.Blocks) > maxBlocks || len(ref.Extents) == 0 {
			return fmt.Errorf("manifest %s has an invalid segment reference at index %d", label, index)
		}
		digest, err := hex.DecodeString(ref.SHA256)
		if err != nil || len(digest) != sha256.Size {
			return fmt.Errorf("manifest %s has an invalid segment digest at index %d", label, index)
		}
		segmentPrefix := VolumePrefix(state.Name) + "segments/"
		manifestPrefix := VolumePrefix(state.Name) + "manifests/"
		storageKinds := 0
		if ref.Key != "" {
			storageKinds++
			if !strings.HasPrefix(ref.Key, segmentPrefix) || !strings.HasSuffix(ref.Key, "-"+ref.SHA256+".seg.gz") {
				return fmt.Errorf("manifest %s has an invalid segment key %s", label, ref.Key)
			}
		}
		if len(ref.InlineData) != 0 {
			storageKinds++
			digest := sha256.Sum256(ref.InlineData)
			if int64(len(ref.InlineData)) != ref.Bytes || hex.EncodeToString(digest[:]) != ref.SHA256 {
				return fmt.Errorf("manifest %s has invalid inline segment data at index %d", label, index)
			}
		}
		if ref.SourceManifest != "" {
			storageKinds++
			if !strings.HasPrefix(ref.SourceManifest, manifestPrefix) || !strings.HasSuffix(ref.SourceManifest, ".json") {
				return fmt.Errorf("manifest %s has an invalid inline segment source %s", label, ref.SourceManifest)
			}
			if ref.SourceBundle != "" {
				bundlePrefix := VolumePrefix(state.Name) + "manifest-bundles/"
				if !strings.HasPrefix(ref.SourceBundle, bundlePrefix) || !strings.HasSuffix(ref.SourceBundle, ".json") {
					return fmt.Errorf("manifest %s has an invalid segment source bundle %s", label, ref.SourceBundle)
				}
			}
		} else if ref.SourceBundle != "" {
			return fmt.Errorf("manifest %s has a segment bundle without a source manifest", label)
		}
		if storageKinds != 1 {
			return fmt.Errorf("manifest %s has invalid segment storage at index %d", label, index)
		}
		blocks, err := validateExtents(ref.Extents, maxBlocks)
		if err != nil || blocks > uint64(ref.Blocks) || manifest.Kind == "delta" && blocks != uint64(ref.Blocks) {
			return fmt.Errorf("manifest %s has invalid segment extents at index %d", label, index)
		}
		if _, err := validateExtents(ref.ZeroExtents, maxBlocks); err != nil || !extentsContain(ref.Extents, ref.ZeroExtents) {
			return fmt.Errorf("manifest %s has invalid zero extents at index %d", label, index)
		}
		if manifestBlocks > ^uint64(0)-blocks {
			return fmt.Errorf("manifest %s extent count overflows", label)
		}
		manifestBlocks += blocks
		manifestExtents = append(manifestExtents, ref.Extents...)
	}
	coveredBlocks, err := validateExtents(unionExtents(nil, manifestExtents), maxBlocks)
	if err != nil || coveredBlocks != manifestBlocks {
		return fmt.Errorf("manifest %s has overlapping segment extents", label)
	}
	return nil
}

// CollectGarbage marks objects outside the retained history, then deletes
// objects that have remained marked for a full grace period. The caller must
// hold the volume's writer lease and must not publish concurrently.
func (l *Lease) CollectGarbage(ctx context.Context, opts GCOptions) (GCResult, error) {
	if opts.HistoryRetention < 0 || opts.GracePeriod < 0 {
		return GCResult{}, errors.New("garbage collection durations cannot be negative")
	}
	l.mu.Lock()
	if l.lost || l.state.Lease == nil {
		l.mu.Unlock()
		return GCResult{}, ErrLeaseLost
	}
	state := cloneState(l.state)
	l.mu.Unlock()
	return l.remote.collectGarbage(ctx, state, opts, l.stillHeld)
}

func (l *Lease) stillHeld() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost || l.state.Lease == nil {
		return ErrLeaseLost
	}
	return nil
}

func (r *Remote) collectGarbage(ctx context.Context, state State, opts GCOptions, stillHeld func() error) (GCResult, error) {
	if stillHeld == nil {
		stillHeld = func() error { return nil }
	}
	history, err := r.loadHistory(ctx, state)
	if err != nil {
		return GCResult{}, err
	}
	keep := map[string]struct{}{}
	sources := newManifestSourceLoader(r.Store, state)
	if len(history) > 0 {
		keepThrough := len(history) - 1
		cutoff := r.now().Add(-opts.HistoryRetention)
		for i, entry := range history {
			if entry.manifest.Kind == "checkpoint" && !entry.manifest.CreatedAt.After(cutoff) {
				keepThrough = i
				break
			}
		}
		for _, entry := range history[:keepThrough+1] {
			if entry.key != "" {
				keep[entry.key] = struct{}{}
			}
			if entry.bundle != "" {
				keep[entry.bundle] = struct{}{}
			}
			for _, ref := range entry.manifest.Segments {
				if ref.Key != "" {
					keep[ref.Key] = struct{}{}
				}
				if ref.SourceManifest != "" {
					keep[ref.SourceManifest] = struct{}{}
				}
				if ref.SourceBundle != "" {
					keep[ref.SourceBundle] = struct{}{}
				}
				if ref.SourceManifest != "" && ref.SourceBundle == "" && len(ref.InlineData) == 0 {
					path, err := sources.findBundlePath(ctx, ref.SourceManifest)
					if err != nil {
						return GCResult{}, fmt.Errorf("locate inline segment source %s: %w", ref.SourceManifest, err)
					}
					for _, bundleKey := range path {
						keep[bundleKey] = struct{}{}
					}
				}
			}
		}
	}

	prefix := VolumePrefix(state.Name)
	gcPrefix := prefix + "gc/"
	keys, err := r.Store.List(ctx, prefix)
	if err != nil {
		return GCResult{}, err
	}
	now := r.now()
	marked := map[string]struct{}{}
	deleted := map[string]struct{}{}
	result := GCResult{}
	for _, key := range keys {
		if !strings.HasPrefix(key, gcPrefix) {
			continue
		}
		obj, err := r.Store.Get(ctx, key)
		if err != nil {
			return result, err
		}
		var record gcRecord
		if err := json.Unmarshal(obj.Data, &record); err != nil {
			return result, fmt.Errorf("decode garbage record %s: %w", key, err)
		}
		if record.Format != Format || record.VolumeID != state.ID || record.CreatedAt.IsZero() {
			return result, fmt.Errorf("invalid garbage record %s", key)
		}
		for _, candidate := range record.Objects {
			marked[candidate] = struct{}{}
		}
		if record.CreatedAt.Add(opts.GracePeriod).After(now) {
			continue
		}
		if err := stillHeld(); err != nil {
			return result, err
		}
		for _, candidate := range record.Objects {
			if _, retained := keep[candidate]; retained || !isImmutableObject(prefix, candidate) {
				continue
			}
			if err := ctx.Err(); err != nil {
				return result, err
			}
			if err := r.Store.Delete(ctx, candidate); err != nil {
				return result, fmt.Errorf("delete garbage object %s: %w", candidate, err)
			}
			deleted[candidate] = struct{}{}
			result.Deleted++
		}
		if err := r.Store.Delete(ctx, key); err != nil {
			return result, fmt.Errorf("delete garbage record %s: %w", key, err)
		}
	}

	var candidates []string
	for _, key := range keys {
		if !isImmutableObject(prefix, key) {
			continue
		}
		if _, retained := keep[key]; retained {
			continue
		}
		if _, alreadyDeleted := deleted[key]; alreadyDeleted {
			continue
		}
		if _, alreadyMarked := marked[key]; alreadyMarked {
			continue
		}
		candidates = append(candidates, key)
	}
	sort.Strings(candidates)
	if len(candidates) > 0 {
		if err := stillHeld(); err != nil {
			return result, err
		}
	}
	const objectsPerRecord = 1000
	for len(candidates) > 0 {
		count := min(len(candidates), objectsPerRecord)
		batch := append([]string(nil), candidates[:count]...)
		candidates = candidates[count:]
		record := gcRecord{Format: Format, VolumeID: state.ID, CreatedAt: now, Objects: batch}
		body, err := json.Marshal(record)
		if err != nil {
			return result, err
		}
		hash := sha256.Sum256(body)
		key := fmt.Sprintf("%s%d-%s.json", gcPrefix, now.UnixNano(), hex.EncodeToString(hash[:8]))
		if _, err := r.Store.PutIfAbsent(ctx, key, body); err != nil && !errors.Is(err, objectstore.ErrPreconditionFailed) {
			return result, fmt.Errorf("write garbage record: %w", err)
		}
		result.Marked += len(batch)
	}
	return result, nil
}

func isImmutableObject(prefix, key string) bool {
	return strings.HasPrefix(key, prefix+"manifests/") || strings.HasPrefix(key, prefix+"manifest-bundles/") ||
		strings.HasPrefix(key, prefix+"segments/")
}

func (r *Remote) restoreManifests(ctx context.Context, state State) ([]manifestEntry, error) {
	var manifests []manifestEntry
	bundles := make(map[string]manifestBundle)
	key := state.Manifest
	expectedGeneration := state.Generation
	for key != "" {
		manifest, bundle, err := r.readManifestWithSourceCache(ctx, state, key, expectedGeneration, bundles)
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, manifestEntry{key: key, manifest: manifest, bundle: bundle})
		if manifest.Kind == "checkpoint" {
			expectedGeneration = 0
			break
		}
		expectedGeneration--
		key = manifest.Parent
		if len(manifests) > 1_000_000 {
			return nil, errors.New("manifest chain is too long")
		}
	}
	if expectedGeneration != 0 {
		return nil, errors.New("manifest chain ended before generation zero")
	}
	return manifests, nil
}

// restorePlan selects the newest non-zero value for every referenced block.
// Its references are disjoint, so a checkpoint can reuse them without copying
// segment data and a restore can skip overwritten segments.
func (r *Remote) restorePlan(ctx context.Context, state State) ([]ObjectRef, error) {
	manifests, err := r.restoreManifests(ctx, state)
	if err != nil {
		return nil, err
	}
	var covered []Extent
	refs := make([]ObjectRef, 0)
	for _, entry := range manifests {
		var manifestExtents []Extent
		for index, original := range entry.manifest.Segments {
			if len(original.InlineData) != 0 && entry.key != "" {
				original.SourceManifest = entry.key
				original.SourceBundle = entry.bundle
				original.SourceIndex = uint32(index)
			}
			visible := subtractExtents(original.Extents, covered)
			nonZero := subtractExtents(visible, original.ZeroExtents)
			if len(nonZero) > 0 {
				ref := original
				ref.Extents = nonZero
				ref.ZeroExtents = nil
				refs = append(refs, ref)
			}
			manifestExtents = append(manifestExtents, original.Extents...)
		}
		covered = unionExtents(covered, manifestExtents)
	}
	return refs, nil
}

func createSparseImage(path string, size int64) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

type manifestSourceLoader struct {
	store    objectstore.Store
	volumeID string
	inline   map[string]json.RawMessage
	head     *ManifestBundleRef
	mu       sync.Mutex
	bundles  map[string]manifestBundle
}

func newManifestSourceLoader(store objectstore.Store, state State) *manifestSourceLoader {
	return &manifestSourceLoader{
		store: store, volumeID: state.ID,
		inline: state.InlineManifests, head: state.ManifestBundle,
		bundles: make(map[string]manifestBundle),
	}
}

func (l *manifestSourceLoader) loadBundleLocked(ctx context.Context, bundleKey string) (manifestBundle, error) {
	bundle, ok := l.bundles[bundleKey]
	if ok {
		return bundle, nil
	}
	obj, err := l.store.Get(ctx, bundleKey)
	if err != nil {
		return manifestBundle{}, fmt.Errorf("read manifest bundle %s: %w", bundleKey, err)
	}
	digest := sha256.Sum256(obj.Data)
	if !strings.HasSuffix(bundleKey, "/manifest-bundles/"+hex.EncodeToString(digest[:])+".json") {
		return manifestBundle{}, fmt.Errorf("manifest bundle %s checksum mismatch", bundleKey)
	}
	if err := json.Unmarshal(obj.Data, &bundle); err != nil {
		return manifestBundle{}, fmt.Errorf("decode manifest bundle %s: %w", bundleKey, err)
	}
	if bundle.Format != Format || bundle.VolumeID != l.volumeID || len(bundle.Manifests) == 0 {
		return manifestBundle{}, fmt.Errorf("invalid manifest bundle %s", bundleKey)
	}
	l.bundles[bundleKey] = bundle
	return bundle, nil
}

func (l *manifestSourceLoader) manifestBody(ctx context.Context, bundleKey, manifestKey string) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	bundle, err := l.loadBundleLocked(ctx, bundleKey)
	if err != nil {
		return nil, err
	}
	for _, entry := range bundle.Manifests {
		if entry.Key == manifestKey {
			return entry.Body, nil
		}
	}
	return nil, fmt.Errorf("manifest %s is absent from bundle %s", manifestKey, bundleKey)
}

// resolve finds a manifest body for a reference published without a bundle
// location. The source was inline in state.json when the reference was
// published, and later bundle rollovers may have moved it, so the search
// covers the current inline area, the whole bundle chain, and finally a
// standalone manifest object.
func (l *manifestSourceLoader) resolve(ctx context.Context, manifestKey string) ([]byte, error) {
	if body, ok := l.inline[manifestKey]; ok {
		return body, nil
	}
	body, err := l.searchBundleChain(ctx, manifestKey, nil)
	if err != nil && !errors.Is(err, objectstore.ErrNotFound) {
		return nil, err
	}
	if body != nil {
		return body, nil
	}
	obj, err := l.store.Get(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	return obj.Data, nil
}

// findBundlePath returns the bundle-chain keys from the head down to and
// including the bundle that holds manifestKey, so garbage collection can keep
// the manifest reachable. It returns nil when the manifest is inline, is a
// standalone object, or cannot be found.
func (l *manifestSourceLoader) findBundlePath(ctx context.Context, manifestKey string) ([]string, error) {
	if _, ok := l.inline[manifestKey]; ok {
		return nil, nil
	}
	var path []string
	body, err := l.searchBundleChain(ctx, manifestKey, &path)
	if err != nil {
		return nil, err
	}
	if body != nil {
		return path, nil
	}
	return nil, nil
}

func (l *manifestSourceLoader) searchBundleChain(ctx context.Context, manifestKey string, path *[]string) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ref := l.head
	for ref != nil {
		bundle, err := l.loadBundleLocked(ctx, ref.Key)
		if err != nil {
			return nil, err
		}
		if path != nil {
			*path = append(*path, ref.Key)
		}
		for _, entry := range bundle.Manifests {
			if entry.Key == manifestKey {
				return entry.Body, nil
			}
		}
		ref = bundle.Parent
	}
	return nil, nil
}

func (l *manifestSourceLoader) inlineSegment(ctx context.Context, ref ObjectRef) ([]byte, string, error) {
	var sourceBody []byte
	var err error
	if ref.SourceBundle != "" {
		sourceBody, err = l.manifestBody(ctx, ref.SourceBundle, ref.SourceManifest)
	} else {
		sourceBody, err = l.resolve(ctx, ref.SourceManifest)
	}
	if err != nil {
		return nil, "", fmt.Errorf("read inline segment manifest %s: %w", ref.SourceManifest, err)
	}
	digest := sha256.Sum256(sourceBody)
	if !strings.HasSuffix(ref.SourceManifest, "/manifests/"+hex.EncodeToString(digest[:])+".json") {
		return nil, "", fmt.Errorf("inline segment manifest %s checksum mismatch", ref.SourceManifest)
	}
	var source Manifest
	if err := json.Unmarshal(sourceBody, &source); err != nil {
		return nil, "", fmt.Errorf("decode inline segment manifest %s: %w", ref.SourceManifest, err)
	}
	if int(ref.SourceIndex) >= len(source.Segments) {
		return nil, "", fmt.Errorf("inline segment index %d is outside manifest %s", ref.SourceIndex, ref.SourceManifest)
	}
	sourceRef := source.Segments[ref.SourceIndex]
	dataDigest := sha256.Sum256(sourceRef.InlineData)
	if len(sourceRef.InlineData) == 0 || sourceRef.SHA256 != ref.SHA256 || sourceRef.Bytes != ref.Bytes || sourceRef.Blocks != ref.Blocks ||
		int64(len(sourceRef.InlineData)) != ref.Bytes || hex.EncodeToString(dataDigest[:]) != ref.SHA256 {
		return nil, "", fmt.Errorf("inline segment %d in manifest %s does not match its reference", ref.SourceIndex, ref.SourceManifest)
	}
	return sourceRef.InlineData, fmt.Sprintf("%s[%d]", ref.SourceManifest, ref.SourceIndex), nil
}

func applyObjectRef(
	ctx context.Context,
	store objectstore.Store,
	sources *manifestSourceLoader,
	f *os.File,
	size int64,
	ref ObjectRef,
) error {
	data := ref.InlineData
	label := ref.Key
	if len(data) == 0 && ref.SourceManifest != "" {
		var err error
		data, label, err = sources.inlineSegment(ctx, ref)
		if err != nil {
			return err
		}
	} else if len(data) == 0 {
		obj, err := store.Get(ctx, ref.Key)
		if err != nil {
			return fmt.Errorf("read segment %s: %w", ref.Key, err)
		}
		data = obj.Data
	}
	segment := Segment{Data: data, SHA256: ref.SHA256, Bytes: ref.Bytes, Blocks: ref.Blocks}
	runs, err := decodeSegment(segment)
	if err != nil {
		return fmt.Errorf("decode segment %s: %w", label, err)
	}
	actual := make([]Extent, len(runs))
	for i, run := range runs {
		actual[i] = run.extent
	}
	if !extentsContain(actual, ref.Extents) {
		return fmt.Errorf("segment %s does not contain its manifest extents", label)
	}
	if err := applyRuns(f, size, runs, ref.Extents); err != nil {
		return fmt.Errorf("apply segment %s: %w", label, err)
	}
	return nil
}

func applyObjectRefs(ctx context.Context, store objectstore.Store, state State, f *os.File, size int64, refs []ObjectRef) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sources := newManifestSourceLoader(store, state)
	jobs := make(chan ObjectRef)
	var firstErr error
	var errOnce sync.Once
	workers := min(4, len(refs))
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for ref := range jobs {
				if err := applyObjectRef(ctx, store, sources, f, size, ref); err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
				}
			}
		}()
	}
sendLoop:
	for _, ref := range refs {
		select {
		case jobs <- ref:
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	group.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func (r *Remote) RestoreState(ctx context.Context, state State, path string) error {
	if err := validateState(state.Name, state); err != nil {
		return err
	}
	refs, err := r.restorePlan(ctx, state)
	if err != nil {
		return err
	}
	f, err := createSparseImage(path, state.Size)
	if err != nil {
		return err
	}
	defer f.Close()
	return applyObjectRefs(ctx, r.Store, state, f, state.Size, refs)
}

func (r *Remote) Exists(ctx context.Context, name string) (bool, error) {
	_, _, err := r.Inspect(ctx, name)
	if errors.Is(err, objectstore.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (r *Remote) List(ctx context.Context) ([]State, error) {
	keys, err := r.Store.List(ctx, "volumes/")
	if err != nil {
		return nil, err
	}
	states := make([]State, 0)
	seen := make(map[string]struct{})
	for _, key := range keys {
		name, ok := headVolumeName(key)
		if !ok {
			continue
		}
		if _, done := seen[name]; done {
			continue
		}
		seen[name] = struct{}{}
		if r.HeadMode() == ConditionalHead && strings.HasPrefix(key, HeadPrefix(name)) {
			return nil, foreignHeadError(name, AppendHead)
		}
		state, _, err := r.Inspect(ctx, name)
		if errors.Is(err, objectstore.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
}

func headVolumeName(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, "volumes/")
	if !ok {
		return "", false
	}
	name, tail, ok := strings.Cut(rest, "/")
	if !ok || !validVolumeName.MatchString(name) {
		return "", false
	}
	return name, tail == "state.json" || strings.HasPrefix(tail, "heads/")
}

func (r *Remote) Delete(ctx context.Context, name string) error {
	state, etag, err := r.Inspect(ctx, name)
	if err != nil {
		return err
	}
	if state.Lease != nil && state.Lease.ExpiresAt.After(r.now()) {
		return &HeldError{Volume: name, Holder: state.Lease.Holder, ExpiresAt: state.Lease.ExpiresAt}
	}
	state.Deleting = true
	state.Lease = nil
	state.Epoch++
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if _, err := r.head().commit(ctx, name, body, etag, state); err != nil {
		currentBody, _, getErr := r.readHeadBody(ctx, name)
		if getErr != nil || !bytes.Equal(currentBody, body) {
			return err
		}
	}
	keys, err := r.Store.List(ctx, VolumePrefix(name))
	if err != nil {
		return err
	}
	for _, key := range keys {
		if r.head().owns(name, key) {
			continue
		}
		if err := r.Store.Delete(ctx, key); err != nil {
			return err
		}
	}
	return r.head().remove(ctx, name)
}

func (r *Remote) DeleteFamily(ctx context.Context, base string) error {
	states, err := r.List(ctx)
	if err != nil {
		return err
	}
	prefix := base + ".r-"
	for _, state := range states {
		if state.Name == base || strings.HasPrefix(state.Name, prefix) {
			if err := r.Delete(ctx, state.Name); err != nil {
				return err
			}
		}
	}
	return nil
}
