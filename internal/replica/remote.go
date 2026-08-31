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
	Format     string       `json:"format"`
	ID         string       `json:"id"`
	Name       string       `json:"name"`
	Size       int64        `json:"size"`
	BlockSize  int64        `json:"block_size"`
	Filesystem string       `json:"filesystem"`
	Generation uint64       `json:"generation"`
	Manifest   string       `json:"manifest,omitempty"`
	Checkpoint uint64       `json:"checkpoint_generation,omitempty"`
	Epoch      uint64       `json:"epoch"`
	Deleting   bool         `json:"deleting,omitempty"`
	Lease      *LeaseRecord `json:"lease,omitempty"`
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
	Key    string `json:"key"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Blocks int64  `json:"blocks"`
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
	Store objectstore.Store
	TTL   time.Duration
	Now   func() time.Time
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
	obj, err := r.Store.Get(ctx, StateKey(name))
	if err != nil {
		return State{}, "", err
	}
	var state State
	if err := json.Unmarshal(obj.Data, &state); err != nil {
		return State{}, "", fmt.Errorf("decode volume state: %w", err)
	}
	if err := validateState(name, state); err != nil {
		return State{}, "", err
	}
	return state, obj.ETag, nil
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
	return nil
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
		etag, err = r.Store.PutIfAbsent(ctx, StateKey(name), body)
	} else {
		etag, err = r.Store.PutIfMatch(ctx, StateKey(name), body, etag)
	}
	if err != nil {
		current, getErr := r.Store.Get(ctx, StateKey(name))
		if getErr == nil && bytes.Equal(current.Data, body) {
			etag, err = current.ETag, nil
		}
	}
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return nil, false, fmt.Errorf("acquire %s: %w", name, ErrLeaseLost)
	}
	if err != nil {
		return nil, false, err
	}
	return &Lease{remote: r, state: state, etag: etag}, created, nil
}

type Lease struct {
	mu     sync.Mutex
	remote *Remote
	state  State
	etag   string
	lost   bool
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
	return l.state
}

func (l *Lease) update(ctx context.Context, mutate func(*State) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost || l.state.Lease == nil {
		return ErrLeaseLost
	}
	candidate := l.state
	if l.state.Lease != nil {
		lease := *l.state.Lease
		candidate.Lease = &lease
	}
	if err := mutate(&candidate); err != nil {
		return err
	}
	body, err := json.Marshal(candidate)
	if err != nil {
		return err
	}
	etag, err := l.commitState(ctx, body)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		l.lost = true
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	l.state = candidate
	l.etag = etag
	return nil
}

func (l *Lease) commitState(ctx context.Context, body []byte) (string, error) {
	etag, err := l.remote.Store.PutIfMatch(ctx, StateKey(l.state.Name), body, l.etag)
	if err == nil {
		return etag, nil
	}
	current, getErr := l.remote.Store.Get(ctx, StateKey(l.state.Name))
	if getErr == nil && bytes.Equal(current.Data, body) {
		return current.ETag, nil
	}
	return "", err
}

func (l *Lease) Renew(ctx context.Context) error {
	return l.update(ctx, func(state *State) error {
		state.Lease.ExpiresAt = l.remote.now().Add(l.remote.ttl())
		return nil
	})
}

func (l *Lease) Publish(ctx context.Context, segment Segment) error {
	if segment.SHA256 == "" || len(segment.Data) == 0 {
		return errors.New("invalid segment")
	}
	l.mu.Lock()
	if l.lost || l.state.Lease == nil {
		l.mu.Unlock()
		return ErrLeaseLost
	}
	snapshot := l.state
	l.mu.Unlock()

	ref, err := l.uploadSegment(ctx, snapshot, segment)
	if err != nil {
		return err
	}
	manifest := Manifest{
		Format: Format, VolumeID: snapshot.ID, Generation: snapshot.Generation + 1,
		Epoch: snapshot.Epoch, Kind: "delta", Parent: snapshot.Manifest,
		Segments:  []ObjectRef{ref},
		CreatedAt: l.remote.now(),
	}
	return l.publishManifest(ctx, snapshot, manifest)
}

func segmentKey(name string, epoch, generation uint64, digest string) string {
	return fmt.Sprintf("%ssegments/e%d-g%d-%s.seg.gz", VolumePrefix(name), epoch, generation, digest)
}

func (l *Lease) uploadSegment(ctx context.Context, snapshot State, segment Segment) (ObjectRef, error) {
	segmentKey := segmentKey(snapshot.Name, snapshot.Epoch, snapshot.Generation+1, segment.SHA256)
	if _, err := l.remote.Store.PutIfAbsent(ctx, segmentKey, segment.Data); err != nil {
		if !errors.Is(err, objectstore.ErrPreconditionFailed) {
			return ObjectRef{}, fmt.Errorf("upload segment: %w", err)
		}
		existing, getErr := l.remote.Store.Get(ctx, segmentKey)
		if getErr != nil || !bytes.Equal(existing.Data, segment.Data) {
			return ObjectRef{}, fmt.Errorf("segment object %s exists with different content", segmentKey)
		}
	}
	return ObjectRef{Key: segmentKey, SHA256: segment.SHA256, Bytes: segment.Bytes, Blocks: segment.Blocks}, nil
}

type Checkpoint struct {
	lease    *Lease
	snapshot State
	refs     []ObjectRef
}

func (l *Lease) BeginCheckpoint() (*Checkpoint, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost || l.state.Lease == nil {
		return nil, ErrLeaseLost
	}
	return &Checkpoint{lease: l, snapshot: l.state}, nil

}

func (c *Checkpoint) Add(ctx context.Context, segment Segment) error {
	ref, err := c.lease.uploadSegment(ctx, c.snapshot, segment)
	if err != nil {
		return err
	}
	c.refs = append(c.refs, ref)
	return nil
}

func (c *Checkpoint) Commit(ctx context.Context) error {
	manifest := Manifest{
		Format: Format, VolumeID: c.snapshot.ID, Generation: c.snapshot.Generation + 1,
		Epoch: c.snapshot.Epoch, Kind: "checkpoint", Parent: c.snapshot.Manifest, Segments: c.refs, CreatedAt: c.lease.remote.now(),
	}
	return c.lease.publishManifest(ctx, c.snapshot, manifest)
}

func (l *Lease) publishManifest(ctx context.Context, snapshot State, manifest Manifest) error {
	manifestBody, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(manifestBody)
	manifestKey := VolumePrefix(snapshot.Name) + "manifests/" + hex.EncodeToString(hash[:]) + ".json"
	if _, err := l.remote.Store.PutIfAbsent(ctx, manifestKey, manifestBody); err != nil {
		if !errors.Is(err, objectstore.ErrPreconditionFailed) {
			return fmt.Errorf("upload manifest: %w", err)
		}
		existing, getErr := l.remote.Store.Get(ctx, manifestKey)
		if getErr != nil || !bytes.Equal(existing.Data, manifestBody) {
			return fmt.Errorf("manifest object %s exists with different content", manifestKey)
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost || l.state.Lease == nil {
		return ErrLeaseLost
	}
	if l.state.Generation != snapshot.Generation || l.state.Manifest != snapshot.Manifest || l.state.Epoch != snapshot.Epoch {
		return errors.New("volume head changed during publish")
	}
	candidate := l.state
	candidate.Generation = manifest.Generation
	candidate.Manifest = manifestKey
	if manifest.Kind == "checkpoint" {
		candidate.Checkpoint = manifest.Generation
	}
	stateBody, err := json.Marshal(candidate)
	if err != nil {
		return err
	}
	etag, err := l.commitState(ctx, stateBody)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		l.lost = true
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("publish volume head: %w", err)
	}
	l.state = candidate
	l.etag = etag
	return nil
}

func (l *Lease) Release(ctx context.Context) error {
	return l.update(ctx, func(state *State) error {
		state.Lease = nil
		return nil
	})
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
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = r.Store.PutIfMatch(ctx, StateKey(name), body, etag)
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
	key := state.Manifest
	expectedGeneration := state.Generation
	allowMissingParent := false
	var history []manifestEntry
	for key != "" {
		manifest, err := r.readManifest(ctx, state, key, expectedGeneration)
		if errors.Is(err, objectstore.ErrNotFound) && allowMissingParent {
			break
		}
		if err != nil {
			return nil, err
		}
		history = append(history, manifestEntry{key: key, manifest: manifest})
		if manifest.Generation == 1 {
			if manifest.Parent != "" {
				return nil, fmt.Errorf("generation one manifest %s has a parent", key)
			}
			break
		}
		allowMissingParent = manifest.Kind == "checkpoint"
		key = manifest.Parent
		expectedGeneration--
		if key == "" && !allowMissingParent {
			return nil, errors.New("manifest history ended before generation one")
		}
		if len(history) > 1_000_000 {
			return nil, errors.New("manifest history is too long")
		}
	}
	return history, nil
}

func (r *Remote) readManifest(ctx context.Context, state State, key string, expectedGeneration uint64) (Manifest, error) {
	obj, err := r.Store.Get(ctx, key)
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest %s: %w", key, err)
	}
	var manifest Manifest
	if err := json.Unmarshal(obj.Data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest %s: %w", key, err)
	}
	hash := sha256.Sum256(obj.Data)
	wantKey := VolumePrefix(state.Name) + "manifests/" + hex.EncodeToString(hash[:]) + ".json"
	if key != wantKey {
		return Manifest{}, fmt.Errorf("manifest %s checksum mismatch", key)
	}
	if manifest.Format != Format || manifest.VolumeID != state.ID || manifest.Epoch == 0 {
		return Manifest{}, fmt.Errorf("manifest %s does not belong to volume %s", key, state.Name)
	}
	if manifest.Generation != expectedGeneration || expectedGeneration == 0 {
		return Manifest{}, fmt.Errorf("manifest %s breaks the generation chain", key)
	}
	switch manifest.Kind {
	case "delta":
		if len(manifest.Segments) != 1 {
			return Manifest{}, fmt.Errorf("delta manifest %s has %d segments", key, len(manifest.Segments))
		}
	case "checkpoint":
	default:
		return Manifest{}, fmt.Errorf("manifest %s has unknown kind %q", key, manifest.Kind)
	}
	return manifest, nil
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
	state := l.state
	l.mu.Unlock()
	return l.remote.collectGarbage(ctx, state, opts)
}

func (r *Remote) collectGarbage(ctx context.Context, state State, opts GCOptions) (GCResult, error) {
	history, err := r.loadHistory(ctx, state)
	if err != nil {
		return GCResult{}, err
	}
	keep := map[string]struct{}{}
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
			keep[entry.key] = struct{}{}
			for _, ref := range entry.manifest.Segments {
				keep[ref.Key] = struct{}{}
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
		for _, candidate := range record.Objects {
			if _, retained := keep[candidate]; retained || !isImmutableObject(prefix, candidate) {
				continue
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
	return strings.HasPrefix(key, prefix+"manifests/") || strings.HasPrefix(key, prefix+"segments/")
}

func (r *Remote) RestoreState(ctx context.Context, state State, path string) error {
	if err := validateState(state.Name, state); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := f.Truncate(state.Size); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	var manifests []Manifest
	key := state.Manifest
	expectedGeneration := state.Generation
	for key != "" {
		manifest, err := r.readManifest(ctx, state, key, expectedGeneration)
		if err != nil {
			return err
		}
		manifests = append(manifests, manifest)
		if manifest.Kind == "checkpoint" {
			expectedGeneration = 0
			break
		}
		expectedGeneration--
		key = manifest.Parent
		if len(manifests) > 1_000_000 {
			return errors.New("manifest chain is too long")
		}
	}
	if expectedGeneration != 0 {
		return errors.New("manifest chain ended before generation zero")
	}
	for i := len(manifests) - 1; i >= 0; i-- {
		for _, ref := range manifests[i].Segments {
			wantKey := segmentKey(state.Name, manifests[i].Epoch, manifests[i].Generation, ref.SHA256)
			if ref.Key != wantKey {
				return fmt.Errorf("segment reference %s has an invalid key", ref.Key)
			}
			obj, err := r.Store.Get(ctx, ref.Key)
			if err != nil {
				return fmt.Errorf("read segment %s: %w", ref.Key, err)
			}
			segment := Segment{Data: obj.Data, SHA256: ref.SHA256, Bytes: ref.Bytes, Blocks: ref.Blocks}
			if err := ApplySegment(path, state.Size, segment); err != nil {
				return fmt.Errorf("apply segment %s: %w", ref.Key, err)
			}
		}
	}
	return nil
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
	for _, key := range keys {
		if len(key) < len("/state.json") || key[len(key)-len("/state.json"):] != "/state.json" {
			continue
		}
		obj, err := r.Store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		var state State
		if err := json.Unmarshal(obj.Data, &state); err != nil {
			return nil, err
		}
		if err := validateState(state.Name, state); err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
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
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if _, err := r.Store.PutIfMatch(ctx, StateKey(name), body, etag); err != nil {
		current, getErr := r.Store.Get(ctx, StateKey(name))
		if getErr != nil || !bytes.Equal(current.Data, body) {
			return err
		}
	}
	keys, err := r.Store.List(ctx, VolumePrefix(name))
	if err != nil {
		return err
	}
	for _, key := range keys {
		if key == StateKey(name) {
			continue
		}
		if err := r.Store.Delete(ctx, key); err != nil {
			return err
		}
	}
	return r.Store.Delete(ctx, StateKey(name))
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
