package replica

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

type HeadMode string

const (
	ConditionalHead HeadMode = "conditional"
	AppendHead      HeadMode = "append"

	DefaultSettle = 3 * time.Second
)

func ParseHeadMode(value string) (HeadMode, error) {
	switch HeadMode(strings.ToLower(strings.TrimSpace(value))) {
	case "", ConditionalHead:
		return ConditionalHead, nil
	case AppendHead:
		return AppendHead, nil
	}
	return "", fmt.Errorf("unknown head mode %q (use conditional or append)", value)
}

var ErrClaimTooSlow = errors.New("head claim took longer than half the settle window")

type volumeHead interface {
	read(ctx context.Context, name string) ([]byte, string, error)
	commit(ctx context.Context, name string, body []byte, prev string, next State) (string, error)
	owns(name, key string) bool
	remove(ctx context.Context, name string) error
	verifyBackend(ctx context.Context) error
}

func (r *Remote) head() volumeHead {
	r.headOnce.Do(func() {
		switch r.Head {
		case AppendHead:
			r.volumeHead = &appendHead{store: r.Store, now: r.now, settle: r.settle(), ttl: r.ttl()}
		default:
			r.volumeHead = conditionalHead{store: r.Store}
		}
	})
	return r.volumeHead
}

func (r *Remote) settle() time.Duration {
	if r.Settle > 0 {
		return r.Settle
	}
	return DefaultSettle
}

func (r *Remote) VerifyBackend(ctx context.Context) error {
	return r.head().verifyBackend(ctx)
}

func (r *Remote) HeadMode() HeadMode {
	if r.Head == AppendHead {
		return AppendHead
	}
	return ConditionalHead
}

func HeadPrefix(name string) string { return VolumePrefix(name) + "heads/" }

func foreignHeadError(name string, mode HeadMode) error {
	return fmt.Errorf("volume %s is published with the %s head; run satchel with --s3-head=%s", name, mode, mode)
}

type conditionalHead struct {
	store objectstore.Store
}

func (h conditionalHead) read(ctx context.Context, name string) ([]byte, string, error) {
	obj, err := h.store.Get(ctx, StateKey(name))
	if err != nil {
		return nil, "", err
	}
	return obj.Data, obj.ETag, nil
}

func (h conditionalHead) commit(ctx context.Context, name string, body []byte, prev string, _ State) (string, error) {
	if prev == "" {
		keys, err := h.store.List(ctx, HeadPrefix(name))
		if err == nil && len(keys) > 0 {
			return "", foreignHeadError(name, AppendHead)
		}
		return h.store.PutIfAbsent(ctx, StateKey(name), body)
	}
	return h.store.PutIfMatch(ctx, StateKey(name), body, prev)
}

func (h conditionalHead) owns(name, key string) bool { return key == StateKey(name) }

func (h conditionalHead) remove(ctx context.Context, name string) error {
	return h.store.Delete(ctx, StateKey(name))
}

func (h conditionalHead) verifyBackend(ctx context.Context) error {
	return objectstore.VerifyConditionalWrites(ctx, h.store)
}

type headVersion struct {
	epoch uint64
	token string
	seq   uint64
	claim int64
}

func (v headVersion) key(name string) string {
	return fmt.Sprintf("%s%020d_%s_%020d_%013d.json", HeadPrefix(name), math.MaxUint64-v.epoch, v.token, math.MaxUint64-v.seq, v.claim)
}

func (v headVersion) sameOwner(other headVersion) bool {
	return v.epoch == other.epoch && v.token == other.token
}

func parseHeadKey(name, key string) (headVersion, bool) {
	rest, ok := strings.CutPrefix(key, HeadPrefix(name))
	if !ok {
		return headVersion{}, false
	}
	rest, ok = strings.CutSuffix(rest, ".json")
	if !ok {
		return headVersion{}, false
	}
	parts := strings.Split(rest, "_")
	if len(parts) != 4 || len(parts[0]) != 20 || len(parts[1]) != headTokenLength || len(parts[2]) != 20 || len(parts[3]) != 13 {
		return headVersion{}, false
	}
	invEpoch, err1 := strconv.ParseUint(parts[0], 10, 64)
	invSeq, err2 := strconv.ParseUint(parts[2], 10, 64)
	claim, err3 := strconv.ParseInt(parts[3], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return headVersion{}, false
	}
	return headVersion{epoch: math.MaxUint64 - invEpoch, token: parts[1], seq: math.MaxUint64 - invSeq, claim: claim}, true
}

const headTokenLength = 36

type headEntry struct {
	key     string
	version headVersion
}

type appendHead struct {
	store  objectstore.Store
	now    func() time.Time
	settle time.Duration
	ttl    time.Duration
	prunes sync.WaitGroup
}

func (h *appendHead) verifyBackend(ctx context.Context) error {
	return objectstore.VerifyListedWrites(ctx, h.store)
}

func (h *appendHead) owns(name, key string) bool {
	return strings.HasPrefix(key, HeadPrefix(name))
}

func (h *appendHead) list(ctx context.Context, name string) ([]headEntry, error) {
	keys, err := h.store.List(ctx, HeadPrefix(name))
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	entries := make([]headEntry, 0, len(keys))
	for _, key := range keys {
		if version, ok := parseHeadKey(name, key); ok {
			entries = append(entries, headEntry{key: key, version: version})
		}
	}
	return entries, nil
}

func (h *appendHead) read(ctx context.Context, name string) ([]byte, string, error) {
	for attempt := 0; attempt < 4; attempt++ {
		entries, err := h.list(ctx, name)
		if err != nil {
			return nil, "", err
		}
		head, ok := committedHead(entries)
		if !ok {
			if _, err := h.store.Get(ctx, StateKey(name)); err == nil {
				return nil, "", foreignHeadError(name, ConditionalHead)
			}
			return nil, "", objectstore.ErrNotFound
		}
		obj, err := h.store.Get(ctx, head.key)
		if errors.Is(err, objectstore.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		return obj.Data, head.key, nil
	}
	return nil, "", fmt.Errorf("head of volume %s kept changing while reading it", name)
}

func committedHead(entries []headEntry) (headEntry, bool) {
	for _, entry := range entries {
		if entry.version.seq > 0 {
			return entry, true
		}
	}
	return headEntry{}, false
}

func (h *appendHead) commit(ctx context.Context, name string, body []byte, prev string, next State) (string, error) {
	var base headVersion
	if prev != "" {
		parsed, ok := parseHeadKey(name, prev)
		if !ok {
			return "", fmt.Errorf("invalid head cursor %q", prev)
		}
		base = parsed
	}
	switch {
	case next.Epoch > base.epoch:
		return h.claim(ctx, name, body, base, next)
	case next.Epoch == base.epoch && prev != "":
		return h.advance(ctx, name, body, base)
	default:
		return "", fmt.Errorf("head epoch cannot move from %d to %d", base.epoch, next.Epoch)
	}
}

func (h *appendHead) claim(ctx context.Context, name string, body []byte, base headVersion, next State) (string, error) {
	token := uuid.NewString()
	if next.Lease != nil {
		token = next.Lease.Token
	}
	if len(token) != headTokenLength {
		return "", fmt.Errorf("lease token %q has the wrong length for a head key", token)
	}
	started := h.now()
	claim := headVersion{epoch: next.Epoch, token: token, claim: started.UnixMilli()}
	claimKey := claim.key(name)
	if _, err := h.store.Put(ctx, claimKey, body); err != nil {
		return "", err
	}
	abort := func(cause error) (string, error) {
		_ = h.store.Delete(context.WithoutCancel(ctx), claimKey)
		return "", cause
	}
	if elapsed := h.now().Sub(started); elapsed > h.settle/2 {
		return abort(fmt.Errorf("%w: %s", ErrClaimTooSlow, elapsed))
	}
	if err := sleepContext(ctx, h.settle); err != nil {
		return abort(err)
	}
	entries, err := h.liveEntries(ctx, name)
	if err != nil {
		return abort(err)
	}
	if !claimWins(entries, claim) || !baseUnchanged(entries, base) {
		return abort(objectstore.ErrPreconditionFailed)
	}
	first := claim
	first.seq = 1
	firstKey := first.key(name)
	if _, err := h.store.Put(ctx, firstKey, body); err != nil {
		return abort(err)
	}
	cursor, err := h.confirm(ctx, name, firstKey, first)
	if err != nil {
		_ = h.store.Delete(context.WithoutCancel(ctx), firstKey)
		return abort(err)
	}
	return cursor, nil
}

func claimWins(entries []headEntry, claim headVersion) bool {
	for _, entry := range entries {
		version := entry.version
		if version.epoch > claim.epoch || version.epoch == claim.epoch && version.seq > 0 {
			return false
		}
		if version.epoch == claim.epoch {
			return version.token == claim.token
		}
	}
	return false
}

func baseUnchanged(entries []headEntry, base headVersion) bool {
	if base.token == "" {
		return true
	}
	for _, entry := range entries {
		if entry.version.sameOwner(base) {
			return entry.version.seq == base.seq
		}
	}
	return false
}

func (h *appendHead) advance(ctx context.Context, name string, body []byte, base headVersion) (string, error) {
	version := base
	version.seq++
	key := version.key(name)
	if _, err := h.store.Put(ctx, key, body); err != nil {
		return "", err
	}
	return h.confirm(ctx, name, key, version)
}

func (h *appendHead) confirm(ctx context.Context, name, key string, version headVersion) (string, error) {
	entries, err := h.liveEntries(ctx, name)
	if err != nil {
		return "", err
	}
	if !ownerStillCurrent(entries, version) || !claimPresent(entries, version) {
		return "", objectstore.ErrPreconditionFailed
	}
	h.pruneBehind(entries, version)
	return key, nil
}

func ownerStillCurrent(entries []headEntry, version headVersion) bool {
	for _, entry := range entries {
		other := entry.version
		if other.epoch > version.epoch || other.epoch == version.epoch && other.token != version.token && other.seq > 0 {
			return false
		}
		if other.sameOwner(version) && other.seq > version.seq {
			return false
		}
	}
	return true
}

func claimPresent(entries []headEntry, owner headVersion) bool {
	for _, entry := range entries {
		if entry.version.sameOwner(owner) && entry.version.seq == 0 {
			return true
		}
	}
	return false
}

func (h *appendHead) liveEntries(ctx context.Context, name string) ([]headEntry, error) {
	entries, err := h.list(ctx, name)
	if err != nil {
		return nil, err
	}
	live := entries[:0]
	for _, entry := range entries {
		if !h.deadClaim(entries, entry) {
			live = append(live, entry)
			continue
		}
		if err := h.store.Delete(ctx, entry.key); err != nil {
			return nil, fmt.Errorf("remove abandoned head claim %s: %w", entry.key, err)
		}
	}
	return live, nil
}

func (h *appendHead) deadClaim(entries []headEntry, entry headEntry) bool {
	if entry.version.seq != 0 {
		return false
	}
	for _, other := range entries {
		if other.version.sameOwner(entry.version) && other.version.seq > 0 {
			return false
		}
	}
	return h.now().Sub(time.UnixMilli(entry.version.claim)) > max(h.ttl, 2*h.settle)
}

const retainedHeadVersions = 2

func (h *appendHead) pruneBehind(entries []headEntry, current headVersion) {
	var stale []string
	for _, entry := range entries {
		version := entry.version
		superseded := version.epoch < current.epoch ||
			version.epoch == current.epoch && version.token != current.token ||
			version.sameOwner(current) && version.seq > 0 && version.seq+retainedHeadVersions <= current.seq
		if superseded {
			stale = append(stale, entry.key)
		}
	}
	if len(stale) == 0 {
		return
	}
	h.prunes.Add(1)
	go func() {
		defer h.prunes.Done()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, key := range stale {
			_ = h.store.Delete(ctx, key)
		}
	}()
}

func (h *appendHead) remove(ctx context.Context, name string) error {
	h.prunes.Wait()
	return h.store.DeletePrefix(ctx, HeadPrefix(name))
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
