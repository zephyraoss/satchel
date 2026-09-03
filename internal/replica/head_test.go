package replica

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

func appendRemote(store objectstore.Store, now *time.Time) *Remote {
	return &Remote{
		Store: store, Head: AppendHead, Settle: 5 * time.Millisecond, TTL: 30 * time.Second,
		Now: func() time.Time { return *now },
	}
}

func TestAppendHeadPublishesAndFencesWithoutConditionalWrites(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewUnconditionalMemory()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	remote := appendRemote(store, &now)

	first, created, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil || !created {
		t.Fatalf("first acquire: created=%v err=%v", created, err)
	}
	segA, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{1: bytes.Repeat([]byte("a"), DefaultBlockSize)}})
	if err := first.Publish(ctx, segA); err != nil {
		t.Fatal(err)
	}
	if err := first.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := remote.Acquire(ctx, "data", "node-b", CreateOptions{}); err == nil {
		t.Fatal("second node acquired a held lease")
	}

	now = now.Add(time.Minute)
	second, _, err := remote.Acquire(ctx, "data", "node-b", CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := second.State().Lease.Epoch; got != 2 {
		t.Fatalf("lease epoch = %d, want 2", got)
	}
	stale, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{2: bytes.Repeat([]byte("x"), DefaultBlockSize)}})
	if err := first.Publish(ctx, stale); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale publish error = %v", err)
	}
	if err := first.Renew(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale renew error = %v", err)
	}
	segB, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{2: bytes.Repeat([]byte("b"), DefaultBlockSize)}})
	if err := second.Publish(ctx, segB); err != nil {
		t.Fatal(err)
	}
	state, _, err := remote.Inspect(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 2 || state.Epoch != 2 {
		t.Fatalf("head after takeover = generation %d epoch %d", state.Generation, state.Epoch)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
	remote.head().(*appendHead).prunes.Wait()
	keys, _ := store.List(ctx, HeadPrefix("data"))
	if len(keys) > 1+retainedHeadVersions {
		t.Fatalf("head history was not pruned: %v", keys)
	}
}

func TestAppendHeadConcurrentClaimsAdmitAtMostOne(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewUnconditionalMemory()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	remote := appendRemote(store, &now)
	lease, _, err := remote.Acquire(ctx, "data", "seed", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}

	const contenders = 6
	var group sync.WaitGroup
	winners := make(chan *Lease, contenders)
	for i := range contenders {
		group.Add(1)
		go func() {
			defer group.Done()
			contender := appendRemote(store, &now)
			holder := "node-" + strings.Repeat("x", i+1)
			if won, _, err := contender.Acquire(ctx, "data", holder, CreateOptions{}); err == nil {
				winners <- won
			}
		}()
	}
	group.Wait()
	close(winners)
	var held []*Lease
	for won := range winners {
		held = append(held, won)
	}
	if len(held) != 1 {
		t.Fatalf("%d contenders acquired the lease, want exactly one", len(held))
	}
	state, _, err := remote.Inspect(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if state.Lease == nil || state.Lease.Token != held[0].State().Lease.Token {
		t.Fatal("published head does not match the winner")
	}
	if err := held[0].Renew(ctx); err != nil {
		t.Fatalf("winner cannot renew: %v", err)
	}
}

type lostAckHeadStore struct {
	objectstore.Store
	mu        sync.Mutex
	dropsLeft int
	failLists int
}

func (s *lostAckHeadStore) Put(ctx context.Context, key string, data []byte) (string, error) {
	etag, err := s.Store.Put(ctx, key, data)
	if err != nil || !strings.Contains(key, "/heads/") {
		return etag, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dropsLeft > 0 {
		s.dropsLeft--
		s.failLists++
		return "", errors.New("write head: connection reset while awaiting response")
	}
	return etag, nil
}

func (s *lostAckHeadStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	if s.failLists > 0 {
		s.failLists--
		s.mu.Unlock()
		return nil, errors.New("list heads: connection reset")
	}
	s.mu.Unlock()
	return s.Store.List(ctx, prefix)
}

func TestAppendHeadRejectsLostAckThenAdoptsCommit(t *testing.T) {
	ctx := context.Background()
	store := &lostAckHeadStore{Store: objectstore.NewUnconditionalMemory()}
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	remote := appendRemote(store, &now)
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.dropsLeft = 1
	store.mu.Unlock()
	segment, _ := EncodeSegment(&Generation{Blocks: map[uint64][]byte{1: bytes.Repeat([]byte("a"), DefaultBlockSize)}})
	if err := lease.Publish(ctx, segment); err == nil {
		t.Fatal("publish with a dropped acknowledgement reported success")
	}
	if err := lease.Publish(ctx, segment); err != nil {
		t.Fatalf("retry after lost ack: %v", err)
	}
	state, _, err := remote.Inspect(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 1 {
		t.Fatalf("generation after adopted retry = %d", state.Generation)
	}
}

func TestAppendHeadBreakAndDeleteBumpEpoch(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewUnconditionalMemory()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	remote := appendRemote(store, &now)
	lease, _, err := remote.Acquire(ctx, "data", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	token := lease.State().Lease.Token
	if err := remote.Break(ctx, "data", token); err != nil {
		t.Fatal(err)
	}
	if err := lease.Renew(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("renew after break = %v", err)
	}
	state, _, err := remote.Inspect(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if state.Lease != nil || state.Epoch != 2 {
		t.Fatalf("state after break = %+v", state)
	}
	if err := remote.Delete(ctx, "data"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := remote.Inspect(ctx, "data"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("inspect after delete = %v", err)
	}
	keys, _ := store.List(ctx, VolumePrefix("data"))
	if len(keys) != 0 {
		t.Fatalf("delete left objects behind: %v", keys)
	}
}

func TestAppendHeadListsVolumes(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewUnconditionalMemory()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	remote := appendRemote(store, &now)
	for _, name := range []string{"alpha", "beta"} {
		lease, _, err := remote.Acquire(ctx, name, "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Release(ctx); err != nil {
			t.Fatal(err)
		}
	}
	states, err := remote.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || states[0].Name != "alpha" || states[1].Name != "beta" {
		t.Fatalf("listed %+v", states)
	}
}

func TestHeadModesRefuseEachOthersVolumes(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemory()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	conditional := &Remote{Store: store, Now: func() time.Time { return now }}
	appendMode := appendRemote(store, &now)

	lease, _, err := conditional.Acquire(ctx, "cond", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := appendMode.Acquire(ctx, "cond", "node-b", CreateOptions{}); err == nil || !strings.Contains(err.Error(), "--s3-head=conditional") {
		t.Fatalf("append head over a conditional volume = %v", err)
	}

	lease, _, err = appendMode.Acquire(ctx, "app", "node-a", CreateOptions{Size: 8 * DefaultBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conditional.Acquire(ctx, "app", "node-b", CreateOptions{Size: 8 * DefaultBlockSize}); err == nil || !strings.Contains(err.Error(), "--s3-head=append") {
		t.Fatalf("conditional head over an append volume = %v", err)
	}
}

func TestHeadKeysSortNewestFirst(t *testing.T) {
	token := strings.Repeat("t", headTokenLength)
	older := headVersion{epoch: 1, token: token, seq: 5, claim: 100}
	newerSeq := headVersion{epoch: 1, token: token, seq: 6, claim: 100}
	newerEpoch := headVersion{epoch: 2, token: token, seq: 0, claim: 50}
	if !(newerEpoch.key("v") < newerSeq.key("v") && newerSeq.key("v") < older.key("v")) {
		t.Fatalf("keys do not sort newest first:\n%s\n%s\n%s", newerEpoch.key("v"), newerSeq.key("v"), older.key("v"))
	}
	parsed, ok := parseHeadKey("v", older.key("v"))
	if !ok || parsed != older {
		t.Fatalf("parse round trip = %+v ok=%v", parsed, ok)
	}
}
