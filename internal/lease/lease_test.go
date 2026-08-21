package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newManagers(t *testing.T) (*Manager, *Manager, *fakeClock) {
	t.Helper()
	store := objectstore.NewMemory()
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	a := &Manager{Store: store, NodeID: "node-a", TTL: 30 * time.Second, Now: clock.now}
	b := &Manager{Store: store, NodeID: "node-b", TTL: 30 * time.Second, Now: clock.now}
	return a, b, clock
}

func TestSecondAcquireFailsWhileHeld(t *testing.T) {
	a, b, _ := newManagers(t)
	ctx := context.Background()
	if _, err := a.Acquire(ctx, "vol"); err != nil {
		t.Fatal(err)
	}
	_, err := b.Acquire(ctx, "vol")
	var held *HeldError
	if !errors.As(err, &held) {
		t.Fatalf("expected HeldError, got %v", err)
	}
	if held.Holder != "node-a" {
		t.Fatalf("holder = %q", held.Holder)
	}
}

func TestAcquireAfterRelease(t *testing.T) {
	a, b, _ := newManagers(t)
	ctx := context.Background()
	l, err := a.Acquire(ctx, "vol")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Acquire(ctx, "vol"); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestExpiredLeaseIsTakenOver(t *testing.T) {
	a, b, clock := newManagers(t)
	ctx := context.Background()
	if _, err := a.Acquire(ctx, "vol"); err != nil {
		t.Fatal(err)
	}
	clock.advance(31 * time.Second)
	if _, err := b.Acquire(ctx, "vol"); err != nil {
		t.Fatalf("takeover of expired lease: %v", err)
	}
}

func TestRenewDetectsTakeover(t *testing.T) {
	a, b, clock := newManagers(t)
	ctx := context.Background()
	la, err := a.Acquire(ctx, "vol")
	if err != nil {
		t.Fatal(err)
	}
	clock.advance(31 * time.Second)
	if _, err := b.Acquire(ctx, "vol"); err != nil {
		t.Fatal(err)
	}
	if err := la.renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("renew after takeover = %v, want ErrLost", err)
	}
	if err := la.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("release after takeover = %v, want ErrLost", err)
	}
}

func TestHeartbeatFencesOnLoss(t *testing.T) {
	a, _, _ := newManagers(t)
	a.TTL = 300 * time.Millisecond
	ctx := context.Background()
	l, err := a.Acquire(ctx, "vol")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Break(ctx, "vol"); err != nil {
		t.Fatal(err)
	}
	lost := make(chan error, 1)
	go l.Heartbeat(ctx, func(err error) { lost <- err })
	select {
	case err := <-lost:
		if !errors.Is(err, ErrLost) {
			t.Fatalf("onLost err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat did not report loss")
	}
}
