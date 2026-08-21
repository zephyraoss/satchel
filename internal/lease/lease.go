package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

const DefaultTTL = 30 * time.Second
const DefaultMaxHeartbeatFailures = 3

type Record struct {
	Holder    string    `json:"holder"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	MountedAt time.Time `json:"mounted_at"`
}

type HeldError struct {
	Volume    string
	Holder    string
	ExpiresAt time.Time
}

func (e *HeldError) Error() string {
	return fmt.Sprintf("volume %s is held by node %s (lease expires %s)", e.Volume, e.Holder, e.ExpiresAt.UTC().Format(time.RFC3339))
}

var ErrLost = errors.New("lease: lost to another holder")

type Manager struct {
	Store                objectstore.Store
	NodeID               string
	TTL                  time.Duration
	MaxHeartbeatFailures int
	Now                  func() time.Time
	Logger               *slog.Logger
}

func Key(volume string) string {
	return "leases/" + volume + ".json"
}

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Manager) ttl() time.Duration {
	if m.TTL > 0 {
		return m.TTL
	}
	return DefaultTTL
}

func (m *Manager) maxFailures() int {
	if m.MaxHeartbeatFailures > 0 {
		return m.MaxHeartbeatFailures
	}
	return DefaultMaxHeartbeatFailures
}

func (m *Manager) logger() *slog.Logger {
	if m.Logger != nil {
		return m.Logger
	}
	return slog.Default()
}

func (m *Manager) Inspect(ctx context.Context, volume string) (*Record, error) {
	obj, err := m.Store.Get(ctx, Key(volume))
	if errors.Is(err, objectstore.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec Record
	if err := json.Unmarshal(obj.Data, &rec); err != nil {
		return nil, fmt.Errorf("decode lease for %s: %w", volume, err)
	}
	return &rec, nil
}

func (m *Manager) Acquire(ctx context.Context, volume string) (*Lease, error) {
	key := Key(volume)
	now := m.now()
	rec := Record{
		Holder:    m.NodeID,
		Token:     uuid.NewString(),
		ExpiresAt: now.Add(m.ttl()),
		MountedAt: now,
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}

	existing, err := m.Store.Get(ctx, key)
	var etag string
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		etag, err = m.Store.PutIfAbsent(ctx, key, body)
	case err != nil:
		return nil, fmt.Errorf("read lease for %s: %w", volume, err)
	default:
		var current Record
		if decodeErr := json.Unmarshal(existing.Data, &current); decodeErr != nil {
			return nil, fmt.Errorf("decode lease for %s: %w", volume, decodeErr)
		}
		if current.ExpiresAt.After(now) {
			return nil, &HeldError{Volume: volume, Holder: current.Holder, ExpiresAt: current.ExpiresAt}
		}
		m.logger().Warn("taking over expired lease", "volume", volume, "previous_holder", current.Holder, "expired_at", current.ExpiresAt)
		etag, err = m.Store.PutIfMatch(ctx, key, body, existing.ETag)
	}
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return nil, m.describeRace(ctx, volume)
	}
	if err != nil {
		return nil, fmt.Errorf("acquire lease for %s: %w", volume, err)
	}
	return &Lease{manager: m, volume: volume, record: rec, etag: etag}, nil
}

func (m *Manager) describeRace(ctx context.Context, volume string) error {
	rec, err := m.Inspect(ctx, volume)
	if err != nil || rec == nil {
		return fmt.Errorf("lost race acquiring lease for %s", volume)
	}
	return &HeldError{Volume: volume, Holder: rec.Holder, ExpiresAt: rec.ExpiresAt}
}

func (m *Manager) Break(ctx context.Context, volume string) error {
	return m.Store.Delete(ctx, Key(volume))
}

type Lease struct {
	manager *Manager
	volume  string
	record  Record
	etag    string
}

func (l *Lease) Volume() string { return l.volume }
func (l *Lease) Record() Record { return l.record }
func (l *Lease) Holder() string { return l.record.Holder }

func (l *Lease) renew(ctx context.Context) error {
	rec := l.record
	rec.ExpiresAt = l.manager.now().Add(l.manager.ttl())
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	etag, err := l.manager.Store.PutIfMatch(ctx, Key(l.volume), body, l.etag)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return ErrLost
	}
	if err != nil {
		return err
	}
	l.record, l.etag = rec, etag
	return nil
}

func (l *Lease) Heartbeat(ctx context.Context, onLost func(error)) {
	interval := l.manager.ttl() / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		renewCtx, cancel := context.WithTimeout(ctx, interval)
		err := l.renew(renewCtx)
		cancel()
		switch {
		case err == nil:
			failures = 0
		case errors.Is(err, ErrLost):
			l.manager.logger().Error("lease taken by another holder; fencing", "volume", l.volume)
			onLost(err)
			return
		case ctx.Err() != nil:
			return
		default:
			failures++
			l.manager.logger().Warn("lease heartbeat failed", "volume", l.volume, "failures", failures, "err", err)
			if failures >= l.manager.maxFailures() {
				onLost(fmt.Errorf("lease heartbeat failed %d times: %w", failures, err))
				return
			}
		}
	}
}

func (l *Lease) Release(ctx context.Context) error {
	obj, err := l.manager.Store.Get(ctx, Key(l.volume))
	if errors.Is(err, objectstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var current Record
	if err := json.Unmarshal(obj.Data, &current); err != nil {
		return err
	}
	if current.Token != l.record.Token {
		return ErrLost
	}
	return l.manager.Store.Delete(ctx, Key(l.volume))
}
