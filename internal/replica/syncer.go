package replica

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type Syncer struct {
	device             *Device
	lease              *Lease
	interval           time.Duration
	checkpointInterval uint64
	log                *slog.Logger
	wake               chan chan error
	done               chan struct{}
	cancel             context.CancelFunc
	onLost             func(error)
	stopOnce           sync.Once
	stopMu             sync.Mutex
	stopped            bool
}

func StartSyncer(device *Device, lease *Lease, interval time.Duration, checkpointInterval uint64, logger *slog.Logger, onLost func(error)) *Syncer {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Syncer{
		device: device, lease: lease, interval: interval, checkpointInterval: checkpointInterval, log: logger,
		wake: make(chan chan error), done: make(chan struct{}), cancel: cancel, onLost: onLost,
	}
	go s.run(ctx)
	return s
}

func (s *Syncer) run(ctx context.Context) {
	defer close(s.done)
	syncTicker := time.NewTicker(s.interval)
	defer syncTicker.Stop()
	var pending *Generation
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-s.wake:
			err := s.syncAll(ctx, &pending)
			if err == nil {
				err = s.checkpoint(ctx)
			}
			result <- err
			if errors.Is(err, ErrLeaseLost) {
				s.lose(err)
				return
			}
		case <-syncTicker.C:
			if err := s.sync(ctx, &pending); err != nil {
				if errors.Is(err, ErrLeaseLost) {
					s.lose(err)
					return
				}
				s.log.Warn("replication failed; retrying", "err", err)
			}
		}
	}
}

func (s *Syncer) checkpoint(ctx context.Context) error {
	if s.checkpointInterval == 0 {
		return nil
	}
	state := s.lease.State()
	if state.Generation-state.Checkpoint < s.checkpointInterval {
		return nil
	}
	checkpoint, err := s.lease.BeginCheckpoint()
	if err != nil {
		return err
	}
	if err := s.device.Checkpoint(ctx, 4096, func(segment Segment) error {
		return checkpoint.Add(ctx, segment)
	}); err != nil {
		return err
	}
	return checkpoint.Commit(ctx)
}

func (s *Syncer) sync(ctx context.Context, pending **Generation) error {
	if *pending == nil {
		*pending = s.device.Seal()
	}
	if (*pending).Empty() {
		*pending = nil
		return nil
	}
	segment, err := EncodeSegment(*pending)
	if err != nil {
		return err
	}
	if err := s.lease.Publish(ctx, segment); err != nil {
		return err
	}
	s.device.Release(*pending)
	*pending = nil
	return nil
}

func (s *Syncer) syncAll(ctx context.Context, pending **Generation) error {
	for {
		if err := s.sync(ctx, pending); err != nil {
			return err
		}
		if *pending == nil && s.device.DirtyBytes() == 0 {
			return nil
		}
	}
}

func (s *Syncer) lose(err error) {
	if s.onLost != nil {
		s.onLost(err)
	}
}

func (s *Syncer) Sync(ctx context.Context) error {
	result := make(chan error, 1)
	select {
	case s.wake <- result:
	case <-s.done:
		return errors.New("replicator stopped")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-s.done:
		return errors.New("replicator stopped")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Syncer) Stop(ctx context.Context) error {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	if s.stopped {
		return nil
	}
	if err := s.Sync(ctx); err != nil {
		return err
	}
	if err := s.lease.Release(ctx); err != nil {
		return err
	}
	s.stopOnce.Do(s.cancel)
	select {
	case <-s.done:
		s.stopped = true
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Syncer) Abandon() {
	s.stopOnce.Do(s.cancel)
	<-s.done
}
