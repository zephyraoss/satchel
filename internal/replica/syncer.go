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
	wake               chan struct{}
	queueMu            sync.Mutex
	requests           []syncRequest
	closed             bool
	urgent             chan struct{}
	done               chan struct{}
	cancel             context.CancelFunc
	onLost             func(error)
	retryInitial       time.Duration
	retryMax           time.Duration
	stopOnce           sync.Once
	stopMu             sync.Mutex
	stopped            bool
}

type syncRequest struct {
	generation *Generation
	checkpoint bool
	result     chan error
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
		wake: make(chan struct{}, 1), urgent: make(chan struct{}, 1), done: make(chan struct{}), cancel: cancel, onLost: onLost,
		retryInitial: 100 * time.Millisecond, retryMax: 2 * time.Second,
	}
	go s.run(ctx)
	return s
}

func (s *Syncer) run(ctx context.Context) {
	defer s.closeRequests()
	syncTicker := time.NewTicker(s.interval)
	defer syncTicker.Stop()
	var pending []*Generation
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
			request, ok := s.dequeue()
			if !ok {
				continue
			}
			err := s.process(ctx, &pending, request)
			if errors.Is(err, ErrLeaseLost) {
				s.lose(err)
				return
			}
		case <-s.urgent:
			err := s.process(ctx, &pending, s.backgroundRequest())
			if err != nil {
				if errors.Is(err, ErrLeaseLost) {
					s.lose(err)
					return
				}
				s.log.Warn("urgent replication failed; retrying", "err", err)
			}
		case <-syncTicker.C:
			err := s.process(ctx, &pending, s.backgroundRequest())
			if err != nil {
				if errors.Is(err, ErrLeaseLost) {
					s.lose(err)
					return
				}
				s.log.Warn("replication failed; retrying", "err", err)
			}
		}
	}
}

func (s *Syncer) backgroundRequest() syncRequest {
	s.device.mu.Lock()
	defer s.device.mu.Unlock()
	if request, ok := s.dequeue(); ok {
		return request
	}
	return syncRequest{generation: s.device.sealLocked()}
}

func (s *Syncer) process(ctx context.Context, pending *[]*Generation, first syncRequest) error {
	requests := []syncRequest{first}
	drain := func() bool {
		if request, ok := s.dequeue(); ok {
			requests = append(requests, request)
			return true
		}
		return false
	}
	for len(requests) < 128 {
		if !drain() {
			goto drained
		}
	}

drained:
	checkpoint := false
	for _, request := range requests {
		if !request.generation.Empty() {
			*pending = append(*pending, request.generation)
		}
		checkpoint = checkpoint || request.checkpoint
	}
	err := s.sync(ctx, pending)
	if err == nil && checkpoint {
		err = s.checkpoint(ctx)
	}
	for _, request := range requests {
		if request.result != nil {
			request.result <- err
		}
	}
	if err == nil && !checkpoint {
		err = s.checkpoint(ctx)
		if err != nil && !errors.Is(err, ErrLeaseLost) {
			s.log.Warn("checkpoint failed; retrying", "err", err)
		}
	}
	return err
}

func (s *Syncer) checkpoint(ctx context.Context) error {
	if s.checkpointInterval == 0 {
		return nil
	}
	state := s.lease.State()
	if state.Generation-state.Checkpoint < s.checkpointInterval {
		return nil
	}
	return s.lease.PublishCheckpoint(ctx)
}

func (s *Syncer) sync(ctx context.Context, pending *[]*Generation) error {
	if len(*pending) == 0 {
		return nil
	}
	generation := mergeGenerations(*pending)
	started := time.Now()
	segments, err := EncodeSegments(generation, DefaultSegmentBlocks)
	s.lease.remote.observe("encode", started)
	if err != nil {
		return err
	}
	s.lease.remote.observeGeneration(generation, segments)
	if err := s.publishWithRetry(ctx, segments); err != nil {
		return err
	}
	for _, published := range *pending {
		s.device.Release(published)
	}
	*pending = (*pending)[:0]
	return nil
}

// publishWithRetry keeps a remote flush waiting through transient S3 failures
// rather than failing the commit. A blocked flush is an unacknowledged commit,
// so stalling is safe: publication only succeeds once the conditional state
// update confirms this node still holds the lease. A real takeover surfaces as
// ErrLeaseLost and returns immediately so the caller fences. Lease expiry
// cancels ctx through the heartbeat, which bounds the stall and returns EIO.
func (s *Syncer) publishWithRetry(ctx context.Context, segments []Segment) error {
	backoff := s.retryInitial
	stalled := false
	for attempt := 0; ; attempt++ {
		err := s.lease.Publish(ctx, segments...)
		if err == nil {
			if stalled {
				s.log.Warn("remote publish recovered; commits resumed", "attempts", attempt+1)
			}
			return nil
		}
		if errors.Is(err, ErrLeaseLost) || ctx.Err() != nil {
			return err
		}
		if !stalled {
			stalled = true
			s.log.Warn("remote publish failing; commits stall until S3 recovers or the lease expires", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, s.retryMax)
	}
}

func mergeGenerations(generations []*Generation) *Generation {
	blocks := make(map[uint64][]byte)
	for _, generation := range generations {
		for block, data := range generation.Blocks {
			blocks[block] = data
		}
	}
	return &Generation{Blocks: blocks, bytes: int64(len(blocks)) * DefaultBlockSize}
}

func (s *Syncer) lose(err error) {
	if s.onLost != nil {
		s.onLost(err)
	}
}

func (s *Syncer) Sync(ctx context.Context) error {
	return s.request(ctx, false)
}

// EnqueueGeneration queues a generation already sealed by Device.Flush and
// returns a result that completes once that generation and every older one
// have reached remote storage.
func (s *Syncer) EnqueueGeneration(generation *Generation) <-chan error {
	result := make(chan error, 1)
	s.enqueue(syncRequest{generation: generation, result: result})
	return result
}

func (s *Syncer) enqueue(request syncRequest) {
	s.queueMu.Lock()
	if s.closed {
		s.queueMu.Unlock()
		if request.result != nil {
			request.result <- errors.New("replicator stopped")
		}
		return
	}
	s.requests = append(s.requests, request)
	s.queueMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Syncer) dequeue() (syncRequest, bool) {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if len(s.requests) == 0 {
		return syncRequest{}, false
	}
	request := s.requests[0]
	s.requests[0] = syncRequest{}
	s.requests = s.requests[1:]
	return request, true
}

func (s *Syncer) closeRequests() {
	s.queueMu.Lock()
	s.closed = true
	requests := s.requests
	s.requests = nil
	close(s.done)
	s.queueMu.Unlock()
	for _, request := range requests {
		if request.result != nil {
			request.result <- errors.New("replicator stopped")
		}
	}
}

// Notify asks the background worker to publish immediately. It never blocks
// the I/O path and duplicate notifications collapse into one wakeup.
func (s *Syncer) Notify() {
	select {
	case s.urgent <- struct{}{}:
	default:
	}
}

// SyncCheckpoint waits for all published data and any due checkpoint. Callers
// use it before garbage collection so publication cannot overlap collection.
func (s *Syncer) SyncCheckpoint(ctx context.Context) error {
	return s.request(ctx, true)
}

func (s *Syncer) request(ctx context.Context, checkpoint bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result := make(chan error, 1)
	s.device.mu.Lock()
	generation := s.device.sealLocked()
	s.enqueue(syncRequest{generation: generation, checkpoint: checkpoint, result: result})
	s.device.mu.Unlock()
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
	if err := s.SyncCheckpoint(ctx); err != nil {
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
