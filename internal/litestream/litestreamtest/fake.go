package litestreamtest

import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/zephyraoss/satchel/internal/litestream"
	"github.com/zephyraoss/satchel/internal/objectstore"
)

type Fake struct {
	Store objectstore.Store
	mu    sync.Mutex
}

func New(store objectstore.Store) *Fake {
	return &Fake{Store: store}
}

func (f *Fake) Key(volume string) string {
	return litestream.ReplicaPath(volume) + "/snapshot.db"
}

func (f *Fake) Restore(ctx context.Context, volume, dbPath string, _ litestream.RestoreOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, err := f.Store.Get(ctx, f.Key(volume))
	if errors.Is(err, objectstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return os.WriteFile(dbPath, obj.Data, 0o600)
}

func (f *Fake) SyncOnce(ctx context.Context, volume, dbPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(dbPath)
	if err != nil {
		return err
	}
	f.Store.Delete(ctx, f.Key(volume))
	_, err = f.Store.PutIfAbsent(ctx, f.Key(volume), data)
	return err
}

type process struct {
	fake   *Fake
	volume string
	dbPath string
	done   chan struct{}
	once   sync.Once
}

func (p *process) Stop(ctx context.Context) error {
	p.once.Do(func() { close(p.done) })
	return p.fake.SyncOnce(ctx, p.volume, p.dbPath)
}

func (p *process) Kill()                 { p.once.Do(func() { close(p.done) }) }
func (p *process) Done() <-chan struct{} { return p.done }

func (f *Fake) Start(_ context.Context, volume, dbPath string, _ litestream.ReplicaOptions) (litestream.Process, error) {
	return &process{fake: f, volume: volume, dbPath: dbPath, done: make(chan struct{})}, nil
}
