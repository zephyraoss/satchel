package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/docker/go-plugins-helpers/volume"

	"github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/lease"
	"github.com/zephyraoss/satchel/internal/litestream"
	"github.com/zephyraoss/satchel/internal/metrics"
	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/seed"
	"github.com/zephyraoss/satchel/internal/store"
)

var validName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

type Config struct {
	NodeID   string
	StateDir string
	Logger   *slog.Logger
	Seeder   *seed.Seeder
}

type Driver struct {
	cfg     Config
	store   objectstore.Store
	leases  *lease.Manager
	ls      litestream.Runner
	backend backend.Backend
	log     *slog.Logger

	mu      sync.Mutex
	volumes map[string]*volumeState
}

type volumeRecord struct {
	Name      string        `json:"name"`
	CreatedAt time.Time     `json:"created_at"`
	Options   VolumeOptions `json:"options"`
}

type volumeState struct {
	mu     sync.Mutex
	record volumeRecord
	mounts map[string]*mountState
}

type mountState struct {
	name       string
	mountIDs   map[string]struct{}
	active     bool
	lease      *lease.Lease
	unmounter  backend.Unmounter
	replicator litestream.Process
	stopBeat   context.CancelFunc
	fenced     error
}

func New(cfg Config, store objectstore.Store, leases *lease.Manager, ls litestream.Runner, be backend.Backend) (*Driver, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	d := &Driver{cfg: cfg, store: store, leases: leases, ls: ls, backend: be, log: cfg.Logger, volumes: map[string]*volumeState{}}
	for _, sub := range []string{"volumes", "dbs", "mounts"} {
		if err := os.MkdirAll(filepath.Join(cfg.StateDir, sub), 0o700); err != nil {
			return nil, err
		}
	}
	if err := d.loadRegistry(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Driver) registryPath(name string) string {
	return filepath.Join(d.cfg.StateDir, "volumes", name+".json")
}

func (d *Driver) dbPath(name string) string {
	return filepath.Join(d.cfg.StateDir, "dbs", name+".db")
}

func (d *Driver) mountpoint(name string) string {
	return filepath.Join(d.cfg.StateDir, "mounts", name)
}

func (d *Driver) loadRegistry() error {
	entries, err := os.ReadDir(filepath.Join(d.cfg.StateDir, "volumes"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(d.cfg.StateDir, "volumes", entry.Name()))
		if err != nil {
			return err
		}
		var rec volumeRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return fmt.Errorf("corrupt registry entry %s: %w", entry.Name(), err)
		}
		d.volumes[rec.Name] = newState(rec)
	}
	return nil
}

func newState(rec volumeRecord) *volumeState {
	return &volumeState{record: rec, mounts: map[string]*mountState{}}
}

func (d *Driver) saveRecord(rec volumeRecord) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(d.registryPath(rec.Name), data, 0o600)
}

func (d *Driver) Create(req *volume.CreateRequest) error {
	if !validName.MatchString(req.Name) {
		return fmt.Errorf("invalid volume name %q", req.Name)
	}
	opts, err := ParseVolumeOptions(req.Options)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.volumes[req.Name]; exists {
		return nil
	}
	rec := volumeRecord{Name: req.Name, CreatedAt: time.Now().UTC(), Options: opts}
	if err := d.saveRecord(rec); err != nil {
		return err
	}
	d.volumes[req.Name] = newState(rec)
	d.log.Info("volume created", "volume", req.Name, "mode", opts.Mode, "scope", opts.Scope)
	return nil
}

func (d *Driver) lookup(ctx context.Context, name string) (*volumeState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if vs, ok := d.volumes[name]; ok {
		return vs, nil
	}
	if !validName.MatchString(name) {
		return nil, fmt.Errorf("volume %s not found", name)
	}
	remote, err := d.existsRemotely(ctx, name)
	if err != nil {
		return nil, err
	}
	if !remote {
		return nil, fmt.Errorf("volume %s not found", name)
	}
	opts, _ := ParseVolumeOptions(nil)
	rec := volumeRecord{Name: name, CreatedAt: time.Now().UTC(), Options: opts}
	if err := d.saveRecord(rec); err != nil {
		return nil, err
	}
	vs := newState(rec)
	d.volumes[name] = vs
	d.log.Info("adopted volume from bucket", "volume", name)
	return vs, nil
}

func (d *Driver) existsRemotely(ctx context.Context, name string) (bool, error) {
	keys, err := d.store.List(ctx, litestream.ReplicaPath(name)+"/")
	if err != nil {
		return false, fmt.Errorf("list bucket: %w", err)
	}
	return len(keys) > 0, nil
}

func (d *Driver) List() (*volume.ListResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	resp := &volume.ListResponse{}
	for _, vs := range d.volumes {
		resp.Volumes = append(resp.Volumes, d.describe(vs))
	}
	sort.Slice(resp.Volumes, func(i, j int) bool { return resp.Volumes[i].Name < resp.Volumes[j].Name })
	return resp, nil
}

func (d *Driver) describe(vs *volumeState) *volume.Volume {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	v := &volume.Volume{
		Name:      vs.record.Name,
		CreatedAt: vs.record.CreatedAt.Format(time.RFC3339),
		Status: map[string]interface{}{
			"backend": d.backend.Name(),
			"node":    d.cfg.NodeID,
			"mode":    vs.record.Options.Mode,
			"scope":   vs.record.Options.Scope,
		},
	}
	if vs.record.Options.Seed != "" {
		v.Status["seed"] = vs.record.Options.Seed
	}
	var mounted []string
	for name, ms := range vs.mounts {
		if ms.active {
			mounted = append(mounted, name)
		}
		if ms.fenced != nil {
			v.Status["fenced"] = ms.fenced.Error()
		}
		if ms.lease != nil {
			v.Status["lease_holder"] = ms.lease.Holder()
		}
	}
	sort.Strings(mounted)
	if len(mounted) == 1 {
		v.Mountpoint = d.mountpoint(mounted[0])
	}
	if len(mounted) > 0 {
		v.Status["mounted_as"] = mounted
	}
	return v
}

func (d *Driver) Get(req *volume.GetRequest) (*volume.GetResponse, error) {
	vs, err := d.lookup(context.Background(), req.Name)
	if err != nil {
		return nil, err
	}
	return &volume.GetResponse{Volume: d.describe(vs)}, nil
}

func (d *Driver) Remove(req *volume.RemoveRequest) error {
	ctx := context.Background()
	vs, err := d.lookup(ctx, req.Name)
	if err != nil {
		return err
	}
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for _, ms := range vs.mounts {
		if ms.active {
			return fmt.Errorf("volume %s is mounted on this node", req.Name)
		}
	}
	if held, err := d.leases.Inspect(ctx, req.Name); err != nil {
		return err
	} else if held != nil && held.ExpiresAt.After(time.Now()) {
		return &lease.HeldError{Volume: req.Name, Holder: held.Holder, ExpiresAt: held.ExpiresAt}
	}
	if !vs.record.Options.ReadOnly() {
		if err := d.store.DeletePrefix(ctx, litestream.ReplicaPath(req.Name)+"/"); err != nil {
			return fmt.Errorf("delete replica: %w", err)
		}
		if err := d.store.Delete(ctx, lease.Key(req.Name)); err != nil {
			return err
		}
	}
	d.cleanupLocal(req.Name)
	for name := range vs.mounts {
		d.cleanupLocal(name)
	}
	if err := os.Remove(d.registryPath(req.Name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	d.mu.Lock()
	delete(d.volumes, req.Name)
	d.mu.Unlock()
	d.log.Info("volume removed", "volume", req.Name)
	return nil
}

func (d *Driver) cleanupLocal(name string) {
	dbPath := d.dbPath(name)
	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm", dbPath + "-litestream"} {
		os.RemoveAll(path)
	}
	os.RemoveAll(filepath.Join(filepath.Dir(dbPath), "."+filepath.Base(dbPath)+"-litestream"))
	os.RemoveAll(d.mountpoint(name))
}

func (d *Driver) Path(req *volume.PathRequest) (*volume.PathResponse, error) {
	vs, err := d.lookup(context.Background(), req.Name)
	if err != nil {
		return nil, err
	}
	return &volume.PathResponse{Mountpoint: d.describe(vs).Mountpoint}, nil
}

func replicaName(base, mountID string) string {
	sum := sha256.Sum256([]byte(mountID))
	return base + ".r-" + hex.EncodeToString(sum[:6])
}

func (d *Driver) mountStateFor(vs *volumeState, mountID string) *mountState {
	name := vs.record.Name
	if vs.record.Options.PerReplica() {
		name = replicaName(name, mountID)
	}
	ms, ok := vs.mounts[name]
	if !ok {
		ms = &mountState{name: name, mountIDs: map[string]struct{}{}}
		vs.mounts[name] = ms
	}
	return ms
}

func (d *Driver) Mount(req *volume.MountRequest) (*volume.MountResponse, error) {
	ctx := context.Background()
	vs, err := d.lookup(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	vs.mu.Lock()
	defer vs.mu.Unlock()

	ms := d.mountStateFor(vs, req.ID)
	if ms.fenced != nil {
		return nil, fmt.Errorf("volume %s was fenced on this node (%v); unmount all users before remounting", req.Name, ms.fenced)
	}
	if ms.active {
		ms.mountIDs[req.ID] = struct{}{}
		return &volume.MountResponse{Mountpoint: d.mountpoint(ms.name)}, nil
	}
	if err := d.mountFresh(ctx, vs, ms); err != nil {
		return nil, err
	}
	ms.mountIDs[req.ID] = struct{}{}
	metrics.MountedVolumes.Inc()
	return &volume.MountResponse{Mountpoint: d.mountpoint(ms.name)}, nil
}

func (d *Driver) mountFresh(ctx context.Context, vs *volumeState, ms *mountState) error {
	opts := vs.record.Options
	if opts.ReadOnly() {
		return d.mountReadOnly(ctx, ms, opts)
	}
	name := ms.name
	l, err := d.leases.Acquire(ctx, name)
	if err != nil {
		var held *lease.HeldError
		if errors.As(err, &held) {
			metrics.MountFailures.WithLabelValues("lease_held").Inc()
			d.log.Warn("mount refused: lease held", "volume", name, "holder", held.Holder)
		} else {
			metrics.MountFailures.WithLabelValues("lease_error").Inc()
		}
		return err
	}
	releaseOnFailure := func(cause error) error {
		if relErr := l.Release(context.WithoutCancel(ctx)); relErr != nil {
			d.log.Error("release lease after failed mount", "volume", name, "err", relErr)
		}
		return cause
	}

	d.cleanupLocal(name)
	remoteExists, err := d.existsRemotely(ctx, name)
	if err != nil {
		return releaseOnFailure(err)
	}
	started := time.Now()
	if err := d.ls.Restore(ctx, name, d.dbPath(name), litestream.RestoreOptions{}); err != nil {
		metrics.MountFailures.WithLabelValues("restore").Inc()
		return releaseOnFailure(fmt.Errorf("restore %s: %w", name, err))
	}
	metrics.RestoreDuration.Observe(time.Since(started).Seconds())

	if !remoteExists && opts.Seed != "" {
		if err := d.applySeed(ctx, name, opts.Seed); err != nil {
			metrics.MountFailures.WithLabelValues("seed").Inc()
			d.cleanupLocal(name)
			return releaseOnFailure(fmt.Errorf("seed %s from %s: %w", name, opts.Seed, err))
		}
	}

	unmounter, err := d.backend.Mount(ctx, d.dbPath(name), d.mountpoint(name), backend.MountOptions{})
	if err != nil {
		metrics.MountFailures.WithLabelValues("backend").Inc()
		return releaseOnFailure(fmt.Errorf("mount backend for %s: %w", name, err))
	}
	replicator, err := d.ls.Start(ctx, name, d.dbPath(name), litestream.ReplicaOptions{SyncInterval: opts.SyncInterval})
	if err != nil {
		metrics.MountFailures.WithLabelValues("replication").Inc()
		unmounter.Abandon()
		return releaseOnFailure(fmt.Errorf("start replication for %s: %w", name, err))
	}

	beatCtx, stopBeat := context.WithCancel(context.Background())
	ms.active, ms.lease, ms.unmounter, ms.replicator, ms.stopBeat = true, l, unmounter, replicator, stopBeat
	metrics.LeaseHeld.WithLabelValues(name).Set(1)
	go l.Heartbeat(beatCtx, func(cause error) { d.fence(vs, ms, cause) })
	d.log.Info("volume mounted", "volume", name, "restore_took", time.Since(started), "seeded", !remoteExists && opts.Seed != "")
	return nil
}

func (d *Driver) applySeed(ctx context.Context, name, source string) error {
	if d.cfg.Seeder == nil {
		return errors.New("seeding is not configured")
	}
	db, err := store.Open(ctx, d.dbPath(name))
	if err != nil {
		return err
	}
	defer db.Close()
	count, err := d.cfg.Seeder.Apply(ctx, db, source)
	if err != nil {
		return err
	}
	d.log.Info("volume seeded", "volume", name, "source", source, "entries", count)
	return nil
}

func (d *Driver) mountReadOnly(ctx context.Context, ms *mountState, opts VolumeOptions) error {
	name := ms.name
	d.cleanupLocal(name)
	started := time.Now()
	if err := d.ls.Restore(ctx, name, d.dbPath(name), litestream.RestoreOptions{}); err != nil {
		metrics.MountFailures.WithLabelValues("restore").Inc()
		return fmt.Errorf("restore %s: %w", name, err)
	}
	metrics.RestoreDuration.Observe(time.Since(started).Seconds())
	unmounter, err := d.backend.Mount(ctx, d.dbPath(name), d.mountpoint(name), backend.MountOptions{ReadOnly: true})
	if err != nil {
		metrics.MountFailures.WithLabelValues("backend").Inc()
		return fmt.Errorf("mount backend for %s: %w", name, err)
	}
	ms.active, ms.unmounter = true, unmounter
	d.log.Info("volume mounted read-only", "volume", name, "restore_took", time.Since(started))
	return nil
}

func (d *Driver) fence(vs *volumeState, ms *mountState, cause error) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if ms.lease == nil {
		return
	}
	name := ms.name
	d.log.Error("fencing volume: lease lost, local changes will NOT be replicated", "volume", name, "cause", cause)
	metrics.LeaseFenced.WithLabelValues(name).Inc()
	metrics.LeaseHeld.WithLabelValues(name).Set(0)
	ms.replicator.Kill()
	if err := ms.unmounter.Abandon(); err != nil {
		d.log.Error("abandon mount", "volume", name, "err", err)
	}
	d.cleanupLocal(name)
	ms.active, ms.lease, ms.unmounter, ms.replicator, ms.stopBeat = false, nil, nil, nil, nil
	ms.fenced = cause
	metrics.MountedVolumes.Dec()
}

func (d *Driver) Unmount(req *volume.UnmountRequest) error {
	ctx := context.Background()
	vs, err := d.lookup(ctx, req.Name)
	if err != nil {
		return err
	}
	vs.mu.Lock()
	defer vs.mu.Unlock()
	ms := d.mountStateFor(vs, req.ID)
	delete(ms.mountIDs, req.ID)
	if ms.fenced != nil {
		if len(ms.mountIDs) == 0 {
			ms.fenced = nil
		}
		return nil
	}
	if !ms.active || len(ms.mountIDs) > 0 {
		return nil
	}
	return d.unmountLast(ctx, ms)
}

func (d *Driver) unmountLast(ctx context.Context, ms *mountState) error {
	name := ms.name
	if err := ms.unmounter.Unmount(ctx); err != nil {
		return fmt.Errorf("unmount %s: %w", name, err)
	}
	if ms.lease != nil {
		ms.stopBeat()
		started := time.Now()
		if err := ms.replicator.Stop(ctx); err != nil {
			return fmt.Errorf("final sync %s: %w", name, err)
		}
		metrics.SyncDuration.Observe(time.Since(started).Seconds())
		if err := ms.lease.Release(ctx); err != nil {
			d.log.Error("release lease", "volume", name, "err", err)
		}
		metrics.LeaseHeld.WithLabelValues(name).Set(0)
		d.log.Info("volume unmounted", "volume", name, "sync_took", time.Since(started))
	} else {
		d.log.Info("read-only volume unmounted", "volume", name)
	}
	metrics.MountedVolumes.Dec()
	d.cleanupLocal(name)
	ms.active, ms.lease, ms.unmounter, ms.replicator, ms.stopBeat = false, nil, nil, nil, nil
	return nil
}

func (d *Driver) Capabilities() *volume.CapabilitiesResponse {
	return &volume.CapabilitiesResponse{Capabilities: volume.Capability{Scope: "global"}}
}

func (d *Driver) Shutdown(ctx context.Context) error {
	d.mu.Lock()
	states := make([]*volumeState, 0, len(d.volumes))
	for _, vs := range d.volumes {
		states = append(states, vs)
	}
	d.mu.Unlock()
	var errs []error
	for _, vs := range states {
		vs.mu.Lock()
		for _, ms := range vs.mounts {
			if ms.active {
				if err := d.unmountLast(ctx, ms); err != nil {
					errs = append(errs, err)
				}
			}
		}
		vs.mu.Unlock()
	}
	return errors.Join(errs...)
}
