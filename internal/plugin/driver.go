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
	"syscall"
	"time"

	"github.com/docker/go-plugins-helpers/volume"

	"github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/metrics"
	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/replica"
	"github.com/zephyraoss/satchel/internal/seed"
)

var validName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

var ErrVolumeNotFound = errors.New("volume not found")

type Config struct {
	NodeID             string
	StateDir           string
	MaxDirty           int64
	SyncInterval       time.Duration
	CheckpointInterval uint64
	HistoryRetention   time.Duration
	GCGrace            time.Duration
	Logger             *slog.Logger
	Seeder             *seed.Seeder
}

type Driver struct {
	cfg     Config
	remote  *replica.Remote
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
	name      string
	mountIDs  map[string]struct{}
	active    bool
	lease     *replica.Lease
	device    *replica.Device
	unmounter backend.Unmounter
	syncer    *replica.Syncer
	stopBeat  context.CancelFunc
	fenced    error
}

func New(cfg Config, remote *replica.Remote, be backend.Backend) (*Driver, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxDirty <= 0 {
		cfg.MaxDirty = 256 << 20
	}
	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = 5 * time.Second
	}
	if cfg.CheckpointInterval == 0 {
		cfg.CheckpointInterval = 128
	}
	if cfg.HistoryRetention <= 0 {
		cfg.HistoryRetention = 7 * 24 * time.Hour
	}
	if cfg.GCGrace <= 0 {
		cfg.GCGrace = 24 * time.Hour
	}
	if remote == nil || remote.Store == nil {
		return nil, errors.New("remote object store is required")
	}
	if be == nil {
		return nil, errors.New("block backend is required")
	}
	d := &Driver{cfg: cfg, remote: remote, backend: be, log: cfg.Logger, volumes: map[string]*volumeState{}}
	for _, sub := range []string{"volumes", "images", "mounts"} {
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

func (d *Driver) imagePath(name string) string {
	return filepath.Join(d.cfg.StateDir, "images", name+".img")
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
		var record volumeRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("corrupt registry entry %s: %w", entry.Name(), err)
		}
		d.volumes[record.Name] = newState(record)
	}
	return nil
}

func newState(record volumeRecord) *volumeState {
	return &volumeState{record: record, mounts: map[string]*mountState{}}
}

func (d *Driver) saveRecord(record volumeRecord) error {
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(d.registryPath(record.Name), data, 0o600)
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
	if current, exists := d.volumes[req.Name]; exists {
		if len(req.Options) == 0 {
			return nil
		}
		if current.record.Options != opts {
			return fmt.Errorf("volume %s already exists with different options", req.Name)
		}
		return nil
	}
	record := volumeRecord{Name: req.Name, CreatedAt: time.Now().UTC(), Options: opts}
	if err := d.saveRecord(record); err != nil {
		return err
	}
	d.volumes[req.Name] = newState(record)
	d.log.Info("volume created", "volume", req.Name, "size", opts.Size, "mode", opts.Mode, "scope", opts.Scope, "durability", opts.Durability)
	return nil
}

func (d *Driver) lookup(ctx context.Context, name string) (*volumeState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if state, ok := d.volumes[name]; ok {
		return state, nil
	}
	if !validName.MatchString(name) {
		return nil, fmt.Errorf("volume %s not found", name)
	}
	remoteState, _, err := d.remote.Inspect(ctx, name)
	if errors.Is(err, objectstore.ErrNotFound) {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, name)
	}
	if err != nil {
		return nil, err
	}
	opts, _ := ParseVolumeOptions(nil)
	opts.Size = remoteState.Size
	opts.Filesystem = remoteState.Filesystem
	record := volumeRecord{Name: name, CreatedAt: time.Now().UTC(), Options: opts}
	if err := d.saveRecord(record); err != nil {
		return nil, err
	}
	state := newState(record)
	d.volumes[name] = state
	d.log.Info("adopted volume from bucket", "volume", name)
	return state, nil
}

func (d *Driver) List() (*volume.ListResponse, error) {
	d.mu.Lock()
	states := make([]*volumeState, 0, len(d.volumes))
	for _, state := range d.volumes {
		states = append(states, state)
	}
	d.mu.Unlock()
	response := &volume.ListResponse{}
	for _, state := range states {
		response.Volumes = append(response.Volumes, d.describe(state))
	}
	sort.Slice(response.Volumes, func(i, j int) bool { return response.Volumes[i].Name < response.Volumes[j].Name })
	return response, nil
}

func (d *Driver) describe(state *volumeState) *volume.Volume {
	state.mu.Lock()
	defer state.mu.Unlock()
	result := &volume.Volume{
		Name: state.record.Name, CreatedAt: state.record.CreatedAt.Format(time.RFC3339),
		Status: map[string]interface{}{
			"backend": "block", "node": d.cfg.NodeID, "mode": state.record.Options.Mode,
			"scope": state.record.Options.Scope, "durability": state.record.Options.Durability, "size": state.record.Options.Size,
			"filesystem": state.record.Options.Filesystem,
		},
	}
	var mounted []string
	for name, mount := range state.mounts {
		if mount.active {
			mounted = append(mounted, name)
		}
		if mount.fenced != nil {
			result.Status["fenced"] = mount.fenced.Error()
		}
	}
	sort.Strings(mounted)
	if len(mounted) == 1 {
		result.Mountpoint = d.mountpoint(mounted[0])
	}
	if len(mounted) > 0 {
		result.Status["mounted_as"] = mounted
	}
	return result
}

func (d *Driver) Get(req *volume.GetRequest) (*volume.GetResponse, error) {
	state, err := d.lookup(context.Background(), req.Name)
	if err != nil {
		return nil, err
	}
	return &volume.GetResponse{Volume: d.describe(state)}, nil
}

func (d *Driver) Remove(req *volume.RemoveRequest) error {
	ctx := context.Background()
	state, err := d.lookup(ctx, req.Name)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, mount := range state.mounts {
		if mount.active {
			return fmt.Errorf("volume %s is mounted on this node", req.Name)
		}
	}
	deleteRemote := d.remote.Delete
	if state.record.Options.PerReplica() {
		deleteRemote = d.remote.DeleteFamily
	}
	if err := deleteRemote(ctx, req.Name); err != nil && !errors.Is(err, objectstore.ErrNotFound) {
		return err
	}
	for name := range state.mounts {
		d.cleanupLocal(name)
	}
	d.cleanupLocal(req.Name)
	if err := os.Remove(d.registryPath(req.Name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	d.mu.Lock()
	delete(d.volumes, req.Name)
	d.mu.Unlock()
	return nil
}

func (d *Driver) Path(req *volume.PathRequest) (*volume.PathResponse, error) {
	state, err := d.lookup(context.Background(), req.Name)
	if err != nil {
		return nil, err
	}
	return &volume.PathResponse{Mountpoint: d.describe(state).Mountpoint}, nil
}

func replicaName(base, mountID string) string {
	sum := sha256.Sum256([]byte(mountID))
	return base + ".r-" + hex.EncodeToString(sum[:6])
}

func (d *Driver) mountStateFor(state *volumeState, mountID string) *mountState {
	name := state.record.Name
	if state.record.Options.PerReplica() {
		name = replicaName(name, mountID)
	}
	mount, ok := state.mounts[name]
	if !ok {
		mount = &mountState{name: name, mountIDs: map[string]struct{}{}}
		state.mounts[name] = mount
	}
	return mount
}

func (d *Driver) Mount(req *volume.MountRequest) (*volume.MountResponse, error) {
	ctx := context.Background()
	state, err := d.lookup(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	mount := d.mountStateFor(state, req.ID)
	if mount.fenced != nil {
		return nil, fmt.Errorf("volume %s was fenced on this node (%v); unmount all users before remounting", req.Name, mount.fenced)
	}
	if mount.active {
		mount.mountIDs[req.ID] = struct{}{}
		return &volume.MountResponse{Mountpoint: d.mountpoint(mount.name)}, nil
	}
	if err := d.mountFresh(ctx, state, mount); err != nil {
		return nil, err
	}
	mount.mountIDs[req.ID] = struct{}{}
	metrics.MountedVolumes.Inc()
	return &volume.MountResponse{Mountpoint: d.mountpoint(mount.name)}, nil
}

func (d *Driver) mountFresh(ctx context.Context, state *volumeState, mount *mountState) error {
	if state.record.Options.ReadOnly() {
		return d.mountReadOnly(ctx, mount)
	}
	opts := state.record.Options
	lease, _, err := d.remote.Acquire(ctx, mount.name, d.cfg.NodeID, replica.CreateOptions{Size: opts.Size, Filesystem: opts.Filesystem})
	if err != nil {
		metrics.MountFailures.WithLabelValues("lease").Inc()
		return err
	}
	setupCtx, cancelSetup := context.WithCancel(ctx)
	defer cancelSetup()
	beatCtx, stopBeat := context.WithCancel(context.Background())
	go lease.Heartbeat(beatCtx, func(cause error) {
		cancelSetup()
		go d.fence(state, mount, cause)
	})
	releaseOnFailure := func(cause error) error {
		stopBeat()
		d.cleanupLocal(mount.name)
		if err := lease.Release(context.WithoutCancel(ctx)); err != nil {
			d.log.Error("release lease after failed mount", "volume", mount.name, "err", err)
		}
		return cause
	}
	d.cleanupLocal(mount.name)
	imagePath := d.imagePath(mount.name)
	remoteState := lease.State()
	initializing := remoteState.Generation == 0
	started := time.Now()
	var lazyImage *replica.LazyImage
	if initializing {
		file, err := os.OpenFile(imagePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
		if err != nil {
			return releaseOnFailure(err)
		}
		if err := file.Truncate(remoteState.Size); err != nil {
			file.Close()
			return releaseOnFailure(err)
		}
		file.Close()
		if err := d.backend.Format(setupCtx, imagePath, remoteState.Filesystem); err != nil {
			return releaseOnFailure(err)
		}
	} else {
		lazyImage, err = lease.PrepareLazyRestore(setupCtx, imagePath)
		if err != nil {
			metrics.MountFailures.WithLabelValues("restore").Inc()
			return releaseOnFailure(err)
		}
	}
	metrics.RestoreDuration.Observe(time.Since(started).Seconds())

	device, err := replica.OpenDevice(imagePath, remoteState.Size, d.cfg.MaxDirty)
	if err != nil {
		if lazyImage != nil {
			_ = lazyImage.Close()
		}
		return releaseOnFailure(err)
	}
	device.SetLazyImage(lazyImage)
	device.SetBackpressureHandler(func() {
		metrics.BackpressureEvents.WithLabelValues(mount.name).Inc()
	})
	if initializing {
		if err := device.MarkAllocated(); err != nil {
			device.Close()
			return releaseOnFailure(err)
		}
	}
	unmounter, err := d.backend.Mount(setupCtx, device, d.mountpoint(mount.name), backend.MountOptions{Filesystem: remoteState.Filesystem})
	if err != nil {
		device.Close()
		return releaseOnFailure(err)
	}
	mount.active, mount.lease, mount.device, mount.unmounter, mount.stopBeat = true, lease, device, unmounter, stopBeat
	cleanupMounted := func(cause error) error {
		_ = unmounter.Abandon()
		_ = device.Close()
		mount.active, mount.lease, mount.device, mount.unmounter, mount.syncer, mount.stopBeat = false, nil, nil, nil, nil, nil
		return releaseOnFailure(cause)
	}
	if opts.Seed != "" && initializing {
		if d.cfg.Seeder == nil {
			return cleanupMounted(errors.New("seeding is not configured"))
		}
		if _, err := d.cfg.Seeder.Apply(ctx, d.mountpoint(mount.name), opts.Seed); err != nil {
			return cleanupMounted(fmt.Errorf("seed %s: %w", mount.name, err))
		}
	}
	if initializing {
		generation := device.Seal()
		segments, err := replica.EncodeSegments(generation, replica.DefaultSegmentBlocks)
		if err != nil {
			return cleanupMounted(err)
		}
		if err := lease.Publish(setupCtx, segments...); err != nil {
			return cleanupMounted(err)
		}
		device.Release(generation)
	}
	interval := opts.SyncInterval
	if interval == 0 {
		interval = d.cfg.SyncInterval
	}
	syncer := replica.StartSyncer(device, lease, interval, d.cfg.CheckpointInterval, d.log.With("volume", mount.name), func(cause error) {
		go d.fence(state, mount, cause)
	})
	mount.syncer = syncer
	dirtyThreshold := min(d.cfg.MaxDirty/4, int64(32<<20))
	device.SetDirtyHandler(dirtyThreshold, syncer.Notify)
	device.SetBackpressureHandler(func() {
		metrics.BackpressureEvents.WithLabelValues(mount.name).Inc()
		syncer.Notify()
	})
	device.SetDirtyObserver(func(bytes int64) {
		metrics.UnpublishedBytes.WithLabelValues(mount.name).Set(float64(bytes))
	})
	if opts.RemoteDurability() {
		device.SetRemoteFlushHandler(syncer.EnqueueGeneration)
	} else {
		device.SetAsyncFlush()
	}
	metrics.LeaseHeld.WithLabelValues(mount.name).Set(1)
	d.log.Info("volume mounted", "volume", mount.name, "restore_took", time.Since(started))
	return nil
}

func (d *Driver) mountReadOnly(ctx context.Context, mount *mountState) error {
	state, _, err := d.remote.Inspect(ctx, mount.name)
	if err != nil {
		return err
	}
	if state.Generation == 0 {
		return fmt.Errorf("volume %s has not been initialized", mount.name)
	}
	d.cleanupLocal(mount.name)
	started := time.Now()
	if err := d.remote.RestoreState(ctx, state, d.imagePath(mount.name)); err != nil {
		return err
	}
	device, err := replica.OpenDevice(d.imagePath(mount.name), state.Size, d.cfg.MaxDirty)
	if err != nil {
		return err
	}
	unmounter, err := d.backend.Mount(ctx, device, d.mountpoint(mount.name), backend.MountOptions{ReadOnly: true, Filesystem: state.Filesystem})
	if err != nil {
		device.Close()
		return err
	}
	mount.active, mount.device, mount.unmounter = true, device, unmounter
	metrics.RestoreDuration.Observe(time.Since(started).Seconds())
	return nil
}

func (d *Driver) fence(state *volumeState, mount *mountState, cause error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !mount.active {
		return
	}
	d.log.Error("fencing volume", "volume", mount.name, "cause", cause)
	metrics.LeaseFenced.WithLabelValues(mount.name).Inc()
	metrics.LeaseHeld.WithLabelValues(mount.name).Set(0)
	if mount.syncer != nil {
		mount.syncer.Abandon()
	}
	if mount.stopBeat != nil {
		mount.stopBeat()
	}
	if mount.unmounter != nil {
		_ = mount.unmounter.Abandon()
	}
	if mount.device != nil {
		_ = mount.device.Close()
	}
	d.cleanupLocal(mount.name)
	mount.active, mount.lease, mount.device, mount.unmounter, mount.syncer, mount.stopBeat = false, nil, nil, nil, nil, nil
	mount.fenced = cause
	metrics.MountedVolumes.Dec()
}

func (d *Driver) Unmount(req *volume.UnmountRequest) error {
	ctx := context.Background()
	state, err := d.lookup(ctx, req.Name)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	mount := d.mountStateFor(state, req.ID)
	delete(mount.mountIDs, req.ID)
	if mount.fenced != nil {
		if len(mount.mountIDs) == 0 {
			mount.fenced = nil
		}
		return nil
	}
	if !mount.active || len(mount.mountIDs) > 0 {
		return nil
	}
	return d.unmountLast(ctx, mount)
}

func (d *Driver) unmountLast(ctx context.Context, mount *mountState) error {
	if err := mount.unmounter.Unmount(ctx); err != nil {
		return err
	}
	if mount.syncer != nil {
		started := time.Now()
		if err := mount.syncer.SyncCheckpoint(ctx); err != nil {
			return d.abandonUnmounted(mount, fmt.Errorf("final sync %s: %w", mount.name, err))
		}
		mount.stopBeat()
		if err := mount.syncer.Stop(ctx); err != nil {
			return d.abandonUnmounted(mount, fmt.Errorf("release %s: %w", mount.name, err))
		}
		metrics.LeaseHeld.WithLabelValues(mount.name).Set(0)
		metrics.SyncDuration.Observe(time.Since(started).Seconds())
	}
	if err := mount.device.Close(); err != nil {
		return err
	}
	metrics.MountedVolumes.Dec()
	metrics.UnpublishedBytes.DeleteLabelValues(mount.name)
	d.cleanupLocal(mount.name)
	mount.active, mount.lease, mount.device, mount.unmounter, mount.syncer, mount.stopBeat = false, nil, nil, nil, nil, nil
	return nil
}

func (d *Driver) abandonUnmounted(mount *mountState, cause error) error {
	d.log.Error("final publication failed after unmount; discarding the local image and releasing the lease", "volume", mount.name, "err", cause)
	metrics.MountFailures.WithLabelValues("final_sync").Inc()
	if mount.syncer != nil {
		mount.syncer.Abandon()
	}
	if mount.stopBeat != nil {
		mount.stopBeat()
	}
	if mount.lease != nil {
		if err := mount.lease.Release(context.Background()); err != nil {
			d.log.Error("release lease after failed final sync", "volume", mount.name, "err", err)
		}
	}
	metrics.LeaseHeld.WithLabelValues(mount.name).Set(0)
	if mount.device != nil {
		_ = mount.device.Close()
	}
	metrics.MountedVolumes.Dec()
	d.cleanupLocal(mount.name)
	mount.active, mount.lease, mount.device, mount.unmounter, mount.syncer, mount.stopBeat = false, nil, nil, nil, nil, nil
	return cause
}

func (d *Driver) cleanupLocal(name string) {
	_ = syscall.Unmount(d.mountpoint(name), syscall.MNT_DETACH)
	_ = os.Remove(d.imagePath(name))
	_ = os.RemoveAll(d.mountpoint(name))
}

func (d *Driver) Capabilities() *volume.CapabilitiesResponse {
	return &volume.CapabilitiesResponse{Capabilities: volume.Capability{Scope: "global"}}
}

func (d *Driver) Shutdown(ctx context.Context) error {
	d.mu.Lock()
	states := make([]*volumeState, 0, len(d.volumes))
	for _, state := range d.volumes {
		states = append(states, state)
	}
	d.mu.Unlock()
	var errs []error
	for _, state := range states {
		state.mu.Lock()
		for _, mount := range state.mounts {
			if mount.active {
				if err := d.unmountLast(ctx, mount); err != nil {
					errs = append(errs, err)
				}
			}
		}
		state.mu.Unlock()
	}
	return errors.Join(errs...)
}
