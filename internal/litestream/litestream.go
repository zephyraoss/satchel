package litestream

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/zephyraoss/satchel/internal/objectstore"
)

type RestoreOptions struct {
	Timestamp time.Time
	TXID      string
}

type ReplicaOptions struct {
	SyncInterval time.Duration
}

type Runner interface {
	Restore(ctx context.Context, volume, dbPath string, opts RestoreOptions) error
	SyncOnce(ctx context.Context, volume, dbPath string) error
	Start(ctx context.Context, volume, dbPath string, opts ReplicaOptions) (Process, error)
}

type Process interface {
	Stop(ctx context.Context) error
	Kill()
	Done() <-chan struct{}
}

type Config struct {
	Binary       string
	ConfigDir    string
	S3           objectstore.S3Config
	SyncInterval time.Duration
	Logger       *slog.Logger
}

type Supervisor struct {
	cfg Config
}

func New(cfg Config) *Supervisor {
	if cfg.Binary == "" {
		cfg.Binary = "litestream"
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Supervisor{cfg: cfg}
}

func ReplicaPath(volume string) string {
	return "vols/" + volume
}

var configTemplate = template.Must(template.New("litestream").Parse(`dbs:
  - path: {{.DBPath}}
    replicas:
      - type: s3
        bucket: {{.S3.Bucket}}
        path: {{.ReplicaPath}}
{{- if .S3.Endpoint}}
        endpoint: {{.S3.Endpoint}}
{{- end}}
{{- if .S3.Region}}
        region: {{.S3.Region}}
{{- end}}
        access-key-id: {{.S3.AccessKeyID}}
        secret-access-key: {{.S3.SecretKey}}
        force-path-style: {{.S3.ForcePathStyle}}
        sync-interval: {{.SyncInterval}}
`))

func (s *Supervisor) RenderConfig(volume, dbPath string, opts ReplicaOptions) ([]byte, error) {
	interval := opts.SyncInterval
	if interval == 0 {
		interval = s.cfg.SyncInterval
	}
	var buf bytes.Buffer
	err := configTemplate.Execute(&buf, struct {
		DBPath       string
		ReplicaPath  string
		S3           objectstore.S3Config
		SyncInterval time.Duration
	}{dbPath, ReplicaPath(volume), s.cfg.S3, interval})
	return buf.Bytes(), err
}

func (s *Supervisor) writeConfig(volume, dbPath string, opts ReplicaOptions) (string, error) {
	rendered, err := s.RenderConfig(volume, dbPath, opts)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(s.cfg.ConfigDir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(s.cfg.ConfigDir, volume+".yml")
	return path, os.WriteFile(path, rendered, 0o600)
}

func (s *Supervisor) Restore(ctx context.Context, volume, dbPath string, opts RestoreOptions) error {
	cfgPath, err := s.writeConfig(volume, dbPath, ReplicaOptions{})
	if err != nil {
		return err
	}
	args := []string{"restore", "-config", cfgPath, "-if-replica-exists", "-o", dbPath}
	if !opts.Timestamp.IsZero() {
		args = append(args, "-timestamp", opts.Timestamp.UTC().Format(time.RFC3339))
	}
	if opts.TXID != "" {
		args = append(args, "-txid", opts.TXID)
	}
	return s.run(ctx, append(args, dbPath)...)
}

func (s *Supervisor) SyncOnce(ctx context.Context, volume, dbPath string) error {
	cfgPath, err := s.writeConfig(volume, dbPath, ReplicaOptions{})
	if err != nil {
		return err
	}
	return s.run(ctx, "replicate", "-config", cfgPath, "-once")
}

func (s *Supervisor) Start(ctx context.Context, volume, dbPath string, opts ReplicaOptions) (Process, error) {
	cfgPath, err := s.writeConfig(volume, dbPath, opts)
	if err != nil {
		return nil, err
	}
	p := &process{
		supervisor: s,
		volume:     volume,
		dbPath:     dbPath,
		cfgPath:    cfgPath,
		log:        s.cfg.Logger.With("volume", volume),
		done:       make(chan struct{}),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	go p.supervise()
	return p, nil
}

type process struct {
	supervisor *Supervisor
	volume     string
	dbPath     string
	cfgPath    string
	log        *slog.Logger
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}

	mu  sync.Mutex
	cmd *exec.Cmd
}

func (p *process) Done() <-chan struct{} { return p.done }

func (p *process) supervise() {
	defer close(p.done)
	backoff := time.Second
	for {
		cmd := exec.Command(p.supervisor.cfg.Binary, "replicate", "-config", p.cfgPath)
		cmd.Stdout = logWriter{p.log}
		cmd.Stderr = logWriter{p.log}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		p.mu.Lock()
		if p.ctx.Err() != nil {
			p.mu.Unlock()
			return
		}
		err := cmd.Start()
		p.cmd = cmd
		p.mu.Unlock()
		if err != nil {
			p.log.Error("start litestream replicate", "err", err)
		} else {
			started := time.Now()
			err = cmd.Wait()
			if p.ctx.Err() != nil {
				return
			}
			p.log.Error("litestream replicate exited unexpectedly; restarting", "err", err, "uptime", time.Since(started))
			if time.Since(started) > time.Minute {
				backoff = time.Second
			}
		}
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (p *process) Stop(ctx context.Context) error {
	p.cancel()
	p.mu.Lock()
	cmd := p.cmd
	p.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-ctx.Done():
			cmd.Process.Kill()
			<-p.done
			return fmt.Errorf("litestream did not stop in time; killed: %w", ctx.Err())
		}
	} else {
		<-p.done
	}
	return p.supervisor.SyncOnce(ctx, p.volume, p.dbPath)
}

func (p *process) Kill() {
	p.cancel()
	p.mu.Lock()
	cmd := p.cmd
	p.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		cmd.Process.Kill()
	}
	<-p.done
}

type logWriter struct{ log *slog.Logger }

func (w logWriter) Write(b []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		if line != "" {
			w.log.Debug("litestream", "line", line)
		}
	}
	return len(b), nil
}

func (s *Supervisor) run(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, s.cfg.Binary, args...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	s.cfg.Logger.Debug("running litestream", "args", args)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("litestream %s: %w\n%s", args[0], err, strings.TrimSpace(output.String()))
	}
	return nil
}

func CheckVersion(ctx context.Context, binary string) (string, error) {
	if binary == "" {
		binary = "litestream"
	}
	out, err := exec.CommandContext(ctx, binary, "version").Output()
	if err != nil {
		return "", fmt.Errorf("litestream binary %q not runnable: %w", binary, err)
	}
	version := strings.TrimSpace(strings.TrimPrefix(string(out), "v"))
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return version, fmt.Errorf("unparseable litestream version %q", version)
	}
	major, errMajor := strconv.Atoi(parts[0])
	minor, errMinor := strconv.Atoi(parts[1])
	if errMajor != nil || errMinor != nil {
		return version, fmt.Errorf("unparseable litestream version %q", version)
	}
	if major == 0 && minor < 5 {
		return version, fmt.Errorf("litestream %s is too old; need >= 0.5", version)
	}
	return version, nil
}
