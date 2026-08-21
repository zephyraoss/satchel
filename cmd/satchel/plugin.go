package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docker/go-plugins-helpers/volume"
	"github.com/spf13/cobra"

	"github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/backend/fuse"
	"github.com/zephyraoss/satchel/internal/backend/pack"
	"github.com/zephyraoss/satchel/internal/lease"
	"github.com/zephyraoss/satchel/internal/litestream"
	"github.com/zephyraoss/satchel/internal/metrics"
	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/plugin"
	"github.com/zephyraoss/satchel/internal/seed"
	"github.com/zephyraoss/satchel/internal/store"
)

type pluginOptions struct {
	nodeID       string
	stateDir     string
	socketName   string
	metricsAddr  string
	litestream   string
	leaseTTL     time.Duration
	syncInterval time.Duration
	s3           objectstore.S3Config
	logLevel     string
	backend      string
	walLimit     string
	fuseDebug    bool
}

func newPluginCommand() *cobra.Command {
	opts := pluginOptions{}
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Run the Docker volume plugin",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPlugin(cmd.Context(), opts)
		},
	}
	bindPluginFlags(cmd, &opts)
	return cmd
}

func bindPluginFlags(cmd *cobra.Command, opts *pluginOptions) {
	hostname, _ := os.Hostname()
	f := cmd.Flags()
	f.StringVar(&opts.nodeID, "node-id", envOr("SATCHEL_NODE_ID", hostname), "identifier recorded in leases held by this node")
	f.StringVar(&opts.stateDir, "state-dir", envOr("SATCHEL_STATE_DIR", "/var/lib/satchel"), "local state directory")
	f.StringVar(&opts.socketName, "socket", envOr("SATCHEL_SOCKET", "satchel"), "plugin socket name under /run/docker/plugins")
	f.StringVar(&opts.metricsAddr, "metrics-addr", envOr("SATCHEL_METRICS_ADDR", ""), "address for the Prometheus /metrics endpoint (empty disables)")
	f.StringVar(&opts.litestream, "litestream", envOr("SATCHEL_LITESTREAM_BIN", "litestream"), "path to the litestream binary")
	f.DurationVar(&opts.leaseTTL, "lease-ttl", envDurationOr("SATCHEL_LEASE_TTL", lease.DefaultTTL), "lease TTL; heartbeat runs every ttl/3")
	f.DurationVar(&opts.syncInterval, "sync-interval", envDurationOr("SATCHEL_SYNC_INTERVAL", 5*time.Second), "litestream sync interval")
	f.StringVar(&opts.s3.Endpoint, "s3-endpoint", envOr("SATCHEL_S3_ENDPOINT", ""), "S3 endpoint URL (empty for AWS)")
	f.StringVar(&opts.s3.Region, "s3-region", envOr("SATCHEL_S3_REGION", "us-east-1"), "S3 region")
	f.StringVar(&opts.s3.Bucket, "s3-bucket", envOr("SATCHEL_S3_BUCKET", ""), "S3 bucket holding volumes and leases")
	f.StringVar(&opts.s3.AccessKeyID, "s3-access-key", envOr("SATCHEL_S3_ACCESS_KEY", os.Getenv("AWS_ACCESS_KEY_ID")), "S3 access key")
	f.StringVar(&opts.s3.SecretKey, "s3-secret-key", envOr("SATCHEL_S3_SECRET_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY")), "S3 secret key")
	f.BoolVar(&opts.s3.ForcePathStyle, "s3-path-style", envOr("SATCHEL_S3_PATH_STYLE", "true") == "true", "use path-style S3 URLs (MinIO, SeaweedFS)")
	f.StringVar(&opts.logLevel, "log-level", envOr("SATCHEL_LOG_LEVEL", "info"), "debug|info|warn|error")
	f.StringVar(&opts.backend, "backend", envOr("SATCHEL_BACKEND", "fuse"), "mount backend: fuse (live replication) or pack (replicate at unmount only)")
	f.StringVar(&opts.walLimit, "wal-limit", envOr("SATCHEL_WAL_LIMIT", "256MiB"), "delay writes while the SQLite WAL exceeds this size (fuse backend)")
	f.BoolVar(&opts.fuseDebug, "fuse-debug", envOr("SATCHEL_FUSE_DEBUG", "") == "true", "log every FUSE request")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func newLogger(level string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q", level)
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})), nil
}

func buildDriver(ctx context.Context, opts pluginOptions) (*plugin.Driver, *slog.Logger, error) {
	logger, err := newLogger(opts.logLevel)
	if err != nil {
		return nil, nil, err
	}
	slog.SetDefault(logger)
	if opts.s3.Bucket == "" {
		return nil, nil, errors.New("--s3-bucket (SATCHEL_S3_BUCKET) is required")
	}

	version, err := litestream.CheckVersion(ctx, opts.litestream)
	if err != nil {
		return nil, nil, err
	}
	logger.Info("litestream detected", "version", version, "sqlite_driver", store.DriverDescription)

	store := objectstore.NewS3(opts.s3)
	probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := objectstore.VerifyConditionalWrites(probeCtx, store); err != nil {
		return nil, nil, fmt.Errorf("S3 backend %s is unusable: %w", opts.s3.Endpoint, err)
	}
	logger.Info("conditional writes verified", "endpoint", opts.s3.Endpoint, "bucket", opts.s3.Bucket)

	be, err := newBackend(opts, logger)
	if err != nil {
		return nil, nil, err
	}
	driver, err := plugin.New(
		plugin.Config{NodeID: opts.nodeID, StateDir: opts.stateDir, Logger: logger, Seeder: &seed.Seeder{Fetch: store.Fetch}},
		store,
		&lease.Manager{Store: store, NodeID: opts.nodeID, TTL: opts.leaseTTL, Logger: logger},
		litestream.New(litestream.Config{
			Binary:       opts.litestream,
			ConfigDir:    filepath.Join(opts.stateDir, "litestream"),
			S3:           opts.s3,
			SyncInterval: opts.syncInterval,
			Logger:       logger,
		}),
		be,
	)
	if err != nil {
		return nil, nil, err
	}
	return driver, logger, nil
}

func runPlugin(ctx context.Context, opts pluginOptions) error {
	driver, logger, err := buildDriver(ctx, opts)
	if err != nil {
		return err
	}

	if opts.metricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", metrics.Handler())
			logger.Info("metrics listening", "addr", opts.metricsAddr)
			if err := http.ListenAndServe(opts.metricsAddr, mux); err != nil {
				logger.Error("metrics server stopped", "err", err)
			}
		}()
	}

	handler := volume.NewHandler(driver)
	errCh := make(chan error, 1)
	go func() {
		logger.Info("plugin listening", "socket", opts.socketName, "node", opts.nodeID)
		errCh <- handler.ServeUnix(opts.socketName, 0)
	}()

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-errCh:
		return err
	case <-sigCtx.Done():
	}
	logger.Info("shutting down: unmounting volumes and releasing leases")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelShutdown()
	if err := driver.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown incomplete", "err", err)
		return err
	}
	return nil
}

func newBackend(opts pluginOptions, logger *slog.Logger) (backend.Backend, error) {
	switch opts.backend {
	case "fuse":
		limit, err := parseBytes(opts.walLimit)
		if err != nil {
			return nil, fmt.Errorf("--wal-limit: %w", err)
		}
		return fuse.New(fuse.Options{WALLimit: limit, Logger: logger, Debug: opts.fuseDebug, AllowOther: true}), nil
	case "pack":
		return pack.New(), nil
	default:
		return nil, fmt.Errorf("unknown backend %q", opts.backend)
	}
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10}, {"B", 1}}
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			n, err := strconv.ParseInt(strings.TrimSuffix(s, u.suffix), 10, 64)
			if err != nil {
				return 0, err
			}
			return n * u.mult, nil
		}
	}
	return strconv.ParseInt(s, 10, 64)
}
