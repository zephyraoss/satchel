package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docker/go-plugins-helpers/volume"
	"github.com/spf13/cobra"

	"github.com/zephyraoss/satchel/internal/backend/block"
	"github.com/zephyraoss/satchel/internal/metrics"
	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/plugin"
	"github.com/zephyraoss/satchel/internal/replica"
	"github.com/zephyraoss/satchel/internal/seed"
)

type pluginOptions struct {
	nodeID             string
	stateDir           string
	socketName         string
	metricsAddr        string
	leaseTTL           time.Duration
	syncInterval       time.Duration
	checkpointInterval uint64
	historyRetention   time.Duration
	gcGrace            time.Duration
	s3                 objectstore.S3Config
	s3Head             string
	logLevel           string
	dirtyLimit         string
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
	f.DurationVar(&opts.leaseTTL, "lease-ttl", envDurationOr("SATCHEL_LEASE_TTL", 30*time.Second), "lease TTL; heartbeat runs every ttl/3")
	f.DurationVar(&opts.syncInterval, "sync-interval", envDurationOr("SATCHEL_SYNC_INTERVAL", 5*time.Second), "interval between S3 block generations")
	f.Uint64Var(&opts.checkpointInterval, "checkpoint-interval", envUint64Or("SATCHEL_CHECKPOINT_INTERVAL", 128), "compact the restore chain after this many generations")
	f.DurationVar(&opts.historyRetention, "history-retention", envDurationOr("SATCHEL_HISTORY_RETENTION", 7*24*time.Hour), "retain point-in-time generations for at least this long")
	f.DurationVar(&opts.gcGrace, "gc-grace", envDurationOr("SATCHEL_GC_GRACE", 24*time.Hour), "wait this long before deleting unreachable objects")
	bindS3Flags(cmd, &opts.s3, &opts.s3Head)
	f.StringVar(&opts.logLevel, "log-level", envOr("SATCHEL_LOG_LEVEL", "info"), "debug|info|warn|error")
	f.StringVar(&opts.dirtyLimit, "dirty-limit", envOr("SATCHEL_DIRTY_LIMIT", "256MiB"), "pause writes when unpublished block generations exceed this size")
}

func bindS3Flags(cmd *cobra.Command, opts *objectstore.S3Config, head *string) {
	f := cmd.Flags()
	f.StringVar(head, "s3-head", envOr("SATCHEL_S3_HEAD", string(replica.ConditionalHead)), "volume head protocol: conditional (If-Match, AWS/MinIO/R2) or append (listing-based, Garage)")
	f.StringVar(&opts.Endpoint, "s3-endpoint", envOr("SATCHEL_S3_ENDPOINT", ""), "S3 endpoint URL (empty for AWS)")
	f.StringVar(&opts.Region, "s3-region", envOr("SATCHEL_S3_REGION", "us-east-1"), "S3 region")
	f.StringVar(&opts.Bucket, "s3-bucket", envOr("SATCHEL_S3_BUCKET", ""), "S3 bucket holding volumes and leases")
	f.StringVar(&opts.AccessKeyID, "s3-access-key", envOr("SATCHEL_S3_ACCESS_KEY", os.Getenv("AWS_ACCESS_KEY_ID")), "S3 access key")
	f.StringVar(&opts.SecretKey, "s3-secret-key", envOr("SATCHEL_S3_SECRET_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY")), "S3 secret key")
	f.BoolVar(&opts.ForcePathStyle, "s3-path-style", envOr("SATCHEL_S3_PATH_STYLE", "true") == "true", "use path-style S3 URLs (MinIO, SeaweedFS)")
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

func envUint64Or(key string, fallback uint64) uint64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
			return parsed
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

	headMode, err := replica.ParseHeadMode(opts.s3Head)
	if err != nil {
		return nil, nil, fmt.Errorf("--s3-head: %w", err)
	}
	store := objectstore.NewS3(opts.s3)
	remote := &replica.Remote{
		Store: store, Head: headMode, TTL: opts.leaseTTL,
		Observe: func(stage string, duration time.Duration) {
			metrics.ReplicationStageDuration.WithLabelValues(stage).Observe(duration.Seconds())
		},
		ObserveGeneration: func(inputBytes, storedBytes int64, segments int) {
			metrics.ReplicationGenerations.Inc()
			metrics.ReplicationInputBytes.Add(float64(inputBytes))
			metrics.ReplicationStoredBytes.Add(float64(storedBytes))
			metrics.ReplicationSegments.Add(float64(segments))
		},
	}
	probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := remote.VerifyBackend(probeCtx); err != nil {
		return nil, nil, fmt.Errorf("S3 backend %s is unusable with the %s head: %w", opts.s3.Endpoint, headMode, err)
	}
	logger.Info("S3 backend verified", "endpoint", opts.s3.Endpoint, "bucket", opts.s3.Bucket, "head", headMode)

	dirtyLimit, err := parseBytes(opts.dirtyLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("--dirty-limit: %w", err)
	}
	driver, err := plugin.New(
		plugin.Config{
			NodeID: opts.nodeID, StateDir: opts.stateDir, MaxDirty: dirtyLimit,
			SyncInterval: opts.syncInterval, CheckpointInterval: opts.checkpointInterval,
			HistoryRetention: opts.historyRetention, GCGrace: opts.gcGrace,
			Logger: logger, Seeder: &seed.Seeder{Fetch: store.Fetch},
		},
		remote,
		block.New(block.Options{Logger: logger}),
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

	startMetricsServer(opts.metricsAddr, logger)

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

func startMetricsServer(addr string, logger *slog.Logger) {
	if addr == "" {
		return
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		logger.Info("metrics listening", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Error("metrics server stopped", "err", err)
		}
	}()
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
