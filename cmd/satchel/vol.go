package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/zephyraoss/satchel/internal/cli"
	"github.com/zephyraoss/satchel/internal/lease"
	"github.com/zephyraoss/satchel/internal/litestream"
	"github.com/zephyraoss/satchel/internal/objectstore"
)

func newVolCommand() *cobra.Command {
	opts := pluginOptions{}
	cmd := cli.NewVolCommand(func(ctx context.Context) (*cli.Env, error) {
		return buildCLIEnv(ctx, opts)
	})
	bindPluginFlags(cmd, &opts)
	cmd.PersistentFlags().AddFlagSet(cmd.Flags())
	return cmd
}

func buildCLIEnv(ctx context.Context, opts pluginOptions) (*cli.Env, error) {
	logger, err := newLogger(opts.logLevel)
	if err != nil {
		return nil, err
	}
	if opts.logLevel == "info" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	slog.SetDefault(logger)
	if opts.s3.Bucket == "" {
		return nil, errors.New("--s3-bucket (SATCHEL_S3_BUCKET) is required")
	}
	if _, err := litestream.CheckVersion(ctx, opts.litestream); err != nil {
		return nil, err
	}
	store := objectstore.NewS3(opts.s3)
	hostname, _ := os.Hostname()
	holder := fmt.Sprintf("cli@%s", hostname)
	if opts.nodeID != "" && opts.nodeID != hostname {
		holder = opts.nodeID
	}
	workDir := filepath.Join(os.TempDir(), "satchel-cli")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		return nil, err
	}
	return &cli.Env{
		Store:  store,
		Leases: &lease.Manager{Store: store, NodeID: holder, TTL: opts.leaseTTL, Logger: logger},
		Litestream: litestream.New(litestream.Config{
			Binary:       opts.litestream,
			ConfigDir:    filepath.Join(workDir, "litestream"),
			S3:           opts.s3,
			SyncInterval: time.Second,
			Logger:       logger,
		}),
		WorkDir: workDir,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	}, nil
}
