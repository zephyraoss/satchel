package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/docker/go-plugins-helpers/volume"
	"github.com/spf13/cobra"

	"github.com/zephyraoss/satchel/internal/plugin"
)

func newMountCommand() *cobra.Command {
	opts := pluginOptions{}
	volumeSize := "10GiB"
	durability := "remote"
	seedSource := ""
	cmd := &cobra.Command{
		Use:   "mount <volume>",
		Short: "Mount a volume locally (no Docker) until interrupted",
		Long:  "Acquires the lease, restores from the bucket, mounts the volume under <state-dir>/mounts/<volume> with live replication, and unmounts cleanly on SIGINT/SIGTERM.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			volumeOpts := map[string]string{}
			if cmd.Flags().Changed("size") {
				volumeOpts["size"] = volumeSize
			}
			if cmd.Flags().Changed("durability") {
				volumeOpts["durability"] = durability
			}
			if cmd.Flags().Changed("seed") {
				volumeOpts["seed"] = seedSource
			}
			return runMount(cmd.Context(), opts, args[0], volumeOpts)
		},
	}
	bindPluginFlags(cmd, &opts)
	cmd.Flags().StringVar(&volumeSize, "size", "10GiB", "size to use when creating a new volume")
	cmd.Flags().StringVar(&durability, "durability", "remote", "remote waits for S3 on fsync; async publishes on the sync interval")
	cmd.Flags().StringVar(&seedSource, "seed", "", "directory, tar archive, or s3:// URL used to initialize a new volume")
	return cmd
}

func runMount(ctx context.Context, opts pluginOptions, name string, volumeOpts map[string]string) error {
	driver, logger, err := buildDriver(ctx, opts)
	if err != nil {
		return err
	}
	if len(volumeOpts) == 0 {
		if resp, err := driver.Mount(&volume.MountRequest{Name: name, ID: "satchel-mount"}); err == nil {
			return waitForUnmount(ctx, driver, logger, name, resp.Mountpoint)
		} else if !errors.Is(err, plugin.ErrVolumeNotFound) {
			return err
		}
	}
	if err := driver.Create(&volume.CreateRequest{Name: name, Options: volumeOpts}); err != nil {
		return err
	}
	resp, err := driver.Mount(&volume.MountRequest{Name: name, ID: "satchel-mount"})
	if err != nil {
		return err
	}
	return waitForUnmount(ctx, driver, logger, name, resp.Mountpoint)
}

func waitForUnmount(ctx context.Context, driver *plugin.Driver, logger *slog.Logger, name, mountpoint string) error {
	fmt.Println(mountpoint)
	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()
	logger.Info("unmounting")
	return driver.Unmount(&volume.UnmountRequest{Name: name, ID: "satchel-mount"})
}
