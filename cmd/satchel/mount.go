package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/docker/go-plugins-helpers/volume"
	"github.com/spf13/cobra"
)

func newMountCommand() *cobra.Command {
	opts := pluginOptions{}
	cmd := &cobra.Command{
		Use:   "mount <volume>",
		Short: "Mount a volume locally (no Docker) until interrupted",
		Long:  "Acquires the lease, restores from the bucket, mounts the volume under <state-dir>/mounts/<volume> with live replication, and unmounts cleanly on SIGINT/SIGTERM.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMount(cmd.Context(), opts, args[0])
		},
	}
	bindPluginFlags(cmd, &opts)
	return cmd
}

func runMount(ctx context.Context, opts pluginOptions, name string) error {
	driver, logger, err := buildDriver(ctx, opts)
	if err != nil {
		return err
	}
	if err := driver.Create(&volume.CreateRequest{Name: name}); err != nil {
		return err
	}
	resp, err := driver.Mount(&volume.MountRequest{Name: name, ID: "satchel-mount"})
	if err != nil {
		return err
	}
	fmt.Println(resp.Mountpoint)
	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()
	logger.Info("unmounting")
	return driver.Unmount(&volume.UnmountRequest{Name: name, ID: "satchel-mount"})
}
