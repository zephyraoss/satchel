package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/zephyraoss/satchel/internal/objectstore"
	"github.com/zephyraoss/satchel/internal/replica"
)

type volOptions struct {
	s3   objectstore.S3Config
	head string
}

func newVolCommand() *cobra.Command {
	opts := volOptions{}
	cmd := &cobra.Command{Use: "vol", Short: "Inspect and manage remote volumes"}
	bindS3Flags(cmd, &opts.s3, &opts.head)
	cmd.PersistentFlags().AddFlagSet(cmd.Flags())
	cmd.AddCommand(
		newVolListCommand(&opts),
		newVolInspectCommand(&opts),
		newVolVerifyCommand(&opts),
		newVolRestoreCommand(&opts),
		newVolRemoveCommand(&opts),
		newVolGCCommand(&opts),
		newVolLeaseCommand(&opts),
	)
	return cmd
}

func newVolGCCommand(opts *volOptions) *cobra.Command {
	var historyRetention time.Duration
	var gracePeriod time.Duration
	cmd := &cobra.Command{
		Use:   "gc <volume>",
		Short: "Mark and sweep expired volume history",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			remote, err := buildRemote(*opts)
			if err != nil {
				return err
			}
			hostname, _ := os.Hostname()
			lease, _, err := remote.Acquire(cmd.Context(), args[0], "gc@"+hostname, replica.CreateOptions{})
			if err != nil {
				return err
			}
			beatCtx, stopBeat := context.WithCancel(cmd.Context())
			collectCtx, cancelCollect := context.WithCancel(cmd.Context())
			defer cancelCollect()
			beatDone := make(chan struct{})
			var heartbeatErr error
			go func() {
				defer close(beatDone)
				lease.Heartbeat(beatCtx, func(err error) {
					heartbeatErr = err
					cancelCollect()
				})
			}()
			result, collectErr := lease.CollectGarbage(collectCtx, replica.GCOptions{
				HistoryRetention: historyRetention,
				GracePeriod:      gracePeriod,
			})
			stopBeat()
			<-beatDone
			releaseErr := lease.Release(context.WithoutCancel(cmd.Context()))
			if err := errors.Join(collectErr, heartbeatErr, releaseErr); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "marked %d objects; deleted %d objects\n", result.Marked, result.Deleted)
			return nil
		},
	}
	cmd.Flags().DurationVar(&historyRetention, "history-retention", envDurationOr("SATCHEL_HISTORY_RETENTION", 7*24*time.Hour), "retain point-in-time generations for at least this long")
	cmd.Flags().DurationVar(&gracePeriod, "gc-grace", envDurationOr("SATCHEL_GC_GRACE", 24*time.Hour), "wait this long before deleting newly unreachable objects")
	return cmd
}

func buildRemote(opts volOptions) (*replica.Remote, error) {
	if opts.s3.Bucket == "" {
		return nil, errors.New("--s3-bucket (SATCHEL_S3_BUCKET) is required")
	}
	head, err := replica.ParseHeadMode(opts.head)
	if err != nil {
		return nil, fmt.Errorf("--s3-head: %w", err)
	}
	return &replica.Remote{Store: objectstore.NewS3(opts.s3), Head: head}, nil
}

func newVolListCommand(opts *volOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List volumes in the bucket",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			remote, err := buildRemote(*opts)
			if err != nil {
				return err
			}
			states, err := remote.List(cmd.Context())
			if err != nil {
				return err
			}
			sort.Slice(states, func(i, j int) bool { return states[i].Name < states[j].Name })
			for _, state := range states {
				holder := "-"
				if state.Lease != nil && state.Lease.ExpiresAt.After(time.Now()) {
					holder = state.Lease.Holder
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s\t%d\t%s\t%d\t%s\n", state.Name, state.Size, state.Filesystem, state.Generation, holder)
			}
			return nil
		},
	}
}

func newVolInspectCommand(opts *volOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <volume>",
		Short: "Print volume metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			remote, err := buildRemote(*opts)
			if err != nil {
				return err
			}
			state, _, err := remote.Inspect(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "name: %s\nid: %s\nsize: %d\nfilesystem: %s\ngeneration: %d\n", state.Name, state.ID, state.Size, state.Filesystem, state.Generation)
			if state.Lease != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "lease: %s epoch=%d expires=%s\n", state.Lease.Holder, state.Lease.Epoch, state.Lease.ExpiresAt.Format(time.RFC3339))
			}
			return nil
		},
	}
}

func newVolVerifyCommand(opts *volOptions) *cobra.Command {
	var deep bool
	cmd := &cobra.Command{
		Use:   "verify <volume>",
		Short: "Check remote metadata integrity without modifying anything",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			remote, err := buildRemote(*opts)
			if err != nil {
				return err
			}
			state, _, err := remote.Inspect(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			report, err := remote.VerifyMetadataWithOptions(cmd.Context(), state, replica.VerifyOptions{Deep: deep})
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "manifests: %d\nbundles: %d\nrefs checked: %d\nexternal objects: %d\nbytes fetched: %d\n",
				report.Manifests, report.Bundles, report.RefsChecked, report.ExternalObjects, report.BytesFetched)
			if report.TruncatedBelowCheckpoint {
				fmt.Fprintf(out, "history truncated below checkpoint at generation %d\n", report.OldestGeneration)
			}
			for _, problem := range report.Problems {
				fmt.Fprintln(out, problem)
			}
			if len(report.Problems) > 0 {
				return fmt.Errorf("found %d metadata problems", len(report.Problems))
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&deep, "deep", false, "fetch external segments and verify content hashes")
	return cmd
}

func newVolRestoreCommand(opts *volOptions) *cobra.Command {
	var generation uint64
	var timestamp string
	cmd := &cobra.Command{
		Use:   "restore <volume> <image-file>",
		Short: "Restore a volume generation to a sparse block image",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			remote, err := buildRemote(*opts)
			if err != nil {
				return err
			}
			if _, err := os.Stat(args[1]); err == nil {
				return fmt.Errorf("destination %s already exists", args[1])
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			restoreOpts := replica.RestoreOptions{Generation: generation}
			if timestamp != "" {
				parsed, err := time.Parse(time.RFC3339, timestamp)
				if err != nil {
					return fmt.Errorf("--timestamp must be RFC3339: %w", err)
				}
				restoreOpts.Timestamp = parsed
			}
			partial := args[1] + ".partial"
			if err := os.Remove(partial); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			state, err := remote.RestoreWithOptions(cmd.Context(), args[0], partial, restoreOpts)
			if err != nil {
				_ = os.Remove(partial)
				return err
			}
			if err := os.Rename(partial, args[1]); err != nil {
				_ = os.Remove(partial)
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "restored %s generation %d to %s\n", state.Name, state.Generation, args[1])
			return nil
		},
	}
	cmd.Flags().Uint64Var(&generation, "generation", 0, "restore this exact retained generation")
	cmd.Flags().StringVar(&timestamp, "timestamp", "", "restore the latest retained generation at or before this RFC3339 time")
	return cmd
}

func newVolRemoveCommand(opts *volOptions) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "rm <volume>",
		Short: "Delete an unmounted volume and its objects",
		Long:  "Deletes the volume head and every immutable object under it. Read-only mounts hold no lease, so stop them on every node first; this command cannot detect them.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			remote, err := buildRemote(*opts)
			if err != nil {
				return err
			}
			if !yes {
				fmt.Fprintf(cmd.ErrOrStderr(), "This permanently deletes %s and all of its history.\nRead-only mounts are not tracked by a lease; stop them on every node first.\nType the volume name to continue: ", args[0])
				answer, _ := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
				if strings.TrimSpace(answer) != args[0] {
					return errors.New("confirmation did not match; volume left intact")
				}
			}
			return remote.Delete(cmd.Context(), args[0])
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "skip the confirmation prompt")
	return cmd
}

func newVolLeaseCommand(opts *volOptions) *cobra.Command {
	lease := &cobra.Command{Use: "lease", Short: "Inspect or break a volume lease"}
	var yes bool
	breakCommand := &cobra.Command{
		Use:   "break <volume>",
		Short: "Fence the current writer by clearing its lease",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			remote, err := buildRemote(*opts)
			if err != nil {
				return err
			}
			state, _, err := remote.Inspect(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if state.Lease == nil {
				fmt.Fprintf(cmd.OutOrStdout(), "%s: no lease to break\n", args[0])
				return nil
			}
			if !yes {
				fmt.Fprintf(cmd.ErrOrStderr(), "Lease on %s is held by %s until %s.\nStop or isolate that node first.\nType the holder name to continue: ", args[0], state.Lease.Holder, state.Lease.ExpiresAt.Format(time.RFC3339))
				answer, _ := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
				if strings.TrimSpace(answer) != state.Lease.Holder {
					return errors.New("confirmation did not match; lease left intact")
				}
			}
			if err := remote.Break(cmd.Context(), args[0], state.Lease.Token); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "lease on %s broken; previous holder was %s\n", args[0], state.Lease.Holder)
			return nil
		},
	}
	breakCommand.Flags().BoolVar(&yes, "yes", false, "skip the holder confirmation prompt")
	lease.AddCommand(
		&cobra.Command{
			Use: "status <volume>", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				remote, err := buildRemote(*opts)
				if err != nil {
					return err
				}
				state, _, err := remote.Inspect(cmd.Context(), args[0])
				if err != nil {
					return err
				}
				if state.Lease == nil {
					fmt.Fprintln(cmd.OutOrStdout(), "unlocked")
					return nil
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s epoch=%d expires=%s\n", state.Lease.Holder, state.Lease.Epoch, state.Lease.ExpiresAt.Format(time.RFC3339))
				return nil
			},
		},
		breakCommand,
	)
	return lease
}
