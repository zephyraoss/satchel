package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:           "satchel",
		Short:         "Portable Docker block volumes replicated to S3",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newPluginCommand(), newMountCommand(), newVolCommand())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
