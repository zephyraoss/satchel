package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:           "satchel",
		Short:         "Portable Docker volumes backed by SQLite + Litestream",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newPluginCommand(), newMountCommand(), newVolCommand())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
