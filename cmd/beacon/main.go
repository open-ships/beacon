package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	root := &cobra.Command{
		Use:     "beacon",
		Short:   "NMEA 2000 gateway: sources, sinks, connectors",
		Version: version,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return fmt.Errorf("core runtime not wired yet (Phase 1 in progress)")
		},
	}
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
