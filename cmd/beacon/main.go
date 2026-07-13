package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/open-ships/beacon/internal/app"
)

var (
	version   = "dev"
	dbPath    string
	dataAddr  string
	adminAddr string
	seedPath  string
	logLevel  string
)

func main() {
	root := &cobra.Command{
		Use:     "beacon",
		Short:   "NMEA 2000 gateway: sources, sinks, connectors",
		Version: version,
		RunE:    run,
	}
	root.Flags().StringVar(&dbPath, "db", "beacon.db", "SQLite database path (config + buffers)")
	root.Flags().StringVar(&dataAddr, "data-address", "0.0.0.0:8080", "data server bind address (sink endpoints)")
	root.Flags().StringVar(&adminAddr, "admin-address", "0.0.0.0:2112", "admin server bind address (/health, /metrics)")
	root.Flags().StringVar(&seedPath, "seed", "", "JSON config to seed an empty database")
	root.Flags().StringVar(&logLevel, "log-level", "info", "debug | info | warn | error")
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	log := buildLogger(logLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := app.Run(ctx, app.Options{
		DBPath: dbPath, DataAddr: dataAddr, AdminAddr: adminAddr,
		SeedPath: seedPath, Version: version, Log: log,
	})
	if err != nil {
		return err
	}

	<-ctx.Done()
	log.Info("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.Close(shutdownCtx); err != nil {
		log.Error("shutdown error", "err", err)
	}
	log.Info("shutdown complete")
	return nil
}

func buildLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
