package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/open-ships/beacon/internal/app"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/store"
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
	root.Flags().StringVar(&adminAddr, "admin-address", "0.0.0.0:2112", "admin server bind address (UI, API, MCP, health, metrics)")
	root.Flags().StringVar(&seedPath, "seed", "", "JSON config to seed an empty database")
	root.Flags().StringVar(&logLevel, "log-level", "info", "debug | info | warn | error")
	root.AddCommand(newExportCmd(), newImportCmd(), newCompactCmd(), newSizeBufferCmd())
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

type outagePlan struct {
	MaxMessages int64
	MaxBytes    int64
	MaxAge      time.Duration
}

// sizeOutageBuffer turns an observed route rate and average canonical
// Envelope size into independent count, byte, and age guards. The reserve is
// deliberately applied to all three dimensions: a route that is exactly at
// its average should not start pruning at the instant the planned outage
// ends.
func sizeOutageBuffer(rate float64, averageBytes int64, outage time.Duration, reservePercent float64) (outagePlan, error) {
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 {
		return outagePlan{}, fmt.Errorf("message rate must be a finite value greater than zero")
	}
	if averageBytes <= 0 {
		return outagePlan{}, fmt.Errorf("average envelope bytes must be greater than zero")
	}
	if outage <= 0 {
		return outagePlan{}, fmt.Errorf("outage duration must be greater than zero")
	}
	if math.IsNaN(reservePercent) || math.IsInf(reservePercent, 0) || reservePercent < 0 || reservePercent > 1000 {
		return outagePlan{}, fmt.Errorf("reserve percent must be between 0 and 1000")
	}

	factor := 1 + reservePercent/100
	messages := math.Ceil(rate * outage.Seconds() * factor)
	bytes := messages * float64(averageBytes)
	age := float64(outage) * factor
	if messages > math.MaxInt64 || bytes > math.MaxInt64 || age > math.MaxInt64 {
		return outagePlan{}, fmt.Errorf("outage plan exceeds supported limits")
	}
	return outagePlan{
		MaxMessages: int64(messages),
		MaxBytes:    int64(math.Ceil(bytes)),
		MaxAge:      time.Duration(math.Ceil(age)),
	}, nil
}

func newSizeBufferCmd() *cobra.Command {
	var (
		rate           float64
		averageBytes   int64
		outage         time.Duration
		reservePercent float64
	)
	cmd := &cobra.Command{
		Use:   "size-buffer",
		Short: "Size one connector buffer for a planned offline interval",
		Long: "Calculate independent message, logical-byte, and age limits from an " +
			"observed Envelope rate and average canonical JSON size. Physical SQLite " +
			"capacity also needs the configured database reserve and filesystem headroom.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			plan, err := sizeOutageBuffer(rate, averageBytes, outage, reservePercent)
			if err != nil {
				return err
			}
			if plan.MaxMessages > model.MaxBufferMessages || plan.MaxBytes > model.MaxBufferBytes || plan.MaxAge > time.Duration(model.MaxBufferAge) {
				return fmt.Errorf("planned outage needs max_messages=%d, max_bytes=%d, max_age=%s; this exceeds Beacon's per-route safety bounds",
					plan.MaxMessages, plan.MaxBytes, plan.MaxAge)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"planned buffer (%.1f%% reserve):\n  max_messages: %d\n  max_bytes: %d\n  max_age: %s\n",
				reservePercent, plan.MaxMessages, plan.MaxBytes, plan.MaxAge)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"minimum logical queue budget for this route: %d bytes; add SQLite overhead and settings.resources.database_reserve_bytes to size the physical database\n",
				plan.MaxBytes)
			return nil
		},
	}
	cmd.Flags().Float64Var(&rate, "rate", 0, "observed peak Envelope rate in messages/second")
	cmd.Flags().Int64Var(&averageBytes, "average-bytes", 0, "observed average canonical Envelope JSON size")
	cmd.Flags().DurationVar(&outage, "outage", 0, "longest planned disconnected interval (for example 168h)")
	cmd.Flags().Float64Var(&reservePercent, "reserve-percent", 25, "capacity margin applied to count, bytes, and age")
	_ = cmd.MarkFlagRequired("rate")
	_ = cmd.MarkFlagRequired("average-bytes")
	_ = cmd.MarkFlagRequired("outage")
	return cmd
}

func newCompactCmd() *cobra.Command {
	var db string
	cmd := &cobra.Command{
		Use:   "compact",
		Short: "Reclaim SQLite high-water disk space (offline)",
		Long: "Compact runs SQLite VACUUM and enables incremental page reclamation. " +
			"Beacon must be stopped, and the volume must have enough temporary free space " +
			"for approximately one additional database copy.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			st, err := store.Open(db)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}
			defer func() { _ = st.Close() }()
			before, err := st.StorageStats(cmd.Context())
			if err != nil {
				return fmt.Errorf("read storage stats: %w", err)
			}
			if err := st.Compact(cmd.Context()); err != nil {
				return fmt.Errorf("compact store: %w", err)
			}
			after, err := st.StorageStats(cmd.Context())
			if err != nil {
				return fmt.Errorf("read compacted storage stats: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "compacted database: %d -> %d bytes\n", before.PhysicalBytes, after.PhysicalBytes)
			return nil
		},
	}
	cmd.Flags().StringVar(&db, "db", "beacon.db", "SQLite database path (Beacon must be stopped)")
	return cmd
}

// newExportCmd builds `beacon export`: an OFFLINE command that opens the
// SQLite database directly and prints its configuration as JSON to stdout.
// It must not be run against a database a live beacon process currently has
// open — modernc.org/sqlite's driver here uses a single connection
// (store.Open sets SetMaxOpenConns(1)), so a second process opening the
// same file races the running one instead of sharing it safely. To read a
// running beacon's configuration, use GET /api/v1/config/export instead.
func newExportCmd() *cobra.Command {
	var db string
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export the stored configuration as JSON (offline; do not run against a live beacon's database)",
		Long: "Export opens the SQLite database directly and prints its sources, sinks, " +
			"and connectors as JSON to stdout.\n\n" +
			"This is an OFFLINE operation: run it only against a database file that is " +
			"not currently held open by a running beacon process. beacon's SQLite driver " +
			"uses a single connection per process, so a second process opening the same " +
			"file does not share it safely. To read a running beacon's configuration " +
			"instead, use GET /api/v1/config/export on its admin API.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runExport(db, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&db, "db", "beacon.db", "SQLite database path")
	return cmd
}

// newImportCmd builds `beacon import`: an OFFLINE command that validates a
// JSON config file (the same structural + CEL rules config.ValidateConfig
// applies to every API write) and writes it directly to the SQLite
// database. See newExportCmd's doc comment for why this must not be run
// against a database a live beacon process currently has open; the live
// equivalent is POST /api/v1/config/import.
func newImportCmd() *cobra.Command {
	var db string
	var merge bool
	cmd := &cobra.Command{
		Use:   "import <file.json>",
		Short: "Import a configuration from a JSON file (offline; do not run against a live beacon's database)",
		Long: "Import reads a JSON config file, validates it (the same structural and " +
			"CEL filter rules the API applies to every write), and writes it directly to " +
			"the SQLite database.\n\n" +
			"By default the file replaces the whole configuration; --merge instead " +
			"upserts the file's entities by id onto the existing configuration, leaving " +
			"entities it does not mention untouched. Either way, an invalid file leaves " +
			"the database untouched.\n\n" +
			"This is an OFFLINE operation: run it only against a database file that is " +
			"not currently held open by a running beacon process. beacon's SQLite driver " +
			"uses a single connection per process, so a second process opening the same " +
			"file does not share it safely. To import into a running beacon instead, use " +
			"POST /api/v1/config/import on its admin API.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runImport(db, args[0], merge, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&db, "db", "beacon.db", "SQLite database path")
	cmd.Flags().BoolVar(&merge, "merge", false, "merge into the existing configuration instead of replacing it")
	return cmd
}

// runExport opens the store at dbPath and writes its whole configuration to
// w as indented JSON. Factored out of newExportCmd's RunE so it is directly
// unit-testable without going through cobra or a built binary.
func runExport(dbPath string, w io.Writer) error {
	st, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = st.Close() }()

	cfg, err := st.LoadConfig(context.Background())
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(cfg)
}

// runImport reads and validates the config file at filePath and writes it
// to the store at dbPath: wholesale (merge=false) or upserted onto the
// existing configuration (merge=true, via config.MergeConfig — the same
// merge logic Service.Import uses for a live beacon's merge mode). It goes
// through config.ValidateConfig before any store write, so an invalid file
// leaves the database untouched. A one-line summary is written to w on
// success. Factored out of newImportCmd's RunE so it is directly
// unit-testable without going through cobra or a built binary.
func runImport(dbPath, filePath string, merge bool, w io.Writer) error {
	raw, err := os.ReadFile(filePath) // #nosec G304 -- filePath is the operator-selected import file.
	if err != nil {
		return fmt.Errorf("read import file: %w", err)
	}
	var incoming model.Config
	if err := json.Unmarshal(raw, &incoming); err != nil {
		return fmt.Errorf("parse import file: %w", err)
	}

	st, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = st.Close() }()

	ctx := context.Background()
	result := incoming
	if merge {
		current, err := st.LoadConfig(ctx)
		if err != nil {
			return fmt.Errorf("load current config: %w", err)
		}
		result = config.MergeConfig(current, incoming)
	}
	if err := config.ValidateConfig(result); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	if err := st.ReplaceConfig(ctx, result); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	_, _ = fmt.Fprintf(w, "imported: %d sources, %d sinks, %d connectors\n",
		len(result.Sources), len(result.Sinks), len(result.Connectors))
	return nil
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
