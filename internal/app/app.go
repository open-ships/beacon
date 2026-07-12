// Package app is beacon's composition root: it owns the store, bus
// manager, data server, supervisor, and admin HTTP server.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
)

// Options configures a Run. DBPath, DataAddr, and AdminAddr are required;
// SeedPath and ExtraN2KOpts (test injection: fake bus, claim timeout) are
// optional.
type Options struct {
	DBPath       string
	DataAddr     string
	AdminAddr    string
	SeedPath     string
	Log          *slog.Logger
	ExtraN2KOpts []n2k.Option
}

// App is a running beacon instance: store, bus manager, data server,
// supervisor, and admin HTTP server, all started by Run and torn down by
// Close.
type App struct {
	log      *slog.Logger
	st       *store.Store
	ds       *sink.DataServer
	sup      *supervisor.Supervisor
	reg      *stats.Registry
	adminSrv *http.Server
	adminLn  net.Listener
}

// Run opens the store, seeds it if requested, starts the bus manager and
// data server, performs an initial supervisor reconcile, and starts the
// admin HTTP server (/health, /metrics). It returns once everything is
// listening.
//
// Components started by the supervisor's reconcile run on the supervisor's
// own background context, not ctx: ctx only bounds the Run call itself (the
// store open, the seed, the initial reconcile, and the admin listener bind).
// Call (*App).Close to stop everything Run started.
func Run(ctx context.Context, opts Options) (*App, error) {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	st, err := store.Open(opts.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	if opts.SeedPath != "" {
		if err := seed(ctx, st, opts.SeedPath, log); err != nil {
			_ = st.Close()
			return nil, err
		}
	}

	met, promHandler, err := metrics.New()
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("init metrics: %w", err)
	}

	busMgr := bus.NewManager(log, met, opts.ExtraN2KOpts...)
	ds := sink.NewDataServer(opts.DataAddr, log)
	if err := ds.Start(); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("start data server: %w", err)
	}

	reg := stats.NewRegistry()
	sup := supervisor.New(st, busMgr, ds, log, met, reg)
	if err := sup.Reconcile(ctx); err != nil {
		sup.Stop()
		_ = ds.Stop(ctx)
		_ = st.Close()
		return nil, fmt.Errorf("initial reconcile: %w", err)
	}

	a := &App{log: log, st: st, ds: ds, sup: sup, reg: reg}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promHandler)
	mux.HandleFunc("GET /health", a.handleHealth)
	a.adminSrv = &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", opts.AdminAddr)
	if err != nil {
		_ = a.Close(ctx)
		return nil, fmt.Errorf("bind admin server: %w", err)
	}
	a.adminLn = ln
	go func() {
		if err := a.adminSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server error", "err", err)
		}
	}()

	log.Info("beacon ready", "data", a.DataAddr(), "admin", a.AdminAddr())
	return a, nil
}

// seed loads a JSON config from path and applies it via ReplaceConfig, but
// only when the store is empty; a non-empty store keeps its existing
// configuration and just logs that the seed was ignored. The config is
// validated (structural rules, then every connector's CEL filters compile)
// before anything is written.
func seed(ctx context.Context, st *store.Store, path string, log *slog.Logger) error {
	empty, err := st.IsEmpty(ctx)
	if err != nil {
		return err
	}
	if !empty {
		log.Info("store not empty; ignoring seed file", "seed", path)
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read seed: %w", err)
	}
	var cfg model.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("parse seed: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("seed config invalid: %w", err)
	}
	for _, c := range cfg.Connectors {
		if _, err := filter.Compile(c.Filters); err != nil {
			return fmt.Errorf("seed connector %q: %w", c.ID, err)
		}
	}
	log.Info("seeding configuration", "seed", path,
		"sources", len(cfg.Sources), "sinks", len(cfg.Sinks), "connectors", len(cfg.Connectors))
	return st.ReplaceConfig(ctx, cfg)
}

// handleHealth reports "ok" when every component is up, "degraded"
// otherwise. It always responds 200; 503 is reserved for a store that
// cannot be reached at all, which handleHealth does not need to check since
// Statuses() reads from the supervisor's in-memory view, not the store.
func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	statuses := a.sup.Statuses()
	status := "ok"
	for _, s := range statuses {
		if s.State != "up" {
			status = "degraded"
			break
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": status, "components": statuses,
	})
}

// DataAddr returns the bound address of the data server (sink endpoints).
func (a *App) DataAddr() string { return a.ds.Addr() }

// AdminAddr returns the bound address of the admin server (/health, /metrics).
func (a *App) AdminAddr() string {
	if a.adminLn == nil {
		return ""
	}
	return a.adminLn.Addr().String()
}

// Reconcile re-converges running components with stored config (Phase 2 API
// hook: hot config apply will call this after writing to the store).
func (a *App) Reconcile(ctx context.Context) error { return a.sup.Reconcile(ctx) }

// Stats returns the live per-connector stats registry (Phase 2 API hook:
// serves rate/throughput data to the config API and, later, the UI
// dashboard).
func (a *App) Stats() *stats.Registry { return a.reg }

// Close shuts the admin server down, stops the supervisor (which stops every
// running connector, sink, and source and flushes final queue checkpoints),
// stops the data server (unblocking any live SSE/WS streams), and closes the
// store. Safe to call once; a second call would double-close the store, so
// callers should not call Close twice.
func (a *App) Close(ctx context.Context) error {
	if a.adminSrv != nil {
		_ = a.adminSrv.Shutdown(ctx)
	}
	a.sup.Stop() // connectors flush final checkpoints
	_ = a.ds.Stop(ctx)
	return a.st.Close()
}
