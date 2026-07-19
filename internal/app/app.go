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
	"net/url"
	"os"
	"sync"
	"time"

	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/api"
	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/identity"
	"github.com/open-ships/beacon/internal/inventory"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
	"github.com/open-ships/beacon/internal/sysinfo"
	"github.com/open-ships/beacon/internal/ui"
)

// Options configures a Run. DBPath, DataAddr, and AdminAddr are required;
// SeedPath, Version, N2KBus (simulation/embedding), and ExtraN2KOpts are
// optional.
type Options struct {
	DBPath       string
	DataAddr     string
	AdminAddr    string
	SeedPath     string
	Version      string // embedded in the config API's OpenAPI document; defaults to "dev"
	Log          *slog.Logger
	N2KBus       n2k.Bus
	ExtraN2KOpts []n2k.Option
}

// App is a running beacon instance: store, bus manager, data server,
// supervisor, and admin HTTP server, all started by Run and torn down by
// Close.
type App struct {
	log       *slog.Logger
	st        *store.Store
	ds        *sink.DataServer
	sup       *supervisor.Supervisor
	reg       *stats.Registry
	cfgSvc    *config.Service
	adminSrv  *http.Server
	adminLn   net.Listener
	identity  identity.Appliance
	inv       *inventory.Registry
	invCancel context.CancelFunc
	invWG     sync.WaitGroup
}

// Run opens the store, seeds it if requested, starts the bus manager and
// data server, performs an initial supervisor reconcile, and starts the
// admin HTTP server (/health, /metrics, /api/). It returns once everything
// is listening.
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
	version := opts.Version
	if version == "" {
		version = "dev"
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
	reg := stats.NewRegistry()
	appliance, err := identity.LoadOrCreate(ctx, st)
	if err != nil {
		_ = st.Close()
		return nil, err
	}

	// Identity options go first so caller-supplied configuration (tests use a
	// shorter claim timeout) can override it.
	n2kOpts := append(appliance.Options(version), opts.ExtraN2KOpts...)
	var busMgr *bus.Manager
	if opts.N2KBus != nil {
		busMgr = bus.NewManagerWithBus(log, met, opts.N2KBus, n2kOpts...)
	} else {
		busMgr = bus.NewManager(log, met, n2kOpts...)
	}
	busMgr.SetStatsRegistry(reg)
	ds := sink.NewDataServer(opts.DataAddr, log)
	if err := ds.Start(); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("start data server: %w", err)
	}

	sup := supervisor.New(st, busMgr, ds, log, met, reg)
	if err := sup.Reconcile(ctx); err != nil {
		sup.Stop()
		_ = ds.Stop(ctx)
		_ = st.Close()
		return nil, fmt.Errorf("initial reconcile: %w", err)
	}
	cfgSvc := config.NewService(st, sup, log)
	inv := inventory.New(st)
	if err := inv.Refresh(ctx); err != nil {
		log.Warn("load N2K inventory", "err", err)
	}

	a := &App{log: log, st: st, ds: ds, sup: sup, reg: reg, cfgSvc: cfgSvc, identity: appliance, inv: inv}
	invCtx, invCancel := context.WithCancel(context.Background())
	a.invCancel = invCancel
	a.invWG.Add(1)
	go func() {
		defer a.invWG.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			if err := inv.Observe(invCtx, sup.BusDevices()); err != nil && invCtx.Err() == nil {
				log.Warn("refresh N2K inventory", "err", err)
			}
			select {
			case <-invCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	apiHandler, _ := api.New(cfgSvc, reg, version, log, api.RuntimeInfo{
		Identity: appliance, Devices: sup.BusDevices, Buses: sup.BusStatuses, Inventory: inv, Statuses: sup.Statuses,
	})
	uiHandler := ui.Handler(cfgSvc, reg, sup.Statuses, sup.BusDevices, version, log, ui.RuntimeInfo{
		Inventory: inv, CANDetails: sysinfo.DiscoverCANDetails,
	})

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promHandler)
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.Handle("/api/", apiHandler)
	mux.Handle("/ui/", uiHandler)
	// The exact-path "/ui" mount (alongside the "/ui/" subtree mount above)
	// is required for GET /ui to reach uiHandler's own "GET /ui" redirect
	// route at all — see ui.Handler's doc comment for why.
	mux.Handle("/ui", uiHandler)
	// "/docs" and "/docs/{slug}" (spec §5) are permanent redirects to their
	// /ui/docs equivalents (internal/ui/docspages.go), not a second copy of
	// the manual — "/docs" is one of model.ReservedPathPrefixes, so no HTTP
	// sink config can ever collide with either pattern.
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/docs", http.StatusMovedPermanently)
	})
	mux.HandleFunc("GET /docs/{slug}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/docs/"+url.PathEscape(r.PathValue("slug")), http.StatusMovedPermanently)
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/dashboard", http.StatusFound)
	})
	a.adminSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
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
	raw, err := os.ReadFile(path) // #nosec G304 -- path is the operator-selected seed file.
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
	status := supervisor.RollupHealth(statuses)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": status, "components": statuses,
	})
}

// DataAddr returns the bound address of the data server (sink endpoints).
func (a *App) DataAddr() string { return a.ds.Addr() }

// AdminAddr returns the bound address of the admin server (/health,
// /metrics, /api/).
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

// Service returns the config service layer: the single
// validation+persist+reconcile choke point that the HTTP config API (Phase
// 2 Task 4) and, later, the Phase 3 UI sit on instead of touching the store
// directly.
func (a *App) Service() *config.Service { return a.cfgSvc }

// Close shuts the admin server down, stops the supervisor (which stops every
// running connector, sink, and source and flushes final queue checkpoints),
// stops the data server (unblocking any live SSE/WS streams), and closes the
// store. Safe to call once; a second call would double-close the store, so
// callers should not call Close twice.
func (a *App) Close(ctx context.Context) error {
	if a.adminSrv != nil {
		_ = a.adminSrv.Shutdown(ctx)
	}
	if a.invCancel != nil {
		a.invCancel()
		a.invWG.Wait()
	}
	a.sup.Stop() // connectors flush final checkpoints
	_ = a.ds.Stop(ctx)
	return a.st.Close()
}
