// Package app is beacon's composition root: it owns the store, bus
// manager, data server, supervisor, and admin HTTP server.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
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
	"github.com/open-ships/beacon/internal/mcpserver"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/netlimit"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
	"github.com/open-ships/beacon/internal/sysinfo"
	"github.com/open-ships/beacon/internal/ui"
)

var (
	adminRetryMin             = 250 * time.Millisecond
	adminRetryMax             = time.Minute
	adminStableServeAge       = 30 * time.Second
	maintenanceInterval       = 6 * time.Hour
	maintenanceAttemptTimeout = 30 * time.Second
)

const (
	readinessTimeout            = time.Second
	adminReadHeaderTimeout      = 5 * time.Second
	adminReadTimeout            = 30 * time.Second
	adminIdleTimeout            = 90 * time.Second
	adminMaxHeaderBytes         = 64 << 10
	maxAdminRequestBytes        = 1 << 20
	maxAcceptedConnections      = 128
	maintenanceFailureThreshold = 3
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
	identity  identity.Appliance
	inv       *inventory.Registry
	invCancel context.CancelFunc
	invWG     sync.WaitGroup
	connLimit *netlimit.Limiter

	maintenanceCancel context.CancelFunc
	maintenanceWG     sync.WaitGroup
	maintenanceMu     sync.RWMutex
	maintenanceErr    error
	maintenanceFails  int

	adminMu       sync.RWMutex
	adminLn       net.Listener
	adminBindAddr string
	adminServeErr error
	adminCancel   context.CancelFunc
	adminWG       sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// Run opens the store, seeds it if requested, starts the bus manager and
// data server, performs an initial supervisor reconcile, and starts the
// admin HTTP server (/health, /metrics, /api/, /mcp). It returns once everything
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

	reg := stats.NewRegistry()
	met, promHandler, err := metrics.New(reg)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("init metrics: %w", err)
	}
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
	connLimit := netlimit.New(maxAcceptedConnections)
	ds := sink.NewDataServer(opts.DataAddr, log, sink.WithListenerWrapper(connLimit.Wrap))
	if err := ds.Start(); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("start data server: %w", err)
	}
	// Load recent source lifecycle events before sources start, then persist
	// new events through the registry's bounded asynchronous writer.
	if err := reg.AttachSourceMetricPersistence(ctx, st.DB()); err != nil {
		_ = ds.Stop(ctx)
		_ = st.Close()
		return nil, fmt.Errorf("load source metric history: %w", err)
	}

	sup := supervisor.New(st, busMgr, ds, log, met, reg)
	if err := sup.Reconcile(ctx); err != nil {
		sup.Stop()
		_ = ds.Stop(ctx)
		_ = reg.CloseSourceMetricPersistence(ctx)
		_ = st.Close()
		return nil, fmt.Errorf("initial reconcile: %w", err)
	}
	cfgSvc := config.NewService(st, sup, log)
	inv := inventory.New(st)
	if err := inv.Refresh(ctx); err != nil {
		log.Warn("load N2K inventory", "err", err)
	}
	a := &App{
		log: log, st: st, ds: ds, sup: sup, reg: reg, cfgSvc: cfgSvc,
		identity: appliance, inv: inv, connLimit: connLimit,
	}
	invCtx, invCancel := context.WithCancel(context.Background())
	a.invCancel = invCancel
	a.invWG.Add(1)
	go func() {
		defer a.invWG.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		failures := 0
		for {
			err := inv.Observe(invCtx, sup.BusDevices())
			if err != nil && invCtx.Err() == nil {
				failures++
				if failures == 1 || failures%720 == 0 {
					log.Warn("refresh N2K inventory failed", "err", err, "consecutive_failures", failures)
				} else {
					log.Debug("refresh N2K inventory still failing", "err", err, "consecutive_failures", failures)
				}
			} else if err == nil && failures > 0 {
				log.Info("N2K inventory persistence recovered", "previous_failures", failures)
				failures = 0
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
		Readiness: a.criticalReadinessStatuses, Advisories: a.readinessAdvisories,
	})
	uiHandler := ui.Handler(cfgSvc, reg, sup.Statuses, sup.BusDevices, version, log, ui.RuntimeInfo{
		Inventory: inv, CANDetails: sysinfo.DiscoverCANDetails,
	})

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promHandler)
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.HandleFunc("GET /health/ready", a.handleHealth)
	mux.HandleFunc("GET /health/live", a.handleLiveness)
	mux.Handle(mcpserver.EndpointPath, mcpserver.Handler(cfgSvc, reg, version, log))
	mux.Handle("/api/", apiHandler)
	// The UI owns the remaining root-level admin paths. More-specific API,
	// MCP, health, and metrics patterns above continue to win.
	mux.Handle("/", uiHandler)
	adminCtx, adminCancel := context.WithCancel(context.Background())
	a.adminCancel = adminCancel
	a.adminSrv = &http.Server{
		Handler:           limitAdminRequestBody(mux),
		ReadHeaderTimeout: adminReadHeaderTimeout,
		ReadTimeout:       adminReadTimeout,
		IdleTimeout:       adminIdleTimeout,
		MaxHeaderBytes:    adminMaxHeaderBytes,
		// No WriteTimeout: MCP and diagnostic responses may stream for the
		// lifetime of their request contexts.
		BaseContext: func(net.Listener) context.Context { return adminCtx },
	}
	rawLn, err := net.Listen("tcp", opts.AdminAddr)
	if err != nil {
		_ = a.Close(ctx)
		return nil, fmt.Errorf("bind admin server: %w", err)
	}
	ln := connLimit.Wrap(rawLn)
	a.adminMu.Lock()
	a.adminLn = ln
	a.adminBindAddr = ln.Addr().String()
	a.adminMu.Unlock()
	a.adminWG.Add(1)
	go a.serveAdmin(adminCtx, ln)
	a.startMaintenance()

	log.Info("beacon ready", "data", a.DataAddr(), "admin", a.AdminAddr())
	return a, nil
}

func limitAdminRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxAdminRequestBytes {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxAdminRequestBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func adminRetryDelay(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	span := d / 5
	return d - span + time.Duration(rand.Int64N(int64(2*span+1))) // #nosec G404 -- scheduling jitter is not security-sensitive.
}

// serveAdmin owns the listener after Run's initial synchronous bind. A fatal
// Accept/Serve error is not merely logged: the same address is rebound with
// bounded jittered backoff so the local control plane can recover without a
// process restart. Shutdown cancels ctx before closing the listener, which is
// how this loop distinguishes expected teardown from a failed serve loop.
func (a *App) serveAdmin(ctx context.Context, ln net.Listener) {
	defer a.adminWG.Done()
	backoff := adminRetryMin
	for {
		servedAt := time.Now()
		err := a.adminSrv.Serve(ln)
		if ctx.Err() != nil || errors.Is(err, http.ErrServerClosed) {
			return
		}
		a.adminMu.Lock()
		a.adminServeErr = err
		if a.adminLn == ln {
			a.adminLn = nil
		}
		bindAddr := a.adminBindAddr
		a.adminMu.Unlock()
		a.log.Error("admin server stopped unexpectedly; rebinding", "err", err)
		if time.Since(servedAt) >= adminStableServeAge {
			backoff = adminRetryMin
		} else {
			backoff = min(backoff*2, adminRetryMax)
		}

		for {
			timer := time.NewTimer(adminRetryDelay(backoff))
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			rawNext, listenErr := net.Listen("tcp", bindAddr)
			if listenErr != nil {
				a.adminMu.Lock()
				a.adminServeErr = listenErr
				a.adminMu.Unlock()
				a.log.Debug("admin server rebind failed", "err", listenErr, "retry_in", backoff)
				backoff = min(backoff*2, adminRetryMax)
				continue
			}
			next := a.connLimit.Wrap(rawNext)
			if ctx.Err() != nil {
				_ = next.Close()
				return
			}
			a.adminMu.Lock()
			a.adminLn = next
			a.adminServeErr = nil
			a.adminMu.Unlock()
			a.log.Info("admin server rebound", "admin", next.Addr())
			ln = next
			break
		}
	}
}

// startMaintenance owns bounded, low-frequency SQLite housekeeping for the
// App lifecycle. WAL checkpoints and incremental vacuum are intentionally
// conservative background work; they never run a full VACUUM.
func (a *App) startMaintenance() {
	ctx, cancel := context.WithCancel(context.Background())
	a.maintenanceCancel = cancel
	a.maintenanceWG.Add(1)
	go func() {
		defer a.maintenanceWG.Done()
		ticker := time.NewTicker(maintenanceInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				attemptCtx, attemptCancel := context.WithTimeout(ctx, maintenanceAttemptTimeout)
				err := a.st.Maintain(attemptCtx)
				attemptCancel()
				if ctx.Err() != nil {
					return
				}
				a.recordMaintenanceResult(err)
			}
		}
	}()
}

func (a *App) recordMaintenanceResult(err error) {
	a.maintenanceMu.Lock()
	if err == nil {
		recovered := a.maintenanceErr != nil
		a.maintenanceErr = nil
		a.maintenanceFails = 0
		a.maintenanceMu.Unlock()
		if recovered {
			a.log.Info("store maintenance recovered")
		}
		return
	}
	a.maintenanceFails++
	failures := a.maintenanceFails
	if failures >= maintenanceFailureThreshold {
		a.maintenanceErr = err
	}
	a.maintenanceMu.Unlock()
	a.log.Warn("store maintenance failed", "err", err, "consecutive_failures", failures)
}

func (a *App) persistentMaintenanceError() error {
	a.maintenanceMu.RLock()
	defer a.maintenanceMu.RUnlock()
	return a.maintenanceErr
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

// handleLiveness answers only whether the HTTP process can run a handler. It
// deliberately ignores downstream components and the store: using expected
// vessel connectivity loss as a liveness failure would create restart loops.
func (a *App) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// criticalReadinessStatuses is the shared local-readiness seam for the
// top-level and REST health endpoints. Remote source/sink connectivity is not
// included: an offline vessel must stay live and avoid restart loops.
func (a *App) criticalReadinessStatuses(ctx context.Context) []supervisor.Status {
	var statuses []supervisor.Status
	if err := a.storeReadiness(ctx); err != nil {
		statuses = append(statuses, supervisor.Status{
			Kind: "system", ID: "store", State: "error", Err: err.Error(),
		})
	}
	a.adminMu.RLock()
	adminErr := a.adminServeErr
	a.adminMu.RUnlock()
	if adminErr != nil {
		statuses = append(statuses, supervisor.Status{
			Kind: "system", ID: "admin_server", State: "error", Err: adminErr.Error(),
		})
	}
	if state, err := a.ds.State(); state != "up" || err != nil {
		status := supervisor.Status{Kind: "system", ID: "data_server", State: state}
		if err != nil {
			status.Err = err.Error()
		}
		statuses = append(statuses, status)
	}
	return statuses
}

func (a *App) readinessAdvisories() []supervisor.Status {
	if err := a.persistentMaintenanceError(); err != nil {
		return []supervisor.Status{{
			Kind: "system", ID: "store_maintenance", State: "degraded", Err: err.Error(),
		}}
	}
	return nil
}

// handleHealth is the readiness surface used by both /health and
// /health/ready. Functional component degradation remains an HTTP 200 with a
// "degraded" body because network endpoints may legitimately be offline for
// long voyages. Failure of the local durable store is different: the
// appliance cannot read desired state or persist delivery, so readiness is
// false and the endpoint returns 503. /health/live stays independent.
func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	statuses := a.sup.Statuses()
	readyCtx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()
	ready := true
	for _, status := range statuses {
		if status.Kind == "system" && status.State != "up" {
			ready = false
		}
	}
	critical := a.criticalReadinessStatuses(readyCtx)
	if len(critical) > 0 {
		ready = false
	}
	statuses = append(statuses, critical...)
	// Maintenance is important but not an immediate inability to serve or
	// persist. Surface it as functional degradation without returning 503; a
	// genuinely unavailable/full store is caught above.
	statuses = append(statuses, a.readinessAdvisories()...)
	status := supervisor.RollupHealth(statuses)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": status, "components": statuses,
	})
}

// storeReadiness checks both reachability and writable headroom. SQLite can
// reuse freelist pages even when the file has reached max_page_count, so the
// store is exhausted only when neither unallocated nor reusable pages remain.
func (a *App) storeReadiness(ctx context.Context) error {
	if err := a.st.DB().PingContext(ctx); err != nil {
		return fmt.Errorf("ping store: %w", err)
	}
	storage, err := a.st.StorageStats(ctx)
	if err != nil {
		return fmt.Errorf("read storage headroom: %w", err)
	}
	if storage.PageCount >= storage.MaxPages && storage.FreePages == 0 {
		return fmt.Errorf("database storage budget exhausted (%d bytes)", storage.MaxBytes)
	}
	return nil
}

// DataAddr returns the bound address of the data server (sink endpoints).
func (a *App) DataAddr() string { return a.ds.Addr() }

// AdminAddr returns the bound address of the admin server (/health,
// /metrics, /api/, /mcp).
func (a *App) AdminAddr() string {
	a.adminMu.RLock()
	defer a.adminMu.RUnlock()
	if a.adminLn != nil {
		return a.adminLn.Addr().String()
	}
	return a.adminBindAddr
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
// store. It is idempotent; concurrent or repeated callers receive the result
// of the first teardown.
func (a *App) Close(ctx context.Context) error {
	a.closeOnce.Do(func() {
		if a.adminCancel != nil {
			a.adminCancel()
		}
		var adminErr error
		var adminCloseErr error
		if a.adminSrv != nil {
			adminErr = a.adminSrv.Shutdown(ctx)
			if adminErr != nil {
				// A timed-out graceful shutdown must not leave admin client
				// descriptors or handlers alive while owned components are torn
				// down underneath them.
				adminCloseErr = a.adminSrv.Close()
			}
		}
		a.adminWG.Wait()
		if a.invCancel != nil {
			a.invCancel()
			a.invWG.Wait()
		}
		if a.maintenanceCancel != nil {
			a.maintenanceCancel()
			a.maintenanceWG.Wait()
		}
		a.sup.Stop() // connectors flush final checkpoints
		dataErr := a.ds.Stop(ctx)
		persistenceErr := a.reg.CloseSourceMetricPersistence(ctx)
		storeErr := a.st.Close()
		a.closeErr = errors.Join(adminErr, adminCloseErr, dataErr, persistenceErr, storeErr)
	})
	return a.closeErr
}
