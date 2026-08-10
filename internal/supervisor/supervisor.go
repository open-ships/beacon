// Package supervisor reconciles desired configuration (the store) against
// running source/sink/connector components.
package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/connector"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/source"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
)

// testStopDelay, when non-zero, is injected right before Reconcile's stop
// phase tears down each component. It exists solely so tests can simulate a
// slow-stopping component deterministically (real component Stop() calls are
// too fast/network-dependent to reliably exercise the non-blocking-Statuses
// guarantee). Always zero in production.
var (
	testStopDelay time.Duration

	// These are variables rather than constants so focused package tests can
	// exercise convergence without sleeping for production-scale intervals.
	convergenceInterval       = 30 * time.Second
	convergenceRetryMin       = time.Second
	convergenceRetryMax       = time.Minute
	convergenceAttemptTimeout = 15 * time.Second
)

type Status struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	State string `json:"state"` // "up" | "degraded" | "error"
	Err   string `json:"err,omitempty"`
}

// RollupHealth reduces a set of component statuses (as returned by
// Supervisor.Statuses / config.Service.Statuses) to a single overall health
// string: "ok" when every component's State is "up" (including the empty
// set — a fresh, unconfigured system is healthy), "degraded" if any
// component is in any other state. Shared by the admin server's top-level
// GET /health (internal/app) and the config API's GET /api/v1/health
// (internal/api) so the two surfaces can never drift on this logic.
func RollupHealth(statuses []Status) string {
	for _, s := range statuses {
		if s.State != "up" {
			return "degraded"
		}
	}
	return "ok"
}

type runningSource struct {
	hash string
	rt   source.Runtime
}

type runningSink struct {
	hash string
	rt   sink.Runtime
}

type runningConnector struct {
	hash string
	c    *connector.Connector
}

// Supervisor reconciles desired configuration (read from the store) against
// running source/sink/connector components: starting, stopping, and
// restarting them to converge. Reconcile is safe to call repeatedly
// (idempotent when nothing changed) and safe to call concurrently: the whole
// operation — including the store read — runs under reconcileMu, so
// overlapping Reconcile calls fully serialize rather than racing a stale
// read against a newer one. Component constructors (source.New, sink.New,
// connector.Start) are expected to connect/dial asynchronously and return
// quickly.
//
// Lock discipline: reconcileMu serializes the bodies of Reconcile and Stop
// (so they never interleave), including component construction and teardown.
// The sources/sinks/connectors/errored maps are guarded separately by stateMu,
// which is never held across constructors or Stop calls. Statuses() only needs
// stateMu, so it never blocks behind a slow Reconcile teardown or Stop.
//
// Components started by Reconcile run on the Supervisor's own background
// context (runCtx below), NOT the context passed into Reconcile. runCtx is
// created in New and cancelled in Stop. This matters because Reconcile will
// eventually be invoked from HTTP handlers (hot config apply): if started
// components inherited that request-scoped context, they would be killed the
// moment the triggering HTTP request ended. Reconcile's ctx therefore only
// bounds the reconcile operation itself — the store read and the
// constructor calls — never a running component's lifetime.
//
// Once Stop has run, runCtx is permanently cancelled (it is never
// recreated), so a later Reconcile call is a deliberate no-op (stopped
// below) rather than silently starting components bound to a dead context
// that would report as running but never actually do anything.
type Supervisor struct {
	st  *store.Store
	bus *bus.Manager
	ds  *sink.DataServer
	log *slog.Logger
	met *metrics.Set
	reg *stats.Registry

	runCtx     context.Context
	runCancel  context.CancelFunc
	converge   chan struct{}
	convergeWG sync.WaitGroup

	// reconcileMu serializes Reconcile and Stop bodies. It also guards
	// `stopped`, `needsPurgeSweep`, and the prevConfigured* sets, all of which are
	// only ever read/written from within those two methods (Statuses never
	// touches them, so stateMu is not involved).
	reconcileMu sync.Mutex
	stopped     bool
	// needsPurgeSweep arms the KnownConnectorIDs purge sweep in Reconcile.
	// It starts true (so the first Reconcile cleans up storage of connectors
	// deleted from config while the process was down), is re-armed whenever
	// the configured-connector id set shrinks, and is disarmed only after a
	// fully successful sweep so a failed purge retries next Reconcile.
	needsPurgeSweep bool
	// The previous configured sets include disabled components and let
	// Reconcile distinguish deletion from disablement or a hot restart.
	prevConfigured        map[string]bool
	prevConfiguredSources map[string]bool
	prevConfiguredSinks   map[string]bool

	// stateMu guards sources/sinks/connectors/errored only. Held briefly for
	// map reads/writes — never across a component constructor or Stop()
	// call — so Statuses() never blocks behind a slow teardown.
	stateMu      sync.Mutex
	sources      map[string]*runningSource
	sinks        map[string]*runningSink
	connectors   map[string]*runningConnector
	errored      []Status
	reconcileErr error
}

func New(st *store.Store, busMgr *bus.Manager, ds *sink.DataServer, log *slog.Logger, met *metrics.Set, reg *stats.Registry) *Supervisor {
	runCtx, cancel := context.WithCancel(context.Background())
	s := &Supervisor{st: st, bus: busMgr, ds: ds, log: log, met: met, reg: reg,
		runCtx: runCtx, runCancel: cancel,
		converge:        make(chan struct{}, 1),
		needsPurgeSweep: true,
		sources:         map[string]*runningSource{},
		sinks:           map[string]*runningSink{},
		connectors:      map[string]*runningConnector{},
	}
	s.convergeWG.Add(1)
	go s.convergenceLoop()
	return s
}

func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	// 80-120% prevents multiple appliances or components recovering from a
	// shared outage from remaining phase-aligned.
	span := d / 5
	return d - span + time.Duration(rand.Int64N(int64(2*span+1))) // #nosec G404 -- scheduling jitter is not security-sensitive.
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

func (s *Supervisor) requestConvergence() {
	if s.runCtx.Err() != nil {
		return
	}
	select {
	case s.converge <- struct{}{}:
	default:
	}
}

func (s *Supervisor) hasStartFailures() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return len(s.errored) != 0
}

// convergenceLoop periodically compares persisted desired state with the
// actual runtime maps. A failed constructor leaves no runtime map entry, so
// the next pass retries it without requiring another config write or process
// restart. Component-reported degraded/error states are deliberately not a
// restart trigger: network runtimes own their reconnect loops, and readiness
// must never create a restart storm during an expected connectivity outage.
func (s *Supervisor) convergenceLoop() {
	defer s.convergeWG.Done()
	backoff := convergenceRetryMin
	failures := 0
	timer := time.NewTimer(jitter(convergenceInterval))
	defer timer.Stop()
	for {
		select {
		case <-s.runCtx.Done():
			return
		case <-s.converge:
			resetTimer(timer, jitter(backoff))
		case <-timer.C:
			ctx, cancel := context.WithTimeout(s.runCtx, convergenceAttemptTimeout)
			err := s.Reconcile(ctx)
			cancel()
			retry := err != nil || s.hasStartFailures()
			// Reconcile signals the channel on a retryable result. Consume that
			// self-signal here so it cannot reset an exponential delay to its
			// previous value on the next select.
			select {
			case <-s.converge:
			default:
			}
			if retry {
				failures++
				if err != nil && s.runCtx.Err() == nil {
					if failures == 1 || failures%60 == 0 {
						s.log.Warn("runtime convergence failed; retrying", "err", err,
							"retry_in", backoff, "consecutive_failures", failures)
					} else {
						s.log.Debug("runtime convergence still failing", "err", err,
							"retry_in", backoff, "consecutive_failures", failures)
					}
				}
				resetTimer(timer, jitter(backoff))
				backoff = min(backoff*2, convergenceRetryMax)
				continue
			}
			failures = 0
			backoff = convergenceRetryMin
			resetTimer(timer, jitter(convergenceInterval))
		}
	}
}

func hashOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// stopItem is a component staged for teardown: its identity (for logging and
// metric hygiene) and the Stop() call itself, captured as a closure so the
// stop phase can run uniformly across sources/sinks/connectors outside
// stateMu. notDesired is true when the component is being stopped because it
// is no longer in the desired set at all (disabled or deleted) as opposed to
// merely being restarted due to a config/endpoint hash change.
type stopItem struct {
	kind       string
	id         string
	stop       func()
	notDesired bool
}

// Reconcile diffs desired config (the store) against running components and
// converges: stop-then-start whatever changed. It returns an error when desired
// state cannot be loaded, fully validated, or have its process-wide resource
// budget applied; individual component failures are recorded as error statuses
// (see Statuses) and never abort or crash the reconcile.
func (s *Supervisor) Reconcile(ctx context.Context) (retErr error) {
	defer func() {
		s.stateMu.Lock()
		s.reconcileErr = retErr
		s.stateMu.Unlock()
		if retErr != nil || s.hasStartFailures() {
			s.requestConvergence()
		}
	}()
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	if s.stopped {
		return nil
	}
	// Rich source exposition is fail-closed. A corrupt persisted document, a
	// cancelled store read, or a physical-budget failure must not leave a
	// previously enabled high-cardinality surface running while convergence
	// retries. It is re-enabled below only after the whole desired config and
	// its resource policy have applied successfully.
	s.met.SetPrometheusSourceDetails(false)
	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}
	compiledFilters, err := validatePersistedConfig(cfg)
	if err != nil {
		return err
	}
	// Resource budgets are desired state too. Apply them before constructing
	// runtimes so startup and hot config imports use the same convergence seam,
	// and return failures so the bounded convergence loop retries them.
	if err := s.st.ConfigureResources(ctx, cfg.EffectiveResources()); err != nil {
		return fmt.Errorf("configure resource budgets: %w", err)
	}
	// The rich per-PGN Prometheus surface is process-wide, but its desired state
	// lives with the rest of the persisted configuration. Reach this enablement
	// only after validation and resource application succeed.
	s.met.SetPrometheusSourceDetails(cfg.PrometheusSourceDetailsEnabled())

	desiredSources := map[string]model.Source{}
	for _, v := range cfg.Sources {
		if v.Enabled {
			desiredSources[v.ID] = v
		}
	}
	desiredSinks := map[string]model.Sink{}
	for _, v := range cfg.Sinks {
		if v.Enabled {
			desiredSinks[v.ID] = v
		}
	}
	desiredConnectors := map[string]model.Connector{}
	for _, v := range cfg.Connectors {
		if v.Enabled {
			desiredConnectors[v.ID] = v
		}
	}
	configuredSources := map[string]bool{}
	for _, v := range cfg.Sources {
		configuredSources[v.ID] = true
	}
	configuredSinks := map[string]bool{}
	for _, v := range cfg.Sinks {
		configuredSinks[v.ID] = true
	}
	// configuredConnectors covers every connector in config regardless of
	// Enabled — used to distinguish "disabled" (queue survives) from
	// "deleted" (queue is purged).
	configuredConnectors := map[string]bool{}
	for _, v := range cfg.Connectors {
		configuredConnectors[v.ID] = true
	}
	// Re-arm the purge sweep only when the configured id set shrank — an id
	// present last Reconcile is gone now. Within-process deletions only ever
	// take effect through Reconcile, so shrink detection (plus the
	// first-Reconcile arm set in New for deletions that happened while the
	// process was down) catches every deletion without running the sweep on
	// the steady-state path.
	// Collect ids that were configured last Reconcile but are absent now. These
	// components were actually deleted, rather than merely disabled. Their
	// stats are dropped below after the stop phase.
	var deletedSources, deletedSinks, deletedConnectors []string
	for id := range s.prevConfiguredSources {
		if !configuredSources[id] {
			deletedSources = append(deletedSources, id)
		}
	}
	for id := range s.prevConfiguredSinks {
		if !configuredSinks[id] {
			deletedSinks = append(deletedSinks, id)
		}
	}
	for id := range s.prevConfigured {
		if !configuredConnectors[id] {
			s.needsPurgeSweep = true
			deletedConnectors = append(deletedConnectors, id)
		}
	}
	s.prevConfiguredSources = configuredSources
	s.prevConfiguredSinks = configuredSinks
	s.prevConfigured = configuredConnectors

	// --- Compute the stop set under stateMu, mutating the maps there, but
	// defer the actual (potentially slow) Stop() calls until after the lock
	// is released. A connector also restarts when its source's or sink's
	// hash changed: its runtime references the old component instance
	// directly, so it must be rebuilt against the new one even though the
	// connector's own config is unchanged.
	var stopConnectors, stopSinks, stopSources []stopItem
	s.stateMu.Lock()
	s.errored = nil
	for id, rc := range s.connectors {
		want, ok := desiredConnectors[id]
		if ok && hashOf(want) == rc.hash &&
			s.sources[want.SourceID] != nil && hashOf(desiredSources[want.SourceID]) == s.sources[want.SourceID].hash &&
			s.sinks[want.SinkID] != nil && hashOf(desiredSinks[want.SinkID]) == s.sinks[want.SinkID].hash {
			continue // unchanged, endpoints unchanged
		}
		stopConnectors = append(stopConnectors, stopItem{kind: "connector", id: id, stop: rc.c.Stop, notDesired: !ok})
		delete(s.connectors, id)
	}
	for id, rs := range s.sinks {
		want, ok := desiredSinks[id]
		if ok && hashOf(want) == rs.hash {
			continue
		}
		stopSinks = append(stopSinks, stopItem{kind: "sink", id: id, stop: rs.rt.Stop, notDesired: !ok})
		delete(s.sinks, id)
	}
	for id, rs := range s.sources {
		want, ok := desiredSources[id]
		if ok && hashOf(want) == rs.hash {
			continue
		}
		stopSources = append(stopSources, stopItem{kind: "source", id: id, stop: rs.rt.Stop, notDesired: !ok})
		delete(s.sources, id)
	}
	s.stateMu.Unlock()

	// --- Stop phase: connectors first, then sinks, then sources. Runs
	// unlocked so Statuses() never blocks behind it. ---
	for _, item := range stopConnectors {
		s.log.Info("stopping connector", "id", item.id)
		if testStopDelay > 0 {
			time.Sleep(testStopDelay)
		}
		item.stop()
		if item.notDesired {
			s.met.RemoveComponent(item.kind, item.id)
		}
	}
	for _, item := range stopSinks {
		s.log.Info("stopping sink", "id", item.id)
		item.stop()
		if item.notDesired {
			s.met.RemoveComponent(item.kind, item.id)
		}
	}
	for _, item := range stopSources {
		s.log.Info("stopping source", "id", item.id)
		item.stop()
		if item.notDesired {
			s.met.RemoveComponent(item.kind, item.id)
		}
	}

	// --- Drop stats/gauge entries for every connector actually deleted from
	// config this Reconcile, regardless of whether it ever had durable
	// storage. Its prune loop calls stats.Registry.SetQueue and
	// metrics.Set.SetQueueDepth on a timer purely from existing — those
	// registry/gauge entries would otherwise linger forever (GET
	// /api/v1/metrics would keep listing it, and the queue_depth gauge would
	// keep exporting its last value) until process restart. This must run
	// after the stop phase above: connector Stop() is synchronous (it waits
	// on the connector's internal WaitGroup), so every deleted connector's
	// pipeline has already fully stopped by this point and cannot race a
	// concurrent Record/SetQueue call that would resurrect the entry (see
	// stats.Registry.Remove's doc comment).
	for _, id := range deletedConnectors {
		s.reg.Remove(id)
		s.met.RemoveConnector(id)
	}
	for _, id := range deletedSources {
		s.reg.RemoveSource(id)
	}
	for _, id := range deletedSinks {
		s.reg.RemoveSink(id)
	}

	// --- Purge sweep: connectors whose storage exists but who are absent
	// from config entirely (deleted, not merely disabled) lose their durable
	// queue + checkpoint. This also catches connectors that were already
	// stopped (e.g. disabled) in an earlier reconcile and only now got
	// deleted, so their storage was never touched by the stop phase above.
	//
	// Gated behind needsPurgeSweep (first Reconcile + configured-set
	// shrinkage, see above) because a sweep is only useful when storage can
	// have become orphaned. KnownConnectorIDs reads the compact aggregate and
	// checkpoint tables rather than retained envelopes. It stays after the
	// stop phase so a just-deleted connector's final flush/ack lands before
	// its rows are purged. ---
	if s.needsPurgeSweep {
		if known, err := s.st.KnownConnectorIDs(ctx); err != nil {
			s.log.Error("list known connector ids failed", "err", err)
		} else {
			swept := true
			for _, id := range known {
				if configuredConnectors[id] {
					continue
				}
				q := queue.NewSQLite(s.st, id, model.BufferLimits{})
				if err := q.Purge(ctx); err != nil {
					s.log.Error("purge deleted connector queue failed", "id", id, "err", err)
					swept = false
					continue
				}
				s.met.RemoveConnector(id)
				s.met.RemoveComponent("connector", id)
				s.reg.Remove(id)
			}
			if swept {
				s.needsPurgeSweep = false
			}
		}
	}

	// --- Start phase: sources, sinks, connectors. Components are handed
	// s.runCtx (the Supervisor's own background context), not ctx — see the
	// Supervisor doc comment for why. Constructors run outside stateMu; only
	// the map insert itself is locked. ---
	for id, want := range desiredSources {
		s.stateMu.Lock()
		_, running := s.sources[id]
		s.stateMu.Unlock()
		if running {
			continue
		}
		if s.bus == nil && needsBus(string(want.Type)) {
			s.fail("source", id, fmt.Errorf("source %q: type %q requires a CAN bus manager but none is configured", id, want.Type))
			continue
		}
		rt, err := source.New(s.runCtx, want, s.bus, s.log, s.met, s.reg)
		if err != nil {
			s.fail("source", id, err)
			continue
		}
		s.log.Info("started source", "id", id)
		s.met.SetComponentState("source", id, 2)
		s.stateMu.Lock()
		s.sources[id] = &runningSource{hash: hashOf(want), rt: rt}
		s.stateMu.Unlock()
	}
	for id, want := range desiredSinks {
		s.stateMu.Lock()
		_, running := s.sinks[id]
		s.stateMu.Unlock()
		if running {
			continue
		}
		if s.bus == nil && needsBus(string(want.Type)) {
			s.fail("sink", id, fmt.Errorf("sink %q: type %q requires a CAN bus manager but none is configured", id, want.Type))
			continue
		}
		rt, err := sink.New(s.runCtx, want, s.bus, s.ds, s.log, s.met)
		if err != nil {
			s.fail("sink", id, err)
			continue
		}
		s.log.Info("started sink", "id", id)
		s.met.SetComponentState("sink", id, 2)
		s.stateMu.Lock()
		s.sinks[id] = &runningSink{hash: hashOf(want), rt: rt}
		s.stateMu.Unlock()
	}
	for id, want := range desiredConnectors {
		s.stateMu.Lock()
		_, running := s.connectors[id]
		src := s.sources[want.SourceID]
		snk := s.sinks[want.SinkID]
		s.stateMu.Unlock()
		if running {
			continue
		}
		if src == nil || snk == nil {
			s.fail("connector", id, unavailableErr(want, src == nil, snk == nil))
			continue
		}
		q := queue.NewSQLite(s.st, id, want.Buffer)
		c := connector.New(want, src.rt, snk.rt, q, compiledFilters[id], s.log, s.met, s.reg)
		c.Start(s.runCtx)
		s.log.Info("started connector", "id", id)
		s.met.SetComponentState("connector", id, 2)
		s.stateMu.Lock()
		s.connectors[id] = &runningConnector{hash: hashOf(want), c: c}
		s.stateMu.Unlock()
	}
	return nil
}

// validatePersistedConfig repeats the same whole-document guarantees enforced
// by config.Service before a write. Store rows can predate current validation
// or be edited outside Beacon, so Reconcile must reject the entire document
// before starting only a valid subset. Returning compiled chains also avoids
// compiling enabled connectors a second time during construction.
func validatePersistedConfig(cfg model.Config) (map[string]*filter.Chain, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate persisted config: %w", err)
	}
	compiled := make(map[string]*filter.Chain, len(cfg.Connectors))
	for _, connector := range cfg.Connectors {
		chain, err := filter.Compile(connector.Filters)
		if err != nil {
			return nil, fmt.Errorf("validate persisted connector %q filters: %w", connector.ID, err)
		}
		compiled[connector.ID] = chain
	}
	return compiled, nil
}

// needsBus reports whether a source/sink type requires a *bus.Manager.
// model.SourceType and model.SinkType use identical literal values for the
// CAN variants ("socketcan", "usbcan"), so one check serves both. The
// tcp_gateway sink also acquires from the manager (a full claiming client
// over TCP); the tcp/udp *sources* deliberately do not — they are passive,
// read-only listeners via n2k.Receive and never claim an address.
func needsBus(t string) bool {
	return t == string(model.SourceSocketCAN) || t == string(model.SourceUSBCAN) ||
		t == string(model.SinkTCPGateway)
}

// unavailableErr describes why a connector's endpoints aren't running,
// naming whichever of source/sink (or both) is missing.
func unavailableErr(c model.Connector, sourceMissing, sinkMissing bool) error {
	switch {
	case sourceMissing && sinkMissing:
		return fmt.Errorf("source %q and sink %q are not running", c.SourceID, c.SinkID)
	case sourceMissing:
		return fmt.Errorf("source %q is not running", c.SourceID)
	default:
		return fmt.Errorf("sink %q is not running", c.SinkID)
	}
}

func (s *Supervisor) fail(kind, id string, err error) {
	s.log.Error("component failed to start", "kind", kind, "id", id, "err", err)
	s.stateMu.Lock()
	s.errored = append(s.errored, Status{Kind: kind, ID: id, State: "error", Err: err.Error()})
	s.stateMu.Unlock()
	s.met.SetComponentState(kind, id, 0)
}

// Statuses reports the current state of every running or errored component,
// for /health. It snapshots runtime references and stored errors under stateMu,
// then releases the lock before invoking component State methods. State may
// need an implementation-local lock (for example a file sink can be flushing),
// and that work must not prevent Reconcile or Stop from updating the supervisor
// maps. Runtime implementations support State racing with Stop.
func (s *Supervisor) Statuses() []Status {
	type sourceRef struct {
		id string
		rt source.Runtime
	}
	type sinkRef struct {
		id string
		rt sink.Runtime
	}
	type connectorRef struct {
		id string
		rt *connector.Connector
	}

	s.stateMu.Lock()
	sources := make([]sourceRef, 0, len(s.sources))
	for id, running := range s.sources {
		sources = append(sources, sourceRef{id: id, rt: running.rt})
	}
	sinks := make([]sinkRef, 0, len(s.sinks))
	for id, running := range s.sinks {
		sinks = append(sinks, sinkRef{id: id, rt: running.rt})
	}
	connectors := make([]connectorRef, 0, len(s.connectors))
	for id, running := range s.connectors {
		connectors = append(connectors, connectorRef{id: id, rt: running.c})
	}
	errored := append([]Status(nil), s.errored...)
	reconcileErr := s.reconcileErr
	s.stateMu.Unlock()

	out := make([]Status, 0, len(sources)+len(sinks)+len(connectors)+len(errored)+1)
	for _, ref := range sources {
		state, err := ref.rt.State()
		st := Status{Kind: "source", ID: ref.id, State: state}
		if err != nil {
			st.Err = err.Error()
		}
		out = append(out, st)
	}
	for _, ref := range sinks {
		state, err := ref.rt.State()
		st := Status{Kind: "sink", ID: ref.id, State: state}
		if err != nil {
			st.Err = err.Error()
		}
		out = append(out, st)
	}
	for _, ref := range connectors {
		state, err := ref.rt.State()
		st := Status{Kind: "connector", ID: ref.id, State: state}
		if err != nil {
			st.Err = err.Error()
		}
		out = append(out, st)
	}
	out = append(out, errored...)
	if reconcileErr != nil {
		out = append(out, Status{
			Kind: "system", ID: "desired_state", State: "error", Err: reconcileErr.Error(),
		})
	}
	return out
}

// BusDevices returns every NMEA-2000 device currently observed across the
// running CAN endpoints (empty when no CAN bus is configured or connected).
// n2k v0.3.0's client tracks these automatically from address-claim traffic,
// so this is a free read once a bus client is up.
func (s *Supervisor) BusDevices() []bus.DeviceInfo {
	if s.bus == nil {
		return nil
	}
	return s.bus.Devices()
}

// BusStatuses returns bounded queue, subscription, lifecycle, and address-
// claim state for every running shared n2k client.
func (s *Supervisor) BusStatuses() []bus.EndpointStatus {
	if s.bus == nil {
		return []bus.EndpointStatus{}
	}
	return s.bus.Statuses()
}

// Stop stops every running component (connectors, then sinks, then sources)
// and cancels the Supervisor's background context. Safe to call more than
// once. After Stop, Reconcile becomes a permanent no-op (see the Supervisor
// doc comment). Like Reconcile, the map mutation is done under stateMu but
// the actual (potentially slow) Stop() calls run outside stateMu so Statuses()
// never blocks behind shutdown either.
func (s *Supervisor) Stop() {
	// Cancel first so an in-flight convergence pass and every component's
	// lifecycle operation receive cancellation while Stop waits for the
	// reconcile serialization lock.
	s.runCancel()
	s.reconcileMu.Lock()
	if s.stopped {
		s.reconcileMu.Unlock()
		s.convergeWG.Wait()
		return
	}
	s.stopped = true

	s.stateMu.Lock()
	connectors := s.connectors
	sinks := s.sinks
	sources := s.sources
	s.connectors = map[string]*runningConnector{}
	s.sinks = map[string]*runningSink{}
	s.sources = map[string]*runningSource{}
	s.stateMu.Unlock()

	for _, rc := range connectors {
		rc.c.Stop()
	}
	for _, rs := range sinks {
		rs.rt.Stop()
	}
	for _, rs := range sources {
		rs.rt.Stop()
	}
	s.reconcileMu.Unlock()
	s.convergeWG.Wait()
}
