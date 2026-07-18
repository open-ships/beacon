// Package supervisor reconciles desired configuration (the store) against
// running source/sink/connector components.
package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
var testStopDelay time.Duration

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
// quickly; Reconcile holding a lock for their duration would otherwise stall
// Statuses() (/health) for as long as a slow constructor blocks.
//
// Lock discipline: reconcileMu serializes the bodies of Reconcile and Stop
// (so they never interleave), but it is NOT held across component
// constructors or Stop() calls — those run unlocked. The sources/sinks/
// connectors/errored maps are guarded separately by stateMu, held only
// briefly for the map reads/writes themselves. This means Statuses() (which
// only needs
// stateMu) never blocks behind a slow Reconcile teardown or a slow Stop().
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

	runCtx    context.Context
	runCancel context.CancelFunc

	// reconcileMu serializes Reconcile and Stop bodies. It also guards
	// `stopped`, `needsPurgeSweep`, and `prevConfigured`, all of which are
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
	// prevConfigured is the configured-connector id set (enabled or not)
	// seen by the previous Reconcile, used to detect shrinkage.
	prevConfigured map[string]bool

	// stateMu guards sources/sinks/connectors/errored only. Held briefly for
	// map reads/writes — never across a component constructor or Stop()
	// call — so Statuses() never blocks behind a slow teardown.
	stateMu    sync.Mutex
	sources    map[string]*runningSource
	sinks      map[string]*runningSink
	connectors map[string]*runningConnector
	errored    []Status
}

func New(st *store.Store, busMgr *bus.Manager, ds *sink.DataServer, log *slog.Logger, met *metrics.Set, reg *stats.Registry) *Supervisor {
	runCtx, cancel := context.WithCancel(context.Background())
	return &Supervisor{st: st, bus: busMgr, ds: ds, log: log, met: met, reg: reg,
		runCtx: runCtx, runCancel: cancel,
		needsPurgeSweep: true,
		sources:         map[string]*runningSource{},
		sinks:           map[string]*runningSink{},
		connectors:      map[string]*runningConnector{},
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
// converges: stop-then-start whatever changed. It returns an error only when
// the store read itself fails; individual component failures are recorded as
// error statuses (see Statuses) and never abort or crash the reconcile.
func (s *Supervisor) Reconcile(ctx context.Context) error {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	if s.stopped {
		return nil
	}
	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}

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
	// deletedConnectors collects every id that was configured last Reconcile
	// but is absent from config now — i.e. actually deleted, not merely
	// disabled (configuredConnectors, like prevConfigured, includes disabled
	// entries). Used below, after the stop phase, to drop their stats/gauge
	// entries unconditionally.
	var deletedConnectors []string
	for id := range s.prevConfigured {
		if !configuredConnectors[id] {
			s.needsPurgeSweep = true
			deletedConnectors = append(deletedConnectors, id)
		}
	}
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
	// storage. An idle connector (never delivered a message, so no
	// queue/checkpoint rows) is invisible to the KnownConnectorIDs purge
	// sweep below, yet its prune loop calls stats.Registry.SetQueue and
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

	// --- Purge sweep: connectors whose storage exists but who are absent
	// from config entirely (deleted, not merely disabled) lose their durable
	// queue + checkpoint. This also catches connectors that were already
	// stopped (e.g. disabled) in an earlier reconcile and only now got
	// deleted, so their storage was never touched by the stop phase above.
	//
	// Gated behind needsPurgeSweep (first Reconcile + configured-set
	// shrinkage, see above) because KnownConnectorIDs is an unfiltered scan
	// of the whole queue table plus a UNION temp b-tree, and the app shares
	// one SQLite connection (store.Open sets SetMaxOpenConns(1)): running it
	// on every Reconcile would stall every connector's Append/Read/Ack
	// system-wide for the duration of the scan. It must stay after the stop
	// phase so a just-deleted connector's final flush/ack lands before its
	// rows are purged. ---
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
				if err := q.Purge(context.Background()); err != nil {
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
		chain, err := filter.Compile(want.Filters)
		if err != nil {
			s.fail("connector", id, err)
			continue
		}
		q := queue.NewSQLite(s.st, id, want.Buffer)
		c := connector.New(want, src.rt, snk.rt, q, chain, s.log, s.met, s.reg)
		c.Start(s.runCtx)
		s.log.Info("started connector", "id", id)
		s.met.SetComponentState("connector", id, 2)
		s.stateMu.Lock()
		s.connectors[id] = &runningConnector{hash: hashOf(want), c: c}
		s.stateMu.Unlock()
	}
	return nil
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
// for /health. It only ever takes stateMu — never reconcileMu — so it
// returns promptly even while a Reconcile (or Stop) is mid-teardown of a
// slow component.
func (s *Supervisor) Statuses() []Status {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	var out []Status
	for id, r := range s.sources {
		state, err := r.rt.State()
		st := Status{Kind: "source", ID: id, State: state}
		if err != nil {
			st.Err = err.Error()
		}
		out = append(out, st)
	}
	for id, r := range s.sinks {
		state, err := r.rt.State()
		st := Status{Kind: "sink", ID: id, State: state}
		if err != nil {
			st.Err = err.Error()
		}
		out = append(out, st)
	}
	for id := range s.connectors {
		out = append(out, Status{Kind: "connector", ID: id, State: "up"})
	}
	out = append(out, s.errored...)
	return out
}

// BusDevices returns every NMEA-2000 device currently observed across the
// running CAN endpoints (empty when no CAN bus is configured or connected).
// n2k v0.2.0's client tracks these automatically from address-claim traffic,
// so this is a free read once a bus client is up.
func (s *Supervisor) BusDevices() []bus.DeviceInfo {
	if s.bus == nil {
		return nil
	}
	return s.bus.Devices()
}

// Stop stops every running component (connectors, then sinks, then sources)
// and cancels the Supervisor's background context. Safe to call more than
// once. After Stop, Reconcile becomes a permanent no-op (see the Supervisor
// doc comment). Like Reconcile, the map mutation is done under stateMu but
// the actual (potentially slow) Stop() calls run unlocked so Statuses()
// never blocks behind shutdown either.
func (s *Supervisor) Stop() {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	if s.stopped {
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
	s.runCancel()
}
