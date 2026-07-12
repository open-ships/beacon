// Package supervisor reconciles desired configuration (the store) against
// running source/sink/connector components.
package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/connector"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/source"
	"github.com/open-ships/beacon/internal/store"
)

type Status struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	State string `json:"state"` // "up" | "degraded" | "error"
	Err   string `json:"err,omitempty"`
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
// operation — including the store read — runs under s.mu, so overlapping
// Reconcile calls fully serialize rather than racing a stale read against a
// newer one. Component constructors (source.New, sink.New, connector.Start)
// are expected to connect/dial asynchronously and return quickly; Reconcile
// holding s.mu for their duration would otherwise stall Statuses() (/health)
// for as long as a slow constructor blocks.
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

	runCtx    context.Context
	runCancel context.CancelFunc

	mu         sync.Mutex
	stopped    bool
	sources    map[string]*runningSource
	sinks      map[string]*runningSink
	connectors map[string]*runningConnector
	errored    []Status
}

func New(st *store.Store, busMgr *bus.Manager, ds *sink.DataServer, log *slog.Logger, met *metrics.Set) *Supervisor {
	runCtx, cancel := context.WithCancel(context.Background())
	return &Supervisor{st: st, bus: busMgr, ds: ds, log: log, met: met,
		runCtx: runCtx, runCancel: cancel,
		sources:    map[string]*runningSource{},
		sinks:      map[string]*runningSink{},
		connectors: map[string]*runningConnector{},
	}
}

func hashOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// Reconcile diffs desired config (the store) against running components and
// converges: stop-then-start whatever changed. It returns an error only when
// the store read itself fails; individual component failures are recorded as
// error statuses (see Statuses) and never abort or crash the reconcile.
func (s *Supervisor) Reconcile(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil
	}
	cfg, err := s.st.LoadConfig(ctx)
	if err != nil {
		return err
	}
	s.errored = nil

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

	// --- Stop phase: connectors first, then sinks, then sources. ---
	// A connector also restarts when its source's or sink's hash changed:
	// its runtime references the old component instance directly, so it
	// must be rebuilt against the new one even though the connector's own
	// config is unchanged.
	for id, rc := range s.connectors {
		want, ok := desiredConnectors[id]
		if ok && hashOf(want) == rc.hash &&
			s.sources[want.SourceID] != nil && hashOf(desiredSources[want.SourceID]) == s.sources[want.SourceID].hash &&
			s.sinks[want.SinkID] != nil && hashOf(desiredSinks[want.SinkID]) == s.sinks[want.SinkID].hash {
			continue // unchanged, endpoints unchanged
		}
		s.log.Info("stopping connector", "id", id)
		rc.c.Stop()
		delete(s.connectors, id)
	}
	for id, rs := range s.sinks {
		if want, ok := desiredSinks[id]; ok && hashOf(want) == rs.hash {
			continue
		}
		s.log.Info("stopping sink", "id", id)
		rs.rt.Stop()
		delete(s.sinks, id)
	}
	for id, rs := range s.sources {
		if want, ok := desiredSources[id]; ok && hashOf(want) == rs.hash {
			continue
		}
		s.log.Info("stopping source", "id", id)
		rs.rt.Stop()
		delete(s.sources, id)
	}

	// --- Start phase: sources, sinks, connectors. Components are handed
	// s.runCtx (the Supervisor's own background context), not ctx — see the
	// Supervisor doc comment for why.
	for id, want := range desiredSources {
		if _, running := s.sources[id]; running {
			continue
		}
		if s.bus == nil && needsBus(string(want.Type)) {
			s.fail("source", id, fmt.Errorf("source %q: type %q requires a CAN bus manager but none is configured", id, want.Type))
			continue
		}
		rt, err := source.New(s.runCtx, want, s.bus, s.log, s.met)
		if err != nil {
			s.fail("source", id, err)
			continue
		}
		s.log.Info("started source", "id", id)
		s.sources[id] = &runningSource{hash: hashOf(want), rt: rt}
	}
	for id, want := range desiredSinks {
		if _, running := s.sinks[id]; running {
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
		s.sinks[id] = &runningSink{hash: hashOf(want), rt: rt}
	}
	for id, want := range desiredConnectors {
		if _, running := s.connectors[id]; running {
			continue
		}
		src := s.sources[want.SourceID]
		snk := s.sinks[want.SinkID]
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
		c := connector.New(want, src.rt, snk.rt, q, chain, s.log, s.met)
		c.Start(s.runCtx)
		s.log.Info("started connector", "id", id)
		s.connectors[id] = &runningConnector{hash: hashOf(want), c: c}
	}
	return nil
}

// needsBus reports whether a source/sink type requires a *bus.Manager.
// model.SourceType and model.SinkType use identical literal values for the
// CAN variants ("socketcan", "usbcan"), so one check serves both.
func needsBus(t string) bool {
	return t == string(model.SourceSocketCAN) || t == string(model.SourceUSBCAN)
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
	s.errored = append(s.errored, Status{Kind: kind, ID: id, State: "error", Err: err.Error()})
	s.met.SetComponentState(kind, id, 0)
}

// Statuses reports the current state of every running or errored component,
// for /health.
func (s *Supervisor) Statuses() []Status {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	for _, st := range s.errored {
		out = append(out, st)
	}
	return out
}

// Stop stops every running component (connectors, then sinks, then sources)
// and cancels the Supervisor's background context. Safe to call more than
// once. After Stop, Reconcile becomes a permanent no-op (see the Supervisor
// doc comment).
func (s *Supervisor) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	for id, rc := range s.connectors {
		rc.c.Stop()
		delete(s.connectors, id)
	}
	for id, rs := range s.sinks {
		rs.rt.Stop()
		delete(s.sinks, id)
	}
	for id, rs := range s.sources {
		rs.rt.Stop()
		delete(s.sources, id)
	}
	s.runCancel()
}
