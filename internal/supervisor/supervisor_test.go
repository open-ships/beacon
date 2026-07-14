package supervisor

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
)

func setup(t *testing.T) (*store.Store, *Supervisor, *sink.DataServer) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ds := sink.NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	sup := New(st, nil, ds, slog.Default(), nil, nil)
	t.Cleanup(sup.Stop)
	return st, sup, ds
}

func find(statuses []Status, kind, id string) *Status {
	for i := range statuses {
		if statuses[i].Kind == kind && statuses[i].ID == id {
			return &statuses[i]
		}
	}
	return nil
}

func baseConfig() model.Config {
	return model.Config{
		Sources: []model.Source{{ID: "up", Name: "Upstream", Type: model.SourceHTTPWS,
			Enabled: true, URL: "ws://127.0.0.1:1/nowhere"}}, // degraded but running
		Sinks: []model.Sink{{ID: "out", Name: "Out", Type: model.SinkHTTPSSE,
			Enabled: true, Path: "/out"}},
		Connectors: []model.Connector{{ID: "link", Name: "Link", SourceID: "up",
			SinkID: "out", Enabled: true, Buffer: model.BufferLimits{MaxMessages: 10}}},
	}
}

func TestReconcileStartsAndStops(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	if err := st.ReplaceConfig(ctx, baseConfig()); err != nil {
		t.Fatal(err)
	}
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	sts := sup.Statuses()
	if find(sts, "source", "up") == nil || find(sts, "sink", "out") == nil || find(sts, "connector", "link") == nil {
		t.Fatalf("missing components: %+v", sts)
	}

	// Remove the connector; source+sink stay.
	cfg := baseConfig()
	cfg.Connectors = nil
	_ = st.ReplaceConfig(ctx, cfg)
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	sts = sup.Statuses()
	if find(sts, "connector", "link") != nil {
		t.Fatal("removed connector still running")
	}
	if find(sts, "source", "up") == nil {
		t.Fatal("source should still be running")
	}
}

func TestReconcileRestartsOnChange(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)

	// Change the sink path; supervisor must swap the route.
	cfg := baseConfig()
	cfg.Sinks[0].Path = "/renamed"
	_ = st.ReplaceConfig(ctx, cfg)
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if s := find(sup.Statuses(), "sink", "out"); s == nil || s.State == "error" {
		t.Fatalf("sink after change: %+v", s)
	}
}

func TestDisabledMeansStopped(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	cfg := baseConfig()
	cfg.Connectors[0].Enabled = false
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if find(sup.Statuses(), "connector", "link") != nil {
		t.Fatal("disabled connector is running")
	}
}

func TestBrokenComponentIsErrorNotCrash(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	cfg := baseConfig()
	// TCP sink on an unbindable address fails to construct.
	cfg.Sinks = append(cfg.Sinks, model.Sink{ID: "bad", Name: "Bad", Type: model.SinkTCP,
		Enabled: true, Address: "256.256.256.256:1"})
	cfg.Connectors = append(cfg.Connectors, model.Connector{ID: "cbad", Name: "CBad",
		SourceID: "up", SinkID: "bad", Enabled: true, Buffer: model.BufferLimits{MaxMessages: 5}})
	_ = st.ReplaceConfig(ctx, cfg)
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	sts := sup.Statuses()
	if s := find(sts, "sink", "bad"); s == nil || s.State != "error" {
		t.Fatalf("bad sink status: %+v", s)
	}
	if s := find(sts, "connector", "cbad"); s == nil || s.State != "error" {
		t.Fatalf("connector on bad sink status: %+v", s)
	}
	// healthy components unaffected
	if s := find(sts, "connector", "link"); s == nil || s.State == "error" {
		t.Fatalf("healthy connector harmed: %+v", s)
	}
}

// A hand-edited DB can hold a connector filter that fails CEL compilation
// (store.ReplaceConfig does not validate; only the HTTP config API would).
// Reconcile must record an error status, not crash.
func TestBadFilterIsErrorNotCrash(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	cfg := baseConfig()
	cfg.Connectors[0].Filters = []string{"not a valid cel expression &&&"}
	_ = st.ReplaceConfig(ctx, cfg)
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	s := find(sup.Statuses(), "connector", "link")
	if s == nil || s.State != "error" {
		t.Fatalf("connector with bad filter status: %+v", s)
	}
}

// A CAN source configured against a Supervisor built with no *bus.Manager
// (a supported mode: HTTP/TCP-only deployments, as in setup() here) must be
// an error status, not a nil-pointer panic from bus.Manager.Acquire.
func TestNilBusManagerIsErrorNotCrash(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	cfg := baseConfig()
	cfg.Sources = append(cfg.Sources, model.Source{ID: "can0", Name: "CAN0", Type: model.SourceSocketCAN,
		Enabled: true, Interface: "vcan0"})
	_ = st.ReplaceConfig(ctx, cfg)
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	s := find(sup.Statuses(), "source", "can0")
	if s == nil || s.State != "error" {
		t.Fatalf("can0 source status with nil bus manager: %+v", s)
	}
	// healthy components unaffected
	if s := find(sup.Statuses(), "connector", "link"); s == nil || s.State == "error" {
		t.Fatalf("healthy connector harmed: %+v", s)
	}
}

// Reconcile called after Stop must be a true no-op: nothing gets started
// against the already-cancelled background context (which would otherwise
// look "running" in Statuses() while never actually functioning).
func TestReconcileAfterStopIsNoop(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	sup.Stop()
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if sts := sup.Statuses(); len(sts) != 0 {
		t.Fatalf("reconcile after stop started components: %+v", sts)
	}
}

func TestDeletedConnectorQueueIsPurged(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)

	// Seed the connector's queue directly, then delete the connector.
	q := queue.NewSQLite(st, "link", model.BufferLimits{MaxMessages: 10})
	_ = q.Append(ctx, []*msg.Envelope{{PGN: 1, Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}})

	cfg := baseConfig()
	cfg.Connectors = nil
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)

	stats, _ := q.Stats(ctx)
	if stats.Depth != 0 {
		t.Fatalf("deleted connector queue depth = %d, want 0 (purged)", stats.Depth)
	}
}

// A connector's live stats must not linger in the registry once it is
// deleted from config entirely (the purge path), mirroring the queue purge
// above: otherwise the API/UI would keep reporting rates for a connector
// that no longer exists.
func TestDeletedConnectorStatsAreRemoved(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ds := sink.NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	reg := stats.NewRegistry()
	sup := New(st, nil, ds, slog.Default(), nil, reg)
	t.Cleanup(sup.Stop)

	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)

	// The purge sweep only considers connectors with storage (queue rows or
	// a checkpoint) — seed one directly, as TestDeletedConnectorQueueIsPurged
	// does, so "link" is "known" to the sweep.
	q := queue.NewSQLite(st, "link", model.BufferLimits{MaxMessages: 10})
	_ = q.Append(ctx, []*msg.Envelope{{PGN: 1, Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}})

	reg.Record("link", 1, 10)
	if _, ok := reg.Snapshot("link"); !ok {
		t.Fatal("precondition: connector stats not recorded")
	}

	cfg := baseConfig()
	cfg.Connectors = nil
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)

	if _, ok := reg.Snapshot("link"); ok {
		t.Fatal("deleted connector stats still present in registry")
	}
}

// An idle connector — one that never delivered a message and so has no
// queue/checkpoint rows at all — is invisible to the KnownConnectorIDs purge
// sweep (it only unions the queue and checkpoints tables). Its prune loop
// still calls stats.Registry.SetQueue and metrics.Set.SetQueueDepth every
// tick purely from existing, so both registry entries must be dropped
// unconditionally on deletion, not just when the sweep happens to find
// storage. Deliberately does NOT seed any queue rows, unlike
// TestDeletedConnectorStatsAreRemoved above (which exercises the sweep
// path) — this test must fail against the current code.
func TestDeletedIdleConnectorStatsAndGaugeAreRemoved(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ds := sink.NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	met, handler, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	reg := stats.NewRegistry()
	sup := New(st, nil, ds, slog.Default(), met, reg)
	t.Cleanup(sup.Stop)

	gaugePresent := func(connector string) bool {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		body, _ := io.ReadAll(rec.Body)
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "beacon_connector_queue_depth") &&
				strings.Contains(line, `connector="`+connector+`"`) {
				return true
			}
		}
		return false
	}

	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)

	// No traffic at all: record the queue depth directly, exactly as the
	// connector's own prune loop would (internal/connector/connector.go
	// prune()), without ever appending a message (no queue/checkpoint rows).
	met.SetQueueDepth("link", 3, 30)
	reg.SetQueue("link", 3, 30)
	if _, ok := reg.Snapshot("link"); !ok {
		t.Fatal("precondition: connector stats not recorded")
	}
	if !gaugePresent("link") {
		t.Fatal("precondition: queue depth gauge not recorded")
	}

	cfg := baseConfig()
	cfg.Connectors = nil
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)

	if _, ok := reg.Snapshot("link"); ok {
		t.Fatal("deleted idle connector stats still present in registry")
	}
	if gaugePresent("link") {
		t.Fatal("deleted idle connector queue depth gauge still exported")
	}
}

func TestDisabledConnectorQueueSurvives(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)
	q := queue.NewSQLite(st, "link", model.BufferLimits{MaxMessages: 10})
	_ = q.Append(ctx, []*msg.Envelope{{PGN: 1, Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}})

	cfg := baseConfig()
	cfg.Connectors[0].Enabled = false
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)

	stats, _ := q.Stats(ctx)
	if stats.Depth != 1 {
		t.Fatalf("disabled connector queue depth = %d, want 1 (kept)", stats.Depth)
	}
}

// TestStatusesDoesNotBlockDuringReconcile asserts the core lock-split
// guarantee: Statuses() (the /health path) must never wait behind a slow
// component teardown in Reconcile's stop phase.
//
// The brief's sketch (delete a connector whose Stop() may or may not
// actually take any measurable time, depending on what the source/sink are
// doing) is not deterministic enough to reliably fail pre-fix and pass
// post-fix: baseConfig's connector talks to an SSE (Broadcaster) sink with
// no retry/backoff path, so its real Stop() usually returns near-instantly
// regardless of locking. To get a deterministic signal we inject a
// test-only fixed delay (testStopDelay, zero in production) at the exact
// point Reconcile's stop phase is about to tear down a component — this
// simulates a slow-stopping component without depending on real network
// timing. A short preroll sleep lets the background Reconcile goroutine
// reach that injected delay before we start timing Statuses().
func TestStatusesDoesNotBlockDuringReconcile(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)

	cfg := baseConfig()
	cfg.Connectors = nil
	_ = st.ReplaceConfig(ctx, cfg)

	testStopDelay = 500 * time.Millisecond
	defer func() { testStopDelay = 0 }()

	done := make(chan struct{})
	go func() { _ = sup.Reconcile(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond) // let Reconcile reach the injected delay

	start := time.Now()
	_ = sup.Statuses()
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Statuses blocked %v during reconcile", elapsed)
	}
	<-done
}

func TestRecoveredComponentStateNotSticky(t *testing.T) {
	// A component that fails to start records error status; after the config
	// is fixed and reconciled, Statuses must no longer report the error.
	st, sup, _ := setup(t)
	ctx := context.Background()
	cfg := baseConfig()
	cfg.Sinks = append(cfg.Sinks, model.Sink{ID: "bad", Name: "Bad", Type: model.SinkTCP,
		Enabled: true, Address: "256.256.256.256:1"})
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if s := find(sup.Statuses(), "sink", "bad"); s == nil || s.State != "error" {
		t.Fatalf("precondition: bad sink should be error, got %+v", s)
	}

	cfg.Sinks[1].Address = "127.0.0.1:0" // now bindable
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if s := find(sup.Statuses(), "sink", "bad"); s == nil || s.State == "error" {
		t.Fatalf("recovered sink still error: %+v", s)
	}
}

// The KnownConnectorIDs purge sweep scans the whole queue table over the
// app's single shared SQLite connection, so it must stay off the steady-state
// Reconcile path: it runs on the first Reconcile after construction and again
// only when the configured-connector id set shrinks.
func TestPurgeSweepGating(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx) // first reconcile: sweep runs (and disarms) here

	// Seed storage for a connector id that was never configured.
	ghost := queue.NewSQLite(st, "ghost", model.BufferLimits{MaxMessages: 10})
	_ = ghost.Append(ctx, []*msg.Envelope{{PGN: 1, Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}})

	// Same config, no shrink: the sweep must not run again.
	_ = sup.Reconcile(ctx)
	if stats, _ := ghost.Stats(ctx); stats.Depth != 1 {
		t.Fatalf("ghost depth = %d after non-shrinking reconcile, want 1 (sweep gated off)", stats.Depth)
	}

	// Shrink the configured set: the sweep re-arms and purges every
	// unconfigured id, the deleted connector and the ghost alike.
	cfg := baseConfig()
	cfg.Connectors = nil
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if stats, _ := ghost.Stats(ctx); stats.Depth != 0 {
		t.Fatalf("ghost depth = %d after shrinking reconcile, want 0 (swept)", stats.Depth)
	}
	link := queue.NewSQLite(st, "link", model.BufferLimits{MaxMessages: 10})
	if stats, _ := link.Stats(ctx); stats.Depth != 0 {
		t.Fatalf("deleted connector depth = %d after shrinking reconcile, want 0", stats.Depth)
	}
}

// The first Reconcile after construction must sweep storage of connectors
// deleted from config while the process was down (they were never in any
// previous in-process configured set, so shrink detection alone would miss
// them).
func TestFirstReconcileSweepsOrphanedStorage(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	ghost := queue.NewSQLite(st, "ghost", model.BufferLimits{MaxMessages: 10})
	_ = ghost.Append(ctx, []*msg.Envelope{{PGN: 1, Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}})

	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)
	if stats, _ := ghost.Stats(ctx); stats.Depth != 0 {
		t.Fatalf("orphaned depth = %d after first reconcile, want 0 (swept)", stats.Depth)
	}
}

// TestComponentStateGaugeTransitions exercises the metric call sites with a
// real metrics.Set (every other supervisor test passes met=nil, which no-ops
// them) and asserts the beacon.component.state gauge through the exported
// Prometheus exposition: error start -> 0, recovered start -> 2, deleted ->
// series gone.
func TestComponentStateGaugeTransitions(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	met, handler, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	ds := sink.NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	sup := New(st, nil, ds, slog.Default(), met, nil)
	t.Cleanup(sup.Stop)

	// gaugeValue scrapes the Prometheus handler and returns the
	// beacon_component_state sample with the given kind/id labels.
	gaugeValue := func(kind, id string) (float64, bool) {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		body, _ := io.ReadAll(rec.Body)
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.HasPrefix(line, "beacon_component_state") ||
				!strings.Contains(line, `kind="`+kind+`"`) ||
				!strings.Contains(line, `id="`+id+`"`) {
				continue
			}
			fields := strings.Fields(line)
			v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
			if err != nil {
				t.Fatalf("unparseable sample %q: %v", line, err)
			}
			return v, true
		}
		return 0, false
	}

	ctx := context.Background()
	cfg := baseConfig()
	cfg.Sinks = append(cfg.Sinks, model.Sink{ID: "bad", Name: "Bad", Type: model.SinkTCP,
		Enabled: true, Address: "256.256.256.256:1"})
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if v, ok := gaugeValue("sink", "bad"); !ok || v != 0 {
		t.Fatalf("failed sink gauge = %v (present=%v), want 0", v, ok)
	}

	cfg.Sinks[1].Address = "127.0.0.1:0" // now bindable
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if v, ok := gaugeValue("sink", "bad"); !ok || v != 2 {
		t.Fatalf("recovered sink gauge = %v (present=%v), want 2", v, ok)
	}

	cfg.Sinks = cfg.Sinks[:1] // delete the sink entirely
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if v, ok := gaugeValue("sink", "bad"); ok {
		t.Fatalf("deleted sink gauge still exported (value %v), want series gone", v)
	}
}

// --- RollupHealth ---

// TestRollupHealth exercises the shared helper directly (both
// internal/app's top-level GET /health and internal/api's GET
// /api/v1/health delegate to it), so this is the single source of truth for
// the "any non-up state -> degraded" rule.
func TestRollupHealth(t *testing.T) {
	cases := []struct {
		name     string
		statuses []Status
		want     string
	}{
		{"empty", nil, "ok"},
		{"all up", []Status{
			{Kind: "source", ID: "s1", State: "up"},
			{Kind: "sink", ID: "k1", State: "up"},
		}, "ok"},
		{"one degraded", []Status{
			{Kind: "source", ID: "s1", State: "up"},
			{Kind: "sink", ID: "k1", State: "degraded"},
		}, "degraded"},
		{"one error", []Status{
			{Kind: "connector", ID: "c1", State: "error", Err: "boom"},
		}, "degraded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RollupHealth(tc.statuses); got != tc.want {
				t.Fatalf("RollupHealth(%+v) = %q, want %q", tc.statuses, got, tc.want)
			}
		})
	}
}
