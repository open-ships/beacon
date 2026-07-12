package supervisor

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/sink"
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
	sup := New(st, nil, ds, slog.Default(), nil)
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
