package config

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
)

// fakeReconciler is a test double for Reconciler: it counts Reconcile calls
// and lets tests inject a canned error (returned, not cleared, on every
// subsequent call) and canned Statuses().
type fakeReconciler struct {
	mu       sync.Mutex
	calls    int
	err      error
	statuses []supervisor.Status
	lastCtx  context.Context
	// onReconcile, if set, runs synchronously as the first thing Reconcile
	// does — before lastCtx is captured — so a test can simulate the
	// request context being cancelled exactly as reconcile begins.
	onReconcile func()
}

func (f *fakeReconciler) Reconcile(ctx context.Context) error {
	if f.onReconcile != nil {
		f.onReconcile()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastCtx = ctx
	return f.err
}

func (f *fakeReconciler) Statuses() []supervisor.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statuses
}

func (f *fakeReconciler) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeReconciler) lastCtxErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastCtx == nil {
		return nil
	}
	return f.lastCtx.Err()
}

func newTestService(t *testing.T) (*Service, *store.Store, *fakeReconciler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rec := &fakeReconciler{}
	svc := NewService(st, rec, slog.Default())
	return svc, st, rec
}

func baseSource(id string) model.Source {
	return model.Source{ID: id, Name: "Source " + id, Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}
}

func baseSink(id, path string) model.Sink {
	return model.Sink{ID: id, Name: "Sink " + id, Type: model.SinkHTTPSSE, Enabled: true, Path: path}
}

func baseConnector(id, srcID, sinkID string) model.Connector {
	return model.Connector{ID: id, Name: "Conn " + id, SourceID: srcID, SinkID: sinkID, Enabled: true,
		Buffer: model.BufferLimits{MaxMessages: 10}}
}

func asValidationError(t *testing.T, err error) *ValidationError {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	return ve
}

// --- Source lifecycle ---

func TestSourceLifecycle(t *testing.T) {
	ctx := context.Background()
	svc, _, rec := newTestService(t)

	if err := svc.PutSource(ctx, baseSource("s1"), true); err != nil {
		t.Fatalf("create s1: %v", err)
	}
	if rec.callCount() != 1 {
		t.Fatalf("reconcile calls after create = %d, want 1", rec.callCount())
	}

	list, err := svc.ListSources(ctx)
	if err != nil || len(list) != 1 || list[0].ID != "s1" {
		t.Fatalf("ListSources = %+v, %v", list, err)
	}
	got, err := svc.GetSource(ctx, "s1")
	if err != nil || got.ID != "s1" {
		t.Fatalf("GetSource = %+v, %v", got, err)
	}

	// Duplicate create.
	if err := svc.PutSource(ctx, baseSource("s1"), true); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate create err = %v, want ErrExists", err)
	}
	if rec.callCount() != 1 {
		t.Fatalf("reconcile calls after failed create = %d, want 1", rec.callCount())
	}

	// Update missing.
	if err := svc.PutSource(ctx, baseSource("nope"), false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing err = %v, want ErrNotFound", err)
	}
	if rec.callCount() != 1 {
		t.Fatalf("reconcile calls after failed update = %d, want 1", rec.callCount())
	}

	// Update existing.
	updated := baseSource("s1")
	updated.Name = "Renamed"
	if err := svc.PutSource(ctx, updated, false); err != nil {
		t.Fatalf("update s1: %v", err)
	}
	if rec.callCount() != 2 {
		t.Fatalf("reconcile calls after update = %d, want 2", rec.callCount())
	}
	got, _ = svc.GetSource(ctx, "s1")
	if got.Name != "Renamed" {
		t.Fatalf("GetSource after update = %+v", got)
	}

	if _, err := svc.GetSource(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetSource missing err = %v, want ErrNotFound", err)
	}
}

// --- Sink lifecycle + path collision ---

func TestSinkLifecycle(t *testing.T) {
	ctx := context.Background()
	svc, _, rec := newTestService(t)

	if err := svc.PutSink(ctx, baseSink("k1", "/a"), true); err != nil {
		t.Fatalf("create k1: %v", err)
	}
	if rec.callCount() != 1 {
		t.Fatalf("reconcile calls = %d, want 1", rec.callCount())
	}

	list, err := svc.ListSinks(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListSinks = %+v, %v", list, err)
	}
	got, err := svc.GetSink(ctx, "k1")
	if err != nil || got.Path != "/a" {
		t.Fatalf("GetSink = %+v, %v", got, err)
	}

	if err := svc.PutSink(ctx, baseSink("k1", "/a"), true); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate create err = %v, want ErrExists", err)
	}

	if err := svc.PutSink(ctx, baseSink("nope", "/z"), false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing err = %v, want ErrNotFound", err)
	}

	// New sink colliding with an existing sink's path -> ValidationError,
	// store unchanged, no reconcile call for this failed write.
	before := rec.callCount()
	err = svc.PutSink(ctx, baseSink("k2", "/a"), true)
	asValidationError(t, err)
	if rec.callCount() != before {
		t.Fatalf("reconcile calls after failed path-collision create = %d, want %d", rec.callCount(), before)
	}
	list, _ = svc.ListSinks(ctx)
	if len(list) != 1 {
		t.Fatalf("store changed after failed path-collision create: %+v", list)
	}
}

// --- Connector validation ---

func TestConnectorUnknownSource(t *testing.T) {
	ctx := context.Background()
	svc, _, rec := newTestService(t)

	// No source/sink exist at all.
	err := svc.PutConnector(ctx, baseConnector("c1", "nope", "alsonope"), true)
	asValidationError(t, err)
	if rec.callCount() != 0 {
		t.Fatalf("reconcile calls = %d, want 0", rec.callCount())
	}
	list, err := svc.ListConnectors(ctx)
	if err != nil || len(list) != 0 {
		t.Fatalf("store changed after invalid connector create: %+v, %v", list, err)
	}
}

func TestConnectorInvalidCEL(t *testing.T) {
	ctx := context.Background()
	svc, _, rec := newTestService(t)

	if err := svc.PutSource(ctx, baseSource("s1"), true); err != nil {
		t.Fatal(err)
	}
	if err := svc.PutSink(ctx, baseSink("k1", "/a"), true); err != nil {
		t.Fatal(err)
	}
	before := rec.callCount()

	c := baseConnector("c1", "s1", "k1")
	badExpr := "msg.pgn =="
	c.Filters = []string{badExpr}
	err := svc.PutConnector(ctx, c, true)
	ve := asValidationError(t, err)
	if !strings.Contains(ve.Msg, badExpr) {
		t.Fatalf("ValidationError.Msg = %q, want it to name the expression %q", ve.Msg, badExpr)
	}
	if rec.callCount() != before {
		t.Fatalf("reconcile calls = %d, want %d", rec.callCount(), before)
	}
}

func TestConnectorLifecycle(t *testing.T) {
	ctx := context.Background()
	svc, _, rec := newTestService(t)

	if err := svc.PutSource(ctx, baseSource("s1"), true); err != nil {
		t.Fatal(err)
	}
	if err := svc.PutSink(ctx, baseSink("k1", "/a"), true); err != nil {
		t.Fatal(err)
	}
	c := baseConnector("c1", "s1", "k1")
	c.Filters = []string{"msg.pgn == 127250"}
	if err := svc.PutConnector(ctx, c, true); err != nil {
		t.Fatalf("create connector: %v", err)
	}
	if rec.callCount() != 3 {
		t.Fatalf("reconcile calls = %d, want 3", rec.callCount())
	}

	got, err := svc.GetConnector(ctx, "c1")
	if err != nil || got.ID != "c1" {
		t.Fatalf("GetConnector = %+v, %v", got, err)
	}
	if err := svc.PutConnector(ctx, c, true); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate create err = %v, want ErrExists", err)
	}
	if err := svc.PutConnector(ctx, baseConnector("nope", "s1", "k1"), false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing err = %v, want ErrNotFound", err)
	}
}

// --- Delete + ErrInUse ---

func TestDeleteSourceInUse(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestService(t)

	must(t, svc.PutSource(ctx, baseSource("s1"), true))
	must(t, svc.PutSink(ctx, baseSink("k1", "/a"), true))
	conn := baseConnector("c1", "s1", "k1")
	conn.Enabled = false // ErrInUse must apply even when the connector is disabled
	must(t, svc.PutConnector(ctx, conn, true))

	if err := svc.DeleteSource(ctx, "s1"); !errors.Is(err, ErrInUse) {
		t.Fatalf("delete in-use source err = %v, want ErrInUse", err)
	}
	must(t, svc.DeleteConnector(ctx, "c1"))
	if err := svc.DeleteSource(ctx, "s1"); err != nil {
		t.Fatalf("delete source after connector removed: %v", err)
	}
	if _, err := svc.GetSource(ctx, "s1"); !errors.Is(err, ErrNotFound) {
		t.Fatal("source still present after delete")
	}
}

func TestDeleteSinkInUse(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestService(t)

	must(t, svc.PutSource(ctx, baseSource("s1"), true))
	must(t, svc.PutSink(ctx, baseSink("k1", "/a"), true))
	must(t, svc.PutConnector(ctx, baseConnector("c1", "s1", "k1"), true))

	if err := svc.DeleteSink(ctx, "k1"); !errors.Is(err, ErrInUse) {
		t.Fatalf("delete in-use sink err = %v, want ErrInUse", err)
	}
	must(t, svc.DeleteConnector(ctx, "c1"))
	if err := svc.DeleteSink(ctx, "k1"); err != nil {
		t.Fatalf("delete sink after connector removed: %v", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestService(t)

	if err := svc.DeleteSource(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteSource missing err = %v, want ErrNotFound", err)
	}
	if err := svc.DeleteSink(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteSink missing err = %v, want ErrNotFound", err)
	}
	if err := svc.DeleteConnector(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteConnector missing err = %v, want ErrNotFound", err)
	}
}

// --- Reconcile failure surfaced, not rolled back ---

func TestReconcileFailureNotRolledBack(t *testing.T) {
	ctx := context.Background()
	svc, _, rec := newTestService(t)
	rec.err = errors.New("boom")
	rec.statuses = []supervisor.Status{{Kind: "source", ID: "s1", State: "error", Err: "boom"}}

	if err := svc.PutSource(ctx, baseSource("s1"), true); err != nil {
		t.Fatalf("PutSource returned error despite reconcile failure being non-fatal: %v", err)
	}
	if rec.callCount() != 1 {
		t.Fatalf("reconcile calls = %d, want 1", rec.callCount())
	}
	// Write persisted despite reconcile failing.
	if _, err := svc.GetSource(ctx, "s1"); err != nil {
		t.Fatalf("source not persisted after reconcile failure: %v", err)
	}
	sts := svc.Statuses()
	if len(sts) != 1 || sts[0].Err != "boom" {
		t.Fatalf("Statuses() = %+v, want the reconcile failure surfaced", sts)
	}
}

// A config write must be applied to the running system even if the request
// that triggered it goes away mid-write: the config is already durably
// persisted (PutSource's store write completed) by the time reconcile()
// invokes the reconciler, and there is no periodic reconcile to pick up the
// slack later (only the next write triggers another one). So reconcile()
// must detach from the inbound context's cancellation rather than passing it
// straight through to Reconcile. Simulated here by cancelling the request
// context from inside the fake reconciler itself, timed to land exactly as
// Reconcile begins — i.e. strictly after the store write it followed has
// already succeeded, matching a real client disconnect mid-PUT.
func TestReconcileRunsDespiteCallerContextCancellation(t *testing.T) {
	svc, _, rec := newTestService(t)

	ctx, cancel := context.WithCancel(context.Background())
	rec.onReconcile = cancel

	if err := svc.PutSource(ctx, baseSource("s1"), true); err != nil {
		t.Fatalf("PutSource returned error despite the request context being cancelled: %v", err)
	}
	if rec.callCount() != 1 {
		t.Fatalf("reconcile calls = %d, want 1", rec.callCount())
	}
	if err := rec.lastCtxErr(); err != nil {
		t.Fatalf("reconcile received a cancelled context (err=%v); it must run detached from the caller's ctx", err)
	}
	// Store write completed before cancellation landed (it happens inside
	// Reconcile, after PutSource's own write) — verify with a fresh ctx,
	// since the caller's ctx above is now cancelled by design.
	if _, err := svc.GetSource(context.Background(), "s1"); err != nil {
		t.Fatalf("source not persisted: %v", err)
	}
}

// --- Import ---

func TestImportReplace(t *testing.T) {
	ctx := context.Background()
	svc, _, rec := newTestService(t)
	must(t, svc.PutSource(ctx, baseSource("a"), true))

	cfg := model.Config{Sources: []model.Source{baseSource("b")}}
	if err := svc.Import(ctx, cfg, true); err != nil {
		t.Fatalf("Import replace: %v", err)
	}
	if rec.callCount() != 2 {
		t.Fatalf("reconcile calls = %d, want 2", rec.callCount())
	}
	got, err := svc.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Sources) != 1 || got.Sources[0].ID != "b" {
		t.Fatalf("Export after replace = %+v, want only source b", got.Sources)
	}
}

func TestImportMerge(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestService(t)
	must(t, svc.PutSource(ctx, baseSource("a"), true))
	must(t, svc.PutSink(ctx, baseSink("x", "/x"), true))

	updatedA := baseSource("a")
	updatedA.Name = "Updated A"
	cfg := model.Config{Sources: []model.Source{updatedA, baseSource("c")}}
	if err := svc.Import(ctx, cfg, false); err != nil {
		t.Fatalf("Import merge: %v", err)
	}

	got, err := svc.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Sources) != 2 {
		t.Fatalf("Export sources after merge = %+v, want 2", got.Sources)
	}
	byID := map[string]model.Source{}
	for _, s := range got.Sources {
		byID[s.ID] = s
	}
	if byID["a"].Name != "Updated A" {
		t.Fatalf("source a not upserted: %+v", byID["a"])
	}
	if _, ok := byID["c"]; !ok {
		t.Fatal("source c not added")
	}
	// Sink x, not mentioned in the import, must survive untouched.
	if len(got.Sinks) != 1 || got.Sinks[0].ID != "x" {
		t.Fatalf("unmentioned sink lost during merge: %+v", got.Sinks)
	}
}

func TestImportInvalidLeavesStoreUnchanged(t *testing.T) {
	ctx := context.Background()
	svc, _, rec := newTestService(t)
	must(t, svc.PutSource(ctx, baseSource("a"), true))
	before := rec.callCount()

	// Connector referencing an unknown source makes the whole import invalid.
	badCfg := model.Config{
		Sources:    []model.Source{baseSource("a")},
		Connectors: []model.Connector{baseConnector("c1", "ghost", "ghost")},
	}
	err := svc.Import(ctx, badCfg, true)
	asValidationError(t, err)
	if rec.callCount() != before {
		t.Fatalf("reconcile calls = %d, want %d", rec.callCount(), before)
	}
	got, err := svc.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Sources) != 1 || got.Sources[0].ID != "a" || len(got.Connectors) != 0 {
		t.Fatalf("store changed after invalid import: %+v", got)
	}
}

// --- ValidateFilters ---

func TestValidateFilters(t *testing.T) {
	svc, _, _ := newTestService(t)
	if err := svc.ValidateFilters([]string{"msg.pgn == 127250"}); err != nil {
		t.Fatalf("valid filter rejected: %v", err)
	}
	err := svc.ValidateFilters([]string{"msg.pgn =="})
	asValidationError(t, err)
}

// --- ValidateConfig package function ---

func TestValidateConfigFunction(t *testing.T) {
	valid := model.Config{
		Sources:    []model.Source{baseSource("s1")},
		Sinks:      []model.Sink{baseSink("k1", "/a")},
		Connectors: []model.Connector{baseConnector("c1", "s1", "k1")},
	}
	if err := ValidateConfig(valid); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	invalid := valid
	invalid.Connectors = []model.Connector{baseConnector("c1", "ghost", "k1")}
	err := ValidateConfig(invalid)
	asValidationError(t, err)
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
