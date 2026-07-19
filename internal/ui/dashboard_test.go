package ui_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
	"github.com/open-ships/beacon/internal/ui"
)

// settableReconciler is a config.Reconciler test double whose Statuses
// return value a test sets directly. dashboard.go's endpoint nodes and
// connector error badges are driven entirely by supervisor.Status.State, so
// exercising every state (up/degraded/error) plus the "component missing
// from Statuses" case (the transient-absence tolerance the dashboard's
// behavior contract requires) needs a live-swappable double — unlike
// ui_test.go's fakeReconciler, which always returns nil. Mirrors
// internal/api/entities_test.go's fakeReconciler.
type settableReconciler struct {
	mu       sync.Mutex
	statuses []supervisor.Status
}

func (r *settableReconciler) Reconcile(ctx context.Context) error { return nil }

func (r *settableReconciler) Statuses() []supervisor.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.statuses
}

func (r *settableReconciler) setStatuses(s []supervisor.Status) {
	r.mu.Lock()
	r.statuses = s
	r.mu.Unlock()
}

// newDashboardTestServer mirrors newUIServerWithServiceAndRegistry
// (forms_test.go) but wires in a *settableReconciler instead of the fixed
// fakeReconciler, so dashboard tests can set exactly the supervisor.Status
// set a scenario needs (including omitting a configured, enabled component
// entirely, to exercise the transient-absence "restarting" chip).
func newDashboardTestServer(t *testing.T) (*httptest.Server, *config.Service, *stats.Registry, *settableReconciler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rec := &settableReconciler{}
	svc := config.NewService(st, rec, nil)
	reg := stats.NewRegistry()
	handler := ui.Handler(svc, reg, rec.Statuses, nil, "test", nil)

	mux := http.NewServeMux()
	mux.Handle("/ui/", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, svc, reg, rec
}

// markerSnippet returns a bounded slice of markup around marker so a test can
// assert badge classes near one specific graph node without depending on
// exact whitespace or nested div boundaries.
func markerSnippet(t *testing.T, body, marker string) string {
	t.Helper()
	idx := strings.Index(body, marker)
	if idx < 0 {
		t.Fatalf("dashboard fragment has no marker %q:\n%s", marker, body)
	}
	start := idx - 450
	if start < 0 {
		start = 0
	}
	end := idx + 500
	if end > len(body) {
		end = len(body)
	}
	return body[start:end]
}

func dashboardFrag(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp, err := http.Get(srv.URL + "/ui/frag/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	return mustBody(t, resp)
}

// --- Empty state ---

func TestDashboardFragEmptyStateNoSourcesPointsAtSources(t *testing.T) {
	srv, _, _, _ := newDashboardTestServer(t)

	body := dashboardFrag(t, srv)
	for _, want := range []string{"Add your first source", `href="/ui/sources/new"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("empty dashboard fragment missing %q:\n%s", want, body)
		}
	}
	empty := markerSnippet(t, body, "Add your first source")
	if !strings.Contains(empty, `href="/ui/sources/new"`) {
		t.Fatalf("initial empty-state CTA should point at sources:\n%s", empty)
	}
}

func TestDashboardFragEmptyStateWithSourceNeedsSink(t *testing.T) {
	srv, svc, _, _ := newDashboardTestServer(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "Main bus", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}, true))

	body := dashboardFrag(t, srv)
	for _, want := range []string{"Add your first sink", `href="/ui/sinks/new"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard fragment (sources, no connectors) missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Add your first connector") {
		t.Fatalf("dashboard fragment with no sinks should not offer to add a connector:\n%s", body)
	}
}

func TestDashboardFragEmptyStateWithSourceAndSinkPointsAtConnectors(t *testing.T) {
	srv, svc, _, _ := newDashboardTestServer(t)
	seedSourceSink(t, svc)

	body := dashboardFrag(t, srv)
	for _, want := range []string{"Add your first connector", `href="/ui/connectors/new"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard fragment (source+sink, no connectors) missing %q:\n%s", want, body)
		}
	}
}

// --- Home DAG ---

func TestDashboardFragRendersConnectorDAG(t *testing.T) {
	srv, svc, reg, _ := newDashboardTestServer(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "heading", Name: "Heading only", SourceID: "src1", SinkID: "sink1", Enabled: true,
	}, true))
	reg.Record("heading", 5, 500)
	reg.SetQueue("heading", 3, 300)

	body := dashboardFrag(t, srv)
	for _, want := range []string{
		"dag-board", "dag-node-source", "dag-node-connector", "dag-node-sink",
		`data-node-id="src1"`, `data-connector-id="heading"`, `data-node-id="sink1"`,
		`href="/ui/sources/src1/"`, `href="/ui/sinks/sink1/"`,
		`href="/ui/sources/new"`, `href="/ui/sinks/new"`, `href="/ui/connectors/new"`,
		"metadata-stack", "metadata-table",
		"<th>Detail</th>", "<code>can0</code>", "<code>127.0.0.1:9000</code>",
		"Heading only", `href="/ui/connectors/heading/"`, "Source One", "Sink One",
		`href="/ui/sources/src1/">Source One</a>`, `href="/ui/sinks/sink1/">Sink One</a>`,
		"badge-success\">enabled</span>", "msg/s", "B/s", "queued",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard fragment missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Add your first") {
		t.Fatalf("dashboard fragment with a connector configured should not show the empty-state hero:\n%s", body)
	}
}

// TestDashboardFragConnectorDAGNameFallsBackToIDWhenEmpty is dashboard.go's
// analogue of TestConnectorsPageNameFallsBackToIDWhenEmpty (forms_test.go):
// a source/sink with no Name set renders its raw id on the graph node
// instead of a blank title.
func TestDashboardFragConnectorDAGNameFallsBackToIDWhenEmpty(t *testing.T) {
	srv, svc, _, _ := newDashboardTestServer(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "src1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "sink1", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9000"}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "heading", Name: "Heading only", SourceID: "src1", SinkID: "sink1", Enabled: true}, true))

	body := dashboardFrag(t, srv)
	if !strings.Contains(body, "src1") || !strings.Contains(body, "sink1") {
		t.Fatalf("dashboard connector DAG should fall back to the raw id when name is empty:\n%s", body)
	}
}

func TestDashboardFragDisabledConnectorBadge(t *testing.T) {
	srv, svc, _, _ := newDashboardTestServer(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "heading", Name: "Heading only", SourceID: "src1", SinkID: "sink1", Enabled: false,
	}, true))

	body := dashboardFrag(t, srv)
	if !strings.Contains(body, "badge-ghost\">disabled</span>") {
		t.Fatalf("dashboard fragment missing disabled connector badge:\n%s", body)
	}
}

func TestDashboardFragConnectorErrorBadge(t *testing.T) {
	srv, svc, _, rec := newDashboardTestServer(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "heading", Name: "Heading only", SourceID: "src1", SinkID: "sink1", Enabled: true,
	}, true))
	rec.setStatuses([]supervisor.Status{
		{Kind: "connector", ID: "heading", State: "error", Err: "boom"},
	})

	body := dashboardFrag(t, srv)
	if !strings.Contains(body, "badge-error\">error</span>") {
		t.Fatalf("dashboard fragment missing connector error badge:\n%s", body)
	}
	// The error badge is additional, not a replacement: the connector is
	// still Enabled, so its enabled badge must still render too.
	if !strings.Contains(body, "badge-success\">enabled</span>") {
		t.Fatalf("dashboard fragment lost the enabled badge alongside the error badge:\n%s", body)
	}
}

// --- Endpoint node states + transient-absence tolerance ---

func TestDashboardFragEndpointNodeStates(t *testing.T) {
	srv, svc, _, rec := newDashboardTestServer(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "up-src", Name: "Up Src", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}, true))
	must(t, svc.PutSource(ctx, model.Source{ID: "degraded-src", Name: "Degraded Src", Type: model.SourceSocketCAN, Enabled: true, Interface: "can1"}, true))
	must(t, svc.PutSource(ctx, model.Source{ID: "restarting-src", Name: "Restarting Src", Type: model.SourceSocketCAN, Enabled: true, Interface: "can2"}, true))
	must(t, svc.PutSource(ctx, model.Source{ID: "off-src", Name: "Off Src", Type: model.SourceSocketCAN, Enabled: false, Interface: "can3"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "err-sink", Name: "Err Sink", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9001"}, true))
	for _, id := range []string{"up", "degraded", "restarting", "off"} {
		must(t, svc.PutConnector(ctx, model.Connector{
			ID: id + "-conn", Name: id + " connector", SourceID: id + "-src", SinkID: "err-sink", Enabled: true,
		}, true))
	}

	// Deliberately omit "restarting-src" (enabled but not yet reported —
	// e.g. mid hot-apply) and "off-src" (disabled, so the supervisor never
	// starts it and never reports a status for it) from statuses.
	rec.setStatuses([]supervisor.Status{
		{Kind: "source", ID: "up-src", State: "up"},
		{Kind: "source", ID: "degraded-src", State: "degraded"},
		{Kind: "sink", ID: "err-sink", State: "error", Err: "boom"},
	})

	body := dashboardFrag(t, srv)

	cases := []struct {
		name, wantBadgeClass, wantText, wantStateClass string
	}{
		{"Up Src", "badge-success", "up", "endpoint-status-surface state-up"},
		{"Degraded Src", "badge-warning", "degraded", "endpoint-status-surface state-degraded"},
		{"Err Sink", "badge-error", "error", "endpoint-status-surface state-error"},
		{"Restarting Src", "badge-ghost", "restarting", "endpoint-status-surface state-restarting"},
		{"Off Src", "badge-ghost", "disabled", "endpoint-status-surface state-disabled"},
	}
	for _, c := range cases {
		snip := markerSnippet(t, body, c.name)
		if !strings.Contains(snip, c.wantBadgeClass) {
			t.Errorf("node for %q = %q, want it to contain badge class %q", c.name, snip, c.wantBadgeClass)
		}
		if !strings.Contains(snip, ">"+c.wantText+"<") {
			t.Errorf("node for %q = %q, want state text %q", c.name, snip, c.wantText)
		}
		if !strings.Contains(snip, c.wantStateClass) {
			t.Errorf("node for %q = %q, want status surface class %q", c.name, snip, c.wantStateClass)
		}
	}

	for _, want := range []string{
		`<tr class="endpoint-status-row state-up" data-href="/ui/sources/up-src/">`,
		`<tr class="endpoint-status-row state-degraded" data-href="/ui/sources/degraded-src/">`,
		`<tr class="endpoint-status-row state-error" data-href="/ui/sinks/err-sink/">`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard metadata missing status-colored row %q", want)
		}
	}

	for _, tc := range []struct {
		path, want string
	}{
		{"/ui/sources", `<tr class="endpoint-status-row state-up" data-href="/ui/sources/up-src/">`},
		{"/ui/sinks", `<tr class="endpoint-status-row state-error" data-href="/ui/sinks/err-sink/">`},
		{"/ui/frag/sources/up-src/overview", `<section class="overview-card endpoint-status-surface state-up" aria-label="Status">`},
		{"/ui/frag/sinks/err-sink/overview", `<section class="overview-card endpoint-status-surface state-error" aria-label="Status">`},
	} {
		resp, err := http.Get(srv.URL + tc.path)
		if err != nil {
			t.Fatal(err)
		}
		mustStatus(t, resp, http.StatusOK)
		page := mustBody(t, resp)
		if !strings.Contains(page, tc.want) {
			t.Errorf("%s missing status background class %q:\n%s", tc.path, tc.want, page)
		}
	}
}

func TestDashboardFragMetadataTablesCountEndpointUsage(t *testing.T) {
	srv, svc, _, _ := newDashboardTestServer(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "src1", Name: "Used source", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}, true))
	must(t, svc.PutSource(ctx, model.Source{ID: "src2", Name: "Unused source", Type: model.SourceSocketCAN, Enabled: true, Interface: "can1"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "sink1", Name: "Used sink", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9001"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "sink2", Name: "Unused sink", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9002"}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "conn1", Name: "First", SourceID: "src1", SinkID: "sink1", Enabled: true}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "conn2", Name: "Second", SourceID: "src1", SinkID: "sink1", Enabled: true}, true))

	body := dashboardFrag(t, srv)
	for _, want := range []string{
		"metadata-stack", "Sources", "Sinks", "Connectors",
		"Used source", "2 connectors", "Unused source", "0 connectors",
		"Used sink", "Unused sink", "usage-dot-used", "usage-dot-unused",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard metadata tables missing %q:\n%s", want, body)
		}
	}
}

// --- Dashboard page shell ---

func TestDashboardPageHostsPollingContainer(t *testing.T) {
	srv, _, _, _ := newDashboardTestServer(t)

	resp, err := http.Get(srv.URL + "/ui/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		`id="dashboard-panel"`,
		`hx-get="/ui/frag/dashboard"`,
		`hx-trigger="load, every 2s"`,
		`hx-swap="innerHTML"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard page missing %q:\n%s", want, body)
		}
	}
	// The dashboard's graph content is fetched client-side, not
	// server-rendered inline (see dashboard.html's doc comment) — the shell
	// response itself carries no graph markup.
	if strings.Contains(body, "dashboard-content") {
		t.Fatalf("dashboard page shell should not itself contain fragment markers:\n%s", body)
	}
}
