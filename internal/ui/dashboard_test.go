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
	mux.Handle("/", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, svc, reg, rec
}

// markerSnippet returns a bounded slice of markup around marker so a test can
// assert badge classes near one specific graph node without depending on
// exact whitespace or nested div boundaries.
func markerSnippet(t *testing.T, body, marker string) string {
	t.Helper()
	idx := strings.Index(body, `class="dag-node-title">`+marker+`</div>`)
	if idx < 0 {
		idx = strings.Index(body, marker)
	}
	if idx < 0 {
		t.Fatalf("dashboard fragment has no marker %q:\n%s", marker, body)
	}
	start := idx - 800
	if start < 0 {
		start = 0
	}
	end := idx + 500
	if end > len(body) {
		end = len(body)
	}
	return body[start:end]
}

func dagNodeSnippet(t *testing.T, body, name string) string {
	t.Helper()
	title := `class="dag-node-title">` + name + `</div>`
	idx := strings.Index(body, title)
	if idx < 0 {
		t.Fatalf("dashboard fragment has no DAG node %q:\n%s", name, body)
	}
	start := strings.LastIndex(body[:idx], "<a ")
	endOffset := strings.Index(body[idx:], "</a>")
	if start < 0 || endOffset < 0 {
		t.Fatalf("dashboard DAG node %q has incomplete markup:\n%s", name, body)
	}
	return body[start : idx+endOffset+len("</a>")]
}

func dashboardFrag(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp, err := http.Get(srv.URL + "/frag/dashboard")
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
	for _, want := range []string{"Add your first source", `href="/sources/new"`, `hx-get="/sources/new"`, `hx-target="#entity-create-dialog-container"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("empty dashboard fragment missing %q:\n%s", want, body)
		}
	}
	empty := markerSnippet(t, body, "Add your first source")
	if !strings.Contains(empty, `href="/sources/new"`) {
		t.Fatalf("initial empty-state CTA should point at sources:\n%s", empty)
	}
}

func TestDashboardFragSourceWithoutConnectorRendersGraph(t *testing.T) {
	srv, svc, _, _ := newDashboardTestServer(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "Main bus", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}, true))

	body := dashboardFrag(t, srv)
	for _, want := range []string{
		"dag-board", "Unused endpoints", "Main bus",
		`dag-node-unused dag-node-unused-source`,
		`data-dag-node="source:can0"`,
		`href="/sources/can0/"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard fragment did not render unconnected source %q:\n%s", want, body)
		}
	}
	for _, notWant := range []string{"Add your first sink", `data-dag-from=`} {
		if strings.Contains(body, notWant) {
			t.Fatalf("unconnected source graph unexpectedly contains %q:\n%s", notWant, body)
		}
	}
}

func TestDashboardFragSourceAndSinkWithoutConnectorRenderGraph(t *testing.T) {
	srv, svc, _, _ := newDashboardTestServer(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{
		ID: "src1", Name: "Source One", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}, true))
	must(t, svc.PutSink(ctx, model.Sink{
		ID: "sink1", Name: "Sink One", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9000",
	}, true))

	body := dashboardFrag(t, srv)
	for _, want := range []string{
		"dag-board", "Unused endpoints", "Source One", "Sink One",
		"dag-unused-group-source", "dag-unused-group-sink",
		`dag-node-unused dag-node-unused-source`,
		`dag-node-unused dag-node-unused-sink`,
		`data-dag-node="source:src1"`,
		`data-dag-node="sink:sink1"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard fragment did not render unconnected endpoints %q:\n%s", want, body)
		}
	}
	for _, notWant := range []string{"Add your first connector", `data-dag-from=`, `data-dag-to=`} {
		if strings.Contains(body, notWant) {
			t.Fatalf("unconnected endpoint graph unexpectedly contains %q:\n%s", notWant, body)
		}
	}
}

func TestDashboardFragKeepsDisabledEndpointsInMainGraph(t *testing.T) {
	srv, svc, _, rec := newDashboardTestServer(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "unused-source", Name: "Unused source", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}, true))
	must(t, svc.PutSource(ctx, model.Source{ID: "disabled-source", Name: "Disabled source", Type: model.SourceSocketCAN, Enabled: false, Interface: "can1"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "unused-sink", Name: "Unused sink", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9001"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "disabled-sink", Name: "Disabled sink", Type: model.SinkTCP, Enabled: false, Address: "127.0.0.1:9002"}, true))
	rec.setStatuses([]supervisor.Status{
		{Kind: "source", ID: "unused-source", State: "up"},
		{Kind: "sink", ID: "unused-sink", State: "up"},
	})

	body := dashboardFrag(t, srv)
	for _, tc := range []struct {
		name       string
		wantClass  string
		avoidClass string
	}{
		{"Unused source", "dag-node-unused-source", ""},
		{"Unused sink", "dag-node-unused-sink", ""},
		{"Disabled source", "state-disabled", "dag-node-unused"},
		{"Disabled sink", "state-disabled", "dag-node-unused"},
	} {
		snip := dagNodeSnippet(t, body, tc.name)
		if !strings.Contains(snip, tc.wantClass) {
			t.Errorf("node for %q = %q, want class %q", tc.name, snip, tc.wantClass)
		}
		if tc.avoidClass != "" && strings.Contains(snip, tc.avoidClass) {
			t.Errorf("node for %q = %q, should stay in main graph without %q", tc.name, snip, tc.avoidClass)
		}
	}

	unusedSink := dagNodeSnippet(t, body, "Unused sink")
	for _, want := range []string{"component-status-surface state-up", "badge-success", ">up<"} {
		if !strings.Contains(unusedSink, want) {
			t.Errorf("unused sink lost live up status %q: %s", want, unusedSink)
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
		`href="/sources/src1/"`, `href="/sinks/sink1/"`,
		`href="/sources/new"`, `href="/sinks/new"`, `href="/connectors/new"`,
		"metadata-stack", "metadata-table",
		"<th>Detail</th>", "<code>can0</code>", "<code>127.0.0.1:9000</code>",
		"Heading only", `href="/connectors/heading/"`, "Source One", "Sink One",
		`href="/sources/src1/">Source One</a>`, `href="/sinks/sink1/">Sink One</a>`,
		"badge-success\">enabled</span>", "msg/s", "B/s", "queued",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard fragment missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Add your first") {
		t.Fatalf("dashboard fragment with a connector configured should not show the empty-state hero:\n%s", body)
	}
	for _, unwanted := range []string{"marker-end", "dag-arrowhead"} {
		if strings.Contains(body, unwanted) {
			t.Fatalf("dashboard fragment still renders DAG arrowhead markup %q:\n%s", unwanted, body)
		}
	}
}

func TestDashboardFragRendersSharedEndpointsOnceWithMultipleEdges(t *testing.T) {
	srv, svc, _, _ := newDashboardTestServer(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "shared-source", Name: "Shared source", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}, true))
	must(t, svc.PutSource(ctx, model.Source{ID: "other-source", Name: "Other source", Type: model.SourceSocketCAN, Enabled: true, Interface: "can1"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "shared-sink", Name: "Shared sink", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9001"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "other-sink", Name: "Other sink", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9002"}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "first", SourceID: "shared-source", SinkID: "shared-sink", Enabled: true}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "second", SourceID: "shared-source", SinkID: "other-sink", Enabled: true}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "third", SourceID: "other-source", SinkID: "shared-sink", Enabled: true}, true))

	body := dashboardFrag(t, srv)
	for marker, want := range map[string]int{
		`data-dag-node="source:shared-source"`: 1,
		`data-dag-node="sink:shared-sink"`:     1,
		`data-dag-from="source:shared-source"`: 2,
		`data-dag-to="sink:shared-sink"`:       2,
	} {
		if got := strings.Count(body, marker); got != want {
			t.Errorf("dashboard marker %q count = %d, want %d:\n%s", marker, got, want, body)
		}
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
	if strings.Contains(body, "Unused endpoints") {
		t.Fatalf("endpoints referenced by a disabled connector should stay in the main graph:\n%s", body)
	}
	for _, name := range []string{"Source One", "Sink One"} {
		if snip := dagNodeSnippet(t, body, name); strings.Contains(snip, "dag-node-unused") {
			t.Errorf("endpoint %q referenced by disabled connector was classified unused: %s", name, snip)
		}
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
	for _, want := range []string{
		`dag-node-connector component-status-surface state-error`,
		`dag-edge state-error`,
		`<tr class="component-status-row state-error" data-href="/connectors/heading/">`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard fragment missing connector error colorization %q:\n%s", want, body)
		}
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
			ID: id + "-conn", Name: id + " connector", SourceID: id + "-src", SinkID: "err-sink", Enabled: id != "off",
		}, true))
	}

	// Deliberately omit "restarting-src" (enabled but not yet reported —
	// e.g. mid hot-apply) and "off-src" (disabled, so the supervisor never
	// starts it and never reports a status for it) from statuses.
	rec.setStatuses([]supervisor.Status{
		{Kind: "source", ID: "up-src", State: "up"},
		{Kind: "source", ID: "degraded-src", State: "degraded"},
		{Kind: "sink", ID: "err-sink", State: "error", Err: "boom"},
		{Kind: "connector", ID: "up-conn", State: "up"},
		{Kind: "connector", ID: "degraded-conn", State: "degraded"},
	})

	body := dashboardFrag(t, srv)

	cases := []struct {
		name, wantBadgeClass, wantText, wantStateClass string
	}{
		{"Up Src", "badge-success", "up", "component-status-surface state-up"},
		{"Degraded Src", "badge-warning", "degraded", "component-status-surface state-degraded"},
		{"Err Sink", "badge-error", "error", "component-status-surface state-error"},
		{"Restarting Src", "badge-ghost", "restarting", "component-status-surface state-restarting"},
		{"Off Src", "badge-ghost", "disabled", "component-status-surface state-disabled"},
		{"up connector", "badge-success", "up", "component-status-surface state-up"},
		{"degraded connector", "badge-warning", "degraded", "component-status-surface state-degraded"},
		{"restarting connector", "badge-ghost", "restarting", "component-status-surface state-restarting"},
		{"off connector", "badge-ghost", "disabled", "component-status-surface state-disabled"},
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
		`<tr class="component-status-row state-up" data-href="/sources/up-src/">`,
		`<tr class="component-status-row state-degraded" data-href="/sources/degraded-src/">`,
		`<tr class="component-status-row state-error" data-href="/sinks/err-sink/">`,
		`<tr class="component-status-row state-up" data-href="/connectors/up-conn/">`,
		`<tr class="component-status-row state-degraded" data-href="/connectors/degraded-conn/">`,
		`<tr class="component-status-row state-restarting" data-href="/connectors/restarting-conn/">`,
		`<tr class="component-status-row state-disabled" data-href="/connectors/off-conn/">`,
		`class="dag-edge state-up"`,
		`class="dag-edge state-degraded"`,
		`class="dag-edge state-restarting"`,
		`class="dag-edge state-disabled"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard metadata missing status-colored row %q", want)
		}
	}

	for _, tc := range []struct {
		path, want string
	}{
		{"/sources", `<tr class="component-status-row state-up" data-href="/sources/up-src/">`},
		{"/sinks", `<tr class="component-status-row state-error" data-href="/sinks/err-sink/">`},
		{"/connectors", `<tr class="component-status-row state-up" data-href="/connectors/up-conn/">`},
		{"/frag/sources/up-src/overview", `<section class="overview-card overview-summary-panel component-status-surface state-up" aria-label="Status and metrics">`},
		{"/frag/sinks/err-sink/overview", `<section class="overview-card overview-summary-panel component-status-surface state-error" aria-label="Status and metrics">`},
		{"/frag/connectors/up-conn/overview", `<section class="overview-card overview-summary-panel component-status-surface state-up" aria-label="Status and metrics">`},
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
		"Unused endpoints", "dag-node-unused-source", "dag-node-unused-sink",
		`data-dag-node="source:src2"`, `data-dag-node="sink:sink2"`,
		`<tr class="component-status-row state-restarting" data-href="/sources/src1/">`,
		`<tr class="component-status-row state-restarting" data-href="/sinks/sink1/">`,
		`<tr class="component-status-row state-restarting" data-href="/connectors/conn1/">`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard metadata tables missing %q:\n%s", want, body)
		}
	}
	for _, notWant := range []string{`>View</a>`, `>Edit</a>`, `<th>Actions</th>`} {
		if strings.Contains(body, notWant) {
			t.Fatalf("dashboard metadata tables unexpectedly contain row action %q:\n%s", notWant, body)
		}
	}
}

// --- Dashboard page shell ---

func TestDashboardPageHostsPollingContainer(t *testing.T) {
	srv, _, _, _ := newDashboardTestServer(t)

	resp, err := http.Get(srv.URL + "/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		`id="dashboard-panel"`,
		`hx-get="/frag/dashboard"`,
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
