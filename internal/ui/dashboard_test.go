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
// return value a test sets directly. dashboard.go's health chips and
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
	handler := ui.Handler(svc, reg, rec.Statuses, "test", nil)

	mux := http.NewServeMux()
	mux.Handle("/ui/", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, svc, reg, rec
}

// chipSnippet returns the health-chip (or connector-card badge cluster)
// markup for the given name — from its "font-semibold" name span up to the
// chip wrapper's closing </div> — so a test can assert which badge class
// appears WITHIN that one chip without being tripped up by other chips'
// badges elsewhere in the fragment (multiple components can legitimately
// share a state, e.g. two "restarting" chips) or by exact whitespace
// formatting.
func chipSnippet(t *testing.T, body, name string) string {
	t.Helper()
	marker := `<span class="font-semibold">` + name + `</span>`
	idx := strings.Index(body, marker)
	if idx < 0 {
		t.Fatalf("dashboard fragment has no chip for %q:\n%s", name, body)
	}
	rest := body[idx:]
	end := strings.Index(rest, "</div>")
	if end < 0 {
		t.Fatalf("chip for %q has no closing </div>:\n%s", name, body)
	}
	return rest[:end]
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
	for _, want := range []string{"Add your first source", `href="/ui/sources"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("empty dashboard fragment missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, `href="/ui/connectors"`) {
		t.Fatalf("empty dashboard fragment with no sources should not link /ui/connectors:\n%s", body)
	}
}

func TestDashboardFragEmptyStateWithSourcesPointsAtConnectors(t *testing.T) {
	srv, svc, _, _ := newDashboardTestServer(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "Main bus", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}, true))

	body := dashboardFrag(t, srv)
	for _, want := range []string{"Add your first connector", `href="/ui/connectors"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard fragment (sources, no connectors) missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Add your first source") {
		t.Fatalf("dashboard fragment with sources configured should not offer to add a source:\n%s", body)
	}
}

// --- Connector cards ---

func TestDashboardFragRendersConnectorCard(t *testing.T) {
	srv, svc, reg, _ := newDashboardTestServer(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "heading", Name: "Heading only", SourceID: "src1", SinkID: "sink1", Enabled: true,
	}, true))
	reg.Record("heading", 5, 500)
	reg.SetQueue("heading", 3, 300)

	body := dashboardFrag(t, srv)
	for _, want := range []string{
		"Heading only", `href="/ui/connectors/heading"`,
		"<code>src1</code>", "<code>sink1</code>",
		"badge-success\">enabled</span>", "Queue depth", "Msg/s", "Bytes/s",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard fragment missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Add your first") {
		t.Fatalf("dashboard fragment with a connector configured should not show the empty-state hero:\n%s", body)
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

// --- Components health strip: state colors + transient-absence tolerance ---

func TestDashboardFragHealthChipStates(t *testing.T) {
	srv, svc, _, rec := newDashboardTestServer(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "up-src", Name: "Up Src", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}, true))
	must(t, svc.PutSource(ctx, model.Source{ID: "degraded-src", Name: "Degraded Src", Type: model.SourceSocketCAN, Enabled: true, Interface: "can1"}, true))
	must(t, svc.PutSource(ctx, model.Source{ID: "restarting-src", Name: "Restarting Src", Type: model.SourceSocketCAN, Enabled: true, Interface: "can2"}, true))
	must(t, svc.PutSource(ctx, model.Source{ID: "off-src", Name: "Off Src", Type: model.SourceSocketCAN, Enabled: false, Interface: "can3"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "err-sink", Name: "Err Sink", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9001"}, true))

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
		name, wantBadgeClass, wantText string
	}{
		{"Up Src", "badge-success", "up"},
		{"Degraded Src", "badge-warning", "degraded"},
		{"Err Sink", "badge-error", "error"},
		{"Restarting Src", "badge-ghost", "restarting"},
		{"Off Src", "badge-ghost", "disabled"},
	}
	for _, c := range cases {
		snip := chipSnippet(t, body, c.name)
		if !strings.Contains(snip, c.wantBadgeClass) {
			t.Errorf("chip for %q = %q, want it to contain badge class %q", c.name, snip, c.wantBadgeClass)
		}
		if !strings.Contains(snip, ">"+c.wantText+"<") {
			t.Errorf("chip for %q = %q, want state text %q", c.name, snip, c.wantText)
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
	// The dashboard's connector/health content is fetched client-side, not
	// server-rendered inline (see dashboard.html's doc comment) — the shell
	// response itself carries no connector card markup.
	if strings.Contains(body, "dashboard-content") {
		t.Fatalf("dashboard page shell should not itself contain fragment markers:\n%s", body)
	}
}
