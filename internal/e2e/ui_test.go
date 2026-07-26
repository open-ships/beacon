package e2e

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/bus/busfake"
)

// postForm issues a form-encoded POST against target — the same
// Content-Type and body shape a browser sends when a beacon UI <form>
// submits (see internal/ui/templates/frag_source_form.html,
// frag_sink_form.html, frag_connector_form.html), as opposed to
// api_test.go's doJSON helper, which drives the separate JSON admin API.
func postForm(t *testing.T, target string, values url.Values) *http.Response {
	t.Helper()
	resp, err := http.PostForm(target, values)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// bodyString reads and closes resp's body, failing the test on a read
// error.
func bodyString(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// getBody issues a GET against target and returns its body as a string,
// failing the test unless the response is 200. Reads the body itself
// (rather than calling mustStatus then reading resp.Body) because
// mustStatus unconditionally defer-closes the body — including on its
// success path — so it can't be composed with a subsequent body read.
func getBody(t *testing.T, target string) string {
	t.Helper()
	resp, err := http.Get(target)
	if err != nil {
		t.Fatal(err)
	}
	body := bodyString(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200; body: %s", target, resp.StatusCode, body)
	}
	return body
}

// totalMessagesPattern extracts the numeric value of the connector stats
// fragment's "Total messages" tile (internal/ui/templates/
// frag_connector_stats.html's "connector-stats" template) — the detail
// page's live counter, used below instead of the dashboard connector
// card's msg/s rate tile: a rate is only ever nonzero within its trailing
// window, so polling for "rate > 0" would be flaky depending on exactly
// when the poll lands relative to delivery, whereas TotalMessages only
// ever grows.
var totalMessagesPattern = regexp.MustCompile(`Total messages</div>\s*<div class="stat-value">(\d+)</div>`)

// connectorTotalMessages parses html (a GET
// /frag/connectors/{id}/stats response body) for its TotalMessages tile.
func connectorTotalMessages(t *testing.T, html string) int64 {
	t.Helper()
	m := totalMessagesPattern.FindStringSubmatch(html)
	if m == nil {
		t.Fatalf("connector stats fragment has no \"Total messages\" tile:\n%s", html)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		t.Fatalf("parse total messages %q: %v", m[1], err)
	}
	return n
}

// TestUIDrivenLifecycle drives an entire gateway lifecycle — create source,
// sink, and connector; observe filtered live delivery over SSE; observe the
// dashboard and connector-detail fragments reflect it; delete the
// connector; observe the dashboard fall back to its empty state — purely
// through the web UI's own form endpoints (form-encoded POSTs to
// /sources, /sinks, /connectors, mirroring exactly what a browser
// submitting internal/ui/templates/frag_source_form.html,
// frag_sink_form.html, and frag_connector_form.html sends), against an
// app.Run started with an empty store (no seed file). This proves the UI
// layer is a real, independent path to a running pipeline — not just a
// view onto config the JSON API wrote — and that its writes hot-apply to
// the already-running gateway exactly like the JSON API's do (see
// api_test.go's TestEndToEndAPIDrivenLifecycle, which proves the same thing
// for the JSON API). Staged as one test function (t.Log per step) rather
// than subtests: every step depends on state the previous step created in
// the one shared app, so t.Parallel subtests would race on that shared
// state for no benefit.
func TestUIDrivenLifecycle(t *testing.T) {
	fake := busfake.New()
	a := startEmptyApp(t, fake)
	adminBase := fmt.Sprintf("http://%s", a.AdminAddr())
	dataBase := fmt.Sprintf("http://%s", a.DataAddr())

	// Step 0: a brand-new, unseeded store starts with zero components, so
	// the dashboard fragment renders its empty state pointing at
	// /sources/new (nothing configured at all yet).
	t.Log("step 0: dashboard fragment shows the initial empty state, CTA -> /sources/new")
	dash := getBody(t, adminBase+"/frag/dashboard")
	if !strings.Contains(dash, `href="/sources/new"`) {
		t.Fatalf("initial dashboard fragment = %s, want empty-state CTA linking /sources/new", dash)
	}

	// Step 1: POST /sources creates source can0 (socketcan), the UI's
	// own create endpoint (see internal/ui/forms.go's handleSourceCreate /
	// writeSource) — form field names taken from frag_source_form.html
	// (id/name/type/enabled) and frag_source_type_fields.html's socketcan
	// branch (interface).
	t.Log("step 1: POST /sources creates source can0")
	mustStatus(t, postForm(t, adminBase+"/sources", url.Values{
		"id":        {"can0"},
		"name":      {"Main bus"},
		"type":      {"socketcan"},
		"enabled":   {"1"},
		"interface": {"can0"},
	}), http.StatusOK)

	// Step 2: POST /sinks creates sink nav (http_sse /nav) — field names
	// from frag_sink_form.html and frag_sink_type_fields.html's http_sse
	// branch (path).
	t.Log("step 2: POST /sinks creates sink nav")
	mustStatus(t, postForm(t, adminBase+"/sinks", url.Values{
		"id":      {"nav"},
		"name":    {"Nav stream"},
		"type":    {"http_sse"},
		"enabled": {"1"},
		"path":    {"/nav"},
	}), http.StatusOK)

	// Step 2b: configured endpoints remain visible in the graph before a
	// connector exists, but no route edges are rendered for them.
	t.Log("step 2b: dashboard graph shows the unconnected source and sink")
	dash = getBody(t, adminBase+"/frag/dashboard")
	for _, want := range []string{`data-dag-node="source:can0"`, `data-dag-node="sink:nav"`} {
		if !strings.Contains(dash, want) {
			t.Fatalf("dashboard fragment (endpoints configured, no connector) = %s, want graph node %q", dash, want)
		}
	}
	for _, notWant := range []string{`data-dag-from=`, `data-dag-to=`, "Add your first connector"} {
		if strings.Contains(dash, notWant) {
			t.Fatalf("dashboard fragment (endpoints configured, no connector) unexpectedly contains %q:\n%s", notWant, dash)
		}
	}

	// Step 3: POST /connectors creates connector heading (filter
	// pgn==127250, buffer max_messages 1000) — field names from
	// frag_connector_form.html (id/name/enabled/source_id/sink_id/filters/
	// max_messages).
	t.Log("step 3: POST /connectors creates connector heading")
	mustStatus(t, postForm(t, adminBase+"/connectors", url.Values{
		"id":           {"heading"},
		"name":         {"Heading only"},
		"enabled":      {"1"},
		"source_id":    {"can0"},
		"sink_id":      {"nav"},
		"filters":      {"msg.pgn == 127250"},
		"max_messages": {"1000"},
	}), http.StatusOK)

	// Step 4: the dashboard fragment's DAG now shows the heading connector
	// path instead of the empty-state hero.
	t.Log("step 4: dashboard fragment contains the connector path")
	dash = getBody(t, adminBase+"/frag/dashboard")
	for _, want := range []string{"Heading only", `href="/connectors/heading/"`} {
		if !strings.Contains(dash, want) {
			t.Fatalf("dashboard fragment = %s, want connector path containing %q", dash, want)
		}
	}
	if strings.Contains(dash, "Add your first") {
		t.Fatalf("dashboard fragment still shows the empty-state hero after creating a connector:\n%s", dash)
	}

	// Step 5: connect an SSE client, inject heading + depth frames, and
	// assert only the heading (127250) message arrives — proving the
	// UI-driven config actually hot-applied to the running gateway's data
	// server, not just to the config store.
	t.Log("step 5: SSE client observes only the filtered PGN (UI config hot-applied)")
	resp, err := http.Get(dataBase + "/nav")
	if err != nil {
		t.Fatal(err)
	}
	// Give the source's bus client time to claim its address (~50ms per
	// ExtraN2KOpts) and the SSE subscription time to register before
	// injecting frames that must be observed live.
	time.Sleep(300 * time.Millisecond)
	fake.Inject(busfake.VesselHeadingFrame()) // PGN 127250 — passes filter
	fake.Inject(busfake.WaterDepthFrame())    // PGN 128267 — filtered out
	fake.Inject(busfake.VesselHeadingFrame())

	events := sseEvents(t, resp, 2)
	for _, e := range events {
		if consumerEnvelopePGN(t, e) != 127250 {
			t.Fatalf("filter leaked event %v", e)
		}
	}
	_ = resp.Body.Close()

	// Step 6: poll the connector detail's live stats fragment until
	// TotalMessages >= 1. The dashboard card itself only shows rates
	// (msg/s, bytes/s), which are only nonzero within their trailing
	// window — polling for a rate > 0 would be flaky depending on timing —
	// so this asserts on the always-monotonic detail stats fragment
	// instead (see totalMessagesPattern's comment).
	t.Log("step 6: connector detail stats fragment reflects delivered messages")
	pollUntil(t, 2*time.Second, "connector detail TotalMessages >= 1", func() bool {
		html := getBody(t, adminBase+"/frag/connectors/heading/stats")
		return connectorTotalMessages(t, html) >= 1
	})

	// Step 7: POST /connectors/{id}/delete removes the connector via the
	// UI's own delete endpoint (see handleConnectorDelete).
	t.Log("step 7: POST /connectors/heading/delete removes it")
	mustStatus(t, postForm(t, adminBase+"/connectors/heading/delete", url.Values{}), http.StatusOK)

	// Step 8: removing the connector removes only its route and card. The
	// surviving source and sink remain visible as isolated graph nodes.
	t.Log("step 8: dashboard graph keeps the source and sink after connector deletion")
	dash = getBody(t, adminBase+"/frag/dashboard")
	if strings.Contains(dash, "Heading only") {
		t.Fatalf("dashboard fragment still shows the deleted connector's card:\n%s", dash)
	}
	for _, want := range []string{`data-dag-node="source:can0"`, `data-dag-node="sink:nav"`} {
		if !strings.Contains(dash, want) {
			t.Fatalf("post-delete dashboard fragment = %s, want surviving graph node %q", dash, want)
		}
	}
	for _, notWant := range []string{`data-dag-from=`, `data-dag-to=`, "Add your first connector"} {
		if strings.Contains(dash, notWant) {
			t.Fatalf("post-delete dashboard fragment unexpectedly contains %q:\n%s", notWant, dash)
		}
	}
}
