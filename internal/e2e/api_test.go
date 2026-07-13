package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/app"
	"github.com/open-ships/beacon/internal/bus/busfake"
	"github.com/open-ships/beacon/internal/model"
)

// startEmptyApp is startApp's sibling for this file's scenario: it boots a
// real app.Run against a brand-new, unseeded store (no SeedPath), so the
// gateway starts with zero components and every source/sink/connector in
// this test is created purely by driving the admin HTTP API — the point of
// this file is to prove the API alone can take a gateway from nothing to a
// running pipeline and back down again.
func startEmptyApp(t *testing.T, fake *busfake.FakeBus) *app.App {
	t.Helper()
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a, err := app.Run(ctx, app.Options{
		DBPath:    filepath.Join(dir, "beacon.db"),
		DataAddr:  "127.0.0.1:0",
		AdminAddr: "127.0.0.1:0",
		Log:       slog.Default(),
		ExtraN2KOpts: []n2k.Option{
			n2k.WithBus(fake), n2k.WithClaimTimeout(50 * time.Millisecond)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	return a
}

// doJSON issues method against url with body JSON-encoded (nil for no
// body), setting Content-Type only when there is a body to describe —
// mirrors internal/api's own test helper of the same name (different
// package, so no collision) since huma requires the header to bind JSON.
func doJSON(t *testing.T, method, url string, body any) *http.Response {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// mustStatus fails the test with the response body if resp's status isn't
// want, then closes the body (the caller is done with it either way).
func mustStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != want {
		var buf bytes.Buffer
		buf.ReadFrom(resp.Body)
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, want, buf.String())
	}
}

// decodeJSON fails the test if resp's body doesn't decode into v as JSON,
// then closes the body.
func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

// pollUntil polls cond every 20ms until it returns true or timeout elapses,
// failing the test in the latter case. Used in place of a fixed sleep
// wherever the thing being waited on (async connector delivery, a stats
// counter) doesn't have a blocking API of its own to wait on directly.
func pollUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// assertNoSSEEvents reads resp's body for window and fails the test if any
// SSE "data:" line arrives in that time; a closed stream before the window
// elapses is not a failure (there's simply nothing left to deliver). Used
// for the negative assertion in step 6: after a connector's filter changes
// to exclude a PGN, a frame of that PGN injected afterward must never reach
// a fresh subscriber.
func assertNoSSEEvents(t *testing.T, resp *http.Response, window time.Duration) {
	t.Helper()
	sc := bufio.NewScanner(resp.Body)
	lines := make(chan string, 8)
	go func() {
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	deadline := time.After(window)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				return
			}
			if strings.HasPrefix(line, "data:") {
				t.Fatalf("unexpected SSE event within %s: %s", window, line)
			}
		case <-deadline:
			return
		}
	}
}

// metricsSnapshot is the JSON shape of GET /api/v1/connectors/{id}/metrics
// (internal/stats.Snapshot), duplicated here rather than imported so this
// test exercises the API's actual wire contract instead of the internal Go
// type.
type metricsSnapshot struct {
	TotalMessages int64   `json:"total_messages"`
	TotalBytes    int64   `json:"total_bytes"`
	MsgPerSec     float64 `json:"msg_per_sec"`
	BytesPerSec   float64 `json:"bytes_per_sec"`
	QueueDepth    int64   `json:"queue_depth"`
	QueueBytes    int64   `json:"queue_bytes"`
}

// TestEndToEndAPIDrivenLifecycle drives an entire gateway lifecycle —
// create source, sink, and connector; observe filtered live delivery over
// SSE; read live metrics; hot-apply a filter change and observe delivery
// stop; delete the connector and observe it fall out of health/metrics;
// export the resulting config — purely through the admin HTTP API, against
// an app.Run started with an empty store (no seed file). Staged as one test
// function (t.Log per step) rather than subtests: every step depends on
// state the previous step created in the one shared app, so t.Parallel
// subtests would race on that shared state for no benefit.
func TestEndToEndAPIDrivenLifecycle(t *testing.T) {
	fake := busfake.New()
	a := startEmptyApp(t, fake)
	adminBase := fmt.Sprintf("http://%s", a.AdminAddr())
	dataBase := fmt.Sprintf("http://%s", a.DataAddr())

	// Step 0: a brand-new, unseeded store starts with zero components.
	t.Log("step 0: verify empty store starts with zero components")
	var health0 struct {
		Status     string `json:"status"`
		Components []struct {
			Kind, ID, State string
		} `json:"components"`
	}
	decodeJSON(t, doJSON(t, http.MethodGet, adminBase+"/health", nil), &health0)
	if len(health0.Components) != 0 {
		t.Fatalf("fresh store components = %+v, want none", health0.Components)
	}

	// Step 1: PUT /api/v1/sources/can0 (socketcan).
	t.Log("step 1: PUT source can0")
	mustStatus(t, doJSON(t, http.MethodPut, adminBase+"/api/v1/sources/can0", model.Source{
		ID: "can0", Name: "Main bus", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}), http.StatusOK)

	// Step 2: PUT /api/v1/sinks/nav (http_sse /nav).
	t.Log("step 2: PUT sink nav")
	mustStatus(t, doJSON(t, http.MethodPut, adminBase+"/api/v1/sinks/nav", model.Sink{
		ID: "nav", Name: "Nav stream", Type: model.SinkHTTPSSE, Enabled: true, Path: "/nav",
	}), http.StatusOK)

	// Step 3: PUT /api/v1/connectors/heading (filter pgn==127250, buffer 1000).
	t.Log("step 3: PUT connector heading")
	mustStatus(t, doJSON(t, http.MethodPut, adminBase+"/api/v1/connectors/heading", model.Connector{
		ID: "heading", Name: "Heading only", SourceID: "can0", SinkID: "nav", Enabled: true,
		Filters: []string{"msg.pgn == 127250"},
		Buffer:  model.BufferLimits{MaxMessages: 1000},
	}), http.StatusOK)

	// Step 4: connect an SSE client, inject heading + depth frames, and
	// assert only the heading (127250) messages arrive, with the "info"
	// object stripped from the payload (wire-freeze regression).
	t.Log("step 4: SSE client observes only the filtered PGN, with info stripped")
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
		if e["pgn"].(float64) != 127250 {
			t.Fatalf("filter leaked pgn %v", e["pgn"])
		}
		payload, ok := e["payload"].(map[string]any)
		if !ok {
			t.Fatalf("payload not an object: %v", e["payload"])
		}
		if _, hasInfo := payload["info"]; hasInfo {
			t.Fatalf("SSE payload still contains info: %v", payload)
		}
	}
	resp.Body.Close()

	// Step 5: GET .../heading/metrics -> total_messages >= 1.
	t.Log("step 5: connector metrics reflect delivered messages")
	pollUntil(t, 2*time.Second, "total_messages >= 1", func() bool {
		var snap metricsSnapshot
		decodeJSON(t, doJSON(t, http.MethodGet, adminBase+"/api/v1/connectors/heading/metrics", nil), &snap)
		return snap.TotalMessages >= 1
	})

	// Step 6: hot-apply a filter change (pgn==999) and confirm a
	// subsequently-injected heading frame is never delivered to a *fresh*
	// SSE connection (no Last-Event-ID -> live-only broadcast, so the old
	// connector instance's stale queue entries can't leak into this
	// assertion; a bounded window with zero events is the pass condition).
	t.Log("step 6: hot-apply filter change; heading frame no longer delivered")
	mustStatus(t, doJSON(t, http.MethodPut, adminBase+"/api/v1/connectors/heading", model.Connector{
		ID: "heading", Name: "Heading only", SourceID: "can0", SinkID: "nav", Enabled: true,
		Filters: []string{"msg.pgn == 999"},
		Buffer:  model.BufferLimits{MaxMessages: 1000},
	}), http.StatusOK)

	resp2, err := http.Get(dataBase + "/nav")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // let the fresh subscription register
	fake.Inject(busfake.VesselHeadingFrame())
	assertNoSSEEvents(t, resp2, 1*time.Second)
	resp2.Body.Close()

	// Step 7: DELETE the connector; its metrics endpoint 404s (the 404
	// comes from svc.GetConnector failing once the config entity is gone,
	// independent of the stats registry — exactly what this asserts) and
	// /health drops to 2 components (source + sink, connector gone).
	t.Log("step 7: DELETE connector; metrics 404s and health drops to 2 components")
	mustStatus(t, doJSON(t, http.MethodDelete, adminBase+"/api/v1/connectors/heading", nil), http.StatusOK)

	mustStatus(t, doJSON(t, http.MethodGet, adminBase+"/api/v1/connectors/heading/metrics", nil), http.StatusNotFound)

	var health1 struct {
		Status     string `json:"status"`
		Components []struct {
			Kind, ID, State string
		} `json:"components"`
	}
	decodeJSON(t, doJSON(t, http.MethodGet, adminBase+"/health", nil), &health1)
	if len(health1.Components) != 2 {
		t.Fatalf("post-delete components = %+v, want 2 (source + sink)", health1.Components)
	}

	// Step 8: export contains the source and sink but no connectors (the
	// JSON connectors field may serialize as null or [] — both mean zero).
	t.Log("step 8: config export has the source and sink, zero connectors")
	var cfg model.Config
	decodeJSON(t, doJSON(t, http.MethodGet, adminBase+"/api/v1/config/export", nil), &cfg)
	if len(cfg.Sources) != 1 || cfg.Sources[0].ID != "can0" {
		t.Fatalf("export sources = %+v, want [can0]", cfg.Sources)
	}
	if len(cfg.Sinks) != 1 || cfg.Sinks[0].ID != "nav" {
		t.Fatalf("export sinks = %+v, want [nav]", cfg.Sinks)
	}
	if len(cfg.Connectors) != 0 {
		t.Fatalf("export connectors = %+v, want none", cfg.Connectors)
	}
}
