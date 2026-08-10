package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/open-ships/beacon/internal/mcpserver"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/sink"
)

func startTestApp(t *testing.T) *App {
	t.Helper()
	dir := t.TempDir()
	a, err := Run(context.Background(), Options{
		DBPath:    filepath.Join(dir, "test.db"),
		DataAddr:  "127.0.0.1:0",
		AdminAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	return a
}

func TestAdminServerSetsBoundedHTTPTimeoutsWithoutWriteTimeout(t *testing.T) {
	a := startTestApp(t)
	if a.adminSrv.ReadHeaderTimeout != adminReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", a.adminSrv.ReadHeaderTimeout, adminReadHeaderTimeout)
	}
	if a.adminSrv.ReadTimeout != adminReadTimeout || a.adminSrv.IdleTimeout != adminIdleTimeout {
		t.Fatalf("read/idle timeouts = %s/%s, want %s/%s",
			a.adminSrv.ReadTimeout, a.adminSrv.IdleTimeout, adminReadTimeout, adminIdleTimeout)
	}
	if a.adminSrv.MaxHeaderBytes != adminMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", a.adminSrv.MaxHeaderBytes, adminMaxHeaderBytes)
	}
	if a.adminSrv.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout = %s, want zero for streaming responses", a.adminSrv.WriteTimeout)
	}
}

func TestAdminRequestBodyLimit(t *testing.T) {
	payload := bytes.Repeat([]byte{'x'}, maxAdminRequestBytes+1)
	called := false
	handler := limitAdminRequestBody(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge || called {
		t.Fatalf("known-length oversized request = status %d called=%v, want 413 before dispatch", rec.Code, called)
	}

	var readBytes int64
	var readErr error
	handler = limitAdminRequestBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readBytes, readErr = io.Copy(io.Discard, r.Body)
		var tooLarge *http.MaxBytesError
		if errors.As(readErr, &tooLarge) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	req.ContentLength = -1 // model Transfer-Encoding: chunked / unknown length
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var tooLarge *http.MaxBytesError
	if rec.Code != http.StatusRequestEntityTooLarge || readBytes != maxAdminRequestBytes ||
		!errors.As(readErr, &tooLarge) {
		t.Fatalf("chunked oversized request = status %d bytes %d err %v", rec.Code, readBytes, readErr)
	}

	app := startTestApp(t)
	resp, err := http.Post("http://"+app.AdminAddr()+"/api/v1/filters/validate", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("admin mux oversized request = %d, want 413", resp.StatusCode)
	}
}

func waitForApp(t *testing.T, timeout time.Duration, desc string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func TestAdminDataAndTCPSinksShareAcceptedConnectionBudget(t *testing.T) {
	a := startTestApp(t)
	if got := a.connLimit.Capacity(); got != maxAcceptedConnections {
		t.Fatalf("connection capacity = %d, want %d", got, maxAcceptedConnections)
	}
	adminConn, err := net.Dial("tcp", a.AdminAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adminConn.Close() }()
	dataConn, err := net.Dial("tcp", a.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dataConn.Close() }()
	tcpRuntime, err := sink.New(context.Background(), model.Sink{
		ID: "shared-budget", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:0",
	}, nil, a.ds, a.log, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tcpRuntime.Stop()
	tcpConn, err := net.Dial("tcp", tcpRuntime.(interface{ Addr() string }).Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tcpConn.Close() }()
	waitForApp(t, time.Second, "all listeners to account accepted connections", func() bool {
		return a.connLimit.InUse() == 3
	})
	_ = adminConn.Close()
	_ = dataConn.Close()
	_ = tcpConn.Close()
	waitForApp(t, time.Second, "accepted connections to release their slots", func() bool {
		return a.connLimit.InUse() == 0
	})
}

func TestPersistedSettingsHotGatePrometheusSourceDetails(t *testing.T) {
	a := startTestApp(t)
	a.Stats().RecordSource("can0", &msg.Envelope{PGN: 127250, Source: 12})

	scrape := func() string {
		t.Helper()
		resp, err := http.Get("http://" + a.AdminAddr() + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}

	if body := scrape(); strings.Contains(body, "beacon_source_pgn_") {
		t.Fatalf("default metrics included rich source details:\n%s", body)
	}
	optIn := model.Config{Settings: &model.Settings{Observability: &model.ObservabilityConfig{
		PrometheusSourceDetails: true,
	}}}
	if err := a.Service().Import(context.Background(), optIn, true); err != nil {
		t.Fatal(err)
	}
	if body := scrape(); !strings.Contains(body, "beacon_source_pgn_messages_total") {
		t.Fatalf("opted-in metrics omitted source details:\n%s", body)
	}
	if err := a.Service().Import(context.Background(), model.Config{}, true); err != nil {
		t.Fatal(err)
	}
	if body := scrape(); strings.Contains(body, "beacon_source_pgn_") {
		t.Fatalf("metrics retained source details after opt-out:\n%s", body)
	}
}

func getHealthResponse(t *testing.T, a *App, path string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get("http://" + a.AdminAddr() + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

func getHealth(t *testing.T, a *App) map[string]any {
	t.Helper()
	_, body := getHealthResponse(t, a, "/health")
	return body
}

func TestHealthSeparatesLivenessAndReadiness(t *testing.T) {
	a := startTestApp(t)
	if code, body := getHealthResponse(t, a, "/health/live"); code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("live health = status %d body %+v", code, body)
	}
	if code, body := getHealthResponse(t, a, "/health/ready"); code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("initial readiness = status %d body %+v", code, body)
	}

	// A closed durable store makes desired-state reads and queue persistence
	// impossible, but the process itself is still live and must not invite an
	// orchestrator restart loop.
	if err := a.st.Close(); err != nil {
		t.Fatal(err)
	}
	if code, body := getHealthResponse(t, a, "/health/ready"); code != http.StatusServiceUnavailable || body["status"] != "degraded" {
		t.Fatalf("closed-store readiness = status %d body %+v", code, body)
	}
	if code, body := getHealthResponse(t, a, "/health"); code != http.StatusServiceUnavailable || body["status"] != "degraded" {
		t.Fatalf("closed-store compatibility health = status %d body %+v", code, body)
	}
	if code, body := getHealthResponse(t, a, "/api/v1/health"); code != http.StatusServiceUnavailable ||
		!healthHasSystemComponent(body, "store", "error") {
		t.Fatalf("closed-store REST readiness = status %d body %+v", code, body)
	}
	if code, body := getHealthResponse(t, a, "/health/live"); code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("liveness after store failure = status %d body %+v", code, body)
	}
}

func healthHasSystemComponent(body map[string]any, id, state string) bool {
	components, _ := body["components"].([]any)
	for _, item := range components {
		component, _ := item.(map[string]any)
		if component["kind"] == "system" && component["id"] == id && component["state"] == state {
			return true
		}
	}
	return false
}

func TestDataServerFailureIsReadinessVisibleButNotLivenessFailure(t *testing.T) {
	a := startTestApp(t)
	if err := a.ds.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	code, body := getHealthResponse(t, a, "/health/ready")
	if code != http.StatusServiceUnavailable || !healthHasSystemComponent(body, "data_server", "error") {
		t.Fatalf("readiness after data stop = status %d body %+v", code, body)
	}
	if code, body = getHealthResponse(t, a, "/api/v1/health"); code != http.StatusServiceUnavailable ||
		!healthHasSystemComponent(body, "data_server", "error") {
		t.Fatalf("REST readiness after data stop = status %d body %+v", code, body)
	}
	if code, body = getHealthResponse(t, a, "/health/live"); code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("liveness after data stop = status %d body %+v", code, body)
	}
}

func TestAdminFailureIsSharedByTopLevelAndRESTReadiness(t *testing.T) {
	a := startTestApp(t)
	a.adminMu.Lock()
	a.adminServeErr = errors.New("accept loop failed")
	a.adminMu.Unlock()

	for _, path := range []string{"/health/ready", "/api/v1/health"} {
		code, body := getHealthResponse(t, a, path)
		if code != http.StatusServiceUnavailable || !healthHasSystemComponent(body, "admin_server", "error") {
			t.Fatalf("%s with admin failure = status %d body %+v", path, code, body)
		}
	}
}

func TestPersistentMaintenanceFailureDegradesHealthWithout503(t *testing.T) {
	a := startTestApp(t)
	for range maintenanceFailureThreshold {
		a.recordMaintenanceResult(errors.New("checkpoint failed"))
	}
	code, body := getHealthResponse(t, a, "/health")
	if code != http.StatusOK || body["status"] != "degraded" ||
		!healthHasSystemComponent(body, "store_maintenance", "degraded") {
		t.Fatalf("maintenance-degraded health = status %d body %+v", code, body)
	}
	if code, body = getHealthResponse(t, a, "/health/live"); code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("maintenance-degraded liveness = status %d body %+v", code, body)
	}
	a.recordMaintenanceResult(nil)
	if code, body = getHealthResponse(t, a, "/health"); code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("health after maintenance recovery = status %d body %+v", code, body)
	}
}

func TestMaintenanceLoopAttemptsWithAppLifecycle(t *testing.T) {
	oldInterval, oldTimeout := maintenanceInterval, maintenanceAttemptTimeout
	maintenanceInterval, maintenanceAttemptTimeout = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { maintenanceInterval, maintenanceAttemptTimeout = oldInterval, oldTimeout })
	a := startTestApp(t)
	if err := a.st.Close(); err != nil {
		t.Fatal(err)
	}
	waitForApp(t, time.Second, "persistent periodic maintenance error", func() bool {
		return a.persistentMaintenanceError() != nil
	})
	if code, body := getHealthResponse(t, a, "/health/live"); code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("liveness after maintenance failures = status %d body %+v", code, body)
	}
}

func TestAdminServeLoopRecoversUnexpectedListenerFailure(t *testing.T) {
	oldMin, oldMax := adminRetryMin, adminRetryMax
	adminRetryMin, adminRetryMax = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { adminRetryMin, adminRetryMax = oldMin, oldMax })
	a := startTestApp(t)
	address := a.AdminAddr()

	a.adminMu.RLock()
	ln := a.adminLn
	a.adminMu.RUnlock()
	if ln == nil {
		t.Fatal("admin listener is nil")
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + address + "/health/live")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if got := a.AdminAddr(); got != address {
					t.Fatalf("admin rebound on %q, want stable address %q", got, address)
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("admin server did not recover listener %s", address)
}

// TestHandleHealthRollup exercises the admin server's top-level GET /health
// end to end for both outcomes of the shared supervisor.RollupHealth helper
// (also used by the config API's get-health, covered separately by
// internal/api's TestGetHealthAPI) so the two endpoints can never drift on
// the "any component not up -> degraded" rule.
func TestHandleHealthRollup(t *testing.T) {
	a := startTestApp(t)

	if body := getHealth(t, a); body["status"] != "ok" {
		t.Fatalf("fresh store /health status = %v, want ok", body["status"])
	}

	// A sink that fails to start (invalid bind address) surfaces as an
	// "error" component status; PutSink's write-triggered reconcile runs
	// synchronously, so the failure is visible as soon as PutSink returns.
	err := a.Service().PutSink(context.Background(), model.Sink{
		ID: "bad", Name: "Bad", Type: model.SinkTCP, Enabled: true, Address: "256.256.256.256:1",
	}, true)
	if err != nil {
		t.Fatalf("PutSink: %v", err)
	}

	body := getHealth(t, a)
	if body["status"] != "degraded" {
		t.Fatalf("/health status = %v, want degraded after broken sink added", body["status"])
	}
	components, ok := body["components"].([]any)
	if !ok || len(components) != 1 {
		t.Fatalf("/health components = %v, want 1 errored component", body["components"])
	}
}

func TestDocsRoutesAreServedAtRoot(t *testing.T) {
	a := startTestApp(t)
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	cases := []struct {
		path       string
		wantStatus int
		want       string
	}{
		{"/docs", http.StatusFound, "/docs/getting-started"},
		{"/docs/getting-started", http.StatusOK, ""},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			resp, err := client.Get("http://" + a.AdminAddr() + c.path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != c.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, c.wantStatus)
			}
			if loc := resp.Header.Get("Location"); loc != c.want {
				t.Fatalf("Location = %q, want %q", loc, c.want)
			}
		})
	}
}

func TestAdminUIIsMountedAtRoot(t *testing.T) {
	a := startTestApp(t)
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	cases := []struct {
		path       string
		wantStatus int
		want       string
	}{
		{"/", http.StatusFound, "/dashboard"},
		{"/dashboard", http.StatusOK, ""},
		{"/sources", http.StatusOK, ""},
		{"/mcp/info", http.StatusOK, ""},
		{"/ui", http.StatusNotFound, ""},
		{"/ui/dashboard", http.StatusNotFound, ""},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			resp, err := client.Get("http://" + a.AdminAddr() + c.path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != c.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, c.wantStatus)
			}
			if loc := resp.Header.Get("Location"); loc != c.want {
				t.Fatalf("Location = %q, want %q", loc, c.want)
			}
		})
	}
}

func TestMCPIsMountedOnAdminServer(t *testing.T) {
	a := startTestApp(t)
	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "app-test", Version: "test"}, nil)
	session, err := client.Connect(context.Background(), &sdkmcp.StreamableClientTransport{
		Endpoint: "http://" + a.AdminAddr() + mcpserver.EndpointPath,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()

	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != len(mcpserver.Catalog()) {
		t.Fatalf("tools = %d, want %d", len(tools.Tools), len(mcpserver.Catalog()))
	}
}
