package app

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/open-ships/beacon/internal/mcpserver"
	"github.com/open-ships/beacon/internal/model"
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

func TestAdminServerSetsReadHeaderTimeout(t *testing.T) {
	a := startTestApp(t)
	if a.adminSrv.ReadHeaderTimeout != 10*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want 10s", a.adminSrv.ReadHeaderTimeout)
	}
}

func getHealth(t *testing.T, a *App) map[string]any {
	t.Helper()
	resp, err := http.Get("http://" + a.AdminAddr() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body
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
