package app

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

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

func getHealth(t *testing.T, a *App) map[string]any {
	t.Helper()
	resp, err := http.Get("http://" + a.AdminAddr() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
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
