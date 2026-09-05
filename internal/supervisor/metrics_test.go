package supervisor

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
)

func TestReconcileRetiresCumulativeMetricsOnlyOnDeletion(t *testing.T) {
	st, unused, ds := setup(t)
	unused.Stop()
	met, handler, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	sup := New(st, nil, ds, slog.Default(), met, nil)
	t.Cleanup(sup.Stop)
	ctx := context.Background()
	apply := func(cfg model.Config) {
		t.Helper()
		if err := st.ReplaceConfig(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		if err := sup.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	scrape := func() string {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		return rec.Body.String()
	}
	cfg := baseConfig()
	apply(cfg)
	met.ConnectorMessages(ctx, "link", "received", 99)
	met.SourceMessages(ctx, "up", 99)
	met.SinkHTTPRequest(ctx, "out", "503", "gzip", 1, 10, 20, time.Second)
	// A hot restart and a disable retain the same configured entity's totals.
	cfg.Connectors[0].Name = "Renamed route"
	apply(cfg)
	cfg.Connectors[0].Enabled, cfg.Sources[0].Enabled, cfg.Sinks[0].Enabled = false, false, false
	apply(cfg)
	body := scrape()
	for _, label := range []string{`connector="link"`, `source="up"`, `sink="out"`} {
		if !strings.Contains(body, label) {
			t.Fatalf("disable lost %s", label)
		}
	}
	if strings.Count(body, " 99\n") != 2 {
		t.Fatalf("hot restart reset counters:\n%s", body)
	}
	apply(model.Config{})
	body = scrape()
	for _, label := range []string{`connector="link"`, `source="up"`, `sink="out"`} {
		if strings.Contains(body, label) {
			t.Fatalf("deletion retained %s:\n%s", label, body)
		}
	}
}
