package api_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/api"
	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
)

// newStatsServer mirrors newTestServer (entities_test.go) but also returns
// the stats.Registry the server was built with, so tests can Record
// deliveries and assert the metrics endpoints reflect them.
func newStatsServer(t *testing.T, runtimeInfo ...api.RuntimeInfo) (srv *httptest.Server, rec *fakeReconciler, reg *stats.Registry) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rec = &fakeReconciler{}
	svc := config.NewService(st, rec, nil)
	reg = stats.NewRegistry()
	handler, _ := api.New(svc, reg, "test-version", nil, runtimeInfo...)
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	return s, rec, reg
}

// --- Filter validation ---

func TestValidateFilters(t *testing.T) {
	srv, _, _ := newStatsServer(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/v1/filters/validate", map[string]any{
		"filters": []string{"msg.pgn == 127250"},
	})
	mustStatus(t, resp, http.StatusOK)
	var body struct {
		Valid bool `json:"valid"`
	}
	decodeInto(t, resp, &body)
	if !body.Valid {
		t.Fatalf("valid = false, want true")
	}
}

func TestValidateFiltersInvalid(t *testing.T) {
	srv, _, _ := newStatsServer(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/v1/filters/validate", map[string]any{
		"filters": []string{"msg.pgn =="},
	})
	mustStatus(t, resp, http.StatusUnprocessableEntity)
	if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q, want application/problem+json", ct)
	}
}

// --- System discovery ---

func TestGetSystem(t *testing.T) {
	srv, _, _ := newStatsServer(t, api.RuntimeInfo{Buses: func() []bus.EndpointStatus {
		return []bus.EndpointStatus{{
			Endpoint: "socketcan:can0", Kind: "socketcan", Name: "can0", State: "up",
			Address: 42, AddressClaimed: true, WriteQueueCapacity: 64, ReceiveSubscribers: 1,
		}}
	}})

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/system", nil)
	mustStatus(t, resp, http.StatusOK)
	var body struct {
		Version       string               `json:"version"`
		CANInterfaces []string             `json:"can_interfaces"`
		SerialPorts   []string             `json:"serial_ports"`
		Buses         []bus.EndpointStatus `json:"n2k_buses"`
	}
	decodeInto(t, resp, &body)
	if body.Version != "test-version" {
		t.Fatalf("version = %q, want %q", body.Version, "test-version")
	}
	if body.CANInterfaces == nil {
		t.Fatal("can_interfaces = null, want an array (possibly empty)")
	}
	if body.SerialPorts == nil {
		t.Fatal("serial_ports = null, want an array (possibly empty)")
	}
	if len(body.Buses) != 1 || body.Buses[0].Endpoint != "socketcan:can0" ||
		!body.Buses[0].AddressClaimed || body.Buses[0].WriteQueueCapacity != 64 {
		t.Fatalf("n2k_buses = %+v, want bounded runtime status", body.Buses)
	}
}

func TestGetN2KCatalogPGN(t *testing.T) {
	srv, _, _ := newStatsServer(t)
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/n2k/pgns/127250", nil)
	mustStatus(t, resp, http.StatusOK)
	var body struct {
		PGN      uint32 `json:"pgn"`
		Variants []struct {
			Description string         `json:"description"`
			Fields      map[string]any `json:"fields"`
		} `json:"variants"`
	}
	decodeInto(t, resp, &body)
	if body.PGN != 127250 || len(body.Variants) == 0 || body.Variants[0].Description != "Vessel Heading" || len(body.Variants[0].Fields) == 0 {
		t.Fatalf("catalog response = %+v", body)
	}
}

func TestCommissioningReportIsAvailableWithoutHardware(t *testing.T) {
	srv, _, _ := newStatsServer(t)
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/n2k/commissioning-report", nil)
	mustStatus(t, resp, http.StatusOK)
	var body struct {
		Devices    []any                     `json:"devices"`
		Buses      []bus.EndpointStatus      `json:"buses"`
		Connectors map[string]stats.Snapshot `json:"connectors"`
	}
	decodeInto(t, resp, &body)
	if body.Devices == nil || body.Buses == nil || body.Connectors == nil {
		t.Fatalf("report contains null collections: %+v", body)
	}
}

// --- Metrics ---

func TestConnectorMetrics(t *testing.T) {
	srv, _, reg := newStatsServer(t)
	mustStatus(t, putEntity(t, srv, "sources", "s1", model.Source{
		ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}), http.StatusOK)
	mustStatus(t, putEntity(t, srv, "sinks", "k1", model.Sink{
		ID: "k1", Name: "K1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a",
	}), http.StatusOK)
	mustStatus(t, putEntity(t, srv, "connectors", "c1", model.Connector{
		ID: "c1", Name: "C1", SourceID: "s1", SinkID: "k1", Enabled: true,
	}), http.StatusOK)

	// No traffic yet: zero snapshot, still 200.
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/connectors/c1/metrics", nil)
	mustStatus(t, resp, http.StatusOK)
	var snap stats.Snapshot
	decodeInto(t, resp, &snap)
	if snap.TotalMessages != 0 {
		t.Fatalf("zero snapshot TotalMessages = %d, want 0", snap.TotalMessages)
	}

	reg.Record("c1", 5, 500)
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/connectors/c1/metrics", nil)
	mustStatus(t, resp, http.StatusOK)
	decodeInto(t, resp, &snap)
	if snap.TotalMessages != 5 || snap.TotalBytes != 500 {
		t.Fatalf("snapshot = %+v, want TotalMessages=5 TotalBytes=500", snap)
	}

	// depth_history (queue-depth sparkline, spec §6): additive JSON field,
	// present once SetQueue has been called.
	reg.SetQueue("c1", 3, 300)
	reg.SetQueue("c1", 8, 800)
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/connectors/c1/metrics", nil)
	mustStatus(t, resp, http.StatusOK)
	decodeInto(t, resp, &snap)
	if want := []int64{3, 8}; len(snap.DepthHistory) != len(want) || snap.DepthHistory[0] != want[0] || snap.DepthHistory[1] != want[1] {
		t.Fatalf("DepthHistory = %v, want %v", snap.DepthHistory, want)
	}
}

func TestConnectorMetricsUnknown(t *testing.T) {
	srv, _, _ := newStatsServer(t)

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/connectors/nope/metrics", nil)
	mustStatus(t, resp, http.StatusNotFound)
}

func TestListMetrics(t *testing.T) {
	srv, _, reg := newStatsServer(t)
	reg.Record("c1", 3, 300)
	reg.Record("c2", 7, 700)
	reg.SetQueue("c1", 4, 400)

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/metrics", nil)
	mustStatus(t, resp, http.StatusOK)
	var body struct {
		Connectors map[string]stats.Snapshot `json:"connectors"`
	}
	decodeInto(t, resp, &body)
	if len(body.Connectors) != 2 {
		t.Fatalf("connectors = %+v, want 2 entries", body.Connectors)
	}
	if body.Connectors["c1"].TotalMessages != 3 {
		t.Fatalf("c1 TotalMessages = %d, want 3", body.Connectors["c1"].TotalMessages)
	}
	if body.Connectors["c2"].TotalBytes != 700 {
		t.Fatalf("c2 TotalBytes = %d, want 700", body.Connectors["c2"].TotalBytes)
	}
	// depth_history (queue-depth sparkline, spec §6): present for c1 (had a
	// SetQueue call), absent for c2 (Record-only, never SetQueue'd).
	if want := []int64{4}; len(body.Connectors["c1"].DepthHistory) != 1 || body.Connectors["c1"].DepthHistory[0] != want[0] {
		t.Fatalf("c1 DepthHistory = %v, want %v", body.Connectors["c1"].DepthHistory, want)
	}
	if body.Connectors["c2"].DepthHistory != nil {
		t.Fatalf("c2 DepthHistory = %v, want nil (never SetQueue'd)", body.Connectors["c2"].DepthHistory)
	}
}

// --- Export / Import ---

func seedConfig(t *testing.T, srv *httptest.Server) {
	t.Helper()
	mustStatus(t, putEntity(t, srv, "sources", "s1", model.Source{
		ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}), http.StatusOK)
	mustStatus(t, putEntity(t, srv, "sinks", "k1", model.Sink{
		ID: "k1", Name: "K1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a",
	}), http.StatusOK)
	mustStatus(t, putEntity(t, srv, "connectors", "c1", model.Connector{
		ID: "c1", Name: "C1", SourceID: "s1", SinkID: "k1", Enabled: true,
		Filters: []string{"msg.pgn == 127250"},
	}), http.StatusOK)
}

// GET /api/v1/config/export on an empty store must serialize its array
// fields as "[]", not "null" — some JSON clients (and the Phase 3 UI) don't
// tolerate a null array where an empty list is expected.
func TestExportEmptyStoreReturnsEmptyArraysNotNull(t *testing.T) {
	srv, _, _ := newStatsServer(t)

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/config/export", nil)
	mustStatus(t, resp, http.StatusOK)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
	for _, field := range []string{`"sources":[]`, `"sinks":[]`, `"connectors":[]`} {
		if !strings.Contains(got, field) {
			t.Fatalf("export body = %s, want it to contain %s", got, field)
		}
	}
	if strings.Contains(got, "null") {
		t.Fatalf("export body = %s, want no null fields", got)
	}
}

func TestExportImportRoundTrip(t *testing.T) {
	srv, _, _ := newStatsServer(t)
	seedConfig(t, srv)

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/config/export", nil)
	mustStatus(t, resp, http.StatusOK)
	var exported model.Config
	decodeInto(t, resp, &exported)
	if len(exported.Sources) != 1 || len(exported.Sinks) != 1 || len(exported.Connectors) != 1 {
		t.Fatalf("exported = %+v, want 1 of each", exported)
	}

	// Import (default mode, i.e. replace) into a fresh server and assert
	// equality with what was exported.
	srv2, _, _ := newStatsServer(t)
	resp = doJSON(t, http.MethodPost, srv2.URL+"/api/v1/config/import", exported)
	mustStatus(t, resp, http.StatusOK)

	resp = doJSON(t, http.MethodGet, srv2.URL+"/api/v1/config/export", nil)
	mustStatus(t, resp, http.StatusOK)
	var reimported model.Config
	decodeInto(t, resp, &reimported)
	if len(reimported.Sources) != 1 || reimported.Sources[0].ID != "s1" {
		t.Fatalf("reimported sources = %+v", reimported.Sources)
	}
	if len(reimported.Connectors) != 1 || reimported.Connectors[0].ID != "c1" {
		t.Fatalf("reimported connectors = %+v", reimported.Connectors)
	}
}

func TestImportReplace(t *testing.T) {
	srv, _, _ := newStatsServer(t)
	seedConfig(t, srv)

	// Replacing with a config that drops the connector removes it.
	repl := model.Config{
		Sources: []model.Source{{ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}},
		Sinks:   []model.Sink{{ID: "k1", Name: "K1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a"}},
	}
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/v1/config/import?mode=replace", repl)
	mustStatus(t, resp, http.StatusOK)

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/config/export", nil)
	var cfg model.Config
	decodeInto(t, resp, &cfg)
	if len(cfg.Connectors) != 0 {
		t.Fatalf("connectors after replace = %+v, want none", cfg.Connectors)
	}
}

func TestImportMerge(t *testing.T) {
	srv, _, _ := newStatsServer(t)
	seedConfig(t, srv)

	// Merge in a new source only; existing entities must survive untouched.
	partial := model.Config{
		Sources: []model.Source{{ID: "s2", Name: "S2", Type: model.SourceSocketCAN, Enabled: true, Interface: "can1"}},
	}
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/v1/config/import?mode=merge", partial)
	mustStatus(t, resp, http.StatusOK)

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/config/export", nil)
	var cfg model.Config
	decodeInto(t, resp, &cfg)
	if len(cfg.Sources) != 2 {
		t.Fatalf("sources after merge = %+v, want 2", cfg.Sources)
	}
	if len(cfg.Connectors) != 1 {
		t.Fatalf("connectors after merge = %+v, want untouched 1", cfg.Connectors)
	}
}

func TestImportInvalid(t *testing.T) {
	srv, _, _ := newStatsServer(t)
	seedConfig(t, srv)

	bad := model.Config{
		Sources: []model.Source{{ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}},
		Sinks:   []model.Sink{{ID: "k1", Name: "K1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a"}},
		Connectors: []model.Connector{
			{ID: "c1", Name: "C1", SourceID: "s1", SinkID: "k1", Enabled: true, Filters: []string{"msg.pgn =="}},
		},
	}
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/v1/config/import", bad)
	mustStatus(t, resp, http.StatusUnprocessableEntity)

	// Store must be unchanged (the original valid c1 filter survives).
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/config/export", nil)
	var cfg model.Config
	decodeInto(t, resp, &cfg)
	if len(cfg.Connectors) != 1 || len(cfg.Connectors[0].Filters) != 1 || cfg.Connectors[0].Filters[0] != "msg.pgn == 127250" {
		t.Fatalf("config after invalid import = %+v, want unchanged", cfg)
	}
}

// --- Health ---

func TestGetHealthAPI(t *testing.T) {
	srv, rec, _ := newStatsServer(t)
	rec.setStatuses([]supervisor.Status{
		{Kind: "source", ID: "s1", State: "up"},
		{Kind: "sink", ID: "k1", State: "up"},
	})

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/health", nil)
	mustStatus(t, resp, http.StatusOK)
	var body struct {
		Status     string              `json:"status"`
		Components []supervisor.Status `json:"components"`
	}
	decodeInto(t, resp, &body)
	if body.Status != "ok" {
		t.Fatalf("status = %q, want ok", body.Status)
	}
	if len(body.Components) != 2 {
		t.Fatalf("components = %+v, want 2", body.Components)
	}

	rec.setStatuses([]supervisor.Status{
		{Kind: "source", ID: "s1", State: "error", Err: "boom"},
	})
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/health", nil)
	mustStatus(t, resp, http.StatusOK)
	decodeInto(t, resp, &body)
	if body.Status != "degraded" {
		t.Fatalf("status = %q, want degraded", body.Status)
	}
}
