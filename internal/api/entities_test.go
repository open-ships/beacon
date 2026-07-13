package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/open-ships/beacon/internal/api"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
)

// fakeReconciler is a test double for config.Reconciler: Reconcile is a
// no-op (so it never disturbs the canned Statuses a test injects) and
// Statuses returns whatever the test last set.
type fakeReconciler struct {
	mu       sync.Mutex
	statuses []supervisor.Status
}

func (f *fakeReconciler) Reconcile(ctx context.Context) error { return nil }

func (f *fakeReconciler) Statuses() []supervisor.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statuses
}

func (f *fakeReconciler) setStatuses(s []supervisor.Status) {
	f.mu.Lock()
	f.statuses = s
	f.mu.Unlock()
}

// newTestServer builds a real config.Service over a temp on-disk store and
// a fake reconciler, wires it through api.New, and serves it via
// httptest.Server so tests exercise the full HTTP round trip (routing,
// huma param/body binding, JSON encoding) rather than calling handlers
// directly.
func newTestServer(t *testing.T) (*httptest.Server, *fakeReconciler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rec := &fakeReconciler{}
	svc := config.NewService(st, rec, nil)
	handler, _ := api.New(svc, stats.NewRegistry(), "test", nil)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv, rec
}

// --- HTTP helpers ---

type statusBody struct {
	Status []supervisor.Status `json:"status"`
}

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

func decodeInto(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func putEntity(t *testing.T, srv *httptest.Server, kind, id string, body any) *http.Response {
	t.Helper()
	return doJSON(t, http.MethodPut, srv.URL+"/api/v1/"+kind+"/"+id, body)
}

func mustStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		defer resp.Body.Close()
		var buf bytes.Buffer
		buf.ReadFrom(resp.Body)
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, want, buf.String())
	}
}

// --- Sources ---

func TestSourceCRUD(t *testing.T) {
	srv, rec := newTestServer(t)
	rec.setStatuses([]supervisor.Status{
		{Kind: "source", ID: "s1", State: "up"},
		{Kind: "sink", ID: "other", State: "up"},
	})

	src := model.Source{ID: "s1", Name: "Source 1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}
	resp := putEntity(t, srv, "sources", "s1", src)
	mustStatus(t, resp, http.StatusOK)
	var sb statusBody
	decodeInto(t, resp, &sb)
	if len(sb.Status) != 1 || sb.Status[0].ID != "s1" {
		t.Fatalf("PUT status = %+v, want only s1's status", sb.Status)
	}

	// List
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/sources", nil)
	mustStatus(t, resp, http.StatusOK)
	var list struct {
		Sources []model.Source `json:"sources"`
	}
	decodeInto(t, resp, &list)
	if len(list.Sources) != 1 || list.Sources[0].ID != "s1" {
		t.Fatalf("list sources = %+v", list.Sources)
	}

	// Get one
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/sources/s1", nil)
	mustStatus(t, resp, http.StatusOK)
	var got model.Source
	decodeInto(t, resp, &got)
	if got.ID != "s1" || got.Name != "Source 1" {
		t.Fatalf("get source = %+v", got)
	}

	// Update (PUT on an existing id)
	src.Name = "Renamed"
	resp = putEntity(t, srv, "sources", "s1", src)
	mustStatus(t, resp, http.StatusOK)

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/sources/s1", nil)
	decodeInto(t, resp, &got)
	if got.Name != "Renamed" {
		t.Fatalf("get after update = %+v, want Renamed", got)
	}

	// Delete
	resp = doJSON(t, http.MethodDelete, srv.URL+"/api/v1/sources/s1", nil)
	mustStatus(t, resp, http.StatusOK)
	decodeInto(t, resp, &sb)
	if len(sb.Status) != 1 || sb.Status[0].ID != "s1" {
		t.Fatalf("DELETE status = %+v, want only s1's status", sb.Status)
	}

	// Get after delete -> 404
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/sources/s1", nil)
	mustStatus(t, resp, http.StatusNotFound)
}

// --- Sinks ---

func TestSinkCRUD(t *testing.T) {
	srv, _ := newTestServer(t)

	sink := model.Sink{ID: "k1", Name: "Sink 1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a"}
	resp := putEntity(t, srv, "sinks", "k1", sink)
	mustStatus(t, resp, http.StatusOK)

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/sinks", nil)
	mustStatus(t, resp, http.StatusOK)
	var list struct {
		Sinks []model.Sink `json:"sinks"`
	}
	decodeInto(t, resp, &list)
	if len(list.Sinks) != 1 || list.Sinks[0].ID != "k1" {
		t.Fatalf("list sinks = %+v", list.Sinks)
	}

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/sinks/k1", nil)
	mustStatus(t, resp, http.StatusOK)
	var got model.Sink
	decodeInto(t, resp, &got)
	if got.Path != "/a" {
		t.Fatalf("get sink = %+v", got)
	}

	resp = doJSON(t, http.MethodDelete, srv.URL+"/api/v1/sinks/k1", nil)
	mustStatus(t, resp, http.StatusOK)

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/sinks/k1", nil)
	mustStatus(t, resp, http.StatusNotFound)
}

// --- Connectors ---

func TestConnectorCRUD(t *testing.T) {
	srv, _ := newTestServer(t)

	mustStatus(t, putEntity(t, srv, "sources", "s1", model.Source{
		ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}), http.StatusOK)
	mustStatus(t, putEntity(t, srv, "sinks", "k1", model.Sink{
		ID: "k1", Name: "K1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a",
	}), http.StatusOK)

	conn := model.Connector{
		ID: "c1", Name: "C1", SourceID: "s1", SinkID: "k1", Enabled: true,
		Filters: []string{"msg.pgn == 127250"},
		Buffer:  model.BufferLimits{MaxMessages: 10},
	}
	resp := putEntity(t, srv, "connectors", "c1", conn)
	mustStatus(t, resp, http.StatusOK)

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/connectors", nil)
	mustStatus(t, resp, http.StatusOK)
	var list struct {
		Connectors []model.Connector `json:"connectors"`
	}
	decodeInto(t, resp, &list)
	if len(list.Connectors) != 1 || list.Connectors[0].ID != "c1" {
		t.Fatalf("list connectors = %+v", list.Connectors)
	}

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/connectors/c1", nil)
	mustStatus(t, resp, http.StatusOK)
	var got model.Connector
	decodeInto(t, resp, &got)
	if got.SourceID != "s1" || got.SinkID != "k1" {
		t.Fatalf("get connector = %+v", got)
	}

	resp = doJSON(t, http.MethodDelete, srv.URL+"/api/v1/connectors/c1", nil)
	mustStatus(t, resp, http.StatusOK)

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/v1/connectors/c1", nil)
	mustStatus(t, resp, http.StatusNotFound)
}

// --- Validation errors (422) ---

func TestPutIDMismatch(t *testing.T) {
	srv, _ := newTestServer(t)

	cases := []struct {
		kind string
		body any
	}{
		{"sources", model.Source{ID: "different", Name: "S", Type: model.SourceSocketCAN, Interface: "can0"}},
		{"sinks", model.Sink{ID: "different", Name: "K", Type: model.SinkHTTPSSE, Path: "/a"}},
		{"connectors", model.Connector{ID: "different", Name: "C", SourceID: "s1", SinkID: "k1"}},
	}
	for _, c := range cases {
		resp := putEntity(t, srv, c.kind, "path-id", c.body)
		mustStatus(t, resp, http.StatusUnprocessableEntity)
		if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
			t.Fatalf("%s: content-type = %q, want application/problem+json", c.kind, ct)
		}
	}
}

func TestConnectorInvalidCEL(t *testing.T) {
	srv, _ := newTestServer(t)
	mustStatus(t, putEntity(t, srv, "sources", "s1", model.Source{
		ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}), http.StatusOK)
	mustStatus(t, putEntity(t, srv, "sinks", "k1", model.Sink{
		ID: "k1", Name: "K1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a",
	}), http.StatusOK)

	conn := model.Connector{
		ID: "c1", Name: "C1", SourceID: "s1", SinkID: "k1", Enabled: true,
		Filters: []string{"msg.pgn =="},
	}
	resp := putEntity(t, srv, "connectors", "c1", conn)
	mustStatus(t, resp, http.StatusUnprocessableEntity)
	if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q, want application/problem+json", ct)
	}
	var body map[string]any
	decodeInto(t, resp, &body)
	detail, _ := body["detail"].(string)
	if detail == "" {
		t.Fatalf("problem body missing detail: %+v", body)
	}
}

// --- Not found (404) ---

func TestNotFound(t *testing.T) {
	srv, _ := newTestServer(t)

	for _, kind := range []string{"sources", "sinks", "connectors"} {
		resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/"+kind+"/nope", nil)
		mustStatus(t, resp, http.StatusNotFound)

		resp = doJSON(t, http.MethodDelete, srv.URL+"/api/v1/"+kind+"/nope", nil)
		mustStatus(t, resp, http.StatusNotFound)
	}
}

// --- In use (409) ---

func TestDeleteInUse(t *testing.T) {
	srv, _ := newTestServer(t)
	mustStatus(t, putEntity(t, srv, "sources", "s1", model.Source{
		ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}), http.StatusOK)
	mustStatus(t, putEntity(t, srv, "sinks", "k1", model.Sink{
		ID: "k1", Name: "K1", Type: model.SinkHTTPSSE, Enabled: true, Path: "/a",
	}), http.StatusOK)
	mustStatus(t, putEntity(t, srv, "connectors", "c1", model.Connector{
		ID: "c1", Name: "C1", SourceID: "s1", SinkID: "k1", Enabled: true,
	}), http.StatusOK)

	resp := doJSON(t, http.MethodDelete, srv.URL+"/api/v1/sources/s1", nil)
	mustStatus(t, resp, http.StatusConflict)
	resp = doJSON(t, http.MethodDelete, srv.URL+"/api/v1/sinks/k1", nil)
	mustStatus(t, resp, http.StatusConflict)

	mustStatus(t, doJSON(t, http.MethodDelete, srv.URL+"/api/v1/connectors/c1", nil), http.StatusOK)
	mustStatus(t, doJSON(t, http.MethodDelete, srv.URL+"/api/v1/sources/s1", nil), http.StatusOK)
	mustStatus(t, doJSON(t, http.MethodDelete, srv.URL+"/api/v1/sinks/k1", nil), http.StatusOK)
}

// --- OpenAPI ---

func TestOpenAPI(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := config.NewService(st, &fakeReconciler{}, nil)
	handler, humaAPI := api.New(svc, stats.NewRegistry(), "1.2.3", nil)

	spec := humaAPI.OpenAPI()
	if _, ok := spec.Paths["/api/v1/sources"]; !ok {
		t.Fatalf("openapi spec missing /api/v1/sources; paths: %v", keysOf(spec.Paths))
	}
	item, ok := spec.Paths["/api/v1/sources/{id}"]
	if !ok || item.Put == nil {
		t.Fatalf("openapi spec missing PUT /api/v1/sources/{id}")
	}
	if item.Put.OperationID != "put-source" {
		t.Fatalf("put-source operationId = %q, want put-source", item.Put.OperationID)
	}

	srv := httptest.NewServer(handler)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	var doc map[string]any
	decodeInto(t, resp, &doc)
	paths, _ := doc["paths"].(map[string]any)
	if _, ok := paths["/api/v1/sources"]; !ok {
		t.Fatalf("GET /api/openapi.json body missing /api/v1/sources: %v", keysOf(paths))
	}
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// --- Real mount shape: /api/ on a stdlib mux, no prefix stripping ---

// TestSchemaLinksUnderAppMount reproduces internal/app's exact mount shape
// (mux.Handle("/api/", handler) with no StripPrefix) and asserts that the
// $schema URL huma stamps into a real response body actually resolves to a
// 200 through that mux — i.e. every URL the API hands out is reachable in
// the real deployment, not just when the handler is served at "/".
func TestSchemaLinksUnderAppMount(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := config.NewService(st, &fakeReconciler{}, nil)
	handler, _ := api.New(svc, stats.NewRegistry(), "test", nil)

	mux := http.NewServeMux()
	mux.Handle("/api/", handler) // exactly how app.go mounts it
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	src := model.Source{ID: "s1", Name: "S1", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}
	resp := doJSON(t, http.MethodPut, srv.URL+"/api/v1/sources/s1", src)
	mustStatus(t, resp, http.StatusOK)
	var body struct {
		Schema string `json:"$schema"`
	}
	decodeInto(t, resp, &body)
	if body.Schema == "" {
		t.Fatal("response body carries no $schema link")
	}

	schemaResp, err := http.Get(body.Schema)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, schemaResp, http.StatusOK)
	var schema map[string]any
	decodeInto(t, schemaResp, &schema)
	if _, ok := schema["properties"]; !ok {
		t.Fatalf("schema at %s does not look like a JSON schema: %v", body.Schema, keysOf(schema))
	}

	// The OpenAPI document must be reachable through the same mux too.
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/openapi.json", nil)
	mustStatus(t, resp, http.StatusOK)
	resp.Body.Close()
}

// --- 500 bodies must not leak internal error text ---

// TestInternalErrorSanitized forces a store failure (closed DB) and asserts
// the resulting 500 problem body says only "internal error" — the real
// error text (driver internals, file paths) must go to the log, not the
// client.
func TestInternalErrorSanitized(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	svc := config.NewService(st, &fakeReconciler{}, nil)

	var logBuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logBuf, nil))
	handler, _ := api.New(svc, stats.NewRegistry(), "test", log)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	_ = st.Close() // every store call from here on fails with a driver error

	resp := doJSON(t, http.MethodGet, srv.URL+"/api/v1/sources", nil)
	mustStatus(t, resp, http.StatusInternalServerError)
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	var problem struct {
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(raw, &problem); err != nil {
		t.Fatalf("decode problem body: %v", err)
	}
	if problem.Detail != "internal error" {
		t.Fatalf("500 detail = %q, want the sanitized \"internal error\"", problem.Detail)
	}
	for _, leak := range []string{"sql", "sqlite", "database", ".db"} {
		if bytes.Contains(bytes.ToLower(raw), []byte(leak)) {
			t.Fatalf("500 body leaks internal detail %q: %s", leak, raw)
		}
	}
	if logBuf.Len() == 0 {
		t.Fatal("underlying error was not logged")
	}
}
