package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
)

type fakeReconciler struct {
	mu         sync.Mutex
	statuses   []supervisor.Status
	reconciles int
}

func (f *fakeReconciler) Reconcile(context.Context) error {
	f.mu.Lock()
	f.reconciles++
	f.mu.Unlock()
	return nil
}

func (f *fakeReconciler) Statuses() []supervisor.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]supervisor.Status, len(f.statuses))
	copy(out, f.statuses)
	return out
}

type testMCP struct {
	svc    *config.Service
	stats  *stats.Registry
	client *sdkmcp.ClientSession
	server *sdkmcp.ServerSession
	store  *store.Store
	rec    *fakeReconciler
}

func newTestMCP(t *testing.T) testMCP {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "beacon.db"))
	if err != nil {
		t.Fatal(err)
	}
	rec := &fakeReconciler{statuses: []supervisor.Status{
		{Kind: "source", ID: "can0", State: "up"},
		{Kind: "sink", ID: "events", State: "up"},
		{Kind: "connector", ID: "navigation", State: "up"},
	}}
	svc := config.NewService(st, rec, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reg := stats.NewRegistry()
	server := NewServer(svc, reg, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	clientTransport, serverTransport := sdkmcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "beacon-test", Version: "test"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		_ = serverSession.Close()
		_ = st.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Close()
		_ = st.Close()
	})
	return testMCP{svc: svc, stats: reg, client: clientSession, server: serverSession, store: st, rec: rec}
}

func callTool(t *testing.T, session *sdkmcp.ClientSession, name string, arguments any) *sdkmcp.CallToolResult {
	t.Helper()
	result, err := session.CallTool(context.Background(), &sdkmcp.CallToolParams{Name: name, Arguments: arguments})
	if err != nil {
		t.Fatalf("call %s: %v", name, err)
	}
	return result
}

func decodeStructured[T any](t *testing.T, result *sdkmcp.CallToolResult) T {
	t.Helper()
	b, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("decode structured output %s: %v", b, err)
	}
	return out
}

func toolErrorText(result *sdkmcp.CallToolResult) string {
	var parts []string
	for _, content := range result.Content {
		if text, ok := content.(*sdkmcp.TextContent); ok {
			parts = append(parts, text.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func TestToolCatalogAndSchemas(t *testing.T) {
	tm := newTestMCP(t)
	result, err := tm.client.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := Catalog()
	if len(result.Tools) != len(want) {
		t.Fatalf("tools = %d, want %d", len(result.Tools), len(want))
	}
	byName := make(map[string]*sdkmcp.Tool, len(result.Tools))
	for _, tool := range result.Tools {
		byName[tool.Name] = tool
	}
	for _, info := range want {
		tool := byName[info.Name]
		if tool == nil {
			t.Fatalf("tool %q was not registered", info.Name)
		}
		if tool.Description != info.Description || tool.InputSchema == nil || tool.OutputSchema == nil {
			t.Fatalf("tool %q missing catalog description or schemas: %+v", tool.Name, tool)
		}
		if tool.Annotations == nil || tool.Annotations.OpenWorldHint == nil || *tool.Annotations.OpenWorldHint {
			t.Fatalf("tool %q should advertise closed-world operation: %+v", tool.Name, tool.Annotations)
		}
		if info.Access == "read" && (!tool.Annotations.ReadOnlyHint || tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint) {
			t.Fatalf("read tool %q has incorrect annotations: %+v", tool.Name, tool.Annotations)
		}
	}
	if got := tm.client.InitializeResult().ServerInfo.Name; got != "beacon" {
		t.Fatalf("server name = %q, want beacon", got)
	}
}

func TestConfigureInspectAndDeleteThroughTools(t *testing.T) {
	tm := newTestMCP(t)

	result := callTool(t, tm.client, "put_source", map[string]any{"source": map[string]any{
		"id": "can0", "name": "Engine room bus", "type": "socketcan", "enabled": true, "interface": "can0",
	}})
	if result.IsError {
		t.Fatalf("put_source: %s", toolErrorText(result))
	}
	putSource := decodeStructured[putSourceOutput](t, result)
	if putSource.Source.ID != "can0" || len(putSource.Status) != 1 || putSource.Status[0].State != "up" {
		t.Fatalf("put_source output = %+v", putSource)
	}

	result = callTool(t, tm.client, "put_sink", map[string]any{"sink": map[string]any{
		"id": "events", "name": "Agent events", "type": "http_sse", "enabled": true, "path": "/events",
	}})
	if result.IsError {
		t.Fatalf("put_sink: %s", toolErrorText(result))
	}

	result = callTool(t, tm.client, "put_connector", map[string]any{"connector": map[string]any{
		"id": "navigation", "name": "Navigation data", "source_id": "can0", "sink_id": "events",
		"filters": []string{"msg.pgn == 127250"}, "buffer": map[string]any{"max_age": "24h"},
		"enabled": true, "mode": "semantic",
	}})
	if result.IsError {
		t.Fatalf("put_connector: %s", toolErrorText(result))
	}

	result = callTool(t, tm.client, "get_config", map[string]any{})
	if result.IsError {
		t.Fatalf("get_config: %s", toolErrorText(result))
	}
	cfg := decodeStructured[getConfigOutput](t, result)
	if len(cfg.Sources) != 1 || len(cfg.Sinks) != 1 || len(cfg.Connectors) != 1 {
		t.Fatalf("config = %+v", cfg)
	}
	if cfg.Connectors[0].Buffer.MaxAge != "24h0m0s" {
		t.Fatalf("max_age = %q, want 24h0m0s", cfg.Connectors[0].Buffer.MaxAge)
	}

	tm.stats.Record("navigation", 4, 512)
	tm.stats.SetQueue("navigation", 3, 384)
	result = callTool(t, tm.client, "get_delivery_statistics", map[string]any{"connector_id": "navigation"})
	if result.IsError {
		t.Fatalf("get_delivery_statistics: %s", toolErrorText(result))
	}
	delivery := decodeStructured[deliveryStatisticsOutput](t, result).Connectors["navigation"]
	if delivery.TotalMessages != 4 || delivery.TotalBytes != 512 || delivery.PendingMessages != 3 || delivery.PendingBytes != 384 {
		t.Fatalf("delivery statistics = %+v", delivery)
	}

	result = callTool(t, tm.client, "get_health", map[string]any{})
	health := decodeStructured[healthOutput](t, result)
	if health.Status != "ok" || len(health.Components) != 3 {
		t.Fatalf("health = %+v", health)
	}

	for _, tc := range []struct{ tool, id string }{
		{"delete_connector", "navigation"},
		{"delete_sink", "events"},
		{"delete_source", "can0"},
	} {
		result = callTool(t, tm.client, tc.tool, map[string]any{"id": tc.id})
		if result.IsError {
			t.Fatalf("%s: %s", tc.tool, toolErrorText(result))
		}
		deleted := decodeStructured[deleteOutput](t, result)
		if !deleted.Deleted || deleted.ID != tc.id {
			t.Fatalf("%s output = %+v", tc.tool, deleted)
		}
	}
	stored, err := tm.svc.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Sources)+len(stored.Sinks)+len(stored.Connectors) != 0 {
		t.Fatalf("config after deletes = %+v", stored)
	}
}

func TestValidationAndDependencyFailuresAreToolErrors(t *testing.T) {
	tm := newTestMCP(t)

	result := callTool(t, tm.client, "put_source", map[string]any{"source": map[string]any{
		"id": "broken", "name": "Broken", "type": "socketcan", "enabled": true,
	}})
	if !result.IsError || !strings.Contains(toolErrorText(result), "requires interface") {
		t.Fatalf("invalid source result = isError %v, content %q", result.IsError, toolErrorText(result))
	}
	if _, err := tm.svc.GetSource(context.Background(), "broken"); !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("invalid source was persisted: %v", err)
	}

	for _, call := range []struct {
		name string
		args map[string]any
	}{
		{"put_source", map[string]any{"source": map[string]any{"id": "can0", "name": "CAN", "type": "socketcan", "enabled": false, "interface": "can0"}}},
		{"put_sink", map[string]any{"sink": map[string]any{"id": "events", "name": "Events", "type": "http_sse", "enabled": false, "path": "/events"}}},
		{"put_connector", map[string]any{"connector": map[string]any{"id": "navigation", "name": "Nav", "source_id": "can0", "sink_id": "events", "buffer": map[string]any{}, "enabled": false}}},
	} {
		result = callTool(t, tm.client, call.name, call.args)
		if result.IsError {
			t.Fatalf("setup %s: %s", call.name, toolErrorText(result))
		}
	}
	result = callTool(t, tm.client, "delete_source", map[string]any{"id": "can0"})
	if !result.IsError || !strings.Contains(toolErrorText(result), config.ErrInUse.Error()) {
		t.Fatalf("in-use delete result = isError %v, content %q", result.IsError, toolErrorText(result))
	}
}

func TestStreamableHTTPAndOriginProtection(t *testing.T) {
	tm := newTestMCP(t)
	server := httptest.NewServer(Handler(tm.svc, tm.stats, "test", slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "http-test", Version: "test"}, nil)
	session, err := client.Connect(context.Background(), &sdkmcp.StreamableClientTransport{Endpoint: server.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil || len(tools.Tools) != len(Catalog()) {
		t.Fatalf("HTTP tools/list = %d tools, err %v", len(tools.Tools), err)
	}

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req, err := http.NewRequest(method, server.URL, strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Origin", "http://evil.example")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s cross-origin status = %d, want 403", method, resp.StatusCode)
		}
	}
}

func TestMCPPathIsReservedForHTTPSinks(t *testing.T) {
	cfg := model.Config{Sinks: []model.Sink{{
		ID: "collision", Name: "Collision", Type: model.SinkHTTPSSE, Enabled: true, Path: "/mcp",
	}}}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("Validate(/mcp) = %v, want reserved path error", err)
	}
}
