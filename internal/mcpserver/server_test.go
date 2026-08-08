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
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
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
	if err := reg.AttachSourceMetricPersistence(context.Background(), st.DB()); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
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
		_ = reg.CloseSourceMetricPersistence(context.Background())
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
	putConnectorSchema, err := json.Marshal(byName["put_connector"].InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(putConnectorSchema), "effective_buffer") {
		t.Fatalf("put_connector input schema should accept authored config only: %s", putConnectorSchema)
	}
	getConfigSchema, err := json.Marshal(byName["get_config"].OutputSchema)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(getConfigSchema), "effective_buffer") {
		t.Fatalf("get_config output schema is missing effective_buffer: %s", getConfigSchema)
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
	result = callTool(t, tm.client, "get_delivery_metrics", map[string]any{"connector_id": "navigation"})
	if result.IsError {
		t.Fatalf("get_delivery_metrics: %s", toolErrorText(result))
	}
	delivery := decodeStructured[deliveryMetricsOutput](t, result).Connectors["navigation"]
	if delivery.TotalMessages != 4 || delivery.TotalBytes != 512 || delivery.PendingMessages != 3 || delivery.PendingBytes != 384 {
		t.Fatalf("delivery metrics = %+v", delivery)
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

func TestConnectorOutputsExposeEffectiveDefaultWithoutChangingAuthoredConfig(t *testing.T) {
	tm := newTestMCP(t)
	for toolName, arguments := range map[string]any{
		"put_source": map[string]any{"source": map[string]any{
			"id": "can0", "name": "CAN", "type": "socketcan", "interface": "can0", "enabled": false,
		}},
		"put_sink": map[string]any{"sink": map[string]any{
			"id": "discard", "name": "Discard", "type": "null", "enabled": false,
		}},
	} {
		if result := callTool(t, tm.client, toolName, arguments); result.IsError {
			t.Fatalf("%s: %s", toolName, toolErrorText(result))
		}
	}
	result := callTool(t, tm.client, "put_connector", map[string]any{"connector": map[string]any{
		"id": "default_buffer", "name": "Default buffer", "source_id": "can0", "sink_id": "discard",
		"buffer": map[string]any{}, "enabled": false,
	}})
	if result.IsError {
		t.Fatalf("put_connector: %s", toolErrorText(result))
	}
	put := decodeStructured[putConnectorOutput](t, result)
	if put.Connector.Buffer.MaxMessages != 0 || put.Connector.EffectiveBuffer.MaxMessages != model.DefaultMaxMessages {
		t.Fatalf("put connector buffers = authored %+v effective %+v", put.Connector.Buffer, put.Connector.EffectiveBuffer)
	}

	result = callTool(t, tm.client, "get_config", map[string]any{})
	if result.IsError {
		t.Fatalf("get_config: %s", toolErrorText(result))
	}
	cfg := decodeStructured[getConfigOutput](t, result)
	if len(cfg.Connectors) != 1 || cfg.Connectors[0].Buffer.MaxMessages != 0 ||
		cfg.Connectors[0].EffectiveBuffer.MaxMessages != model.DefaultMaxMessages {
		t.Fatalf("get config connector = %+v", cfg.Connectors)
	}
	stored, err := tm.svc.GetConnector(context.Background(), "default_buffer")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Buffer.MaxMessages != 0 {
		t.Fatalf("stored authored max_messages = %d, want zero", stored.Buffer.MaxMessages)
	}
}

func TestHTTPPostSinkFieldsRoundTripThroughMCP(t *testing.T) {
	tm := newTestMCP(t)
	result := callTool(t, tm.client, "put_sink", map[string]any{"sink": map[string]any{
		"id": "webhook", "name": "Telemetry API", "type": "http_post", "enabled": false,
		"url": "https://api.example.com/v1/envelopes", "batch_size": 250, "request_timeout": "15s",
		"gzip":    true,
		"headers": map[string]any{"Authorization": "Bearer token", "X-API-Key": "secret"},
	}})
	if result.IsError {
		t.Fatalf("put_sink: %s", toolErrorText(result))
	}
	put := decodeStructured[putSinkOutput](t, result)
	if put.Sink.Type != "http_post" || put.Sink.BatchSize != 250 || put.Sink.RequestTimeout != "15s" || !put.Sink.Gzip ||
		put.Sink.Headers["Authorization"] != "Bearer token" {
		t.Fatalf("put sink output = %+v", put.Sink)
	}
	stored, err := tm.svc.GetSink(context.Background(), "webhook")
	if err != nil {
		t.Fatal(err)
	}
	if stored.URL != "https://api.example.com/v1/envelopes" || stored.BatchSize != 250 || !stored.Gzip ||
		time.Duration(stored.RequestTimeout) != 15*time.Second || stored.Headers["X-API-Key"] != "secret" {
		t.Fatalf("stored sink = %+v", stored)
	}

	bad := callTool(t, tm.client, "put_sink", map[string]any{"sink": map[string]any{
		"id": "bad", "name": "Bad", "type": "http_post", "url": "https://api.example.com", "request_timeout": "soon",
	}})
	if !bad.IsError {
		t.Fatal("put_sink accepted an invalid request_timeout")
	}
}

func TestGetSourceMetricsReturnsAndFiltersSharedPGNStore(t *testing.T) {
	tm := newTestMCP(t)
	mustPutSource := model.Source{ID: "can0", Name: "CAN", Type: model.SourceSocketCAN, Interface: "can0"}
	if err := tm.svc.PutSource(context.Background(), mustPutSource, true); err != nil {
		t.Fatal(err)
	}
	deviceName := uint64(0x1122334455667788)
	tm.stats.RecordSource("can0", &msg.Envelope{
		PGN: 127250, PGNName: "Vessel Heading", Source: 12,
		DeviceName: &deviceName, Raw: []byte{1, 2, 3, 4, 5, 6, 7, 8}, Payload: json.RawMessage(`{"heading":1.5}`),
	})
	tm.stats.RecordSource("can0", &msg.Envelope{PGN: 128259, Source: 44, Raw: []byte{1, 2}})

	result := callTool(t, tm.client, "get_source_metrics", map[string]any{
		"source_id": "can0", "pgn": 127250, "source_address": 12,
		"device_name_hex": "0x1122334455667788",
	})
	if result.IsError {
		t.Fatalf("get_source_metrics: %s", toolErrorText(result))
	}
	out := decodeStructured[sourceMetricsOutput](t, result)
	streams, ok := out.Sources["can0"]
	if !ok || len(streams) != 1 {
		t.Fatalf("source metrics = %+v", out)
	}
	stream := streams[0]
	if stream.PGN != 127250 || stream.SourceAddress != 12 || stream.PayloadBytesLast != 8 ||
		stream.Messages != 1 || stream.DiagnosticSamples != 1 {
		t.Fatalf("filtered stream = %+v", stream)
	}
	if out.GeneratedAt.IsZero() {
		t.Fatal("generated_at was not populated")
	}

	result = callTool(t, tm.client, "get_source_metrics", map[string]any{"source_id": "missing"})
	if !result.IsError || !strings.Contains(toolErrorText(result), config.ErrNotFound.Error()) {
		t.Fatalf("unknown source result = isError %v, content %q", result.IsError, toolErrorText(result))
	}
}

func TestGetLatestPayloadsReturnsEverySensorPGNAndSupportsScopedFilters(t *testing.T) {
	tm := newTestMCP(t)
	if err := tm.svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "CAN", Type: model.SourceSocketCAN, Interface: "can0",
	}, true); err != nil {
		t.Fatal(err)
	}
	deviceName := uint64(0x1122334455667788)
	for _, payload := range []string{`{"heading":1.5}`, `{"heading":2.5}`} {
		tm.stats.RecordSource("can0", &msg.Envelope{
			PGN: 127250, PGNName: "Vessel Heading", Source: 12,
			DeviceName: &deviceName, Payload: json.RawMessage(payload),
		})
	}
	tm.stats.RecordSource("can0", &msg.Envelope{
		PGN: 129025, PGNName: "Position, Rapid Update", Source: 12,
		DeviceName: &deviceName, Payload: json.RawMessage(`{"latitude":42.1,"longitude":-71.2}`),
	})
	tm.stats.RecordSource("can0", &msg.Envelope{
		PGN: 128259, PGNName: "Speed", Source: 44,
		Payload: json.RawMessage(`{"speedWaterReferenced":3.2}`),
	})

	result := callTool(t, tm.client, "get_latest_payloads", map[string]any{"source_id": "can0"})
	if result.IsError {
		t.Fatalf("get_latest_payloads: %s", toolErrorText(result))
	}
	out := decodeStructured[latestPayloadsOutput](t, result)
	if len(out.Payloads) != 3 {
		t.Fatalf("latest payloads = %+v", out.Payloads)
	}
	if out.Payloads[0].SensorID != "1122334455667788" || out.Payloads[0].PGN != 127250 {
		t.Fatalf("first sensor payload = %+v", out.Payloads[0])
	}
	heading, ok := out.Payloads[0].Payload.(map[string]any)
	if !ok || heading["heading"] != 2.5 {
		t.Fatalf("latest heading payload = %#v", out.Payloads[0].Payload)
	}
	if out.Payloads[2].SensorID != "address:44" || out.Payloads[2].PGN != 128259 {
		t.Fatalf("fallback sensor id payload = %+v", out.Payloads[2])
	}

	result = callTool(t, tm.client, "get_latest_payloads", map[string]any{
		"source_id": "can0", "sensor_id": "1122334455667788", "pgn": 129025,
	})
	if result.IsError {
		t.Fatalf("filtered get_latest_payloads: %s", toolErrorText(result))
	}
	filtered := decodeStructured[latestPayloadsOutput](t, result)
	if len(filtered.Payloads) != 1 || filtered.Payloads[0].PGN != 129025 || filtered.Payloads[0].SourceAddress != 12 {
		t.Fatalf("device/PGN filtered payloads = %+v", filtered.Payloads)
	}

	result = callTool(t, tm.client, "get_latest_payloads", map[string]any{
		"source_id": "can0", "sensor_id": "address:44",
	})
	if result.IsError {
		t.Fatalf("address-filtered get_latest_payloads: %s", toolErrorText(result))
	}
	filtered = decodeStructured[latestPayloadsOutput](t, result)
	if len(filtered.Payloads) != 1 || filtered.Payloads[0].SensorID != "address:44" {
		t.Fatalf("address-filtered payloads = %+v", filtered.Payloads)
	}

	result = callTool(t, tm.client, "get_latest_payloads", map[string]any{
		"source_id": "can0", "sensor_id": "address:44", "source_address": 12,
	})
	if !result.IsError || !strings.Contains(toolErrorText(result), "conflicts with source_address") {
		t.Fatalf("conflicting sensor filter = isError %v content %q", result.IsError, toolErrorText(result))
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
