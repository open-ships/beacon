package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/n2kcatalog"
	"github.com/open-ships/beacon/internal/stats"
)

func TestNilSetIsSafe(t *testing.T) {
	var s *Set
	s.ConnectorMessages(context.Background(), "c", "received", 1)
	s.ConnectorBytes(context.Background(), "c", 10)
	s.SetQueueDepth("c", 5, 100)
	s.SetComponentState("source", "can0", 2)
	s.SourceMessages(context.Background(), "can0", 1)
	s.SourceDrops(context.Background(), "can0", 1)
	s.SinkClients("sse", 1)
	s.SinkHTTPRequest(context.Background(), "webhook", "202", "gzip", 2, 100, 250, 10*time.Millisecond)
	s.SinkHTTPRetryAfter(context.Background(), "webhook", "503", 2*time.Second)
	s.SetPrometheusSourceDetails(true)
	if s.PrometheusSourceDetailsEnabled() {
		t.Fatal("nil metrics set reported source details enabled")
	}
	s.RemoveComponent("source", "can0")
	s.RemoveConnector("c")
	s.RemoveSource("can0")
	s.RemoveSourceDrops("bus:socketcan:can0")
	s.RemoveSink("sse")
}

func TestCumulativeMetricsRetireDeletedEntitiesWithoutOverflow(t *testing.T) {
	s, handler, err := New()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	record := func(id string) {
		s.ConnectorMessages(ctx, id, "received", 1)
		s.ConnectorBytes(ctx, id, 100)
		s.SourceMessages(ctx, id, 1)
		s.SourceDrops(ctx, id, 1)
		s.SourceDrops(ctx, "bus:"+id, 1)
		s.SinkClients(id, 1)
		s.SinkHTTPRequest(ctx, id, "503", "gzip", 2, 100, 200, time.Second)
		s.SinkHTTPRetryAfter(ctx, id, "503", time.Second)
	}
	// Surpass the SDK's default cumulative cardinality limit. After deletion,
	// neither counters nor histogram buckets may occupy historical series.
	for i := range 2100 {
		id := fmt.Sprintf("removed-%d", i)
		record(id)
		s.RemoveConnector(id)
		s.RemoveSource(id)
		s.RemoveSourceDrops("bus:" + id)
		s.RemoveSink(id)
	}
	record("active")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	body = strings.ReplaceAll(body, `otel_scope_name="beacon",otel_scope_schema_url="",otel_scope_version="",`, "")
	if strings.Contains(body, "removed-") || strings.Contains(body, "otel_metric_overflow") {
		t.Fatalf("retired/overflow series survived:\n%s", body)
	}
	for _, want := range []string{
		`beacon_connector_messages_total{connector="active",stage="received"} 1`,
		`beacon_source_messages_total{source="active"} 1`,
		`beacon_sink_http_payload_size_bytes_count{encoding="gzip",sink="active",status="503"} 1`,
		`beacon_sink_http_retry_after_seconds_count{sink="active",status="503"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing active series %s:\n%s", want, body)
		}
	}
}

func TestPrometheusExposesHTTPSinkMetrics(t *testing.T) {
	s, handler, err := New()
	if err != nil {
		t.Fatal(err)
	}
	s.SinkHTTPRequest(context.Background(), "webhook", "429", "gzip", 3, 120, 420, 25*time.Millisecond)
	s.SinkHTTPRetryAfter(context.Background(), "webhook", "429", 2*time.Second)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"beacon_sink_http_requests_total",
		"beacon_sink_http_payload_envelopes_total",
		"beacon_sink_http_payload_size_bytes_bucket",
		"beacon_sink_http_payload_uncompressed_size_bytes_bucket",
		"beacon_sink_http_request_latency_seconds_bucket",
		"beacon_sink_http_retry_after_seconds_bucket",
		`le="0.005"`, `le="0.25"`, `le="512"`,
		`encoding="gzip"`, `sink="webhook"`, `status="429"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("HTTP sink exposition missing %q:\n%s", want, body)
		}
	}
}

func TestRemoveComponentAndConnectorDropMapEntries(t *testing.T) {
	s, _, err := New()
	if err != nil {
		t.Fatal(err)
	}
	s.SetComponentState("source", "can0", 2)
	s.SetQueueDepth("nav", 5, 100)

	s.mu.Lock()
	if _, ok := s.states[gaugeKey{"source", "can0"}]; !ok {
		s.mu.Unlock()
		t.Fatal("precondition: state not recorded")
	}
	if _, ok := s.depths["nav"]; !ok {
		s.mu.Unlock()
		t.Fatal("precondition: depth not recorded")
	}
	s.mu.Unlock()

	s.RemoveComponent("source", "can0")
	s.RemoveConnector("nav")

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.states[gaugeKey{"source", "can0"}]; ok {
		t.Fatal("RemoveComponent did not drop state entry")
	}
	if _, ok := s.depths["nav"]; ok {
		t.Fatal("RemoveConnector did not drop depth entry")
	}
}

func TestPrometheusExposition(t *testing.T) {
	s, handler, err := New()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s.ConnectorMessages(ctx, "nav", "delivered", 3)
	s.SourceMessages(ctx, "can0", 7)
	s.SourceDrops(ctx, "can0", 2)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{"beacon_connector_messages_total", "beacon_source_messages_total", "beacon_subscriber_dropped_total"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("exposition missing %s:\n%s", want, body)
		}
	}
}

func TestPrometheusSourcePGNMetricsRequireExplicitOptIn(t *testing.T) {
	reg := stats.NewRegistry()
	reg.RecordSource("can0", &msg.Envelope{
		PGN: 127250, PGNName: "Vessel Heading", Source: 12,
		Raw:     []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Payload: json.RawMessage(`{"heading":1.5,"reference":"magnetic"}`),
		Physical: map[string]n2kcatalog.PhysicalField{
			"heading": {Value: 1.5, Unit: "rad"},
		},
	})
	set, handler, err := New(reg)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if body := rec.Body.String(); strings.Contains(body, "beacon_source_pgn_") {
		t.Fatalf("default exposition included opt-in source details:\n%s", body)
	}

	set.SetPrometheusSourceDetails(true)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"beacon_source_pgn_messages_total",
		"beacon_source_pgn_frequency_hz",
		"beacon_source_pgn_expected_period_seconds",
		"beacon_source_pgn_last_seen_unixtime",
		"beacon_source_pgn_payload_bytes",
		"beacon_source_pgn_gap_active",
		"beacon_source_pgn_timing_seconds",
		"beacon_source_pgn_traffic",
		"beacon_source_pgn_decode_messages_total",
		"beacon_source_pgn_decode_outcomes_total",
		"beacon_source_pgn_payload_length_messages_total",
		"beacon_source_pgn_destination_messages_total",
		"beacon_source_pgn_priority_messages_total",
		"beacon_source_pgn_status",
		"beacon_source_pgn_raw_payload",
		"beacon_source_pgn_raw_byte",
		"beacon_source_pgn_field_value",
		"beacon_source_pgn_field_state",
		"beacon_source_pgn_field_quality_total",
		"beacon_source_pgn_field_category_summary",
		`pgn="127250"`,
		`source="can0"`,
		`source_address="12"`,
		`statistic="p90"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("source PGN exposition missing %q:\n%s", want, body)
		}
	}

	set.SetPrometheusSourceDetails(false)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if body := rec.Body.String(); strings.Contains(body, "beacon_source_pgn_") {
		t.Fatalf("disabled exposition retained source details:\n%s", body)
	}
}
