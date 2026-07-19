package metrics

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

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
	s.RemoveComponent("source", "can0")
	s.RemoveConnector("c")
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

func TestPrometheusExposesSharedSourcePGNMetrics(t *testing.T) {
	reg := stats.NewRegistry()
	reg.RecordSource("can0", &msg.Envelope{
		PGN: 127250, PGNName: "Vessel Heading", Source: 12,
		Raw:     []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Payload: json.RawMessage(`{"heading":1.5,"reference":"magnetic"}`),
		Physical: map[string]n2kcatalog.PhysicalField{
			"heading": {Value: 1.5, Unit: "rad"},
		},
	})
	_, handler, err := New(reg)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"beacon_source_pgn_messages_total",
		"beacon_source_pgn_frequency_hz",
		"beacon_source_pgn_expected_period_seconds",
		"beacon_source_pgn_last_seen_unixtime",
		"beacon_source_pgn_payload_bytes",
		"beacon_source_pgn_gap_active",
		"beacon_source_pgn_anomaly_active",
		"beacon_source_pgn_timing_seconds",
		"beacon_source_pgn_traffic",
		"beacon_source_pgn_decode_messages_total",
		"beacon_source_pgn_decode_outcomes_total",
		"beacon_source_pgn_payload_length_messages_total",
		"beacon_source_pgn_destination_messages_total",
		"beacon_source_pgn_priority_messages_total",
		"beacon_source_pgn_baseline_state",
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
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("source PGN exposition missing %q:\n%s", want, body)
		}
	}
}
