package metrics

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
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
