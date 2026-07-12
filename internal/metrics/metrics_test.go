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
	s.SinkClients("sse", 1)
}

func TestPrometheusExposition(t *testing.T) {
	s, handler, err := New()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s.ConnectorMessages(ctx, "nav", "delivered", 3)
	s.SourceMessages(ctx, "can0", 7)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{"beacon_connector_messages_total", "beacon_source_messages_total"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("exposition missing %s:\n%s", want, body)
		}
	}
}
