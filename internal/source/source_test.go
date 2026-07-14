package source

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/msg"
)

func TestHubFanOutAndUnsubscribe(t *testing.T) {
	h := newHub(nil, "test")
	a, unsubA := h.subscribe(4)
	b, _ := h.subscribe(4)

	h.publish(&msg.Envelope{PGN: 1})
	if e := <-a; e.PGN != 1 {
		t.Fatal("a missed message")
	}
	if e := <-b; e.PGN != 1 {
		t.Fatal("b missed message")
	}

	unsubA()
	h.publish(&msg.Envelope{PGN: 2})
	if e := <-b; e.PGN != 2 {
		t.Fatal("b missed second message")
	}
	select {
	case _, ok := <-a:
		if ok {
			t.Fatal("a received after unsubscribe")
		}
	default:
	}
}

func TestHubDropsWhenSubscriberFull(t *testing.T) {
	h := newHub(nil, "test")
	ch, _ := h.subscribe(1)
	h.publish(&msg.Envelope{PGN: 1})
	h.publish(&msg.Envelope{PGN: 2}) // must not block
	if e := <-ch; e.PGN != 1 {
		t.Fatal("first message lost")
	}
	select {
	case e := <-ch:
		t.Fatalf("unexpected second message %d (buffer 1 should have dropped it)", e.PGN)
	default:
	}
}

// Regression: a full subscriber channel must be counted, not silently
// dropped — spec §3.2 promises drop accounting for slow subscribers.
func TestHubDropCountsAgainstMetrics(t *testing.T) {
	met, handler, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	h := newHub(met, "src1")
	ch, _ := h.subscribe(1)
	h.publish(&msg.Envelope{PGN: 1}) // fills the buffer
	h.publish(&msg.Envelope{PGN: 2}) // dropped: buffer still full
	<-ch                             // drain so nothing blocks the test

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	found := false
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "beacon_subscriber_dropped_total") &&
			strings.Contains(line, `component="src1"`) && strings.HasSuffix(line, " 1") {
			found = true
		}
	}
	if !found {
		t.Fatalf("beacon_subscriber_dropped_total{component=\"src1\"} not found or wrong value:\n%s", body)
	}
}
