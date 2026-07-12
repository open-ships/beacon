package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

// sinkClientsGauge scrapes the prometheus handler for the sink-clients
// gauge of the given sink; ok is false if the series is not exposed yet.
func sinkClientsGauge(t *testing.T, h http.Handler, sink string) (float64, bool) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, "#") || !strings.Contains(line, "sink_clients") ||
			!strings.Contains(line, `sink="`+sink+`"`) {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatalf("bad gauge line %q: %v", line, err)
		}
		return v, true
	}
	return 0, false
}

func TestTCPLiveTail(t *testing.T) {
	rt, err := New(context.Background(), model.Sink{
		ID: "tcp", Name: "TCP", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:0",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	addr := rt.(interface{ Addr() string }).Addr()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(100 * time.Millisecond)

	rt.(Broadcaster).Broadcast([]queue.Entry{entry("nav", 7, 127250)})

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatal(err)
	}
	if m["pgn"].(float64) != 127250 || m["connector"].(string) != "nav" {
		t.Fatalf("line = %s", line)
	}
}

// TestTCPStopDecrementsClientGauge asserts Stop returns the sink-clients
// gauge to zero for connections it force-closes (no drift across restarts).
func TestTCPStopDecrementsClientGauge(t *testing.T) {
	met, prom, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	rt, err := New(context.Background(), model.Sink{
		ID: "tcp", Name: "TCP", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:0",
	}, nil, nil, slog.Default(), met)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", rt.(interface{ Addr() string }).Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitFor(t, 3*time.Second, "TCP client gauge to reach 1", func() bool {
		v, ok := sinkClientsGauge(t, prom, "tcp")
		return ok && v == 1
	})

	rt.Stop()
	if v, ok := sinkClientsGauge(t, prom, "tcp"); !ok || v != 0 {
		t.Fatalf("gauge after Stop = %v (present=%v), want 0", v, ok)
	}
}
