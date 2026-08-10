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
	"github.com/open-ships/beacon/internal/retry"
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
	defer func() { _ = conn.Close() }()
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
	_, metadata := consumerEnvelopeParts(t, m)
	if consumerEnvelopePGN(t, m) != 127250 || metadata["connector"] != "nav" {
		t.Fatalf("line = %s", line)
	}
}

type deadlineRecordingConn struct {
	writeDeadlines []time.Time
}

func (c *deadlineRecordingConn) Read([]byte) (int, error)        { return 0, net.ErrClosed }
func (c *deadlineRecordingConn) Write(p []byte) (int, error)     { return len(p), nil }
func (c *deadlineRecordingConn) Close() error                    { return nil }
func (c *deadlineRecordingConn) LocalAddr() net.Addr             { return deadlineRecordingAddr("local") }
func (c *deadlineRecordingConn) RemoteAddr() net.Addr            { return deadlineRecordingAddr("remote") }
func (c *deadlineRecordingConn) SetDeadline(time.Time) error     { return nil }
func (c *deadlineRecordingConn) SetReadDeadline(time.Time) error { return nil }
func (c *deadlineRecordingConn) SetWriteDeadline(d time.Time) error {
	c.writeDeadlines = append(c.writeDeadlines, d)
	return nil
}

type deadlineRecordingAddr string

func (a deadlineRecordingAddr) Network() string { return "test" }
func (a deadlineRecordingAddr) String() string  { return string(a) }

func TestTCPBroadcastUsesOneOperationDeadline(t *testing.T) {
	clients := []*deadlineRecordingConn{{}, {}, {}}
	s := &tcpSink{conns: map[net.Conn]bool{
		clients[0]: true,
		clients[1]: true,
		clients[2]: true,
	}}
	report := s.Broadcast([]queue.Entry{entry("nav", 1, 127250), entry("nav", 2, 127251)})
	if len(report.Accepted) != 2 || !report.Accepted[0] || !report.Accepted[1] {
		t.Fatalf("accepted = %v, want both entries accepted", report.Accepted)
	}

	var operationDeadline time.Time
	for clientIndex, client := range clients {
		if len(client.writeDeadlines) != 2 {
			t.Fatalf("client %d deadlines = %d, want 2", clientIndex, len(client.writeDeadlines))
		}
		for _, deadline := range client.writeDeadlines {
			if operationDeadline.IsZero() {
				operationDeadline = deadline
			}
			if !deadline.Equal(operationDeadline) {
				t.Fatalf("deadline = %s, want shared operation deadline %s", deadline, operationDeadline)
			}
		}
	}
}

func TestTCPAcceptBackoffSurvivesImmediateRebindFailures(t *testing.T) {
	backoff := retry.NewBackoff(250*time.Millisecond, time.Minute)
	now := time.Now()
	for attempt, bounds := range [][2]time.Duration{
		{125 * time.Millisecond, 250 * time.Millisecond},
		{250 * time.Millisecond, 500 * time.Millisecond},
		{500 * time.Millisecond, time.Second},
	} {
		delay := tcpAcceptRetryDelay(backoff, now, now)
		if delay < bounds[0] || delay > bounds[1] {
			t.Fatalf("immediate failure delay %d = %s, want within [%s, %s]",
				attempt+1, delay, bounds[0], bounds[1])
		}
	}

	// A listener that was genuinely healthy for a sustained interval earns a
	// return to the minimum range before recovery starts again.
	delay := tcpAcceptRetryDelay(backoff, now.Add(-tcpStableListenerAge), now)
	if delay < 125*time.Millisecond || delay > 250*time.Millisecond {
		t.Fatalf("stable-listener failure delay = %s, want reset minimum range", delay)
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
	defer func() { _ = conn.Close() }()
	waitFor(t, 3*time.Second, "TCP client gauge to reach 1", func() bool {
		v, ok := sinkClientsGauge(t, prom, "tcp")
		return ok && v == 1
	})

	rt.Stop()
	if v, ok := sinkClientsGauge(t, prom, "tcp"); !ok || v != 0 {
		t.Fatalf("gauge after Stop = %v (present=%v), want 0", v, ok)
	}
}

func TestTCPIdleDisconnectRemovesClient(t *testing.T) {
	met, prom, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	rt, err := New(context.Background(), model.Sink{
		ID: "tcp-idle", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:0",
	}, nil, nil, slog.Default(), met)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	conn, err := net.Dial("tcp", rt.(interface{ Addr() string }).Addr())
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "idle TCP client to register", func() bool {
		return rt.(*tcpSink).clientCount() == 1
	})
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "idle TCP disconnect to be removed", func() bool {
		v, ok := sinkClientsGauge(t, prom, "tcp-idle")
		return rt.(*tcpSink).clientCount() == 0 && ok && v == 0
	})
}

func TestTCPClientLimit(t *testing.T) {
	rt, err := New(context.Background(), model.Sink{
		ID: "tcp-limit", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:0",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	addr := rt.(interface{ Addr() string }).Addr()
	conns := make([]net.Conn, 0, maxTCPClients)
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	for range maxTCPClients {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
	}
	waitFor(t, 3*time.Second, "TCP client limit to fill", func() bool {
		return rt.(*tcpSink).clientCount() == maxTCPClients
	})

	extra, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = extra.Close() }()
	if err := extra.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := extra.Read(one[:]); err == nil {
		t.Fatal("TCP client beyond the limit remained open")
	}
	if got := rt.(*tcpSink).clientCount(); got != maxTCPClients {
		t.Fatalf("client count = %d, want capped at %d", got, maxTCPClients)
	}
}

func TestTCPListenerFailureRebindsInsteadOfReportingFalseHealth(t *testing.T) {
	rt, err := New(context.Background(), model.Sink{
		ID: "tcp-rebind", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:0",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	s := rt.(*tcpSink)
	addr := s.Addr()
	s.mu.Lock()
	failed := s.ln
	s.mu.Unlock()
	if err := failed.Close(); err != nil {
		t.Fatal(err)
	}

	var conn net.Conn
	waitFor(t, 3*time.Second, "TCP listener to rebind", func() bool {
		candidate, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err != nil {
			return false
		}
		conn = candidate
		return true
	})
	defer func() { _ = conn.Close() }()
	if state, err := s.State(); state != "up" || err != nil {
		t.Fatalf("state after rebind = %q/%v", state, err)
	}
	waitFor(t, time.Second, "rebound client to register", func() bool { return s.clientCount() == 1 })
	s.Broadcast([]queue.Entry{entry("nav", 1, 127250)})
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := bufio.NewReader(conn).ReadBytes('\n'); err != nil {
		t.Fatalf("rebound listener did not serve data: %v", err)
	}
}
