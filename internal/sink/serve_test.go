package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

// memReplay is an in-memory ReplayReader.
type memReplay struct{ entries []queue.Entry }

func (m *memReplay) Read(_ context.Context, after int64, limit int) ([]queue.Entry, error) {
	var out []queue.Entry
	for _, e := range m.entries {
		if e.Seq > after && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

func entry(connector string, seq int64, pgn uint32) queue.Entry {
	return queue.Entry{Seq: seq, Env: &msg.Envelope{
		Seq: seq, ConnectorID: connector, PGN: pgn, Source: 1, Dest: 255, Priority: 2,
		Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}}
}

func startSSE(t *testing.T) (*DataServer, Runtime) {
	t.Helper()
	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	rt, err := New(context.Background(), model.Sink{
		ID: "sse", Name: "SSE", Type: model.SinkHTTPSSE, Enabled: true, Path: "/events",
	}, nil, ds, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Stop)
	return ds, rt
}

func readSSEEvents(t *testing.T, resp *http.Response, n int, timeout time.Duration) []map[string]any {
	t.Helper()
	var out []map[string]any
	deadline := time.After(timeout)
	lines := make(chan string, 64)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	for len(out) < n {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("stream closed after %d events, want %d", len(out), n)
			}
			if strings.HasPrefix(line, "data:") {
				var m map[string]any
				if err := json.Unmarshal([]byte(strings.TrimSpace(line[5:])), &m); err != nil {
					t.Fatalf("bad data line %q: %v", line, err)
				}
				out = append(out, m)
			}
		case <-deadline:
			t.Fatalf("timeout after %d events, want %d", len(out), n)
		}
	}
	return out
}

func TestSSELiveBroadcast(t *testing.T) {
	ds, rt := startSSE(t)
	resp, err := http.Get(fmt.Sprintf("http://%s/events", ds.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	time.Sleep(100 * time.Millisecond) // let the client register
	rt.(Broadcaster).Broadcast([]queue.Entry{entry("nav", 1, 127250)})
	events := readSSEEvents(t, resp, 1, 3*time.Second)
	if events[0]["pgn"].(float64) != 127250 {
		t.Fatalf("event = %v", events[0])
	}
}

func TestSSEReplayWithLastEventID(t *testing.T) {
	ds, rt := startSSE(t)
	replay := &memReplay{entries: []queue.Entry{
		entry("nav", 1, 127250), entry("nav", 2, 128259), entry("nav", 3, 129026)}}
	rt.(ConnectorRegistrar).RegisterConnector("nav", replay)

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/events", ds.Addr()), nil)
	req.Header.Set("Last-Event-ID", "nav:1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	events := readSSEEvents(t, resp, 2, 3*time.Second)
	if events[0]["pgn"].(float64) != 128259 || events[1]["pgn"].(float64) != 129026 {
		t.Fatalf("replay wrong: %v", events)
	}
}

func TestDataServerRouteLifecycle(t *testing.T) {
	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	defer ds.Stop(context.Background())

	resp, _ := http.Get(fmt.Sprintf("http://%s/nope", ds.Addr()))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unrouted path = %d, want 404", resp.StatusCode)
	}
	ds.SetRoute("/x", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	resp, _ = http.Get(fmt.Sprintf("http://%s/x", ds.Addr()))
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("routed path = %d, want 418", resp.StatusCode)
	}
	ds.RemoveRoute("/x")
	resp, _ = http.Get(fmt.Sprintf("http://%s/x", ds.Addr()))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("removed path = %d, want 404", resp.StatusCode)
	}
}
