package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/open-ships/n2k/pgn"
	"github.com/open-ships/n2k/raw"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

// waitFor polls cond until true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

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

type blockingReplay struct {
	started  chan struct{}
	canceled chan error
}

func (r *blockingReplay) Read(ctx context.Context, _ int64, _ int) ([]queue.Entry, error) {
	close(r.started)
	<-ctx.Done()
	r.canceled <- ctx.Err()
	return nil, ctx.Err()
}

func entry(connector string, seq int64, pgnNumber uint32) queue.Entry {
	receivedAt := time.Now()
	payload, _ := json.Marshal(struct {
		Info pgn.MessageInfo `json:"info"`
	}{
		Info: pgn.MessageInfo{
			Timestamp:  receivedAt,
			ReceivedAt: receivedAt,
			AdapterID:  "socketcan:can0",
			NetworkID:  "can0",
			Direction:  raw.DirectionReceived,
			Priority:   pgn.Priority(2),
			PGN:        pgnNumber,
			SourceId:   1,
		},
	})
	return queue.Entry{Seq: seq, Env: &msg.Envelope{
		Seq: seq, ConnectorID: connector, PGN: pgnNumber, Source: 1, Dest: 255, Priority: 2,
		Timestamp: receivedAt, Payload: payload, Raw: []byte{1, 2, 3}}}
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

func TestDataServerSetsBoundedHTTPTimeoutsWithoutWriteTimeout(t *testing.T) {
	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if ds.srv.ReadHeaderTimeout != dataReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", ds.srv.ReadHeaderTimeout, dataReadHeaderTimeout)
	}
	if ds.srv.ReadTimeout != dataReadTimeout || ds.srv.IdleTimeout != dataIdleTimeout {
		t.Fatalf("read/idle timeouts = %s/%s, want %s/%s",
			ds.srv.ReadTimeout, ds.srv.IdleTimeout, dataReadTimeout, dataIdleTimeout)
	}
	if ds.srv.MaxHeaderBytes != dataMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", ds.srv.MaxHeaderBytes, dataMaxHeaderBytes)
	}
	if ds.srv.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout = %s, want zero for streaming endpoints", ds.srv.WriteTimeout)
	}
}

func TestDataServerRecoversUnexpectedListenerFailure(t *testing.T) {
	oldMin, oldMax := dataRetryMin, dataRetryMax
	dataRetryMin, dataRetryMax = 100*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() { dataRetryMin, dataRetryMax = oldMin, oldMax })

	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	address := ds.Addr()
	ds.mu.RLock()
	ln := ds.ln
	ds.mu.RUnlock()
	if ln == nil {
		t.Fatal("data listener is nil")
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	// Hold the stable address through at least one retry so the failure is
	// observable rather than racing directly from closed to recovered.
	blocker, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, "failed data server rebind to remain observable", func() bool {
		state, stateErr := ds.State()
		return state == "error" && stateErr != nil && strings.Contains(stateErr.Error(), "rebind data endpoints")
	})
	if err := blocker.Close(); err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Timeout: 100 * time.Millisecond}
	waitFor(t, 2*time.Second, "data listener recovery", func() bool {
		resp, err := client.Get("http://" + address + "/not-configured")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		state, stateErr := ds.State()
		return resp.StatusCode == http.StatusNotFound && state == "up" && stateErr == nil
	})
	if got := ds.Addr(); got != address {
		t.Fatalf("recovered address = %q, want stable %q", got, address)
	}
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

func consumerEnvelopeParts(t *testing.T, event map[string]any) (map[string]any, map[string]any) {
	t.Helper()
	if len(event) != 3 {
		t.Fatalf("consumer envelope must have only payload, metadata, and raw at the top level: %v", event)
	}
	payload, ok := event["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload is not an object: %v", event["payload"])
	}
	metadata, ok := event["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata is not an object: %v", event["metadata"])
	}
	if _, ok := event["raw"]; !ok {
		t.Fatalf("raw CAN bytes are not a separate top-level value: %v", event)
	}
	return payload, metadata
}

func consumerEnvelopePGN(t *testing.T, event map[string]any) float64 {
	t.Helper()
	payload, _ := consumerEnvelopeParts(t, event)
	info, ok := payload["info"].(map[string]any)
	if !ok {
		t.Fatalf("payload does not contain the n2k info struct: %v", payload)
	}
	pgnNumber, ok := info["pgn"].(float64)
	if !ok {
		t.Fatalf("payload.info.pgn is not a number: %v", info["pgn"])
	}
	return pgnNumber
}

func assertNativeMessageInfoPreserved(t *testing.T, event map[string]any) {
	t.Helper()
	payload, metadata := consumerEnvelopeParts(t, event)
	info, ok := payload["info"].(map[string]any)
	if !ok {
		t.Fatalf("payload does not contain the n2k info struct: %v", payload)
	}
	for _, key := range []string{"receivedAt", "transportTimestamp", "hasTransportTimestamp", "adapterId", "networkId", "direction"} {
		if key == "transportTimestamp" || key == "hasTransportTimestamp" {
			continue // omitted by n2k when this synthetic message has no transport timestamp
		}
		if _, ok := info[key]; !ok {
			t.Fatalf("native MessageInfo field %q missing from payload.info: %v", key, info)
		}
	}
	for _, key := range []string{"received_at", "adapter_id", "network_id", "direction"} {
		if _, ok := metadata[key]; ok {
			t.Fatalf("native MessageInfo field %q was duplicated into metadata: %v", key, metadata)
		}
	}
}

func TestSSELiveBroadcast(t *testing.T) {
	ds, rt := startSSE(t)
	resp, err := http.Get(fmt.Sprintf("http://%s/events", ds.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	time.Sleep(100 * time.Millisecond) // let the client register
	rt.(Broadcaster).Broadcast([]queue.Entry{entry("nav", 1, 127250)})
	events := readSSEEvents(t, resp, 1, 3*time.Second)
	if consumerEnvelopePGN(t, events[0]) != 127250 {
		t.Fatalf("event = %v", events[0])
	}
	_, metadata := consumerEnvelopeParts(t, events[0])
	if metadata["connector"] != "nav" || metadata["id"] != float64(1) {
		t.Fatalf("Beacon route metadata is not nested under metadata: %v", metadata)
	}
	if _, ok := events[0]["raw"].(string); !ok {
		t.Fatalf("raw CAN bytes are not a top-level base64 value: %v", events[0]["raw"])
	}
	assertNativeMessageInfoPreserved(t, events[0])
}

func TestServeSinkClientLimit(t *testing.T) {
	ds, rt := startSSE(t)
	s := rt.(*serveSink)
	clients := make([]*client, 0, maxServeClients)
	defer func() {
		for _, c := range clients {
			s.removeClient(c)
		}
	}()
	for range maxServeClients {
		c, ok := s.addClient(func() {})
		if !ok {
			t.Fatalf("client %d rejected before limit %d", len(clients)+1, maxServeClients)
		}
		clients = append(clients, c)
	}
	if c, ok := s.addClient(func() {}); ok {
		s.removeClient(c)
		t.Fatalf("client %d accepted beyond limit %d", maxServeClients+1, maxServeClients)
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/events", ds.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("SSE over client limit = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
}

func TestWebSocketClientLimitRejectsBeforeUpgrade(t *testing.T) {
	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	rt, err := New(context.Background(), model.Sink{
		ID: "ws-limit", Type: model.SinkHTTPWS, Enabled: true, Path: "/ws-limit",
	}, nil, ds, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Stop)
	s := rt.(*serveSink)
	clients := make([]*client, 0, maxServeClients)
	defer func() {
		for _, c := range clients {
			s.removeClient(c)
		}
	}()
	for range maxServeClients {
		c, ok := s.addClient(func() {})
		if !ok {
			t.Fatalf("client %d rejected before limit %d", len(clients)+1, maxServeClients)
		}
		clients = append(clients, c)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, fmt.Sprintf("ws://%s/ws-limit", ds.Addr()), nil)
	if conn != nil {
		_ = conn.CloseNow()
	}
	if err == nil {
		t.Fatal("WebSocket client beyond the limit was upgraded")
	}
	if resp == nil {
		t.Fatalf("WebSocket rejection did not return an HTTP response: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("WebSocket over client limit = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
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
	defer func() { _ = resp.Body.Close() }()
	events := readSSEEvents(t, resp, 2, 3*time.Second)
	if consumerEnvelopePGN(t, events[0]) != 128259 || consumerEnvelopePGN(t, events[1]) != 129026 {
		t.Fatalf("replay wrong: %v", events)
	}
}

func TestServeSinkStopCancelsBlockedReplayAndWaitsForHandler(t *testing.T) {
	ds, rt := startSSE(t)
	replay := &blockingReplay{started: make(chan struct{}), canceled: make(chan error, 1)}
	rt.(ConnectorRegistrar).RegisterConnector("nav", replay)

	resp, err := http.Get(fmt.Sprintf("http://%s/events?after=nav:0", ds.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	select {
	case <-replay.started:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not enter blocked replay")
	}

	stopped := make(chan struct{})
	go func() {
		rt.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("serve sink Stop did not cancel and join blocked replay handler")
	}
	select {
	case err := <-replay.canceled:
		if err != context.Canceled {
			t.Fatalf("replay context error = %v, want context.Canceled", err)
		}
	default:
		t.Fatal("serve sink Stop returned before replay observed cancellation")
	}
	if got := rt.(*serveSink).clientCount(); got != 0 {
		t.Fatalf("client count after Stop = %d, want 0", got)
	}
}

func TestDataServerRouteLifecycle(t *testing.T) {
	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ds.Stop(context.Background()) }()

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

// TestDataServerStopWithActiveSSEStream asserts Stop returns promptly while
// a client is connected and streaming: cancelling the server's base context
// must end the handler so Shutdown is not held open until ctx expiry.
func TestDataServerStopWithActiveSSEStream(t *testing.T) {
	ds, rt := startSSE(t)

	resp, err := http.Get(fmt.Sprintf("http://%s/events", ds.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Prove the stream is live before stopping.
	waitFor(t, 3*time.Second, "SSE client to register",
		func() bool { return rt.(*serveSink).clientCount() == 1 })
	rt.(Broadcaster).Broadcast([]queue.Entry{entry("nav", 1, 127250)})
	_ = readSSEEvents(t, resp, 1, 3*time.Second)

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := ds.Stop(stopCtx); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Stop took %v with an active stream, want < 1s", elapsed)
	}
}

// TestWSDeadClientDetected asserts the server notices a WS client that went
// away without any Broadcast traffic: the read pump (CloseRead) must observe
// the closed connection and unwind the handler, releasing the client slot.
func TestWSDeadClientDetected(t *testing.T) {
	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	rt, err := New(context.Background(), model.Sink{
		ID: "ws", Name: "WS", Type: model.SinkHTTPWS, Enabled: true, Path: "/ws",
	}, nil, ds, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Stop)
	s := rt.(*serveSink)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://%s/ws", ds.Addr()), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "WS client to register",
		func() bool { return s.clientCount() == 1 })

	_ = conn.CloseNow() // abrupt client death, no traffic in flight
	waitFor(t, 3*time.Second, "dead WS client to be dropped",
		func() bool { return s.clientCount() == 0 })
}

func TestWSLiveBroadcastUsesConsumerEnvelope(t *testing.T) {
	ds := NewDataServer("127.0.0.1:0", slog.Default())
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ds.Stop(context.Background()) })
	rt, err := New(context.Background(), model.Sink{
		ID: "ws-contract", Name: "WS", Type: model.SinkHTTPWS, Enabled: true, Path: "/ws-contract",
	}, nil, ds, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://%s/ws-contract", ds.Addr()), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.CloseNow() }()
	waitFor(t, 3*time.Second, "WS client to register",
		func() bool { return rt.(*serveSink).clientCount() == 1 })

	rt.(Broadcaster).Broadcast([]queue.Entry{entry("nav", 7, 127250)})
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var event map[string]any
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	if consumerEnvelopePGN(t, event) != 127250 {
		t.Fatalf("event = %v", event)
	}
	_, metadata := consumerEnvelopeParts(t, event)
	if metadata["connector"] != "nav" || metadata["id"] != float64(7) {
		t.Fatalf("Beacon route metadata is not nested under metadata: %v", metadata)
	}
	if _, ok := event["raw"].(string); !ok {
		t.Fatalf("raw CAN bytes are not a top-level base64 value: %v", event["raw"])
	}
	assertNativeMessageInfoPreserved(t, event)
}
