package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/app"
	"github.com/open-ships/beacon/internal/bus/busfake"
)

const seedJSON = `{
  "sources": [{"id": "can0", "name": "Main bus", "type": "socketcan", "enabled": true, "interface": "can0"}],
  "sinks": [{"id": "nav", "name": "Nav stream", "type": "http_sse", "enabled": true, "path": "/nav"}],
  "connectors": [{"id": "heading", "name": "Heading only", "source_id": "can0", "sink_id": "nav",
    "enabled": true, "filters": ["msg.pgn == 127250"], "buffer": {"max_messages": 1000}}]
}`

func startApp(t *testing.T, fake *busfake.FakeBus) *app.App {
	t.Helper()
	dir := t.TempDir()
	seed := filepath.Join(dir, "seed.json")
	if err := os.WriteFile(seed, []byte(seedJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a, err := app.Run(ctx, app.Options{
		DBPath:    filepath.Join(dir, "beacon.db"),
		DataAddr:  "127.0.0.1:0",
		AdminAddr: "127.0.0.1:0",
		SeedPath:  seed,
		Log:       slog.Default(),
		N2KBus:    fake,
		ExtraN2KOpts: []n2k.Option{
			n2k.WithClaimTimeout(50 * time.Millisecond)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	return a
}

func sseEvents(t *testing.T, resp *http.Response, n int) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(resp.Body)
	done := time.After(5 * time.Second)
	lines := make(chan string, 64)
	go func() {
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	for len(out) < n {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("stream closed at %d/%d events", len(out), n)
			}
			if strings.HasPrefix(line, "data:") {
				var m map[string]any
				_ = json.Unmarshal([]byte(strings.TrimSpace(line[5:])), &m)
				out = append(out, m)
			}
		case <-done:
			t.Fatalf("timeout at %d/%d events", len(out), n)
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

func TestEndToEndFilteredSSEWithReplay(t *testing.T) {
	fake := busfake.New()
	a := startApp(t, fake)

	resp, err := http.Get(fmt.Sprintf("http://%s/nav", a.DataAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	time.Sleep(300 * time.Millisecond)        // client registered, bus client up
	fake.Inject(busfake.VesselHeadingFrame()) // PGN 127250 — passes filter
	fake.Inject(busfake.WaterDepthFrame())    // PGN 128267 — filtered out
	fake.Inject(busfake.VesselHeadingFrame())

	events := sseEvents(t, resp, 2)
	for _, e := range events {
		if consumerEnvelopePGN(t, e) != 127250 {
			t.Fatalf("filter leaked event %v", e)
		}
		payload, metadata := consumerEnvelopeParts(t, e)
		if payload["heading"] == nil {
			t.Fatalf("SSE payload lost the n2k VesselHeading fields: %v", payload)
		}
		if metadata["connector"] != "heading" {
			t.Fatalf("connector metadata is not nested under metadata: %v", metadata)
		}
		if _, ok := e["raw"].(string); !ok {
			t.Fatalf("raw CAN bytes are not a top-level base64 value: %v", e["raw"])
		}
	}
	_ = resp.Body.Close()

	// Replay: reconnect from before the second heading.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/nav", a.DataAddr()), nil)
	_, firstMetadata := consumerEnvelopeParts(t, events[0])
	req.Header.Set("Last-Event-ID", fmt.Sprintf("heading:%d", int64(firstMetadata["id"].(float64))))
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	replayed := sseEvents(t, resp2, 1)
	if consumerEnvelopePGN(t, replayed[0]) != 127250 {
		t.Fatalf("replayed event %v", replayed[0])
	}

	// Health endpoint reports components.
	hresp, err := http.Get(fmt.Sprintf("http://%s/health", a.AdminAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hresp.Body.Close() }()
	var health struct {
		Status     string                             `json:"status"`
		Components []struct{ Kind, ID, State string } `json:"components"`
	}
	if err := json.NewDecoder(hresp.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	if len(health.Components) != 3 {
		t.Fatalf("components = %+v", health.Components)
	}

	// Metrics endpoint exposes connector counters.
	mresp, err := http.Get(fmt.Sprintf("http://%s/metrics", a.AdminAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mresp.Body.Close() }()
	var buf strings.Builder
	sc := bufio.NewScanner(mresp.Body)
	for sc.Scan() {
		buf.WriteString(sc.Text() + "\n")
	}
	if !strings.Contains(buf.String(), "beacon_connector_messages_total") {
		t.Fatal("metrics missing beacon_connector_messages_total")
	}
}
