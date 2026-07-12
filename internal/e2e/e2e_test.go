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
		ExtraN2KOpts: []n2k.Option{
			n2k.WithBus(fake), n2k.WithClaimTimeout(50 * time.Millisecond)},
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

func TestEndToEndFilteredSSEWithReplay(t *testing.T) {
	fake := busfake.New()
	a := startApp(t, fake)

	resp, err := http.Get(fmt.Sprintf("http://%s/nav", a.DataAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	time.Sleep(300 * time.Millisecond)        // client registered, bus client up
	fake.Inject(busfake.VesselHeadingFrame()) // PGN 127250 — passes filter
	fake.Inject(busfake.WaterDepthFrame())    // PGN 128267 — filtered out
	fake.Inject(busfake.VesselHeadingFrame())

	events := sseEvents(t, resp, 2)
	for _, e := range events {
		if e["pgn"].(float64) != 127250 {
			t.Fatalf("filter leaked pgn %v", e["pgn"])
		}
	}
	resp.Body.Close()

	// Replay: reconnect from before the second heading.
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/nav", a.DataAddr()), nil)
	req.Header.Set("Last-Event-ID", fmt.Sprintf("heading:%d", int64(events[0]["id"].(float64))))
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	replayed := sseEvents(t, resp2, 1)
	if replayed[0]["pgn"].(float64) != 127250 {
		t.Fatalf("replayed pgn %v", replayed[0]["pgn"])
	}

	// Health endpoint reports components.
	hresp, err := http.Get(fmt.Sprintf("http://%s/health", a.AdminAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer hresp.Body.Close()
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
	defer mresp.Body.Close()
	var buf strings.Builder
	sc := bufio.NewScanner(mresp.Body)
	for sc.Scan() {
		buf.WriteString(sc.Text() + "\n")
	}
	if !strings.Contains(buf.String(), "beacon_connector_messages_total") {
		t.Fatal("metrics missing beacon_connector_messages_total")
	}
}
