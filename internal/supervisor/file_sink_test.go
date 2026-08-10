package supervisor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

// TestFileSinkDeliversNDJSON is a supervisor-level end-to-end check for the
// file sink: a connector wired to a file sink, reconciled through the real
// supervisor/connector pipeline, must deliver a queued envelope to disk as
// an NDJSON line. Seeds the connector's durable queue directly before
// Reconcile starts it (same pattern as TestDeletedConnectorQueueIsPurged):
// queue.NewSQLite against the same store + connector id resolves to the
// table the running connector will read from.
func TestFileSinkDeliversNDJSON(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()

	path := filepath.Join(t.TempDir(), "out.ndjson")
	cfg := baseConfig()
	cfg.Sinks = append(cfg.Sinks, model.Sink{ID: "log", Name: "Log", Type: model.SinkFile,
		Enabled: true, FilePath: path, Format: model.FileFormatNDJSON})
	cfg.Connectors = append(cfg.Connectors, model.Connector{ID: "tolog", Name: "To log",
		SourceID: "up", SinkID: "log", Enabled: true, Buffer: model.BufferLimits{MaxMessages: 10}})

	q := queue.NewSQLite(st, "tolog", model.BufferLimits{MaxMessages: 10})
	if _, err := q.Append(ctx, []*msg.Envelope{
		{PGN: 127250, Source: 12, Dest: 255, Priority: 2, Timestamp: time.Now(),
			Payload: json.RawMessage(`{"heading":1.5}`)},
	}); err != nil {
		t.Fatal(err)
	}

	if err := st.ReplaceConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := sup.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	var body []byte
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
			body = b
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(body) == 0 {
		t.Fatal("timed out waiting for file sink to write the delivered envelope")
	}

	var got msg.Envelope
	if err := json.Unmarshal(body[:len(body)-1], &got); err != nil { // trim trailing '\n'
		t.Fatalf("delivered line not valid JSON: %v (%q)", err, body)
	}
	if got.PGN != 127250 || got.ConnectorID != "tolog" {
		t.Fatalf("delivered envelope = %+v, want pgn 127250 connector \"tolog\"", &got)
	}
}
