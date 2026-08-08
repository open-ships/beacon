package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/open-ships/beacon/internal/model"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestReplaceAndLoad(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	empty, err := s.IsEmpty(ctx)
	if err != nil || !empty {
		t.Fatalf("fresh store IsEmpty = %v, %v", empty, err)
	}

	cfg := model.Config{
		Sources:    []model.Source{{ID: "can0", Name: "Bus", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}},
		Sinks:      []model.Sink{{ID: "sse", Name: "SSE", Type: model.SinkHTTPSSE, Enabled: true, Path: "/events"}},
		Connectors: []model.Connector{{ID: "all", Name: "All", SourceID: "can0", SinkID: "sse", Enabled: true, Buffer: model.BufferLimits{MaxMessages: 10}}},
	}
	if err := s.ReplaceConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Sources) != 1 || got.Sources[0].Interface != "can0" ||
		len(got.Sinks) != 1 || got.Sinks[0].Path != "/events" ||
		len(got.Connectors) != 1 || got.Connectors[0].Buffer.MaxMessages != 10 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestUpsertAndDelete(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	src := model.Source{ID: "u1", Name: "USB", Type: model.SourceUSBCAN, Enabled: true, Port: "/dev/ttyUSB0"}
	if err := s.PutSource(ctx, src); err != nil {
		t.Fatal(err)
	}
	src.Name = "USB adapter"
	if err := s.PutSource(ctx, src); err != nil {
		t.Fatal(err)
	}
	cfg, _ := s.LoadConfig(ctx)
	if len(cfg.Sources) != 1 || cfg.Sources[0].Name != "USB adapter" {
		t.Fatalf("upsert failed: %+v", cfg.Sources)
	}
	if err := s.DeleteSource(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	cfg, _ = s.LoadConfig(ctx)
	if len(cfg.Sources) != 0 {
		t.Fatal("delete failed")
	}
}

// A fresh store's LoadConfig must return non-nil empty slices, not nil, so
// callers built on it (config.Service, the /api/v1/config/export JSON body)
// serialize "sources":[] rather than "sources":null.
func TestLoadConfigEmptySlicesNotNil(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	cfg, err := s.LoadConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Sources == nil || cfg.Sinks == nil || cfg.Connectors == nil {
		t.Fatalf("LoadConfig on empty store returned nil slice(s): sources=%v sinks=%v connectors=%v",
			cfg.Sources, cfg.Sinks, cfg.Connectors)
	}
	if len(cfg.Sources) != 0 || len(cfg.Sinks) != 0 || len(cfg.Connectors) != 0 {
		t.Fatalf("LoadConfig on empty store returned non-empty slice(s): %+v", cfg)
	}

	doc, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := string(doc)
	want := `{"sources":[],"sinks":[],"connectors":[]}`
	if got != want {
		t.Fatalf("LoadConfig JSON = %s, want %s", got, want)
	}
}

func TestKnownConnectorIDs(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	// "a" has aggregate state, "b" has a checkpoint only, "a" appears in
	// both — the result must be the deduplicated union.
	if _, err := s.DB().ExecContext(ctx,
		`INSERT INTO queue_aggregates (connector_id) VALUES ('a')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().ExecContext(ctx,
		`INSERT INTO checkpoints (connector_id, last_seq) VALUES ('a', 1), ('b', 0)`); err != nil {
		t.Fatal(err)
	}

	got, err := s.KnownConnectorIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, id := range got {
		seen[id] = true
	}
	if len(seen) != 2 || !seen["a"] || !seen["b"] {
		t.Fatalf("KnownConnectorIDs = %v, want [a b]", got)
	}
}

func TestReopenKeepsData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = s.PutSink(ctx, model.Sink{ID: "t", Name: "T", Type: model.SinkTCP, Enabled: true, Address: "0.0.0.0:9090"})
	_ = s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	cfg, _ := s2.LoadConfig(ctx)
	if len(cfg.Sinks) != 1 {
		t.Fatal("data lost across reopen")
	}
}

func TestSourceTrafficBaselineTableIsRemovedOnUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-removal.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	for i, migration := range migrations[:3] {
		if _, err := db.Exec(migration); err != nil {
			t.Fatalf("migration %d setup: %v", i+1, err)
		}
		if _, err := db.Exec(`INSERT INTO schema_migrations(version) VALUES (?)`, i+1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO source_metric_baselines (source_id, identity, pgn, approved_at, doc)
		VALUES ('source-1', 'address:10', 127250, 1, '{}')
	`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	var count int
	if err := s.DB().QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'source_metric_baselines'`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("source_metric_baselines table count = %d, want 0", count)
	}
}

func TestQueueAggregateMigrationBackfillsPartialCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-aggregate.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	for i, migration := range migrations[:4] {
		if _, err := db.Exec(migration); err != nil {
			t.Fatalf("migration %d setup: %v", i+1, err)
		}
		if _, err := db.Exec(`INSERT INTO schema_migrations(version) VALUES (?)`, i+1); err != nil {
			t.Fatal(err)
		}
	}
	for i, ts := range []int64{300, 100, 200} {
		if _, err := db.Exec(`INSERT INTO queue(connector_id, ts, envelope, bytes) VALUES ('route', ?, '{}', ?)`, ts, 10+i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO checkpoints(connector_id, last_seq) VALUES ('route', 1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	var pendingCount, pendingBytes, retainedCount, retainedBytes, oldestPending, oldestRetained, tail int64
	if err := st.DB().QueryRow(`
		SELECT pending_count, pending_bytes, retained_count, retained_bytes,
		       oldest_pending_ts, oldest_retained_ts, tail_id
		FROM queue_aggregates WHERE connector_id = 'route'`).Scan(
		&pendingCount, &pendingBytes, &retainedCount, &retainedBytes,
		&oldestPending, &oldestRetained, &tail); err != nil {
		t.Fatal(err)
	}
	if pendingCount != 2 || pendingBytes != 23 || retainedCount != 3 || retainedBytes != 33 ||
		oldestPending != 100 || oldestRetained != 300 || tail != 3 {
		t.Fatalf("backfilled aggregate = pending %d/%d retained %d/%d oldest %d/%d tail %d",
			pendingCount, pendingBytes, retainedCount, retainedBytes, oldestPending, oldestRetained, tail)
	}
}
