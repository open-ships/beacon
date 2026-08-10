package queue

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/store"
)

func testQueue(t *testing.T, limits model.BufferLimits) Queue {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return NewSQLite(st, "test-conn", limits)
}

func env(pgn uint32, ts time.Time) *msg.Envelope {
	return &msg.Envelope{PGN: pgn, Source: 1, Dest: 255, Priority: 3,
		Timestamp: ts, ObservedAt: ts, Payload: json.RawMessage(`{"x":1}`)}
}

func appendN(t *testing.T, q Queue, n int, start time.Time) PruneResult {
	t.Helper()
	var batch []*msg.Envelope
	for i := 0; i < n; i++ {
		batch = append(batch, env(uint32(127000+i), start.Add(time.Duration(i)*time.Second)))
	}
	pruned, err := q.Append(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	return pruned
}

func TestAppendReadAck(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxMessages: 100})
	ctx := context.Background()
	appendN(t, q, 5, time.Now())

	entries, err := q.Read(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("read %d entries, want 5", len(entries))
	}
	if entries[0].Env.Seq != entries[0].Seq || entries[0].Env.ConnectorID != "test-conn" {
		t.Fatalf("entry env not annotated: %+v", entries[0].Env)
	}
	if entries[0].Seq >= entries[4].Seq {
		t.Fatal("entries not in ascending seq order")
	}

	// resume after partial read
	mid := entries[2].Seq
	rest, _ := q.Read(ctx, mid, 10)
	if len(rest) != 2 {
		t.Fatalf("read after mid = %d entries, want 2", len(rest))
	}

	if err := q.Ack(ctx, entries[4].Seq); err != nil {
		t.Fatal(err)
	}
	cur, err := q.Cursor(ctx)
	if err != nil || cur != entries[4].Seq {
		t.Fatalf("cursor = %d, %v; want %d", cur, err, entries[4].Seq)
	}
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Depth != 0 || stats.RetainedDepth != 5 || stats.Cursor != entries[4].Seq {
		t.Fatalf("stats after ack = %+v; want 0 pending, 5 retained", stats)
	}
}

func TestStatsSeparatesPendingFromRetainedHistory(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxMessages: 100})
	ctx := context.Background()
	appendN(t, q, 5, time.Now())
	entries, err := q.Read(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(ctx, entries[1].Seq); err != nil {
		t.Fatal(err)
	}
	got, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Depth != 3 || got.RetainedDepth != 5 || got.Tail != entries[4].Seq {
		t.Fatalf("stats = %+v; want 3 pending and 5 retained", got)
	}
}

func TestPruneByCount(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxMessages: 3})
	ctx := context.Background()
	pruned := appendN(t, q, 10, time.Now())
	if pruned.Total != 7 || pruned.Pending != 7 {
		t.Fatalf("pruned %+v, want total/pending 7", pruned)
	}
	if pruned.TotalBytes == 0 || pruned.PendingBytes != pruned.TotalBytes {
		t.Fatalf("pruned bytes = %+v, want pending bytes reported", pruned)
	}
	st, _ := q.Stats(ctx)
	if st.Depth != 3 {
		t.Fatalf("depth = %d, want 3", st.Depth)
	}
}

func TestPruneReportsRetainedVersusPendingLoss(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxMessages: 10})
	ctx := context.Background()
	appendN(t, q, 10, time.Now())
	entries, err := q.Read(ctx, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(ctx, entries[len(entries)-1].Seq); err != nil {
		t.Fatal(err)
	}
	q.(*sqliteQueue).limits.MaxMessages = 3
	pruned, err := q.Prune(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pruned.Total != 7 || pruned.Pending != 0 {
		t.Fatalf("pruned = %+v; want retained-only removal", pruned)
	}
}

func TestPruneByAge(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxAge: model.Duration(time.Hour)})
	old := time.Now().Add(-2 * time.Hour)
	pruned := appendN(t, q, 4, old) // all stale
	appendN(t, q, 3, time.Now())    // fresh
	if pruned.Total != 4 || pruned.Pending != 4 {
		t.Fatalf("pruned %+v, want total/pending 4", pruned)
	}
}

func TestPruneByAgeUsesLocalObservationNotWireTimestamp(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxAge: model.Duration(time.Hour)})
	ctx := context.Background()
	now := time.Now().UTC()
	replay := env(1, now.Add(-24*time.Hour))
	replay.ObservedAt = now
	futureClock := env(2, now.Add(24*time.Hour))
	futureClock.ObservedAt = now.Add(-2 * time.Hour)
	pruned, err := q.Append(ctx, []*msg.Envelope{replay, futureClock})
	if err != nil {
		t.Fatal(err)
	}
	if pruned.Total != 1 || pruned.Pending != 1 {
		t.Fatalf("pruned %+v, want only the locally stale observation", pruned)
	}
	entries, err := q.Read(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Env.PGN != replay.PGN || !entries[0].Env.Timestamp.Equal(replay.Timestamp) {
		t.Fatalf("retained entries = %+v, want recent replay with canonical wire timestamp", entries)
	}
}

func TestPruneByBytes(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxBytes: 1}) // absurdly small: keep nothing but newest
	ctx := context.Background()
	pruned := appendN(t, q, 5, time.Now())
	if pruned.Total != 5 || pruned.Pending != 5 {
		t.Fatalf("pruned = %+v, want every oversized row removed", pruned)
	}
	st, _ := q.Stats(ctx)
	if st.Depth > 1 {
		t.Fatalf("depth = %d after byte prune, want <= 1", st.Depth)
	}
}

func TestPurgeRemovesRowsAndCheckpoint(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxMessages: 100})
	ctx := context.Background()
	appendN(t, q, 5, time.Now())
	entries, _ := q.Read(ctx, 0, 10)
	_ = q.Ack(ctx, entries[4].Seq)

	if err := q.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := q.Stats(ctx)
	if st.Depth != 0 {
		t.Fatalf("depth = %d after purge", st.Depth)
	}
	cur, _ := q.Cursor(ctx)
	if cur != 0 {
		t.Fatalf("cursor = %d after purge, want 0", cur)
	}
	var aggregateRows int
	sqlite := q.(*sqliteQueue)
	if err := sqlite.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM queue_aggregates WHERE connector_id = ?`, sqlite.connectorID).Scan(&aggregateRows); err != nil {
		t.Fatal(err)
	}
	if aggregateRows != 0 {
		t.Fatalf("aggregate rows = %d after purge and stats read, want 0", aggregateRows)
	}
}

func TestQueuesAreIsolated(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	qa := NewSQLite(st, "a", model.BufferLimits{MaxMessages: 100})
	qb := NewSQLite(st, "b", model.BufferLimits{MaxMessages: 100})
	ctx := context.Background()
	_, _ = qa.Append(ctx, []*msg.Envelope{env(1, time.Now())})
	entries, _ := qb.Read(ctx, 0, 10)
	if len(entries) != 0 {
		t.Fatal("queue b sees queue a's entries")
	}
}

func TestStatsAggregateSelfHealsWithoutChangingDeliveryTruth(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	q := NewSQLite(st, "repair", model.BufferLimits{MaxMessages: 100})
	ctx := context.Background()
	appendN(t, q, 5, time.Now())
	entries, err := q.Read(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(ctx, entries[1].Seq); err != nil {
		t.Fatal(err)
	}
	want, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `DELETE FROM queue_aggregates WHERE connector_id = ?`, "repair"); err != nil {
		t.Fatal(err)
	}
	got, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Depth != want.Depth || got.Bytes != want.Bytes ||
		got.RetainedDepth != want.RetainedDepth || got.RetainedBytes != want.RetainedBytes ||
		got.Cursor != want.Cursor || got.Tail != want.Tail ||
		!got.Oldest.Equal(want.Oldest) || !got.OldestRetained.Equal(want.OldestRetained) {
		t.Fatalf("rebuilt stats = %+v, want %+v", got, want)
	}
}

func TestStatsOldestUsesQueueOrderForOutOfOrderTimestamps(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxMessages: 100})
	ctx := context.Background()
	first := time.Unix(200, 0)
	second := time.Unix(100, 0)
	if _, err := q.Append(ctx, []*msg.Envelope{env(1, first)}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Append(ctx, []*msg.Envelope{env(2, second)}); err != nil {
		t.Fatal(err)
	}
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Oldest.Equal(first) || !stats.OldestRetained.Equal(first) {
		t.Fatalf("oldest = %v retained = %v; want first queued timestamp %v", stats.Oldest, stats.OldestRetained, first)
	}
	entries, err := q.Read(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(ctx, entries[0].Seq); err != nil {
		t.Fatal(err)
	}
	stats, err = q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Oldest.Equal(second) || !stats.OldestRetained.Equal(first) {
		t.Fatalf("oldest after ack = %v retained = %v; want %v / %v", stats.Oldest, stats.OldestRetained, second, first)
	}
}
