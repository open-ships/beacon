package buffer_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/buffer"
	"github.com/open-ships/beacon/internal/n2k"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeMsg(pgn uint32, source uint8) *n2k.ParsedMessage {
	return &n2k.ParsedMessage{
		PGN:       pgn,
		Source:    source,
		Dest:      255,
		Priority:  2,
		Timestamp: time.Now(),
		Payload:   json.RawMessage(`{"speed":3.5}`),
	}
}

func openBuf(t *testing.T, maxRows int) *buffer.SQLiteBuffer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	buf, err := buffer.Open(path, maxRows)
	require.NoError(t, err)
	t.Cleanup(func() { buf.Close() })
	return buf
}

func TestInsertRead(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	msgs := []*n2k.ParsedMessage{
		makeMsg(127250, 1),
		makeMsg(128259, 2),
		makeMsg(129026, 3),
	}
	for _, m := range msgs {
		require.NoError(t, buf.Insert(ctx, m))
	}

	got, err := buf.Read(ctx, 0, 100)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for i, m := range got {
		assert.Equal(t, msgs[i].PGN, m.PGN, "msg[%d] PGN", i)
		assert.NotZero(t, m.ID, "msg[%d] ID should be non-zero", i)
	}
}

func TestReadAfterID(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	for i := 0; i < 5; i++ {
		require.NoError(t, buf.Insert(ctx, makeMsg(127250, uint8(i))))
	}

	all, err := buf.Read(ctx, 0, 100)
	require.NoError(t, err)
	require.Len(t, all, 5)

	second := all[1].ID
	rest, err := buf.Read(ctx, second, 100)
	require.NoError(t, err)
	assert.Len(t, rest, 3)
}

func TestReadLimit(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	for i := 0; i < 10; i++ {
		require.NoError(t, buf.Insert(ctx, makeMsg(127250, uint8(i))))
	}

	got, err := buf.Read(ctx, 0, 3)
	require.NoError(t, err)
	assert.Len(t, got, 3)
}

func TestReadEmpty(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	got, err := buf.Read(ctx, 0, 100)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMaxID(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	id, err := buf.MaxID(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), id)

	require.NoError(t, buf.Insert(ctx, makeMsg(127250, 1)))
	require.NoError(t, buf.Insert(ctx, makeMsg(127250, 2)))

	id, err = buf.MaxID(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), id)
}

func TestRingPruning(t *testing.T) {
	ctx := context.Background()
	maxRows := 10
	buf := openBuf(t, maxRows)

	for i := 0; i < maxRows+5; i++ {
		require.NoError(t, buf.Insert(ctx, makeMsg(127250, uint8(i%256))))
	}

	count, err := buf.RowCount(ctx)
	require.NoError(t, err)
	assert.Less(t, count, int64(maxRows+5), "pruning should have reduced row count")
	assert.NotEqual(t, int64(maxRows+5), count, "no pruning occurred")
}

func TestCheckpointDefault(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	id, err := buf.GetCheckpoint(ctx, "unknown_sink")
	require.NoError(t, err)
	assert.Equal(t, int64(0), id)
}

func TestCheckpointUpdateAndGet(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	require.NoError(t, buf.UpdateCheckpoint(ctx, "test-sink", 42))

	id, err := buf.GetCheckpoint(ctx, "test-sink")
	require.NoError(t, err)
	assert.Equal(t, int64(42), id)
}

func TestCheckpointOverwrite(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	require.NoError(t, buf.UpdateCheckpoint(ctx, "sink", 10))
	require.NoError(t, buf.UpdateCheckpoint(ctx, "sink", 99))

	id, err := buf.GetCheckpoint(ctx, "sink")
	require.NoError(t, err)
	assert.Equal(t, int64(99), id)
}

func TestCheckpointMultipleSinks(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	require.NoError(t, buf.UpdateCheckpoint(ctx, "sse", 10))
	require.NoError(t, buf.UpdateCheckpoint(ctx, "tcp", 20))

	a, err := buf.GetCheckpoint(ctx, "sse")
	require.NoError(t, err)
	b, err := buf.GetCheckpoint(ctx, "tcp")
	require.NoError(t, err)
	assert.Equal(t, int64(10), a)
	assert.Equal(t, int64(20), b)
}

func TestCheckpointRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "checkpoint.db")

	// First session: insert and checkpoint
	var lastID int64
	{
		b, err := buffer.Open(path, 10000)
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			require.NoError(t, b.Insert(ctx, makeMsg(127250, uint8(i))))
		}
		all, err := b.Read(ctx, 0, 100)
		require.NoError(t, err)
		lastID = all[len(all)-1].ID
		require.NoError(t, b.UpdateCheckpoint(ctx, "sink_test", lastID))
		b.Close()
	}

	// Second session: verify checkpoint survives
	{
		b, err := buffer.Open(path, 10000)
		require.NoError(t, err)
		defer b.Close()

		id, err := b.GetCheckpoint(ctx, "sink_test")
		require.NoError(t, err)
		assert.Equal(t, lastID, id)

		msgs, err := b.Read(ctx, id, 100)
		require.NoError(t, err)
		assert.Empty(t, msgs, "no messages should exist after checkpoint")
	}
}

func TestInsertNilPayload(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	msg := &n2k.ParsedMessage{PGN: 127250, Timestamp: time.Now(), Payload: nil}
	require.NoError(t, buf.Insert(ctx, msg))

	got, err := buf.Read(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, json.RawMessage("null"), got[0].Payload)
}

func TestHealthCheck(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)
	assert.NoError(t, buf.HealthCheck(ctx))
}

func TestCheckpointFlusher(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	require.NoError(t, buf.Insert(ctx, makeMsg(127250, 1)))
	all, err := buf.Read(ctx, 0, 100)
	require.NoError(t, err)
	lastID := all[0].ID

	flusher := buffer.NewCheckpointFlusher(buf, 50*time.Millisecond)
	flusher.Start(ctx)
	flusher.Update("sink_test", lastID)
	time.Sleep(200 * time.Millisecond)
	flusher.Stop()

	id, err := buf.GetCheckpoint(ctx, "sink_test")
	require.NoError(t, err)
	assert.Equal(t, lastID, id)
}

func TestCheckpointFlusherFlushAll(t *testing.T) {
	ctx := context.Background()
	buf := openBuf(t, 10000)

	flusher := buffer.NewCheckpointFlusher(buf, time.Hour) // long interval — flush manually
	flusher.Update("a", 10)
	flusher.Update("b", 20)
	require.NoError(t, flusher.FlushAll(ctx))

	a, err := buf.GetCheckpoint(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, int64(10), a)

	b, err := buf.GetCheckpoint(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, int64(20), b)
}
