package sink_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/buffer"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/n2k"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordSink captures sent messages via a channel.
type recordSink struct {
	name string
	ch   chan *n2k.ParsedMessage
}

func (s *recordSink) Name() string                     { return s.name }
func (s *recordSink) Start(_ context.Context) error    { return nil }
func (s *recordSink) Send(msg *n2k.ParsedMessage) error { s.ch <- msg; return nil }
func (s *recordSink) Close() error                     { return nil }

func newRecordSink(name string) *recordSink {
	return &recordSink{name: name, ch: make(chan *n2k.ParsedMessage, 32)}
}

func openTestBuf(t *testing.T) *buffer.SQLiteBuffer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	buf, err := buffer.Open(path, 10000)
	require.NoError(t, err)
	t.Cleanup(func() { buf.Close() })
	return buf
}

func TestRunDispatcher_SendsAllMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := openTestBuf(t)
	s := newRecordSink("test")
	chain := filter.Chain{}

	for i := 0; i < 3; i++ {
		require.NoError(t, buf.Insert(ctx, &n2k.ParsedMessage{
			PGN:     127250,
			Source:  uint8(i),
			Payload: json.RawMessage(`{}`),
		}))
	}

	go sink.RunDispatcher(ctx, buf, s, chain, slog.Default(), nil)

	for i := 0; i < 3; i++ {
		select {
		case msg := <-s.ch:
			assert.Equal(t, uint32(127250), msg.PGN)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

func TestRunDispatcher_FilterDropsMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := openTestBuf(t)
	s := newRecordSink("test")

	// Only pass PGN 127250
	f, err := filter.NewCELFilter("msg.pgn == 127250", slog.Default())
	require.NoError(t, err)
	chain := filter.Chain{f}

	require.NoError(t, buf.Insert(ctx, &n2k.ParsedMessage{PGN: 127250, Payload: json.RawMessage(`{}`)}))
	require.NoError(t, buf.Insert(ctx, &n2k.ParsedMessage{PGN: 128259, Payload: json.RawMessage(`{}`)}))
	require.NoError(t, buf.Insert(ctx, &n2k.ParsedMessage{PGN: 127250, Payload: json.RawMessage(`{}`)}))

	go sink.RunDispatcher(ctx, buf, s, chain, slog.Default(), nil)

	// Expect exactly 2 messages (PGN 127250 only)
	for i := 0; i < 2; i++ {
		select {
		case msg := <-s.ch:
			assert.Equal(t, uint32(127250), msg.PGN)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	// No more messages should arrive
	select {
	case extra := <-s.ch:
		t.Errorf("unexpected extra message: PGN=%d", extra.PGN)
	case <-time.After(100 * time.Millisecond):
		// correct — nothing more
	}
}

func TestRunDispatcher_ResumesFromCheckpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := openTestBuf(t)
	s := newRecordSink("test")

	// Insert 5 messages
	for i := 0; i < 5; i++ {
		require.NoError(t, buf.Insert(ctx, &n2k.ParsedMessage{
			PGN:     uint32(127250 + i),
			Payload: json.RawMessage(`{}`),
		}))
	}

	// Set checkpoint after 3rd message
	all, err := buf.Read(ctx, 0, 100)
	require.NoError(t, err)
	require.NoError(t, buf.UpdateCheckpoint(ctx, "test", all[2].ID))

	go sink.RunDispatcher(ctx, buf, s, chain2(), slog.Default(), nil)

	// Only the last 2 messages should be delivered
	for i := 0; i < 2; i++ {
		select {
		case msg := <-s.ch:
			assert.Equal(t, uint32(127253+i), msg.PGN)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

func chain2() filter.Chain { return filter.Chain{} }
