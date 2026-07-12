package connector

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/store"
)

// fakeSource implements source.Runtime.
type fakeSource struct {
	mu   sync.Mutex
	subs []chan *msg.Envelope
}

func (f *fakeSource) ID() string { return "fake-src" }
func (f *fakeSource) Subscribe(buf int) (<-chan *msg.Envelope, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan *msg.Envelope, buf)
	f.subs = append(f.subs, ch)
	return ch, func() {}
}
func (f *fakeSource) Stop()                  {}
func (f *fakeSource) State() (string, error) { return "up", nil }
func (f *fakeSource) emit(e *msg.Envelope) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.subs {
		ch <- e
	}
}

// pushSink records pushes; can fail N times first.
type pushSink struct {
	mu       sync.Mutex
	failures int
	got      []*msg.Envelope
}

func (p *pushSink) ID() string             { return "fake-push" }
func (p *pushSink) Stop()                  {}
func (p *pushSink) State() (string, error) { return "up", nil }
func (p *pushSink) Push(_ context.Context, e *msg.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return errors.New("bus down")
	}
	p.got = append(p.got, e)
	return nil
}
func (p *pushSink) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.got)
}

// bcastSink records broadcasts and registrations.
type bcastSink struct {
	mu         sync.Mutex
	got        []queue.Entry
	registered map[string]sink.ReplayReader
}

func (b *bcastSink) ID() string             { return "fake-bcast" }
func (b *bcastSink) Stop()                  {}
func (b *bcastSink) State() (string, error) { return "up", nil }
func (b *bcastSink) Broadcast(entries []queue.Entry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.got = append(b.got, entries...)
}
func (b *bcastSink) RegisterConnector(id string, r sink.ReplayReader) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.registered == nil {
		b.registered = map[string]sink.ReplayReader{}
	}
	b.registered[id] = r
}
func (b *bcastSink) UnregisterConnector(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.registered, id)
}
func (b *bcastSink) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.got)
}

// hangSink delivers the first push then fails every subsequent one,
// pinning the delivery loop in retry on the second entry.
type hangSink struct {
	mu  sync.Mutex
	got []*msg.Envelope
}

func (h *hangSink) ID() string             { return "fake-hang" }
func (h *hangSink) Stop()                  {}
func (h *hangSink) State() (string, error) { return "up", nil }
func (h *hangSink) Push(_ context.Context, e *msg.Envelope) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.got) >= 1 {
		return errors.New("stuck")
	}
	h.got = append(h.got, e)
	return nil
}
func (h *hangSink) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.got)
}

func testQueue(t *testing.T) queue.Queue {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return queue.NewSQLite(st, "conn1", model.BufferLimits{MaxMessages: 1000})
}

func env(pgnNum uint32) *msg.Envelope {
	return &msg.Envelope{PGN: pgnNum, Source: 1, Dest: 255, Priority: 2,
		Timestamp: time.Now(), Payload: json.RawMessage(`{}`), Raw: []byte{1}}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestFilterThenBroadcast(t *testing.T) {
	src := &fakeSource{}
	snk := &bcastSink{}
	chain, _ := filter.Compile([]string{"msg.pgn == 127250"})
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, testQueue(t), chain, slog.Default(), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	src.emit(env(127250))
	src.emit(env(999)) // filtered out
	src.emit(env(127250))

	waitFor(t, 3*time.Second, func() bool { return snk.count() == 2 }, "2 broadcasts")
	if snk.registered["conn1"] == nil {
		t.Fatal("connector did not register for replay")
	}
}

func TestPushRetriesUntilSinkRecovers(t *testing.T) {
	src := &fakeSource{}
	snk := &pushSink{failures: 3}
	chain, _ := filter.Compile(nil)
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, testQueue(t), chain, slog.Default(), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	src.emit(env(127250))
	waitFor(t, 10*time.Second, func() bool { return snk.count() == 1 }, "delivery after retries")
}

func TestResumeFromCheckpointAfterRestart(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	q := queue.NewSQLite(st, "conn1", model.BufferLimits{MaxMessages: 1000})
	chain, _ := filter.Compile(nil)

	// First run: deliver 2 messages.
	src1 := &fakeSource{}
	snk1 := &pushSink{}
	c1 := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true},
		src1, snk1, q, chain, slog.Default(), nil, nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	c1.Start(ctx1)
	src1.emit(env(1))
	src1.emit(env(2))
	waitFor(t, 3*time.Second, func() bool { return snk1.count() == 2 }, "first run deliveries")
	c1.Stop()
	cancel1()

	// Second run on the same queue: nothing replays (checkpoint held).
	src2 := &fakeSource{}
	snk2 := &pushSink{}
	c2 := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true},
		src2, snk2, q, chain, slog.Default(), nil, nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	c2.Start(ctx2)
	defer c2.Stop()
	src2.emit(env(3))
	waitFor(t, 3*time.Second, func() bool { return snk2.count() == 1 }, "second run delivery")
	if got := snk2.got[0].PGN; got != 3 {
		t.Fatalf("redelivered old message, pgn=%d", got)
	}
}

// Regression: messages emitted just before Stop (before the 50ms flush
// timer fires) must still be persisted to the queue by the shutdown flush.
func TestStopFlushesPendingBatch(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "f.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	q := queue.NewSQLite(st, "conn1", model.BufferLimits{MaxMessages: 1000})
	chain, _ := filter.Compile(nil)

	src := &fakeSource{}
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true},
		src, &pushSink{}, q, chain, slog.Default(), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	src.emit(env(1))
	src.emit(env(2))
	src.emit(env(3))
	c.Stop() // immediately, before the batch flush timer fires

	entries, err := q.Read(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("queue has %d entries after Stop, want 3", len(entries))
	}
}

// Regression: when Stop lands mid-retry, entries already pushed from the
// in-flight batch must be checkpointed so a restart does not redeliver
// them to a non-idempotent sink.
func TestStopMidRetryCheckpointsPartialProgress(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "p.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	q := queue.NewSQLite(st, "conn1", model.BufferLimits{MaxMessages: 1000})
	chain, _ := filter.Compile(nil)

	// First run: entry 1 delivers, entry 2 fails forever (sink stuck).
	src1 := &fakeSource{}
	snk1 := &hangSink{}
	c1 := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true},
		src1, snk1, q, chain, slog.Default(), nil, nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	c1.Start(ctx1)
	src1.emit(env(1))
	src1.emit(env(2))
	waitFor(t, 3*time.Second, func() bool { return snk1.count() == 1 }, "first entry delivered")
	c1.Stop() // mid-retry on entry 2
	cancel1()

	// The checkpoint must reflect entry 1's seq.
	entries, err := q.Read(context.Background(), 0, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("queue read: %d entries, err=%v; want 2", len(entries), err)
	}
	cur, err := q.Cursor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cur != entries[0].Seq {
		t.Fatalf("checkpoint=%d, want seq of delivered entry 1 (%d)", cur, entries[0].Seq)
	}

	// Second run with a healthy sink: only entry 2 replays, never entry 1.
	src2 := &fakeSource{}
	snk2 := &pushSink{}
	c2 := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true},
		src2, snk2, q, chain, slog.Default(), nil, nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	c2.Start(ctx2)
	defer c2.Stop()
	waitFor(t, 3*time.Second, func() bool { return snk2.count() == 1 }, "entry 2 replay")
	if got := snk2.got[0].PGN; got != 2 {
		t.Fatalf("redelivered already-pushed entry, pgn=%d, want 2", got)
	}
}

func TestStopBeforeStartDoesNotPanic(t *testing.T) {
	chain, _ := filter.Compile(nil)
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true},
		&fakeSource{}, &pushSink{}, testQueue(t), chain, slog.Default(), nil, nil)
	c.Stop() // must be a no-op, not a nil-cancel panic
}
