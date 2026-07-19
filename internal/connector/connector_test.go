package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/stats"
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

// pushSink records pushes; can fail N times first. Envelopes whose PGN
// equals skipPGN return sink.ErrSkip instead of being recorded, simulating
// a CAN sink skipping an undecodable envelope.
type pushSink struct {
	mu       sync.Mutex
	failures int
	skipPGN  uint32
	got      []*msg.Envelope
}

type wireSink struct {
	pushSink
	wire []*msg.Envelope
}

func (w *wireSink) PushWire(_ context.Context, e *msg.Envelope) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.wire = append(w.wire, e)
	return nil
}

func (w *wireSink) wireCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.wire)
}

func (p *pushSink) ID() string             { return "fake-push" }
func (p *pushSink) Stop()                  {}
func (p *pushSink) State() (string, error) { return "up", nil }
func (p *pushSink) Push(_ context.Context, e *msg.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.skipPGN != 0 && e.PGN == p.skipPGN {
		return sink.ErrSkip
	}
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
	reject     bool
	err        error
}

func (b *bcastSink) ID() string             { return "fake-bcast" }
func (b *bcastSink) Stop()                  {}
func (b *bcastSink) State() (string, error) { return "up", nil }
func (b *bcastSink) Broadcast(entries []queue.Entry) sink.BroadcastReport {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.got = append(b.got, entries...)
	accepted := make([]bool, len(entries))
	for i := range accepted {
		accepted[i] = !b.reject
	}
	return sink.BroadcastReport{Accepted: accepted, Err: b.err}
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

func TestRejectedBroadcastIsReportedAsDropNotDelivery(t *testing.T) {
	src := &fakeSource{}
	snk := &bcastSink{reject: true, err: errors.New("no recipient accepted message")}
	chain, _ := filter.Compile(nil)
	reg := stats.NewRegistry()
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, testQueue(t), chain, slog.Default(), nil, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	src.emit(env(127250))
	waitFor(t, 3*time.Second, func() bool {
		snap, _ := reg.Snapshot("conn1")
		return snap.StageTotals["dropped"] == 1
	}, "rejected broadcast accounting")
	snap, _ := reg.Snapshot("conn1")
	if snap.TotalMessages != 0 {
		t.Fatalf("accepted messages = %d, want 0", snap.TotalMessages)
	}
	if state, err := c.State(); state != "degraded" || err == nil {
		t.Fatalf("state = %q, err = %v; want degraded with cause", state, err)
	}
}

func TestTransparentModeUsesWirePathAndPreservesUnknownEnvelope(t *testing.T) {
	src := &fakeSource{}
	snk := &wireSink{}
	chain, _ := filter.Compile(nil)
	reg := stats.NewRegistry()
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Mode: model.BridgeTransparent, Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, testQueue(t), chain, slog.Default(), nil, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	e := env(130999)
	e.Source = 42
	e.Raw = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	src.emit(e)
	waitFor(t, 3*time.Second, func() bool { return snk.wireCount() == 1 }, "transparent wire forwarding")
	if snk.wire[0].Source != 42 || !bytes.Equal(snk.wire[0].Raw, e.Raw) {
		t.Fatalf("wire envelope changed: %+v", snk.wire[0])
	}
	snap, _ := reg.Snapshot("conn1")
	if snap.StageTotals["forwarded"] != 1 {
		t.Fatalf("stage totals = %+v", snap.StageTotals)
	}
}

func TestTransparentModeFiltersManagementByDefault(t *testing.T) {
	src := &fakeSource{}
	snk := &wireSink{}
	chain, _ := filter.Compile(nil)
	reg := stats.NewRegistry()
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Mode: model.BridgeTransparent, Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, testQueue(t), chain, slog.Default(), nil, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()
	src.emit(env(60928))
	waitFor(t, time.Second, func() bool {
		snap, _ := reg.Snapshot("conn1")
		return snap.StageTotals["management_filtered"] == 1
	}, "management policy count")
	if snk.wireCount() != 0 {
		t.Fatal("address claim was transparently forwarded")
	}
}

func TestObserveModeNeverCallsSink(t *testing.T) {
	src := &fakeSource{}
	snk := &pushSink{}
	chain, _ := filter.Compile(nil)
	reg := stats.NewRegistry()
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Mode: model.BridgeObserve, Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, testQueue(t), chain, slog.Default(), nil, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()
	src.emit(env(127250))
	waitFor(t, 3*time.Second, func() bool {
		snap, _ := reg.Snapshot("conn1")
		return snap.StageTotals["observed"] == 1
	}, "observe checkpoint")
	if snk.count() != 0 {
		t.Fatal("observe mode wrote to sink")
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

// Regression: a Pusher returning ErrSkip (e.g. a CAN sink skipping an
// undecodable envelope, see sink.canSink.Push) must still advance the
// cursor — otherwise the connector would retry the unskippable envelope
// forever — and must be counted under a "skipped" metrics stage, not
// double-counted as "delivered".
func TestPushSkipAdvancesCursorAndCountsSkipped(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "skip.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	q := queue.NewSQLite(st, "conn1", model.BufferLimits{MaxMessages: 1000})
	chain, _ := filter.Compile(nil)

	met, handler, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}

	src := &fakeSource{}
	snk := &pushSink{skipPGN: 130999}
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, q, chain, slog.Default(), met, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	src.emit(env(130999)) // skipped by the sink
	src.emit(env(1))      // delivered normally

	waitFor(t, 3*time.Second, func() bool { return snk.count() == 1 }, "the deliverable entry")
	// Stop forces the final ack (ack(true) bypasses the ackInterval gate —
	// see deliver's defer) so the checkpoint reflects both entries, exactly
	// like the checkpoint assertions in TestResumeFromCheckpointAfterRestart
	// and TestStopMidRetryCheckpointsPartialProgress above.
	c.Stop()

	entries, err := q.Read(context.Background(), 0, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("queue entries = %d, err=%v; want 2", len(entries), err)
	}
	cur, err := q.Cursor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cur != entries[1].Seq {
		t.Fatalf("checkpoint = %d, want seq of last entry %d (skip must still advance the cursor)", cur, entries[1].Seq)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	var sawSkipped, sawDelivered bool
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "beacon_connector_messages_total{") || !strings.Contains(line, `connector="conn1"`) {
			continue
		}
		switch {
		case strings.Contains(line, `stage="skipped"`) && strings.HasSuffix(line, " 1"):
			sawSkipped = true
		case strings.Contains(line, `stage="delivered"`) && strings.HasSuffix(line, " 1"):
			sawDelivered = true
		}
	}
	if !sawSkipped {
		t.Fatalf("beacon_connector_messages_total{connector=\"conn1\",stage=\"skipped\"} != 1:\n%s", body)
	}
	if !sawDelivered {
		t.Fatalf("beacon_connector_messages_total{connector=\"conn1\",stage=\"delivered\"} != 1 (skip must not be double-counted as delivered):\n%s", body)
	}
}

func TestResumeFromCheckpointAfterRestart(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
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
	defer func() { _ = st.Close() }()
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
	defer func() { _ = st.Close() }()
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

// Regression: a just-started (idle, no deliveries yet) connector must show
// up in the stats registry as soon as Start returns, not only after the
// first prune tick (pruneInterval later) — the UI dashboard's live tiles
// depend on this to list a brand-new connector without a startup gap.
// Presence only: Start seeds zero values without touching the DB (it runs
// under the supervisor's reconcile lock); the real queue depth arrives
// asynchronously moments later (see the backlog test below).
func TestStartRegistersInStatsRegistryImmediately(t *testing.T) {
	src := &fakeSource{}
	snk := &bcastSink{}
	chain, _ := filter.Compile(nil)
	reg := stats.NewRegistry()
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Buffer: model.BufferLimits{MaxMessages: 1000}},
		src, snk, testQueue(t), chain, slog.Default(), nil, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	if _, ok := reg.Snapshot("conn1"); !ok {
		t.Fatal("connector not present in stats registry immediately after Start (no sleep)")
	}
	all := reg.All()
	if _, ok := all["conn1"]; !ok {
		t.Fatalf("connector not present in Registry.All() immediately after Start: %+v", all)
	}
}

// A connector restarted over an existing backlog registers with zero queue
// stats (Start must not read the DB while the supervisor holds its
// reconcile lock), but the prune goroutine refreshes the real depth/bytes
// immediately on entry — so the true backlog must appear well before the
// first 5s prune tick, without any delivery having to happen (the sink here
// always fails).
func TestBacklogQueueDepthAppearsPromptlyAfterStart(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "b.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	q := queue.NewSQLite(st, "conn1", model.BufferLimits{MaxMessages: 1000})
	// Pre-fill a backlog before the connector starts, simulating a restart
	// (e.g. a config-change reconcile) over undelivered entries.
	if err := q.Append(context.Background(), []*msg.Envelope{env(1), env(2), env(3)}); err != nil {
		t.Fatal(err)
	}

	chain, _ := filter.Compile(nil)
	reg := stats.NewRegistry()
	snk := &pushSink{failures: 1 << 30} // never delivers: the backlog must persist
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true,
		Buffer: model.BufferLimits{MaxMessages: 1000}},
		&fakeSource{}, snk, q, chain, slog.Default(), nil, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	if _, ok := reg.Snapshot("conn1"); !ok {
		t.Fatal("connector not present in stats registry immediately after Start")
	}
	waitFor(t, 3*time.Second, func() bool {
		snap, _ := reg.Snapshot("conn1")
		return snap.QueueDepth == 3 && snap.QueueBytes > 0
	}, "real backlog depth from the prune goroutine's on-entry stats refresh")
}

func TestStopBeforeStartDoesNotPanic(t *testing.T) {
	chain, _ := filter.Compile(nil)
	c := New(model.Connector{ID: "conn1", SourceID: "s", SinkID: "k", Enabled: true},
		&fakeSource{}, &pushSink{}, testQueue(t), chain, slog.Default(), nil, nil)
	c.Stop() // must be a no-op, not a nil-cancel panic
}
