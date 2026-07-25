package stats

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/msg"
)

func TestNilRegistrySafe(t *testing.T) {
	var r *Registry
	r.Record("c", 1, 10)
	r.SetQueue("c", 1, 10)
	r.Touch("c")
	r.Remove("c")
	r.RemoveSource("s")
	r.RemoveSink("s")
	if _, ok := r.Snapshot("c"); ok {
		t.Fatal("nil registry returned a snapshot")
	}
	if len(r.All()) != 0 {
		t.Fatal("nil registry All() non-empty")
	}
}

func TestRemoveDropsComponentCountersAndEvents(t *testing.T) {
	r := NewRegistry()
	e := &msg.Envelope{PGN: 127250}
	r.RecordSource("in", e)
	r.RecordSink("out", "route", e)
	r.Record("route", 1, 10)
	r.RecordConnectorEvent("route", "received", e)

	r.RemoveSource("in")
	r.RemoveSink("out")
	r.Remove("route")

	if _, ok := r.SourceSnapshot("in"); ok {
		t.Fatal("removed source still has snapshot")
	}
	if _, ok := r.SinkSnapshot("out"); ok {
		t.Fatal("removed sink still has snapshot")
	}
	if _, ok := r.Snapshot("route"); ok {
		t.Fatal("removed connector still has snapshot")
	}
	for _, key := range []struct{ kind, id string }{{"source", "in"}, {"sink", "out"}, {"connector", "route"}} {
		if events := r.Recent(key.kind, key.id, 10); len(events) != 0 {
			t.Fatalf("removed %s %q still has events: %+v", key.kind, key.id, events)
		}
	}
}

func TestStreamSubscriptionsReceiveCanonicalFutureEnvelopesByBoundary(t *testing.T) {
	r := NewRegistry()
	sourceStream, stopSource := r.SubscribeStream("source", "in", 2)
	defer stopSource()
	sinkStream, stopSink := r.SubscribeStream("sink", "out", 2)
	defer stopSink()

	envelope := &msg.Envelope{
		PGN:      130314,
		Source:   6,
		Dest:     255,
		Priority: 5,
		Payload: json.RawMessage(
			`{"info":{"timestamp":"2026-06-29T10:10:17.530566931-04:00","priority":5,"pgn":130314,"sourceId":6,"targetId":null},"instance":0,"source":0,"pressure":1020690}`,
		),
		Raw: []byte{0xff, 0x00, 0x00, 0x12},
	}

	r.RecordSource("in", envelope)
	sourceDocument := <-sourceStream
	select {
	case unexpected := <-sinkStream:
		t.Fatalf("source event crossed into sink stream: %s", unexpected)
	default:
	}

	var wire struct {
		Payload  json.RawMessage `json:"payload"`
		Metadata json.RawMessage `json:"metadata"`
		Raw      []byte          `json:"raw"`
	}
	if err := json.Unmarshal(sourceDocument, &wire); err != nil {
		t.Fatal(err)
	}
	if string(wire.Payload) != string(envelope.Payload) {
		t.Fatalf("stream payload = %s, want verbatim %s", wire.Payload, envelope.Payload)
	}
	if string(wire.Raw) != string(envelope.Raw) {
		t.Fatalf("stream raw = %v, want %v", wire.Raw, envelope.Raw)
	}
	if len(wire.Metadata) == 0 {
		t.Fatal("stream envelope omitted metadata")
	}

	r.RecordSink("out", "route", envelope)
	if sinkDocument := <-sinkStream; len(sinkDocument) == 0 {
		t.Fatal("sink stream received an empty document")
	}

	// Removing an entity closes its active preview streams.
	r.RemoveSink("out")
	if _, ok := <-sinkStream; ok {
		t.Fatal("sink stream remained open after entity removal")
	}
}

func TestTotalsAndRates(t *testing.T) {
	r := NewRegistry()
	for i := 0; i < 10; i++ {
		r.Record("nav", 1, 100)
	}
	r.SetQueue("nav", 7, 700)
	s, ok := r.Snapshot("nav")
	if !ok {
		t.Fatal("no snapshot")
	}
	if s.TotalMessages != 10 || s.TotalBytes != 1000 || s.QueueDepth != 7 || s.QueueBytes != 700 {
		t.Fatalf("snapshot = %+v", s)
	}
	if s.MsgPerSec <= 0 {
		t.Fatalf("rate should be positive right after recording, got %v", s.MsgPerSec)
	}
}

func TestRateDecaysToZero(t *testing.T) {
	r := newRegistryAt(func() time.Time { return time.Unix(1000, 0) }) // test hook
	r.Record("nav", 5, 500)
	r.now = func() time.Time { return time.Unix(1020, 0) } // 20s later, window is 10s
	s, _ := r.Snapshot("nav")
	if s.MsgPerSec != 0 {
		t.Fatalf("rate after window = %v, want 0", s.MsgPerSec)
	}
	if s.TotalMessages != 5 {
		t.Fatalf("totals must not decay: %+v", s)
	}
}

func TestRemove(t *testing.T) {
	r := NewRegistry()
	r.Record("nav", 1, 1)
	r.Remove("nav")
	if _, ok := r.Snapshot("nav"); ok {
		t.Fatal("removed connector still has snapshot")
	}
}

// --- Queue-depth history ring ---

func TestQueueDepthHistoryAbsentUntilFirstSetQueue(t *testing.T) {
	r := NewRegistry()
	r.Record("nav", 1, 1) // Record alone must not seed the depth ring
	s, ok := r.Snapshot("nav")
	if !ok {
		t.Fatal("no snapshot")
	}
	if s.DepthHistory != nil {
		t.Fatalf("DepthHistory = %v, want nil before any SetQueue call", s.DepthHistory)
	}
}

func TestQueueDepthHistoryOrderedOldestFirst(t *testing.T) {
	r := NewRegistry()
	r.SetQueue("nav", 3, 300)
	r.SetQueue("nav", 7, 700)
	r.SetQueue("nav", 1, 100)
	s, _ := r.Snapshot("nav")
	want := []int64{3, 7, 1}
	if len(s.DepthHistory) != len(want) {
		t.Fatalf("DepthHistory = %v, want %v", s.DepthHistory, want)
	}
	for i, v := range want {
		if s.DepthHistory[i] != v {
			t.Fatalf("DepthHistory = %v, want %v", s.DepthHistory, want)
		}
	}
}

func TestQueueDepthHistoryCapsAtRingSizeFIFO(t *testing.T) {
	r := NewRegistry()
	for i := 0; i < depthRingSize+5; i++ {
		r.SetQueue("nav", int64(i), 0)
	}
	s, _ := r.Snapshot("nav")
	if len(s.DepthHistory) != depthRingSize {
		t.Fatalf("DepthHistory len = %d, want %d", len(s.DepthHistory), depthRingSize)
	}
	// The oldest 5 samples (depths 0..4) must have fallen off; the ring
	// should now hold depths 5..64, oldest-first.
	if s.DepthHistory[0] != 5 {
		t.Fatalf("DepthHistory[0] = %d, want 5 (oldest 5 samples evicted)", s.DepthHistory[0])
	}
	if last := s.DepthHistory[len(s.DepthHistory)-1]; last != int64(depthRingSize+4) {
		t.Fatalf("DepthHistory last = %d, want %d", last, depthRingSize+4)
	}
}

func TestRemoveDropsDepthHistory(t *testing.T) {
	r := NewRegistry()
	r.SetQueue("nav", 9, 900)
	r.Remove("nav")
	r.Record("nav", 1, 1) // resurrects a fresh, zeroed entry (see Remove's doc comment)
	s, _ := r.Snapshot("nav")
	if s.DepthHistory != nil {
		t.Fatalf("DepthHistory = %v, want nil after Remove", s.DepthHistory)
	}
}

// TestTouchDoesNotNotchDepthHistory pins the hot-apply-restart regression:
// a connector's registry entry survives a config-edit restart (Remove only
// fires on delete), so Connector.Start's presence registration must not
// append to the depth ring — a Start-time SetQueue(id, 0, 0) would draw a
// fake dip-to-zero notch mid-sparkline. Touch zeroes the gauges and leaves
// the history exactly as it was.
func TestTouchDoesNotNotchDepthHistory(t *testing.T) {
	r := NewRegistry()
	r.SetQueue("nav", 5, 500)
	r.SetQueue("nav", 7, 700)

	r.Touch("nav") // hot-apply restart: Start re-registers the surviving entry

	s, ok := r.Snapshot("nav")
	if !ok {
		t.Fatal("no snapshot after Touch")
	}
	want := []int64{5, 7}
	if len(s.DepthHistory) != len(want) || s.DepthHistory[0] != want[0] || s.DepthHistory[1] != want[1] {
		t.Fatalf("DepthHistory = %v, want %v (Touch must not append)", s.DepthHistory, want)
	}
	if s.QueueDepth != 0 || s.QueueBytes != 0 {
		t.Fatalf("gauges = depth %d bytes %d, want both zeroed by Touch", s.QueueDepth, s.QueueBytes)
	}
}

// TestTouchRegistersPresence covers Touch's other job: a fresh (never
// Record/SetQueue'd) connector must show up in All()/Snapshot immediately
// after Touch, with zero gauges and no depth history.
func TestTouchRegistersPresence(t *testing.T) {
	r := NewRegistry()
	r.Touch("nav")

	all := r.All()
	s, ok := all["nav"]
	if !ok {
		t.Fatalf("All() = %v, want a nav entry after Touch", all)
	}
	if s.QueueDepth != 0 || s.QueueBytes != 0 || s.TotalMessages != 0 {
		t.Fatalf("fresh Touch snapshot = %+v, want all-zero", s)
	}
	if s.DepthHistory != nil {
		t.Fatalf("DepthHistory = %v, want nil (Touch must not seed history)", s.DepthHistory)
	}
	if _, ok := r.Snapshot("nav"); !ok {
		t.Fatal("Snapshot ok = false after Touch, want true")
	}
}
