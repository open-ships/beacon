package bus

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brutella/can"
	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/msg"
)

// fakeBus implements n2k.Bus. Frames written by the client are recorded;
// test-injected frames are delivered to the client's handler.
type fakeBus struct {
	mu      sync.Mutex
	handler func(can.Frame)
	written []can.Frame
	closed  chan struct{}
}

func newFakeBus() *fakeBus { return &fakeBus{closed: make(chan struct{})} }

func (f *fakeBus) Run(ctx context.Context, handler func(can.Frame)) error {
	f.mu.Lock()
	f.handler = handler
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.closed:
		return nil
	}
}

func (f *fakeBus) WriteFrame(frame can.Frame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, frame)
	return nil
}

func (f *fakeBus) Close() error {
	select {
	case <-f.closed:
	default:
		close(f.closed)
	}
	return nil
}

func (f *fakeBus) inject(frame can.Frame) {
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()
	if h != nil {
		h(frame)
	}
}

func (f *fakeBus) writtenFrames() []can.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]can.Frame(nil), f.written...)
}

// vesselHeadingFrame is PGN 127250 (VesselHeading, single-frame): priority 2,
// source 12, heading raw 15708 (0x3D5C), deviation/variation null, reference 0.
//
// The CAN ID is built the same way n2k's own internal/framer.BuildCANID does
// (verified against n2k's canid_test.go): for a PDU2 (broadcast) PGN such as
// 127250 (PF=0xF1=241 >= 240), id = priority<<26 | pgn<<8 | source. That
// yields 0x09F1120C, not the 0x89F1120C originally guessed for this fixture —
// the extra top bit doesn't affect n2k's ParseCANID (it masks priority with
// 0x1C000000 and PGN with 0x3FFFF00, both below bit 31) but 0x09F1120C is the
// value n2k itself would actually produce when writing this frame, so it's
// the correct fixture rather than a lucky guess.
func vesselHeadingFrame() can.Frame {
	return can.Frame{
		ID:     0x09F1120C, // (2<<26)|(127250<<8)|12
		Length: 8,
		Data:   [8]uint8{0xFF, 0x5C, 0x3D, 0xFF, 0x7F, 0xFF, 0x7F, 0xFC},
	}
}

func testManager(t *testing.T, fake *fakeBus) *Manager {
	t.Helper()
	return NewManager(slog.Default(), nil,
		n2k.WithBus(fake), n2k.WithClaimTimeout(50*time.Millisecond))
}

func TestSubscribeReceivesDecodedEnvelope(t *testing.T) {
	fake := newFakeBus()
	m := testManager(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	ch, unsub := h.Subscribe(16)
	defer unsub()

	// give the client's read loop a moment, then inject
	time.Sleep(200 * time.Millisecond)
	fake.inject(vesselHeadingFrame())

	select {
	case e := <-ch:
		if e.PGN != 127250 || e.Source != 12 {
			t.Fatalf("envelope = %+v", e)
		}
		if e.PayloadMap()["heading"] == nil {
			t.Fatalf("payload missing heading: %s", e.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no envelope received")
	}
}

func TestSharedClientRefcount(t *testing.T) {
	fake := newFakeBus()
	m := testManager(t, fake)
	ctx := context.Background()

	h1, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	if m.clientCount() != 1 {
		t.Fatalf("clientCount = %d, want 1 (shared)", m.clientCount())
	}
	h1.Release()
	if m.clientCount() != 1 {
		t.Fatal("client closed while still referenced")
	}
	h2.Release()
	if m.clientCount() != 0 {
		t.Fatal("client not closed after last release")
	}
}

func TestWriteEncodesToBus(t *testing.T) {
	fake := newFakeBus()
	m := testManager(t, fake)
	ctx := context.Background()

	h, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	// Build an envelope by decoding a known frame's payload
	src, _ := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	defer src.Release()
	ch, unsub := src.Subscribe(1)
	defer unsub()
	time.Sleep(200 * time.Millisecond)
	fake.inject(vesselHeadingFrame())
	e := <-ch

	before := len(fake.writtenFrames())
	if err := h.Write(ctx, e); err != nil {
		t.Fatal(err)
	}
	if len(fake.writtenFrames()) <= before {
		t.Fatal("no frame written to bus")
	}
}

// stallBus is a fakeBus whose WriteFrame can be gated shut. stall starts
// false so address claiming (which writes a claim frame during NewClient)
// proceeds normally; tests flip it once the client is up to make the next
// write block until open() is called.
type stallBus struct {
	*fakeBus
	stall    atomic.Bool
	gate     chan struct{}
	openOnce sync.Once
}

func newStallBus() *stallBus {
	return &stallBus{fakeBus: newFakeBus(), gate: make(chan struct{})}
}

func (s *stallBus) WriteFrame(f can.Frame) error {
	if s.stall.Load() {
		<-s.gate
	}
	return s.fakeBus.WriteFrame(f)
}

func (s *stallBus) open() { s.openOnce.Do(func() { close(s.gate) }) }

// headingEnvelope hand-builds a writable envelope for PGN 127250 with the
// same payload bytes as vesselHeadingFrame.
func headingEnvelope() *msg.Envelope {
	return &msg.Envelope{
		PGN: 127250, Source: 12, Dest: 255, Priority: 2,
		Timestamp: time.Now(),
		Raw:       []byte{0xFF, 0x5C, 0x3D, 0xFF, 0x7F, 0xFF, 0x7F, 0xFC},
	}
}

func waitUp(t *testing.T, h *Handle) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := h.State(); s == "up" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("client never reached up state")
}

func TestWriteRejectsEmptyRaw(t *testing.T) {
	fake := newFakeBus()
	m := testManager(t, fake)
	ctx := context.Background()

	h, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	e := &msg.Envelope{PGN: 127250, Source: 12}
	if err := h.Write(ctx, e); err == nil {
		t.Fatal("expected error for envelope with empty Raw")
	}
}

// TestWriteToConcurrentlyClosedClient closes the shared client out from
// under a handle and asserts Write surfaces an error rather than panicking.
// (The exact n2k panic window — Close landing between client.Write's closed
// check and its channel send — cannot be forced deterministically without
// hooks into n2k; this exercises the closed-client path, and Handle.Write's
// recover covers the racy window.)
func TestWriteToConcurrentlyClosedClient(t *testing.T) {
	fake := newFakeBus()
	m := testManager(t, fake)
	ctx := context.Background()

	h, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	waitUp(t, h)

	h.bc.mu.Lock()
	client := h.bc.client
	h.bc.mu.Unlock()
	if client == nil {
		t.Fatal("no client after up state")
	}
	_ = client.Close() // close it out from under the handle

	if err := h.Write(ctx, headingEnvelope()); err == nil {
		t.Fatal("Write to closed client returned nil, want error")
	}
}

// TestWriteReturnsOnCtxCancel stalls the fake bus's WriteFrame so the write
// never completes, then cancels the Write ctx and asserts Write returns
// promptly with context.Canceled instead of blocking on Wait.
func TestWriteReturnsOnCtxCancel(t *testing.T) {
	sb := newStallBus()
	m := NewManager(slog.Default(), nil,
		n2k.WithBus(sb), n2k.WithClaimTimeout(50*time.Millisecond))

	h, err := m.Acquire(context.Background(), Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	defer sb.open() // unblock the stalled write before Release tears down
	waitUp(t, h)

	sb.stall.Store(true)
	wctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- h.Write(wctx, headingEnvelope()) }()

	time.Sleep(100 * time.Millisecond) // let the write reach the stalled WriteFrame
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Write err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Write did not return promptly after ctx cancel")
	}
}
