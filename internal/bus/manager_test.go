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

	"github.com/open-ships/beacon/internal/bus/busfake"
	"github.com/open-ships/beacon/internal/msg"
)

func testManager(t *testing.T, fake *busfake.FakeBus) *Manager {
	t.Helper()
	return NewManager(slog.Default(), nil,
		n2k.WithBus(fake), n2k.WithClaimTimeout(50*time.Millisecond))
}

func TestSubscribeReceivesDecodedEnvelope(t *testing.T) {
	fake := busfake.New()
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
	fake.Inject(busfake.VesselHeadingFrame())

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
	fake := busfake.New()
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
	fake := busfake.New()
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
	fake.Inject(busfake.VesselHeadingFrame())
	e := <-ch

	before := len(fake.Written())
	if err := h.Write(ctx, e); err != nil {
		t.Fatal(err)
	}
	if len(fake.Written()) <= before {
		t.Fatal("no frame written to bus")
	}
}

// stallBus is a busfake.FakeBus whose WriteFrame can be gated shut. stall
// starts false so address claiming (which writes a claim frame during
// NewClient) proceeds normally; tests flip it once the client is up to make
// the next write block until open() is called.
type stallBus struct {
	*busfake.FakeBus
	stall    atomic.Bool
	gate     chan struct{}
	openOnce sync.Once
}

func newStallBus() *stallBus {
	return &stallBus{FakeBus: busfake.New(), gate: make(chan struct{})}
}

func (s *stallBus) WriteFrame(f can.Frame) error {
	if s.stall.Load() {
		<-s.gate
	}
	return s.FakeBus.WriteFrame(f)
}

func (s *stallBus) open() { s.openOnce.Do(func() { close(s.gate) }) }

// headingEnvelope hand-builds a writable envelope for PGN 127250 with the
// same payload bytes as busfake.VesselHeadingFrame.
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
	fake := busfake.New()
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
	fake := busfake.New()
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
