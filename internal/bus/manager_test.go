package bus

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
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
	waitState(t, h, "up")
}

func waitState(t *testing.T, h *Handle, want string) error {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s, err := h.State(); s == want {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	state, err := h.State()
	t.Fatalf("client state = %q, err = %v; want %q", state, err, want)
	return nil
}

func TestUSBCANMissingPortReportsError(t *testing.T) {
	m := NewManager(slog.Default(), nil, n2k.WithClaimTimeout(10*time.Millisecond))
	h, err := m.Acquire(context.Background(), Endpoint{
		Kind: "usbcan",
		Name: filepath.Join(t.TempDir(), "missing-tty"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	err = waitState(t, h, "error")
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("state err = %v, want missing-port error", err)
	}
}

// writeFailBus is a Bus whose frame writes always fail, simulating a bus that
// cannot transmit the initial NMEA address claim. n2k v0.2.0 performs claiming
// during NewClient and returns the write error ("n2k: starting address
// claim: ..."), which the manager must surface as an "error" state rather than
// hang or crash. (Before v0.2.0 the equivalent test used a bus that *panicked*
// on write; v0.2.0 runs the claim on an n2k-owned goroutine that does not
// recover panics — an upstream robustness gap — so a panicking bus now crashes
// the process and can no longer be exercised here. Real socketcan/usbcan buses
// return errors, not panics, so this covers the production path.)
type writeFailBus struct{}

func (writeFailBus) Run(ctx context.Context, _ func(can.Frame)) error {
	<-ctx.Done()
	return ctx.Err()
}

func (writeFailBus) WriteFrame(can.Frame) error {
	return errors.New("claim write failed")
}

func (writeFailBus) Close() error { return nil }

func TestClientStartupWriteFailureReportsError(t *testing.T) {
	m := NewManager(slog.Default(), nil,
		n2k.WithBus(writeFailBus{}),
		n2k.WithClaimTimeout(10*time.Millisecond))
	h, err := m.Acquire(context.Background(), Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	err = waitState(t, h, "error")
	if err == nil || !strings.Contains(err.Error(), "starting address claim") ||
		!strings.Contains(err.Error(), "claim write failed") {
		t.Fatalf("state err = %v, want surfaced claim write failure", err)
	}
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

// TestWriteUncatalogedPGNReturnsErrNotEncodable is the regression for the
// CAN-sink poison-message wedge: an UnknownPGN envelope (beacon always runs
// n2k.IncludeUnknown(), so every uncataloged PGN produces one, see
// msg.FromPGN) carries non-empty Raw bytes but has no registered PGN struct
// to decode back into for re-encoding. Write must surface a distinguishable
// error (ErrNotEncodable) rather than a generic one, so sink.canSink.Push
// can map it to sink.ErrSkip instead of retrying forever.
func TestWriteUncatalogedPGNReturnsErrNotEncodable(t *testing.T) {
	fake := busfake.New()
	m := testManager(t, fake)
	ctx := context.Background()

	h, err := m.Acquire(ctx, Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	waitUp(t, h)

	e := &msg.Envelope{
		PGN: 130999, Source: 12, Dest: 255, Priority: 6,
		Timestamp: time.Now(), Raw: []byte{1, 2, 3},
	}
	if err := h.Write(ctx, e); !errors.Is(err, ErrNotEncodable) {
		t.Fatalf("Write err = %v, want error matching ErrNotEncodable", err)
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
