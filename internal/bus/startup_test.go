package bus

import (
	"context"
	"encoding/binary"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brutella/can"
	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/bus/busfake"
	"github.com/open-ships/beacon/internal/msg"
)

// startupBus lets traffic arrive while transport readiness or claiming is
// still pending, without relying on scheduler timing to fill a backlog.
type startupBus struct {
	*busfake.FakeBus
	ready  chan struct{}
	frames chan can.Frame
	runs   atomic.Int32
}

func newStartupBus() *startupBus {
	return &startupBus{FakeBus: busfake.New(), ready: make(chan struct{}), frames: make(chan can.Frame)}
}

func (b *startupBus) Ready() <-chan struct{} { return b.ready }

func (b *startupBus) Run(ctx context.Context, handler func(can.Frame)) error {
	b.runs.Add(1)
	for {
		select {
		case frame := <-b.frames:
			handler(frame)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func receiveStartupHeading(t *testing.T, b *startupBus, ch <-chan *msg.Envelope, sid uint8) {
	t.Helper()
	frame := busfake.VesselHeadingFrame()
	frame.Data[0] = sid
	select {
	case b.frames <- frame:
	case <-time.After(2 * time.Second):
		t.Fatal("bus stopped accepting frames")
	}
	select {
	case e := <-ch:
		if e.PGN != 127250 || len(e.Raw) != 8 || e.Raw[0] != sid || e.Ingress != "socketcan:can0" {
			t.Fatalf("startup envelope = %+v, want heading SID %d", e, sid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message was not delivered during startup")
	}
}

func TestStartupTrafficIsConsumedBeforeClientReady(t *testing.T) {
	b := newStartupBus()
	m := NewManagerWithBus(slog.Default(), nil, b,
		n2k.WithReadyTimeout(10*time.Second), n2k.WithClaimTimeout(20*time.Millisecond), n2k.WithHeartbeatInterval(0))
	h, err := m.Acquire(context.Background(), Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	ch, unsub := h.Subscribe(4)
	defer unsub()

	// More than the default 256-message receive buffer, all consumed before
	// releasing the handshake. This failed with the blocking NewClient call.
	for i := range 1024 {
		receiveStartupHeading(t, b, ch, uint8(i%250))
	}
	if state, err := h.State(); state != "degraded" || err != nil {
		t.Fatalf("startup state = %q, %v; want degraded until the claim completes", state, err)
	}
	// Discovery seen during startup must not consume the PGN-list retry
	// window before the client can transmit its request.
	name := n2k.DeviceName{IdentityNumber: 123, DeviceClass: 25, DeviceFunction: 130}.Pack(true)
	claim := can.Frame{ID: 0x18EEFF0C, Length: 8}
	binary.LittleEndian.PutUint64(claim.Data[:], name)
	b.frames <- claim
	select {
	case e := <-ch:
		if e.PGN != 60928 {
			t.Fatalf("claim envelope PGN = %d", e.PGN)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("address claim was not delivered during startup")
	}
	h.bc.mu.Lock()
	requestedEarly := len(h.bc.requested)
	h.bc.mu.Unlock()
	if requestedEarly != 0 {
		t.Fatal("PGN-list request attempted before the client was ready")
	}
	close(b.ready)
	waitUp(t, h)
	receiveStartupHeading(t, b, ch, 42)
	h.bc.mu.Lock()
	requested := h.bc.requested[name]
	h.bc.mu.Unlock()
	if requested.IsZero() {
		t.Fatal("PGN-list request was not attempted after startup")
	}
	if b.runs.Load() != 1 {
		t.Fatalf("bus opened %d times, want one connection", b.runs.Load())
	}
}

func TestStartupTrafficDuringClaimAndRelease(t *testing.T) {
	b := newStartupBus()
	close(b.ready)
	m := NewManagerWithBus(slog.Default(), nil, b,
		n2k.WithClaimTimeout(time.Minute), n2k.WithHeartbeatInterval(0))
	h, err := m.Acquire(context.Background(), Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	ch, unsub := h.Subscribe(4)
	defer unsub()

	deadline := time.Now().Add(2 * time.Second)
	for len(b.Written()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(b.Written()) == 0 {
		t.Fatal("client did not begin address claiming")
	}
	for i := range 1024 {
		receiveStartupHeading(t, b, ch, uint8(i%250))
	}
	if state, _ := h.State(); state != "degraded" {
		t.Fatalf("state = %q before claim completed", state)
	}

	// Last-handle release must interrupt Start and join the receive pump,
	// rather than waiting for the minute-long claim window to finish.
	released := make(chan struct{})
	go func() {
		h.Release()
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("Release did not interrupt startup")
	}
	if m.clientCount() != 0 || b.runs.Load() != 1 {
		t.Fatalf("clients=%d starts=%d after release", m.clientCount(), b.runs.Load())
	}
}
