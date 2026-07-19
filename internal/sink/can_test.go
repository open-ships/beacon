package sink

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/bus/busfake"
	"github.com/open-ships/beacon/internal/msg"
)

// waitBusUp polls h.State() until the shared client reports "up" or the
// test times out — needed before Write since a nil client fails earlier
// than the decode step this test exercises.
func waitBusUp(t *testing.T, h *bus.Handle) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := h.State(); s == "up" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("bus client never reached up state")
}

// Regression: the CAN-sink poison-message wedge. An UnknownPGN envelope
// (every uncataloged PGN, since beacon always runs n2k.IncludeUnknown())
// carries non-empty Raw bytes but has no registered PGN struct to decode
// back into for re-encoding — bus.Handle.Write returns an error wrapping
// bus.ErrNotEncodable for it. canSink.Push must map that to ErrSkip so the
// connector's pushAll advances past it instead of retrying forever.
func TestCanSinkPushSkipsUndecodableEnvelope(t *testing.T) {
	fake := busfake.New()
	mgr := bus.NewManagerWithBus(slog.Default(), nil, fake,
		n2k.WithClaimTimeout(50*time.Millisecond))
	ctx := context.Background()

	h, err := mgr.Acquire(ctx, bus.Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	waitBusUp(t, h)

	s := &canSink{id: "can-sink", handle: h}
	e := &msg.Envelope{
		PGN: 130999, Source: 12, Dest: 255, Priority: 6,
		Timestamp: time.Now(), Raw: []byte{1, 2, 3},
	}

	if err := s.Push(ctx, e); !errors.Is(err, ErrSkip) {
		t.Fatalf("Push err = %v, want ErrSkip", err)
	}
}

// canSink.Push's other skip path — no Raw at all — must still work
// alongside the new decode-failure mapping (regression against the new
// errors.Is check shadowing the pre-existing empty-Raw fast path).
func TestCanSinkPushSkipsEmptyRaw(t *testing.T) {
	fake := busfake.New()
	mgr := bus.NewManagerWithBus(slog.Default(), nil, fake,
		n2k.WithClaimTimeout(50*time.Millisecond))
	ctx := context.Background()

	h, err := mgr.Acquire(ctx, bus.Endpoint{Kind: "socketcan", Name: "can0"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	s := &canSink{id: "can-sink", handle: h}
	e := &msg.Envelope{PGN: 127250, Source: 12}

	if err := s.Push(ctx, e); !errors.Is(err, ErrSkip) {
		t.Fatalf("Push err = %v, want ErrSkip", err)
	}
}
