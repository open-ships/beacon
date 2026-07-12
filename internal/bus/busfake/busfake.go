// Package busfake provides an in-memory fake implementing n2k.Bus for tests:
// frames written by the client are recorded, and test-injected frames are
// delivered to the client's registered handler. It is exported (rather than
// kept package-private under internal/bus) because both internal/bus's own
// tests and internal/e2e's end-to-end test need it.
package busfake

import (
	"context"
	"sync"

	"github.com/brutella/can"
)

// FakeBus implements n2k.Bus.
type FakeBus struct {
	mu      sync.Mutex
	handler func(can.Frame)
	written []can.Frame
	closed  chan struct{}
}

// New returns a ready-to-use FakeBus.
func New() *FakeBus { return &FakeBus{closed: make(chan struct{})} }

// Run registers handler and blocks until ctx is cancelled or Close is called.
func (f *FakeBus) Run(ctx context.Context, handler func(can.Frame)) error {
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

// WriteFrame records frame as written.
func (f *FakeBus) WriteFrame(frame can.Frame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, frame)
	return nil
}

// Close is idempotent: it unblocks any goroutine waiting in Run.
func (f *FakeBus) Close() error {
	select {
	case <-f.closed:
	default:
		close(f.closed)
	}
	return nil
}

// Inject delivers frame to the handler registered via Run, as if it had
// arrived on the physical bus. A no-op before Run has registered a handler.
func (f *FakeBus) Inject(frame can.Frame) {
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()
	if h != nil {
		h(frame)
	}
}

// Written returns every frame written to the bus so far.
func (f *FakeBus) Written() []can.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]can.Frame(nil), f.written...)
}

// VesselHeadingFrame is PGN 127250 (VesselHeading, single-frame): priority 2,
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
func VesselHeadingFrame() can.Frame {
	return can.Frame{
		ID:     0x09F1120C, // (2<<26)|(127250<<8)|12
		Length: 8,
		Data:   [8]uint8{0xFF, 0x5C, 0x3D, 0xFF, 0x7F, 0xFF, 0x7F, 0xFC},
	}
}

// WaterDepthFrame is PGN 128267 (WaterDepth, single-frame, 4 byte-aligned
// fields totalling 64 bits): priority 3, source 5. Every field is left at
// its null sentinel (all-ones), which is a valid, cleanly-decodable payload
// for this PGN — callers only care that this frame carries a *different*
// PGN than VesselHeadingFrame for filter-pass/filter-reject assertions.
//
// PGN 128267 = 0x1F50B; its PDU Format byte is 0xF5 = 245 >= 240, so like
// VesselHeading it's PDU2 (broadcast) and the CAN ID is built the same way:
// id = priority<<26 | pgn<<8 | source = (3<<26)|(128267<<8)|5 = 0x0DF50B05.
func WaterDepthFrame() can.Frame {
	return can.Frame{
		ID:     0x0DF50B05, // (3<<26)|(128267<<8)|5
		Length: 8,
		Data:   [8]uint8{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
	}
}
