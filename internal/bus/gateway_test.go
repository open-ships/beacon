package bus

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	n2k "github.com/open-ships/n2k"
)

// fakeGateway is a local TCP listener standing in for a Yacht Devices WiFi
// gateway: it accepts connections and counts every byte the client sends
// (address claim, heartbeats, and test writes all land here as YD RAW lines).
type fakeGateway struct {
	ln       net.Listener
	received atomic.Int64
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &fakeGateway{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 4096)
				for {
					n, err := conn.Read(buf)
					g.received.Add(int64(n))
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return g
}

func (g *fakeGateway) waitBytes(t *testing.T, min int64, d time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if n := g.received.Load(); n >= min {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("gateway received %d bytes, want >= %d", g.received.Load(), min)
	return 0
}

// TestTCPGatewayEndpointWritesToGateway drives the tcp endpoint kind end to
// end against a synthetic gateway: Acquire dials it, the n2k client's address
// claim reaches the listener as YD RAW traffic, the endpoint reports "up",
// and a Handle.Write lands more bytes on the same connection. This is the
// closest verification possible without gateway hardware.
func TestTCPGatewayEndpointWritesToGateway(t *testing.T) {
	gw := newFakeGateway(t)
	m := NewManager(slog.Default(), nil, n2k.WithClaimTimeout(50*time.Millisecond))
	ctx := context.Background()

	h, err := m.Acquire(ctx, Endpoint{Kind: "tcp", Name: gw.ln.Addr().String(), Format: "ydraw"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()

	if err := waitState(t, h, "up"); err != nil {
		t.Fatalf("unexpected state err: %v", err)
	}
	// The address claim must already have reached the gateway.
	before := gw.waitBytes(t, 1, 2*time.Second)

	if err := h.Write(ctx, headingEnvelope()); err != nil {
		t.Fatalf("write via gateway: %v", err)
	}
	gw.waitBytes(t, before+1, 2*time.Second)
}

// TestTCPGatewayEndpointRejectsUnknownFormat covers the config error path:
// an endpoint with a format string the stream decoder doesn't know must fail
// Acquire outright rather than start a client that can never speak to the
// gateway.
func TestTCPGatewayEndpointRejectsUnknownFormat(t *testing.T) {
	m := NewManager(slog.Default(), nil)
	_, err := m.Acquire(context.Background(), Endpoint{Kind: "tcp", Name: "127.0.0.1:1457", Format: "nmea0183"})
	if err == nil || !strings.Contains(err.Error(), "unknown stream format") {
		t.Fatalf("Acquire err = %v, want unknown stream format", err)
	}
}
