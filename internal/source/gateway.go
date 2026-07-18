package source

import (
	"context"
	"sync"

	n2k "github.com/open-ships/n2k"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

// runReceive drives one of n2k's read-only sources (file/tcp/udp) through
// n2k.Receive, converting each decoded message to an envelope. n2k.IncludeUnknown
// matches beacon's other sources so uncataloged PGNs still flow through as
// UnknownPGN envelopes. When hold is true the loop parks on ctx after the
// stream ends (a file's EOF) rather than returning, so the dialer keeps the
// source "up" and idle instead of flapping it through a replay-reconnect loop.
// When hold is false (a live gateway), returning lets the dialer reconnect.
func runReceive(ctx context.Context, publish func(*msg.Envelope), connected func(), hold bool, src n2k.Option) error {
	var connectedOnce sync.Once
	markConnected := func() { connectedOnce.Do(connected) }
	for m, err := range n2k.Receive(ctx, src, n2k.IncludeUnknown()) {
		if err != nil {
			// Receive reports a source's terminal open/read error as the final
			// iterator value. Returning it is what gives the dialer a useful
			// lastErr and, for live sources, triggers reconnect backoff. In
			// particular, a missing replay file must not be mistaken for a clean
			// EOF and parked forever in the hold path below.
			return err
		}
		// n2k.Receive does not currently expose a transport-open callback. The
		// first decoded message is therefore the first reliable proof that a
		// live TCP/UDP source is usable; do not report "up" merely because the
		// lazy iterator was constructed. Empty files are handled as a successful
		// clean EOF below.
		markConnected()
		e, err := msg.FromPGN(m)
		if err != nil {
			continue
		}
		publish(e)
	}
	if hold {
		// A clean EOF means the replay file opened and was consumed
		// successfully, even when it contained no decodable frames.
		markConnected()
		if ctx.Err() == nil {
			<-ctx.Done()
		}
	}
	return ctx.Err()
}

// runFile replays a capture log once at its recorded timing, then holds the
// source "up" until stopped. n2k.File auto-detects candump/canboat/YD/Actisense
// log formats.
func runFile(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error {
	return runReceive(ctx, publish, connected, true, n2k.File(cfg.FilePath, n2k.OriginalTiming()))
}

// runTCP ingests from a TCP NMEA-2000 gateway; the dialer's reconnect loop
// re-dials when the gateway connection drops.
func runTCP(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error {
	f, err := bus.StreamFormat(cfg.Format)
	if err != nil {
		return err
	}
	return runReceive(ctx, publish, connected, false, n2k.TCP(cfg.Address, f))
}

// runUDP ingests from a UDP NMEA-2000 gateway (a listener bound to Address).
func runUDP(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error {
	f, err := bus.StreamFormat(cfg.Format)
	if err != nil {
		return err
	}
	return runReceive(ctx, publish, connected, false, n2k.UDP(cfg.Address, f))
}
