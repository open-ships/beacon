package source

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
// source "up" until stopped. If the configured path does not exist, its .gz
// counterpart is tried. Gzip is detected by content and expanded to a
// short-lived file because n2k.File accepts paths rather than readers.
func runFile(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error {
	sourcePath := discoverReplayFile(cfg.FilePath)
	replayPath, cleanup, err := prepareReplayFile(ctx, sourcePath)
	if err != nil {
		return err
	}

	var connectedOnce sync.Once
	markConnected := func() { connectedOnce.Do(connected) }
	err = runReceive(ctx, publish, markConnected, false, n2k.File(replayPath, n2k.OriginalTiming()))
	cleanup()
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("replaying capture file %q: %w", sourcePath, err)
	}
	if err != nil {
		return err
	}

	// A clean EOF proves that even an empty capture opened successfully. The
	// decompressed copy is already gone; keep only the source state up and idle.
	markConnected()
	if ctx.Err() == nil {
		<-ctx.Done()
	}
	return ctx.Err()
}

// discoverReplayFile preserves an explicitly configured file when it exists.
// Only a missing path earns the conventional .gz fallback; this avoids
// silently choosing a compressed sidecar over an operator's exact selection.
func discoverReplayFile(path string) string {
	if _, err := os.Stat(path); err == nil || !errors.Is(err, os.ErrNotExist) {
		return path
	}
	if strings.EqualFold(filepath.Ext(path), ".gz") {
		return path
	}
	gzipPath := path + ".gz"
	if _, err := os.Stat(gzipPath); err == nil || !errors.Is(err, os.ErrNotExist) {
		return gzipPath
	}
	return path
}

func prepareReplayFile(ctx context.Context, path string) (string, func(), error) {
	f, err := os.Open(path) // #nosec G304 -- replaying the configured capture file is this source's purpose.
	if err != nil {
		return "", func() {}, fmt.Errorf("opening log file %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var magic [2]byte
	n, readErr := f.ReadAt(magic[:], 0)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return "", func() {}, fmt.Errorf("inspecting capture file %q: %w", path, readErr)
	}
	if n < len(magic) || magic != [2]byte{0x1f, 0x8b} {
		return path, func() {}, nil
	}

	zr, err := gzip.NewReader(f)
	if err != nil {
		return "", func() {}, fmt.Errorf("opening gzip capture file %q: %w", path, err)
	}

	tmp, err := os.CreateTemp("", "beacon-replay-*.log")
	if err != nil {
		_ = zr.Close()
		return "", func() {}, fmt.Errorf("creating temporary file for gzip capture %q: %w", path, err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }

	_, copyErr := io.Copy(tmp, contextReader{ctx: ctx, r: zr})
	closeErr := errors.Join(zr.Close(), tmp.Close())
	if err := errors.Join(copyErr, closeErr); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("decompressing gzip capture file %q: %w", path, err)
	}
	return tmpPath, cleanup, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.r.Read(p)
	}
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
