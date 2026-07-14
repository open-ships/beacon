package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/open-ships/n2k/pgn"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

// maxFastPacketPayload is the largest payload the NMEA 2000 fast-packet
// protocol can carry: 6 bytes in frame 0 plus 31 continuation frames of 7
// bytes each (the frame counter is 5 bits, so at most 32 frames per
// sequence). Mirrors n2k's internal/framer.maxFastPacketPayload.
const maxFastPacketPayload = 6 + 31*7

// fpKey identifies one fast-packet stream. NMEA 2000 multiplexes concurrent
// fast-packet transmissions by (PGN, source address); the sequence counter
// in the frame header only needs to be unique within a stream.
type fpKey struct {
	pgn    uint32
	source uint8
}

// canFrame is one 8-byte CAN data frame with its 29-bit extended ID, the
// unit the candump format writes one text line per.
type canFrame struct {
	id   uint32
	data []byte // always 8 bytes, 0xFF-padded
}

// fileSink writes delivered envelopes to a local file in either NDJSON or
// candump -L text format, rotating by size. A single file sink instance can
// be shared by multiple connectors (each running Push from its own
// goroutine), so every piece of mutable state — the open file/writer, the
// tracked size, and the per-stream fast-packet sequence counters — is
// guarded by mu.
type fileSink struct {
	id           string
	format       string
	path         string
	maxFileBytes int64
	maxFiles     int
	log          *slog.Logger

	mu      sync.Mutex
	f       *os.File
	bw      *bufio.Writer
	size    int64
	lastErr error
	fpSeq   map[fpKey]uint8
}

func newFileSink(cfg model.Sink, log *slog.Logger) (Runtime, error) {
	switch cfg.Format {
	case model.FileFormatNDJSON, model.FileFormatCANDump:
	default:
		return nil, fmt.Errorf("sink %q: unknown file format %q", cfg.ID, cfg.Format)
	}
	if !strings.HasPrefix(cfg.FilePath, "/") {
		return nil, fmt.Errorf("sink %q: file_path must be an absolute path", cfg.ID)
	}
	if cfg.MaxFileBytes < 0 || cfg.MaxFiles < 0 {
		return nil, fmt.Errorf("sink %q: max_file_bytes and max_files must not be negative", cfg.ID)
	}

	f, err := os.OpenFile(cfg.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sink %q: open %s: %w", cfg.ID, cfg.FilePath, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("sink %q: stat %s: %w", cfg.ID, cfg.FilePath, err)
	}

	maxBytes := cfg.MaxFileBytes
	if maxBytes == 0 {
		maxBytes = model.DefaultMaxFileBytes
	}
	maxFiles := cfg.MaxFiles
	if maxFiles == 0 {
		maxFiles = model.DefaultMaxFiles
	}

	return &fileSink{
		id: cfg.ID, format: cfg.Format, path: cfg.FilePath,
		maxFileBytes: maxBytes, maxFiles: maxFiles, log: log,
		f: f, bw: bufio.NewWriter(f), size: info.Size(),
		fpSeq: map[fpKey]uint8{},
	}, nil
}

func (s *fileSink) ID() string { return s.id }

func (s *fileSink) State() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastErr != nil {
		return "error", s.lastErr
	}
	return "up", nil
}

func (s *fileSink) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.bw.Flush(); err != nil {
		s.log.Error("file sink flush on stop", "sink", s.id, "err", err)
	}
	if err := s.f.Close(); err != nil {
		s.log.Error("file sink close on stop", "sink", s.id, "err", err)
	}
}

// Push encodes e per the configured format and appends it to the file.
// Permanent per-message conditions (no raw bytes for candump, a payload too
// large to fast-packet-frame) return ErrSkip so the connector counts the
// message as skipped and advances past it. Anything else — a write or flush
// failure — is a real error so the connector retries with backoff instead of
// silently dropping data (e.g. disk full is expected to clear up).
func (s *fileSink) Push(_ context.Context, e *msg.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		line []byte
		err  error
	)
	switch s.format {
	case model.FileFormatNDJSON:
		line, err = encodeNDJSON(e)
	case model.FileFormatCANDump:
		line, err = s.encodeCANDump(e)
	default:
		// Unreachable in practice: newFileSink validates Format. Guarded
		// here too so a future format value never silently writes nothing.
		return fmt.Errorf("file sink %q: unknown format %q", s.id, s.format)
	}
	if err != nil {
		return err
	}

	// The whole encoded message — for candump, ALL frames of a multi-frame
	// fast-packet message — goes through one Write+Flush under mu, so a
	// message's lines never interleave with another connector's and never
	// span a rotation boundary. On-disk atomicity is still only best-effort:
	// a genuinely short write (e.g. ENOSPC mid-line) can leave a torn
	// trailing line, and since the error return below makes the connector
	// retry this envelope, a full duplicate of the message is appended once
	// the condition clears. Both are acceptable: a torn line no longer
	// matches the candump "(ts) token ID#HEX" shape (or NDJSON's one-object-
	// per-line), so replay/ingest tools skip it, and duplicates are the
	// documented at-least-once delivery semantics.
	n, werr := s.bw.Write(line)
	if werr == nil {
		werr = s.bw.Flush()
	}
	if werr != nil {
		s.lastErr = werr
		return werr
	}
	s.lastErr = nil
	s.size += int64(n)

	if s.size > s.maxFileBytes {
		if rerr := s.rotate(); rerr != nil {
			s.lastErr = rerr
			return rerr
		}
	}
	return nil
}

// encodeNDJSON marshals the envelope as-is (it already carries Seq/
// ConnectorID from the queue read, matching the SSE/TCP wire format) plus a
// trailing newline. A marshal failure means the envelope's payload contains
// bytes that can never be encoded as valid JSON — not a transient condition
// — so it is skipped rather than retried.
func encodeNDJSON(e *msg.Envelope) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, ErrSkip
	}
	return append(b, '\n'), nil
}

// encodeCANDump renders e as one candump -L line per CAN frame:
// "(<epoch>.<micros>) <connector> <8-hex-id>#<hex-data>".
func (s *fileSink) encodeCANDump(e *msg.Envelope) ([]byte, error) {
	if len(e.Raw) == 0 {
		return nil, ErrSkip
	}
	frames, err := s.fragmentCAN(e)
	if err != nil {
		return nil, err // wraps ErrSkip with the skip reason
	}

	sec := e.Timestamp.Unix()
	usec := e.Timestamp.Nanosecond() / 1000
	var buf strings.Builder
	for _, fr := range frames {
		fmt.Fprintf(&buf, "(%d.%06d) %s %08X#%X\n", sec, usec, e.ConnectorID, fr.id, fr.data)
	}
	return []byte(buf.String()), nil
}

// fragmentCAN builds the CAN frame(s) that would carry e.Raw on the wire.
// Single frame if the payload fits in 8 bytes AND the PGN is not
// fast-packet; otherwise fast-packet framing. Must be called with s.mu held
// (it consumes the per-stream fast-packet sequence counter).
func (s *fileSink) fragmentCAN(e *msg.Envelope) ([]canFrame, error) {
	canID := buildCANID(e.Priority, e.PGN, e.Source, e.Dest)
	payload := e.Raw

	if len(payload) <= 8 && !isFastPacketPGN(e.PGN, len(payload)) {
		return []canFrame{{id: canID, data: padTo8(payload)}}, nil
	}
	if len(payload) > maxFastPacketPayload {
		// Permanently oversized for the fast-packet protocol: wrap ErrSkip
		// (so pushAll advances past it) while keeping the reason loggable.
		return nil, fmt.Errorf("payload %d bytes exceeds fast-packet max %d: %w",
			len(payload), maxFastPacketPayload, ErrSkip)
	}

	key := fpKey{pgn: e.PGN, source: e.Source}
	seqID := s.fpSeq[key]
	s.fpSeq[key] = (seqID + 1) % 8
	return frameFastPacket(canID, payload, seqID), nil
}

// buildCANID constructs a 29-bit extended CAN ID from NMEA 2000 parameters,
// matching n2k's internal/framer.BuildCANID bit layout (cross-checked
// against n2k/internal/framer/canid.go and canid_test.go; verified against
// the known-good vector in internal/bus/busfake.VesselHeadingFrame: priority
// 2, PGN 127250, source 12, dest 255 -> 0x09F1120C). That function lives in
// an internal package n2k does not export, so it is reimplemented here.
//
//	Bits 28-26: Priority
//	Bits 25-8:  PGN (already encodes Data Page + PDU Format + PDU
//	            Specific/Group Extension for PDU2 PGNs)
//	Bits 7-0:   Source address
//
// For PDU1 (addressed, PDU Format < 240) PGNs the PGN's low byte is 0 and
// the destination address is OR'd into that byte (bits 15-8).
func buildCANID(priority uint8, pgnNum uint32, source, dest uint8) uint32 {
	id := uint32(priority&0x07) << 26
	id |= pgnNum << 8
	pduFormat := uint8((pgnNum >> 8) & 0xFF)
	if pduFormat < 240 {
		id |= uint32(dest) << 8
	}
	id |= uint32(source)
	return id
}

// isFastPacketPGN reports whether pgnNum uses the NMEA 2000 fast-packet
// protocol. The n2k pgn package exports per-PGN metadata for exactly this
// (pgn.PgnInfoLookup[pgn][*].Fast) and is used here as the source of truth.
// Scanning for ANY Fast variant relies on all variants of a PGN agreeing on
// Fast — an upstream-data invariant, empirically true at the pinned n2k
// version (54 multi-entry PGNs, 0 disagreements).
//
// Caveat: PGNs the manifest has no entry for at all (proprietary/uncataloged
// PGNs beacon only sees as pgn.UnknownPGN) fall back to a size heuristic:
// payload > 8 bytes implies fast-packet. This means a <=8-byte payload on an
// unmanifested fast-packet PGN logs as a plain single frame instead of a
// (degenerate, one-frame) fast-packet sequence — a caveat, not a bug, since
// there is no metadata available to do better.
func isFastPacketPGN(pgnNum uint32, payloadLen int) bool {
	if infos, ok := pgn.PgnInfoLookup[pgnNum]; ok {
		for _, info := range infos {
			if info.Fast {
				return true
			}
		}
		return false
	}
	return payloadLen > 8
}

// padTo8 returns payload copied into an 8-byte buffer, 0xFF-padded — the
// NMEA 2000 convention for unused single-frame bytes (mirrors n2k's
// framer.FrameSingle).
func padTo8(payload []byte) []byte {
	data := make([]byte, 8)
	for i := range data {
		data[i] = 0xFF
	}
	copy(data, payload)
	return data
}

// frameFastPacket splits payload across NMEA 2000 fast-packet frames,
// mirroring n2k's framer.FrameFastPacket exactly (verified against
// n2k/internal/framer/framer_test.go's frame-layout vectors):
//
//	Frame 0:            data[0] = seqID<<5 | 0, data[1] = len(payload), data[2:8] = payload[0:6]
//	Continuation N>=1:  data[0] = seqID<<5 | N,  data[1:8] = next 7 bytes of payload
//
// The last frame is 0xFF-padded. Caller (fragmentCAN) already bounds payload
// to maxFastPacketPayload (223 bytes -> at most 32 frames, frame numbers
// 0-31 fit the 5-bit counter).
func frameFastPacket(canID uint32, payload []byte, seqID uint8) []canFrame {
	total := len(payload)

	d0 := make([]byte, 8)
	for i := range d0 {
		d0[i] = 0xFF
	}
	d0[0] = (seqID & 0x07) << 5
	d0[1] = uint8(total)
	n := min(total, 6)
	copy(d0[2:2+n], payload[:n])
	frames := []canFrame{{id: canID, data: d0}}

	offset := n
	frameNum := uint8(1)
	for offset < total {
		d := make([]byte, 8)
		for i := range d {
			d[i] = 0xFF
		}
		d[0] = ((seqID & 0x07) << 5) | (frameNum & 0x1F)
		m := min(total-offset, 7)
		copy(d[1:1+m], payload[offset:offset+m])
		frames = append(frames, canFrame{id: canID, data: d})
		offset += m
		frameNum++
	}
	return frames
}

// rotate closes the active file, shifts rotated backups (file.(N-1) ->
// file.N, ..., file -> file.1, dropping file.(maxFiles-1)), and reopens a
// fresh active file. maxFiles counts the active file plus its backups, so
// backups occupy indices 1..maxFiles-1.
//
// Must be called with s.mu held. Every step before the reopen — flush,
// close, and each shift rename/remove — is best-effort: the first failure is
// recorded but rotation always proceeds all the way to reopening the active
// path, because leaving s.f/s.bw bound to a closed (or half-rotated) file
// descriptor would make every subsequent Push fail permanently and wedge the
// connector in retry until process restart. The recorded error still
// propagates to the caller so the connector's retry/backoff (and the sink's
// State()) surface it.
//
// The flush here only has bytes to lose if Push's own per-line flush already
// failed — and in that case Push returned that error without reaching
// rotate, so at entry the buffer is empty in practice and a flush failure
// here loses nothing.
func (s *fileSink) rotate() error {
	var rotErr error
	setErr := func(err error) {
		if err != nil && !os.IsNotExist(err) && rotErr == nil {
			rotErr = err
		}
	}
	setErr(s.bw.Flush())
	setErr(s.f.Close())

	if s.maxFiles > 1 {
		setErr(os.Remove(fmt.Sprintf("%s.%d", s.path, s.maxFiles-1)))
		for n := s.maxFiles - 2; n >= 1; n-- {
			setErr(os.Rename(fmt.Sprintf("%s.%d", s.path, n), fmt.Sprintf("%s.%d", s.path, n+1)))
		}
		setErr(os.Rename(s.path, s.path+".1"))
	} else {
		setErr(os.Remove(s.path))
	}

	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		if rotErr == nil {
			rotErr = err
		}
		return rotErr
	}
	s.f = f
	s.bw = bufio.NewWriter(f)
	s.size = 0
	// Fresh file, fresh streams: any in-flight fast-packet sequence numbers
	// from before rotation are meaningless in the new file.
	s.fpSeq = map[fpKey]uint8{}
	return rotErr
}
