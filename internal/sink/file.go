package sink

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/brutella/can"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/n2kwire"
	"github.com/open-ships/beacon/internal/queue"
)

const (
	fileBatchSize        = 64
	fileWriteBufferBytes = 32 << 10
)

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

	mu       sync.Mutex
	f        *os.File
	bw       *bufio.Writer
	size     int64
	fragment *n2kwire.Fragmenter

	statusMu sync.RWMutex
	lastErr  error
}

func newFileSink(cfg model.Sink, log *slog.Logger) (Runtime, error) {
	if log == nil {
		log = slog.Default()
	}
	switch cfg.Format {
	case model.FileFormatNDJSON, model.FileFormatCANDump:
	default:
		return nil, fmt.Errorf("sink %q: unknown file format %q", cfg.ID, cfg.Format)
	}
	if !filepath.IsAbs(cfg.FilePath) && !strings.HasPrefix(cfg.FilePath, "/") {
		return nil, fmt.Errorf("sink %q: file_path must be an absolute path", cfg.ID)
	}
	if cfg.MaxFileBytes < 0 || cfg.MaxFiles < 0 {
		return nil, fmt.Errorf("sink %q: max_file_bytes and max_files must not be negative", cfg.ID)
	}

	f, err := os.OpenFile(cfg.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
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

	s := &fileSink{
		id: cfg.ID, format: cfg.Format, path: cfg.FilePath,
		maxFileBytes: maxBytes, maxFiles: maxFiles, log: log,
		f: f, bw: bufio.NewWriterSize(f, fileWriteBufferBytes), size: info.Size(),
		fragment: n2kwire.NewFragmenter(),
	}
	if err := s.enforceExistingBudget(); err != nil {
		_ = s.f.Close()
		return nil, fmt.Errorf("sink %q: enforce existing file budget: %w", cfg.ID, err)
	}
	return s, nil
}

func (s *fileSink) ID() string                   { return s.id }
func (s *fileSink) BatchSize() int               { return fileBatchSize }
func (s *fileSink) DeliveryClass() DeliveryClass { return DeliveryConfirmed }

func (s *fileSink) State() (string, error) {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	if s.lastErr != nil {
		return "error", s.lastErr
	}
	return "up", nil
}

func (s *fileSink) setLastError(err error) {
	s.statusMu.Lock()
	s.lastErr = err
	s.statusMu.Unlock()
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
	if err := s.writeEnvelope(e); err != nil {
		return s.prepareRetry(err)
	}
	if err := s.flush(); err != nil {
		return s.prepareRetry(err)
	}
	return nil
}

// PushBatchSelective writes an ordered connector chunk under one lock and
// performs one steady-state Flush. Rotation and errors may flush earlier;
// after any transient error the connector retries the whole chunk, which is
// the file sink's documented at-least-once behavior.
func (s *fileSink) PushBatchSelective(ctx context.Context, entries []queue.Entry) (BatchPushReport, error) {
	report := BatchPushReport{Skipped: make([]bool, len(entries))}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, entry := range entries {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := s.writeEnvelope(entry.Env); err != nil {
			if errors.Is(err, ErrSkip) {
				report.Skipped[i] = true
				continue
			}
			return report, s.prepareRetry(err)
		}
	}
	if err := s.flush(); err != nil {
		return report, s.prepareRetry(err)
	}
	return report, nil
}

func (s *fileSink) writeEnvelope(e *msg.Envelope) error {

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
	if int64(len(line)) > s.maxFileBytes {
		return fmt.Errorf("encoded file record is %d bytes, above max_file_bytes %d: %w",
			len(line), s.maxFileBytes, ErrSkip)
	}
	// Rotate before a record that would cross the configured threshold. This
	// makes max_file_bytes a physical ceiling rather than a threshold that
	// can be exceeded by one record.
	if s.size > 0 && s.size+int64(len(line)) > s.maxFileBytes {
		if err := s.rotate(); err != nil {
			s.setLastError(err)
			return err
		}
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
	if werr != nil {
		s.setLastError(werr)
		return werr
	}
	s.setLastError(nil)
	s.size += int64(n)

	return nil
}

func (s *fileSink) flush() error {
	if err := s.bw.Flush(); err != nil {
		s.setLastError(err)
		return err
	}
	s.setLastError(nil)
	return nil
}

// prepareRetry clears bufio.Writer's sticky error and discards any unwritten
// tail before the connector retries the whole batch. A flush may already have
// written a prefix, so duplicates or one torn trailing record remain possible
// (the file delivery contract is at-least-once), but a transient ENOSPC or
// descriptor error can recover without a process restart.
func (s *fileSink) prepareRetry(cause error) error {
	if errors.Is(cause, ErrSkip) {
		return cause
	}
	next, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		joined := errors.Join(cause, fmt.Errorf("reopen file sink for retry: %w", err))
		s.setLastError(joined)
		return joined
	}
	info, statErr := next.Stat()
	if statErr != nil {
		_ = next.Close()
		joined := errors.Join(cause, fmt.Errorf("stat file sink for retry: %w", statErr))
		s.setLastError(joined)
		return joined
	}
	previous := s.f
	s.f = next
	s.bw.Reset(next) // discards the failed writer's buffered remainder and error
	s.size = info.Size()
	if previous != nil && previous != next {
		_ = previous.Close()
	}
	s.setLastError(cause)
	return cause
}

// encodeNDJSON marshals the envelope as-is (it already carries Seq/
// ConnectorID from the queue read, matching the SSE/TCP wire format) plus a
// trailing newline. A marshal failure means the envelope's payload contains
// bytes that can never be encoded as valid JSON — not a transient condition
// — so it is skipped rather than retried.
func encodeNDJSON(e *msg.Envelope) ([]byte, error) {
	b, err := e.WireBytes()
	if err != nil {
		return nil, ErrSkip
	}
	line := make([]byte, len(b)+1)
	copy(line, b)
	line[len(b)] = '\n'
	return line, nil
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
		fmt.Fprintf(&buf, "(%d.%06d) %s %08X#%X\n", sec, usec, e.ConnectorID, fr.ID, fr.Data[:fr.Length])
	}
	return []byte(buf.String()), nil
}

// fragmentCAN builds the CAN frame(s) that would carry e.Raw on the wire.
// Single frame if the payload fits in 8 bytes AND the PGN is not
// fast-packet; otherwise fast-packet framing. Must be called with s.mu held
// (it consumes the per-stream fast-packet sequence counter).
func (s *fileSink) fragmentCAN(e *msg.Envelope) ([]can.Frame, error) {
	frames, err := s.fragment.Frames(e)
	if errors.Is(err, n2kwire.ErrUnsupportedTransport) {
		return nil, fmt.Errorf("%v: %w", err, ErrSkip)
	}
	return frames, err
}

// buildCANID is retained as a package-local test seam; n2kwire owns the
// production framing implementation shared by candump and transparent sinks.
//
//	Bits 28-26: Priority
//	Bits 25-8:  PGN (already encodes Data Page + PDU Format + PDU
//	            Specific/Group Extension for PDU2 PGNs)
//	Bits 7-0:   Source address
//
// For PDU1 (addressed, PDU Format < 240) PGNs the PGN's low byte is 0 and
// the destination address is OR'd into that byte (bits 15-8).
func buildCANID(priority uint8, pgnNum uint32, source, dest uint8) uint32 {
	return n2kwire.BuildCANID(priority, pgnNum, source, dest)
}

// frameFastPacket adapts n2kwire frames to the historical test shape.
//
//	Frame 0:            data[0] = seqID<<5 | 0, data[1] = len(payload), data[2:8] = payload[0:6]
//	Continuation N>=1:  data[0] = seqID<<5 | N,  data[1:8] = next 7 bytes of payload
func frameFastPacket(canID uint32, payload []byte, seqID uint8) []canFrame {
	wireFrames := n2kwire.FrameFastPacket(canID, payload, seqID)
	frames := make([]canFrame, 0, len(wireFrames))
	for _, frame := range wireFrames {
		frames = append(frames, canFrame{id: frame.ID, data: append([]byte(nil), frame.Data[:frame.Length]...)})
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

	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		if rotErr == nil {
			rotErr = err
		}
		return rotErr
	}
	s.f = f
	s.bw = bufio.NewWriterSize(f, fileWriteBufferBytes)
	s.size = 0
	// Fresh file, fresh streams: any in-flight fast-packet sequence numbers
	// from before rotation are meaningless in the new file.
	s.fragment = n2kwire.NewFragmenter()
	return rotErr
}

// enforceExistingBudget handles a configuration reduction without leaving
// old rotations outside the new hard physical ceiling. Line-oriented records
// cannot be split safely: an oversized active file is rotated out, and any
// oversized or now-out-of-range backup is removed with an operator-visible
// warning. Normal writes can no longer create these files because
// writeEnvelope rotates before crossing and skips a single oversized record.
func (s *fileSink) enforceExistingBudget() error {
	if s.size > s.maxFileBytes {
		s.log.Warn("active file exceeds configured budget; rotating it out",
			"sink", s.id, "path", s.path, "bytes", s.size, "limit", s.maxFileBytes)
		if err := s.rotate(); err != nil {
			return err
		}
	}
	dir := filepath.Dir(s.path)
	base := filepath.Base(s.path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, base+".") {
			continue
		}
		index, err := strconv.Atoi(strings.TrimPrefix(name, base+"."))
		if err != nil || index < 1 {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if index < s.maxFiles && info.Size() <= s.maxFileBytes {
			continue
		}
		s.log.Warn("removing rotated file outside configured budget",
			"sink", s.id, "path", path, "bytes", info.Size(), "index", index)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
