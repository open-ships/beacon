package sink

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

func newTestFileSink(t *testing.T, format string, maxFileBytes int64, maxFiles int) (Runtime, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "out."+format)
	rt, err := New(context.Background(), model.Sink{
		ID: "log", Name: "Log", Type: model.SinkFile, Enabled: true,
		FilePath: path, Format: format, MaxFileBytes: maxFileBytes, MaxFiles: maxFiles,
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return rt, path
}

func TestFileSinkNDJSONRoundTrips(t *testing.T) {
	rt, path := newTestFileSink(t, model.FileFormatNDJSON, 0, 0)
	p := rt.(Pusher)

	want := []*msg.Envelope{
		{Seq: 1, ConnectorID: "nav", PGN: 127250, Source: 1, Dest: 255, Priority: 2,
			Timestamp: time.Now(), Payload: json.RawMessage(`{"heading":1.2}`)},
		{Seq: 2, ConnectorID: "nav", PGN: 128267, Source: 5, Dest: 255, Priority: 3,
			Timestamp: time.Now(), Payload: json.RawMessage(`{}`)},
		{Seq: 3, ConnectorID: "nav", PGN: 127250, Source: 1, Dest: 255, Priority: 2,
			Timestamp: time.Now(), Payload: json.RawMessage(`{"heading":1.3}`)},
	}
	for _, e := range want {
		if err := p.Push(context.Background(), e); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	rt.Stop()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), b)
	}
	for i, line := range lines {
		var got msg.Envelope
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("line %d not valid JSON: %v (%q)", i, err, line)
		}
		if got.Seq != want[i].Seq || got.PGN != want[i].PGN || got.ConnectorID != want[i].ConnectorID {
			t.Fatalf("line %d = %+v, want seq/pgn/connector matching %+v", i, &got, want[i])
		}
	}
}

// TestFileSinkCANDumpSingleFrameGolden cross-checks against the known-good
// vector in internal/bus/busfake.VesselHeadingFrame (priority 2, PGN 127250,
// source 12 -> CAN ID 0x09F1120C, verified there against n2k's own
// framer.BuildCANID): PGN 127250 is not fast-packet (confirmed via
// n2k/pgn.PgnInfoLookup[127250][0].Fast == false), so an 8-byte raw payload
// must produce exactly one candump line with that ID and the raw bytes as
// uppercase hex, unpadded (already 8 bytes).
func TestFileSinkCANDumpSingleFrameGolden(t *testing.T) {
	rt, path := newTestFileSink(t, model.FileFormatCANDump, 0, 0)
	p := rt.(Pusher)

	ts := time.Unix(1700000000, 123456000)
	e := &msg.Envelope{
		ConnectorID: "nav", PGN: 127250, Source: 12, Dest: 255, Priority: 2,
		Timestamp: ts, Raw: []byte{0xFF, 0x5C, 0x3D, 0xFF, 0x7F, 0xFF, 0x7F, 0xFC},
	}
	if err := p.Push(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	rt.Stop()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "(1700000000.123456) nav 09F1120C#FF5C3DFF7FFF7FFC\n"
	if string(b) != want {
		t.Fatalf("candump line = %q, want %q", b, want)
	}
}

func TestFileSinkCANDumpNoRawSkips(t *testing.T) {
	rt, _ := newTestFileSink(t, model.FileFormatCANDump, 0, 0)
	defer rt.Stop()
	e := &msg.Envelope{PGN: 127250, Source: 12, Dest: 255, Priority: 2, Timestamp: time.Now()}
	if err := rt.(Pusher).Push(context.Background(), e); !errors.Is(err, ErrSkip) {
		t.Fatalf("Push err = %v, want ErrSkip", err)
	}
}

// parseCANDumpLine splits a "(ts) connector ID#DATA" line into the hex CAN
// ID and decoded data bytes.
func parseCANDumpLine(t *testing.T, line string) (idHex string, data []byte) {
	t.Helper()
	parts := strings.SplitN(line, "#", 2)
	if len(parts) != 2 {
		t.Fatalf("bad candump line %q", line)
	}
	fields := strings.Fields(parts[0])
	idHex = fields[len(fields)-1]
	data, err := hex.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("bad hex data %q: %v", parts[1], err)
	}
	return idHex, data
}

// TestFileSinkCANDumpFastPacketFraming exercises a 20-byte raw payload on
// PGN 129029 (GNSS Position Data — confirmed fast-packet via
// n2k/pgn.PgnInfoLookup[129029][0].Fast == true), which fragments into
// exactly 3 frames: frame 0 (6 bytes), frame 1 (7 bytes), frame 2 (7 bytes)
// — 6+7+7 = 20 exactly, so this particular length happens to leave the last
// frame fully packed with no 0xFF padding. Padding is instead cross-checked
// against n2k's own test vectors in TestFrameFastPacketMatchesN2K below.
func TestFileSinkCANDumpFastPacketFraming(t *testing.T) {
	rt, path := newTestFileSink(t, model.FileFormatCANDump, 0, 0)
	p := rt.(Pusher)

	raw := make([]byte, 20)
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	e := &msg.Envelope{
		ConnectorID: "nav", PGN: 129029, Source: 5, Dest: 255, Priority: 3,
		Timestamp: time.Now(), Raw: raw,
	}
	if err := p.Push(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	rt.Stop()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d frames, want 3: %q", len(lines), b)
	}

	wantID := fmt.Sprintf("%08X", buildCANID(3, 129029, 5, 255))
	var assembled []byte
	var seq uint8
	for i, line := range lines {
		id, data := parseCANDumpLine(t, line)
		if id != wantID {
			t.Fatalf("frame %d id = %s, want %s", i, id, wantID)
		}
		if len(data) != 8 {
			t.Fatalf("frame %d data len = %d, want 8", i, len(data))
		}
		gotSeq, frameNum := (data[0]&0xE0)>>5, data[0]&0x1F
		if int(frameNum) != i {
			t.Fatalf("frame %d frame-number nibble = %d, want %d", i, frameNum, i)
		}
		if i == 0 {
			seq = gotSeq
			if data[1] != 20 {
				t.Fatalf("frame 0 length byte = %d, want 20", data[1])
			}
			assembled = append(assembled, data[2:8]...)
		} else {
			if gotSeq != seq {
				t.Fatalf("frame %d seq = %d, want %d (must match frame 0)", i, gotSeq, seq)
			}
			assembled = append(assembled, data[1:8]...)
		}
	}
	assembled = assembled[:20]
	if !bytes.Equal(assembled, raw) {
		t.Fatalf("reassembled payload = %v, want %v", assembled, raw)
	}
}

// TestFrameFastPacketMatchesN2K reproduces n2k's own
// internal/framer/framer_test.go vectors (TestFrameFastPacket_SmallPayload
// and TestFrameFastPacket_LargerPayload) byte-for-byte against beacon's
// independent frameFastPacket implementation, including the 0xFF padding on
// an underfull last frame that the 20-byte integration test above cannot
// exercise (its length happens to divide evenly across 3 frames).
func TestFrameFastPacketMatchesN2K(t *testing.T) {
	t.Run("10 bytes, seq 3 -> 2 frames with padding", func(t *testing.T) {
		payload := make([]byte, 10)
		for i := range payload {
			payload[i] = byte(i + 1)
		}
		frames := frameFastPacket(0x1234, payload, 3)
		if len(frames) != 2 {
			t.Fatalf("got %d frames, want 2", len(frames))
		}
		if frames[0].data[0] != 3<<5 {
			t.Fatalf("frame 0 header = %#x, want %#x", frames[0].data[0], byte(3<<5))
		}
		if frames[0].data[1] != 10 {
			t.Fatalf("frame 0 length byte = %d, want 10", frames[0].data[1])
		}
		if !bytes.Equal(frames[0].data[2:8], payload[:6]) {
			t.Fatalf("frame 0 payload = %x, want %x", frames[0].data[2:8], payload[:6])
		}
		if frames[1].data[0] != (3<<5)|1 {
			t.Fatalf("frame 1 header = %#x, want %#x", frames[1].data[0], byte((3<<5)|1))
		}
		if !bytes.Equal(frames[1].data[1:5], payload[6:10]) {
			t.Fatalf("frame 1 payload = %x, want %x", frames[1].data[1:5], payload[6:10])
		}
		for _, padByte := range frames[1].data[5:8] {
			if padByte != 0xFF {
				t.Fatalf("frame 1 padding = %x, want 0xFF", frames[1].data[5:8])
			}
		}
	})

	t.Run("24 bytes, seq 0 -> 4 frames", func(t *testing.T) {
		payload := make([]byte, 24)
		for i := range payload {
			payload[i] = byte(i)
		}
		frames := frameFastPacket(0x1234, payload, 0)
		if len(frames) != 4 {
			t.Fatalf("got %d frames, want 4", len(frames))
		}
		wantSlices := [][]byte{payload[:6], payload[6:13], payload[13:20], payload[20:24]}
		for i, fr := range frames {
			if fr.data[0]&0x1F != byte(i) {
				t.Fatalf("frame %d number nibble = %d, want %d", i, fr.data[0]&0x1F, i)
			}
			if i == 0 {
				if !bytes.Equal(fr.data[2:8], wantSlices[0]) {
					t.Fatalf("frame 0 payload = %x, want %x", fr.data[2:8], wantSlices[0])
				}
				continue
			}
			n := len(wantSlices[i])
			if !bytes.Equal(fr.data[1:1+n], wantSlices[i]) {
				t.Fatalf("frame %d payload = %x, want %x", i, fr.data[1:1+n], wantSlices[i])
			}
		}
		// Last frame (index 3) carries 4 bytes, 3 bytes of 0xFF padding.
		for _, padByte := range frames[3].data[5:8] {
			if padByte != 0xFF {
				t.Fatalf("frame 3 padding = %x, want 0xFF", frames[3].data[5:8])
			}
		}
	})
}

// TestFileSinkFastPacketSeqCounterWraps pushes 9 fast-packet envelopes for
// the same (PGN, source) stream and asserts the 9th reuses the same
// sequence ID as the 1st: the counter is a 3-bit value that must wrap mod 8.
func TestFileSinkFastPacketSeqCounterWraps(t *testing.T) {
	rt, path := newTestFileSink(t, model.FileFormatCANDump, 0, 0)
	p := rt.(Pusher)

	raw := make([]byte, 20)
	push := func() string {
		e := &msg.Envelope{ConnectorID: "nav", PGN: 129029, Source: 5, Dest: 255, Priority: 3,
			Timestamp: time.Now(), Raw: raw}
		if err := p.Push(context.Background(), e); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
		_, data := parseCANDumpLine(t, lines[len(lines)-3]) // frame 0 of the just-written message
		return fmt.Sprintf("%d", (data[0]&0xE0)>>5)
	}

	first := push()
	for i := 0; i < 7; i++ {
		push()
	}
	ninth := push()
	rt.Stop()

	if first != ninth {
		t.Fatalf("9th message seq = %s, want it to wrap back to 1st message's seq %s", ninth, first)
	}
}

// TestFileSinkRotation forces a rotation on every push (MaxFileBytes: 1, so
// any write exceeds it) with MaxFiles: 3 (active + 2 backups) and checks the
// oldest backup is dropped once the cap is reached.
func TestFileSinkRotation(t *testing.T) {
	rt, path := newTestFileSink(t, model.FileFormatNDJSON, 1, 3)
	p := rt.(Pusher)

	readSeq := func(name string) int64 {
		t.Helper()
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
		if len(lines) != 1 || lines[0] == "" {
			t.Fatalf("%s: got %d lines, want exactly 1: %q", name, len(lines), b)
		}
		var e msg.Envelope
		if err := json.Unmarshal([]byte(lines[0]), &e); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return e.Seq
	}

	for i := int64(1); i <= 3; i++ {
		e := &msg.Envelope{Seq: i, ConnectorID: "nav", PGN: 127250, Source: 1, Dest: 255,
			Priority: 2, Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}
		if err := p.Push(context.Background(), e); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	rt.Stop()

	// Active file was freshly reopened by the 3rd rotation and never
	// written to again.
	if b, err := os.ReadFile(path); err != nil {
		t.Fatal(err)
	} else if len(b) != 0 {
		t.Fatalf("active file = %q, want empty", b)
	}
	if got := readSeq(path + ".1"); got != 3 {
		t.Fatalf("file.1 seq = %d, want 3 (most recent)", got)
	}
	if got := readSeq(path + ".2"); got != 2 {
		t.Fatalf("file.2 seq = %d, want 2", got)
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf("file.3 exists (or unexpected error %v), want deleted (MaxFiles=3 caps at 2 backups)", err)
	}
}

// TestFileSinkConcurrentPushNoCorruption exercises the shared-mutex
// requirement: a file sink can serve multiple connectors, each pushing from
// its own goroutine. This package cannot run under -race (transitive n2k
// import ICEs the race detector — see the n2k upstream bug notes), so this
// asserts the observable symptom of a missing lock instead: every line in
// the output file must be complete, valid JSON, and the total line count
// must match the total pushes (no torn/interleaved writes, no lost writes).
func TestFileSinkConcurrentPushNoCorruption(t *testing.T) {
	rt, path := newTestFileSink(t, model.FileFormatNDJSON, 0, 0)
	p := rt.(Pusher)

	const perGoroutine = 200
	const goroutines = 4
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				e := &msg.Envelope{
					Seq: int64(g*perGoroutine + i), ConnectorID: fmt.Sprintf("c%d", g),
					PGN: 127250, Source: 1, Dest: 255, Priority: 2,
					Timestamp: time.Now(), Payload: json.RawMessage(`{}`),
				}
				if err := p.Push(context.Background(), e); err != nil {
					t.Errorf("push: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
	rt.Stop()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != goroutines*perGoroutine {
		t.Fatalf("got %d lines, want %d (lost or torn writes)", len(lines), goroutines*perGoroutine)
	}
	for i, line := range lines {
		var e msg.Envelope
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("line %d is corrupt: %v (%q)", i, err, line)
		}
	}
}

func TestFileSinkStateReflectsWriteFailure(t *testing.T) {
	rt, path := newTestFileSink(t, model.FileFormatNDJSON, 0, 0)
	p := rt.(Pusher)

	if state, err := rt.State(); state != "up" || err != nil {
		t.Fatalf("initial state = %q/%v, want up/nil", state, err)
	}

	// Force a write failure by closing the underlying file out from under
	// the sink, simulating e.g. the disk becoming unavailable. This must be
	// a real (retryable) error, not ErrSkip.
	fs := rt.(*fileSink)
	fs.mu.Lock()
	_ = fs.f.Close()
	fs.mu.Unlock()

	e := &msg.Envelope{Seq: 1, ConnectorID: "nav", PGN: 127250, Source: 1, Dest: 255,
		Priority: 2, Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}
	err := p.Push(context.Background(), e)
	if err == nil || errors.Is(err, ErrSkip) {
		t.Fatalf("Push after fd closed = %v, want a plain (retryable) error", err)
	}
	if state, serr := rt.State(); state != "error" || serr == nil {
		t.Fatalf("state after write failure = %q/%v, want error/non-nil", state, serr)
	}

	// Recover: reopening the file (as a real restart would) and pushing
	// again must clear the error state.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fs.mu.Lock()
	fs.f = f
	fs.bw = bufio.NewWriter(f)
	fs.mu.Unlock()
	if err := p.Push(context.Background(), e); err != nil {
		t.Fatalf("push after recovery: %v", err)
	}
	if state, serr := rt.State(); state != "up" || serr != nil {
		t.Fatalf("state after recovery = %q/%v, want up/nil", state, serr)
	}
	rt.Stop()
}
