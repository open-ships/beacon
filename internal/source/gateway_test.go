package source

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

const replayHeadingLine = "(1720000000.000000) can0 09F11201#015C3DFF7FFF7FFC\n"

func writeGzipCapture(t *testing.T, path, content string) {
	t.Helper()
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, compressed.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func replayFirstEnvelope(t *testing.T, path string) *msg.Envelope {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	published := make(chan *msg.Envelope, 1)
	done := make(chan error, 1)
	go func() {
		done <- runFile(ctx, model.Source{
			ID: "gzip-replay", Type: model.SourceFile, FilePath: path,
		}, func(e *msg.Envelope) {
			published <- e
		}, func() {})
	}()

	var envelope *msg.Envelope
	select {
	case envelope = <-published:
	case <-time.After(time.Second):
		cancel()
		t.Fatalf("timed out replaying %q", path)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runFile() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("runFile() did not stop for %q", path)
	}
	return envelope
}

func assertReplayMessageInfoPreserved(t *testing.T, envelope *msg.Envelope) {
	t.Helper()
	var payload struct {
		Info struct {
			ReceivedAt time.Time `json:"receivedAt"`
			AdapterID  string    `json:"adapterId"`
			NetworkID  string    `json:"networkId"`
			Direction  string    `json:"direction"`
		} `json:"info"`
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Info.ReceivedAt.IsZero() || !strings.HasPrefix(payload.Info.AdapterID, "file:") ||
		payload.Info.NetworkID != "can0" || payload.Info.Direction != "received" {
		t.Fatalf("file source MessageInfo was not preserved in payload.info: %s", envelope.Payload)
	}
}

func waitStateError(t *testing.T, rt Runtime, wantState, wantErr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := rt.State()
		if state == wantState && err != nil && strings.Contains(err.Error(), wantErr) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	state, err := rt.State()
	t.Fatalf("state = %q/%v, want %q/error containing %q", state, err, wantState, wantErr)
}

func TestFileSourceMissingPathReportsOpenError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Source{
		ID:       "missing-replay",
		Type:     model.SourceFile,
		FilePath: filepath.Join(t.TempDir(), "missing.log"),
	}, nil, slog.Default(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	waitStateError(t, rt, "degraded", "opening log file", time.Second)
}

func TestEmptyFileSourceReportsUpAndHolds(t *testing.T) {
	for _, gzipCompressed := range []bool{false, true} {
		name := "plain"
		if gzipCompressed {
			name = "gzip"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "empty.log")
			if gzipCompressed {
				writeGzipCapture(t, path, "")
			} else if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rt, err := New(ctx, model.Source{
				ID: "empty-replay", Type: model.SourceFile, FilePath: path,
			}, nil, slog.Default(), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Stop()

			if !eventuallyState(rt, "up", time.Second) {
				state, stateErr := rt.State()
				t.Fatalf("state = %q/%v, want up after clean EOF", state, stateErr)
			}
		})
	}
}

func TestFileSourceReplaysGzipByContent(t *testing.T) {
	for _, name := range []string{"capture.log.gz", "capture.log"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			writeGzipCapture(t, path, replayHeadingLine)

			envelope := replayFirstEnvelope(t, path)
			if envelope.PGN != 127250 {
				t.Fatalf("PGN = %d, want 127250", envelope.PGN)
			}
			assertReplayMessageInfoPreserved(t, envelope)
		})
	}
}

func TestFileSourceDiscoversGzipSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.log")
	writeGzipCapture(t, path+".gz", replayHeadingLine)

	envelope := replayFirstEnvelope(t, path)
	if envelope.PGN != 127250 {
		t.Fatalf("PGN = %d, want 127250", envelope.PGN)
	}
}

func TestFileSourcePrefersExactPathOverGzipSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.log")
	if err := os.WriteFile(path, []byte(replayHeadingLine), 0o600); err != nil {
		t.Fatal(err)
	}
	writeGzipCapture(t, path+".gz", "not a capture\n")

	if got := discoverReplayFile(path); got != path {
		t.Fatalf("discoverReplayFile() = %q, want exact path %q", got, path)
	}
	envelope := replayFirstEnvelope(t, path)
	if envelope.PGN != 127250 {
		t.Fatalf("PGN = %d, want 127250 from exact path", envelope.PGN)
	}
}

func TestPrepareReplayFileRejectsCorruptGzip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.log")
	if err := os.WriteFile(path, []byte{0x1f, 0x8b, 0x00}, 0o600); err != nil {
		t.Fatal(err)
	}

	_, cleanup, err := prepareReplayFile(context.Background(), path)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "gzip capture file") {
		t.Fatalf("prepareReplayFile() error = %v, want gzip-specific error", err)
	}
}

func TestCorruptGzipFileSourceReportsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.log.gz")
	if err := os.WriteFile(path, []byte{0x1f, 0x8b, 0x00}, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Source{
		ID: "corrupt-replay", Type: model.SourceFile, FilePath: path,
	}, nil, slog.Default(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	waitStateError(t, rt, "degraded", "gzip capture file", time.Second)
}

func TestPrepareReplayFileCleansUpDecompressedCopy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.log.gz")
	writeGzipCapture(t, path, replayHeadingLine)

	replayPath, cleanup, err := prepareReplayFile(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if replayPath == path {
		t.Fatal("prepareReplayFile() returned compressed source path")
	}
	content, err := os.ReadFile(replayPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != replayHeadingLine {
		t.Fatalf("decompressed content = %q, want %q", content, replayHeadingLine)
	}
	cleanup()
	if _, err := os.Stat(replayPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary replay file still exists after cleanup: %v", err)
	}
}

func TestTCPSourceNeverReportsUpWhenDialFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Source{
		ID: "dead-gateway", Type: model.SourceTCP, Address: addr, Format: model.StreamFormatYDRaw,
	}, nil, slog.Default(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	waitStateError(t, rt, "degraded", "dialing", time.Second)
	deadline := time.Now().Add(750 * time.Millisecond)
	for time.Now().Before(deadline) {
		if state, _ := rt.State(); state == "up" {
			t.Fatal("unreachable TCP gateway reported up")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestUDPSourceReportsBindError(t *testing.T) {
	occupied, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Source{
		ID: "occupied-udp", Type: model.SourceUDP,
		Address: occupied.LocalAddr().String(), Format: model.StreamFormatYDRaw,
	}, nil, slog.Default(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	waitStateError(t, rt, "degraded", "listening on", time.Second)
}
