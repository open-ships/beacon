package source

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
)

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
	path := filepath.Join(t.TempDir(), "empty.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
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
