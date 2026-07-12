package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/open-ships/beacon/internal/model"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestReplaceAndLoad(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	empty, err := s.IsEmpty(ctx)
	if err != nil || !empty {
		t.Fatalf("fresh store IsEmpty = %v, %v", empty, err)
	}

	cfg := model.Config{
		Sources:    []model.Source{{ID: "can0", Name: "Bus", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}},
		Sinks:      []model.Sink{{ID: "sse", Name: "SSE", Type: model.SinkHTTPSSE, Enabled: true, Path: "/events"}},
		Connectors: []model.Connector{{ID: "all", Name: "All", SourceID: "can0", SinkID: "sse", Enabled: true, Buffer: model.BufferLimits{MaxMessages: 10}}},
	}
	if err := s.ReplaceConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Sources) != 1 || got.Sources[0].Interface != "can0" ||
		len(got.Sinks) != 1 || got.Sinks[0].Path != "/events" ||
		len(got.Connectors) != 1 || got.Connectors[0].Buffer.MaxMessages != 10 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestUpsertAndDelete(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	src := model.Source{ID: "u1", Name: "USB", Type: model.SourceUSBCAN, Enabled: true, Port: "/dev/ttyUSB0"}
	if err := s.PutSource(ctx, src); err != nil {
		t.Fatal(err)
	}
	src.Name = "USB adapter"
	if err := s.PutSource(ctx, src); err != nil {
		t.Fatal(err)
	}
	cfg, _ := s.LoadConfig(ctx)
	if len(cfg.Sources) != 1 || cfg.Sources[0].Name != "USB adapter" {
		t.Fatalf("upsert failed: %+v", cfg.Sources)
	}
	if err := s.DeleteSource(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	cfg, _ = s.LoadConfig(ctx)
	if len(cfg.Sources) != 0 {
		t.Fatal("delete failed")
	}
}

func TestReopenKeepsData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = s.PutSink(ctx, model.Sink{ID: "t", Name: "T", Type: model.SinkTCP, Enabled: true, Address: "0.0.0.0:9090"})
	_ = s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	cfg, _ := s2.LoadConfig(ctx)
	if len(cfg.Sinks) != 1 {
		t.Fatal("data lost across reopen")
	}
}
