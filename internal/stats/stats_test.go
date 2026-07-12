package stats

import (
	"testing"
	"time"
)

func TestNilRegistrySafe(t *testing.T) {
	var r *Registry
	r.Record("c", 1, 10)
	r.SetQueue("c", 1, 10)
	r.Remove("c")
	if _, ok := r.Snapshot("c"); ok {
		t.Fatal("nil registry returned a snapshot")
	}
	if len(r.All()) != 0 {
		t.Fatal("nil registry All() non-empty")
	}
}

func TestTotalsAndRates(t *testing.T) {
	r := NewRegistry()
	for i := 0; i < 10; i++ {
		r.Record("nav", 1, 100)
	}
	r.SetQueue("nav", 7, 700)
	s, ok := r.Snapshot("nav")
	if !ok {
		t.Fatal("no snapshot")
	}
	if s.TotalMessages != 10 || s.TotalBytes != 1000 || s.QueueDepth != 7 || s.QueueBytes != 700 {
		t.Fatalf("snapshot = %+v", s)
	}
	if s.MsgPerSec <= 0 {
		t.Fatalf("rate should be positive right after recording, got %v", s.MsgPerSec)
	}
}

func TestRateDecaysToZero(t *testing.T) {
	r := newRegistryAt(func() time.Time { return time.Unix(1000, 0) }) // test hook
	r.Record("nav", 5, 500)
	r.now = func() time.Time { return time.Unix(1020, 0) } // 20s later, window is 10s
	s, _ := r.Snapshot("nav")
	if s.MsgPerSec != 0 {
		t.Fatalf("rate after window = %v, want 0", s.MsgPerSec)
	}
	if s.TotalMessages != 5 {
		t.Fatalf("totals must not decay: %+v", s)
	}
}

func TestRemove(t *testing.T) {
	r := NewRegistry()
	r.Record("nav", 1, 1)
	r.Remove("nav")
	if _, ok := r.Snapshot("nav"); ok {
		t.Fatal("removed connector still has snapshot")
	}
}
