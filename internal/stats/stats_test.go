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

// --- Queue-depth history ring ---

func TestQueueDepthHistoryAbsentUntilFirstSetQueue(t *testing.T) {
	r := NewRegistry()
	r.Record("nav", 1, 1) // Record alone must not seed the depth ring
	s, ok := r.Snapshot("nav")
	if !ok {
		t.Fatal("no snapshot")
	}
	if s.DepthHistory != nil {
		t.Fatalf("DepthHistory = %v, want nil before any SetQueue call", s.DepthHistory)
	}
}

func TestQueueDepthHistoryOrderedOldestFirst(t *testing.T) {
	r := NewRegistry()
	r.SetQueue("nav", 3, 300)
	r.SetQueue("nav", 7, 700)
	r.SetQueue("nav", 1, 100)
	s, _ := r.Snapshot("nav")
	want := []int64{3, 7, 1}
	if len(s.DepthHistory) != len(want) {
		t.Fatalf("DepthHistory = %v, want %v", s.DepthHistory, want)
	}
	for i, v := range want {
		if s.DepthHistory[i] != v {
			t.Fatalf("DepthHistory = %v, want %v", s.DepthHistory, want)
		}
	}
}

func TestQueueDepthHistoryCapsAtRingSizeFIFO(t *testing.T) {
	r := NewRegistry()
	for i := 0; i < depthRingSize+5; i++ {
		r.SetQueue("nav", int64(i), 0)
	}
	s, _ := r.Snapshot("nav")
	if len(s.DepthHistory) != depthRingSize {
		t.Fatalf("DepthHistory len = %d, want %d", len(s.DepthHistory), depthRingSize)
	}
	// The oldest 5 samples (depths 0..4) must have fallen off; the ring
	// should now hold depths 5..64, oldest-first.
	if s.DepthHistory[0] != 5 {
		t.Fatalf("DepthHistory[0] = %d, want 5 (oldest 5 samples evicted)", s.DepthHistory[0])
	}
	if last := s.DepthHistory[len(s.DepthHistory)-1]; last != int64(depthRingSize+4) {
		t.Fatalf("DepthHistory last = %d, want %d", last, depthRingSize+4)
	}
}

func TestRemoveDropsDepthHistory(t *testing.T) {
	r := NewRegistry()
	r.SetQueue("nav", 9, 900)
	r.Remove("nav")
	r.Record("nav", 1, 1) // resurrects a fresh, zeroed entry (see Remove's doc comment)
	s, _ := r.Snapshot("nav")
	if s.DepthHistory != nil {
		t.Fatalf("DepthHistory = %v, want nil after Remove", s.DepthHistory)
	}
}
