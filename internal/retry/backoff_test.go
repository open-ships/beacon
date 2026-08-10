package retry

import (
	"context"
	"testing"
	"time"
)

func TestBackoffIsJitteredBoundedAndResettable(t *testing.T) {
	b := NewBackoff(100*time.Millisecond, 400*time.Millisecond)
	b.random = func(n int64) int64 { return n - 1 }

	for i, want := range []time.Duration{100, 200, 400, 400} {
		if got := b.Next(); got != want*time.Millisecond {
			t.Fatalf("Next[%d] = %s, want %dms", i, got, want)
		}
	}
	b.Reset()
	if got := b.Next(); got != 100*time.Millisecond {
		t.Fatalf("Next after Reset = %s", got)
	}
}

func TestBackoffEqualJitterFloor(t *testing.T) {
	b := NewBackoff(100*time.Millisecond, 100*time.Millisecond)
	b.random = func(int64) int64 { return 0 }
	if got := b.Next(); got != 50*time.Millisecond {
		t.Fatalf("Next = %s, want equal-jitter floor", got)
	}
}

func TestSleepStopsOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Sleep(ctx, time.Hour) {
		t.Fatal("Sleep returned true after cancellation")
	}
}
