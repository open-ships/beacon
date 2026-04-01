package buffer

import (
	"context"
	"sync"
	"time"
)

// CheckpointFlusher periodically flushes in-memory checkpoint state to SQLite.
type CheckpointFlusher struct {
	buf      *SQLiteBuffer
	mu       sync.Mutex
	pending  map[string]int64
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewCheckpointFlusher creates a flusher that writes checkpoints every interval.
func NewCheckpointFlusher(buf *SQLiteBuffer, interval time.Duration) *CheckpointFlusher {
	return &CheckpointFlusher{
		buf:      buf,
		pending:  make(map[string]int64),
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Update records an in-memory checkpoint update (does not write to DB yet).
func (f *CheckpointFlusher) Update(sinkName string, lastID int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending[sinkName] = lastID
}

// Start begins the periodic flush goroutine. It runs until Stop is called.
func (f *CheckpointFlusher) Start(ctx context.Context) {
	go func() {
		defer close(f.doneCh)
		ticker := time.NewTicker(f.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = f.flush(ctx)
			case <-f.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop halts the ticker goroutine.
func (f *CheckpointFlusher) Stop() {
	close(f.stopCh)
	<-f.doneCh
}

// FlushAll writes all pending checkpoints immediately. Used during graceful shutdown.
func (f *CheckpointFlusher) FlushAll(ctx context.Context) error {
	return f.flush(ctx)
}

func (f *CheckpointFlusher) flush(ctx context.Context) error {
	f.mu.Lock()
	snapshot := make(map[string]int64, len(f.pending))
	for k, v := range f.pending {
		snapshot[k] = v
	}
	// Clear pending entries that we are about to flush.
	for k := range snapshot {
		delete(f.pending, k)
	}
	f.mu.Unlock()

	for name, id := range snapshot {
		if err := f.buf.UpdateCheckpoint(ctx, name, id); err != nil {
			// Re-add failed entries back to pending so they can be retried.
			f.mu.Lock()
			// Only re-add if a newer value hasn't been set in the meantime.
			if current, ok := f.pending[name]; !ok || current < id {
				f.pending[name] = id
			}
			f.mu.Unlock()
			return err
		}
	}
	return nil
}
