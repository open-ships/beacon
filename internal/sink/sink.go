// Package sink defines the Sink interface and dispatcher loop.
package sink

import (
	"context"
	"log/slog"
	"time"

	"github.com/open-ships/beacon/internal/admin"
	"github.com/open-ships/beacon/internal/buffer"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/n2k"
)

// Sink receives decoded, filtered N2K messages and delivers them to consumers.
type Sink interface {
	// Name returns a stable identifier used for checkpointing.
	Name() string
	// Start begins serving; blocks until ctx is cancelled or fatal error.
	Start(ctx context.Context) error
	// Send delivers a single message. Must be safe to call from multiple goroutines.
	Send(msg *n2k.ParsedMessage) error
	// Close performs cleanup.
	Close() error
}

// ReadySink is an optional interface that sinks may implement to signal readiness.
type ReadySink interface {
	Ready() <-chan struct{}
}

// CursorTracker is an optional interface for sinks that track per-client delivery cursors.
type CursorTracker interface {
	// MinDeliveredID returns the minimum ID successfully delivered to all live clients.
	// If no clients are connected, returns the fallback value.
	MinDeliveredID(fallback int64) int64
}

// RunDispatcher reads messages from the buffer, applies the filter chain, and sends to the sink.
// It runs until ctx is cancelled.
func RunDispatcher(ctx context.Context, buf buffer.Buffer, s Sink, chain filter.Chain, log *slog.Logger, metrics *admin.Metrics) {
	lastID, err := buf.GetCheckpoint(ctx, s.Name())
	if err != nil {
		log.Error("get checkpoint failed", "sink", s.Name(), "err", err)
		lastID = 0
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msgs, err := buf.Read(ctx, lastID, 100)
			if err != nil {
				log.Error("buffer read failed", "sink", s.Name(), "err", err)
				continue
			}
			for _, msg := range msgs {
				// Increment filter evaluation counter
				if metrics != nil {
					metrics.FilterEvaluatedTotal.Add(ctx, 1, admin.AttrSink(s.Name()))
				}

				if chain.Match(msg) {
					if err := s.Send(msg); err != nil {
						log.Debug("send failed", "sink", s.Name(), "err", err)
						if metrics != nil {
							metrics.SinkDroppedTotal.Add(ctx, 1, admin.AttrSink(s.Name()), admin.AttrReason("send_error"))
						}
					} else {
						if metrics != nil {
							metrics.SinkSentTotal.Add(ctx, 1, admin.AttrSink(s.Name()))
						}
					}
				} else {
					if metrics != nil {
						metrics.SinkDroppedTotal.Add(ctx, 1, admin.AttrSink(s.Name()), admin.AttrReason("filtered"))
					}
				}
				lastID = msg.ID
			}
			if len(msgs) > 0 {
				// If the sink supports per-client cursors, use the minimum delivered ID
				checkpointID := lastID
				if ct, ok := s.(CursorTracker); ok {
					checkpointID = ct.MinDeliveredID(lastID)
				}
				if err := buf.UpdateCheckpoint(ctx, s.Name(), checkpointID); err != nil {
					log.Error("update checkpoint failed", "sink", s.Name(), "err", err)
				}
			}

			// Update lag gauge
			if metrics != nil {
				if maxID, err := buf.MaxID(ctx); err == nil && maxID > 0 {
					lag := maxID - lastID
					if lag < 0 {
						lag = 0
					}
					metrics.SinkLagMessages.Record(ctx, lag, admin.AttrSink(s.Name()))
				}
			}
		}
	}
}
