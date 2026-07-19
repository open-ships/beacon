// Package queue provides the durable per-connector buffer. The interface is
// deliberately broker-shaped so an embedded broker (e.g. NATS JetStream)
// can replace the SQLite implementation without touching connector logic.
package queue

import (
	"context"
	"time"

	"github.com/open-ships/beacon/internal/msg"
)

type Entry struct {
	Seq int64
	Env *msg.Envelope
}

type Stats struct {
	// Depth/Bytes/Oldest describe pending delivery, not every retained row.
	// The names are kept for source compatibility with existing callers.
	Depth  int64
	Bytes  int64
	Oldest time.Time

	RetainedDepth    int64
	RetainedBytes    int64
	OldestRetained   time.Time
	Cursor           int64
	Tail             int64
	LimitMessages    int64
	LimitBytes       int64
	HeadroomMessages int64
	HeadroomBytes    int64
}

type PruneResult struct {
	Total   int64
	Pending int64
}

type Queue interface {
	// Append persists envelopes in order. Seq is assigned by the queue.
	Append(ctx context.Context, envs []*msg.Envelope) error
	// Read returns up to limit entries with Seq > after, ascending.
	Read(ctx context.Context, after int64, limit int) ([]Entry, error)
	// Cursor returns the delivery checkpoint (0 if none).
	Cursor(ctx context.Context) (int64, error)
	// Ack advances the delivery checkpoint.
	Ack(ctx context.Context, upTo int64) error
	// Prune enforces the configured limits and reports total rows removed plus
	// the subset that had not crossed the connector delivery checkpoint.
	Prune(ctx context.Context) (PruneResult, error)
	Stats(ctx context.Context) (Stats, error)
	// Purge deletes every queue row and the checkpoint row for the
	// connector. Used when a connector is removed from config entirely (not
	// merely disabled) so its durable storage doesn't linger forever.
	Purge(ctx context.Context) error
}
