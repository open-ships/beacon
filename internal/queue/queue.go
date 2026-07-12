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
	Depth  int64
	Bytes  int64
	Oldest time.Time
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
	// Prune enforces the configured limits; returns rows removed.
	Prune(ctx context.Context) (int64, error)
	Stats(ctx context.Context) (Stats, error)
}
