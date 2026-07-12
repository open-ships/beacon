// Package connector runs the per-connector pipeline: source subscription →
// CEL filter → durable queue → sink delivery with checkpointing.
package connector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/source"
	"github.com/open-ships/beacon/internal/stats"
)

const (
	batchSize     = 64
	batchInterval = 50 * time.Millisecond
	readLimit     = 256
	ackInterval   = 500 * time.Millisecond
	pruneInterval = 5 * time.Second
	maxBackoff    = 5 * time.Second
	appendTimeout = 5 * time.Second
)

type Connector struct {
	cfg   model.Connector
	src   source.Runtime
	snk   sink.Runtime
	q     queue.Queue
	chain *filter.Chain
	log   *slog.Logger
	met   *metrics.Set
	st    *stats.Registry

	cancel context.CancelFunc
	notify chan struct{}
	wg     sync.WaitGroup

	stopOnce sync.Once
}

func New(cfg model.Connector, src source.Runtime, snk sink.Runtime, q queue.Queue,
	chain *filter.Chain, log *slog.Logger, met *metrics.Set, st *stats.Registry) *Connector {
	return &Connector{cfg: cfg, src: src, snk: snk, q: q, chain: chain,
		log: log.With("connector", cfg.ID), met: met, st: st, notify: make(chan struct{}, 1)}
}

func (c *Connector) ID() string { return c.cfg.ID }

func (c *Connector) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	if reg, ok := c.snk.(sink.ConnectorRegistrar); ok {
		reg.RegisterConnector(c.cfg.ID, c.q)
	}
	// Subscribe synchronously so no envelopes published right after Start
	// returns can race the intake goroutine's startup and be dropped.
	in, unsub := c.src.Subscribe(readLimit)
	c.wg.Add(3)
	go c.intake(runCtx, in, unsub)
	go c.deliver(runCtx)
	go c.prune(runCtx)
}

// Stop cancels the pipeline and waits for the final flush and ack. Safe to
// call more than once, and a no-op if Start was never called.
func (c *Connector) Stop() {
	c.stopOnce.Do(func() {
		if reg, ok := c.snk.(sink.ConnectorRegistrar); ok {
			reg.UnregisterConnector(c.cfg.ID)
		}
		if c.cancel == nil { // Stop before Start
			return
		}
		c.cancel()
		c.wg.Wait()
	})
}

func (c *Connector) wake() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *Connector) intake(ctx context.Context, in <-chan *msg.Envelope, unsub func()) {
	defer c.wg.Done()
	defer unsub()

	batch := make([]*msg.Envelope, 0, batchSize)
	flushTimer := time.NewTicker(batchInterval)
	defer flushTimer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		// Append with a fresh short-timeout context, not the run ctx: on
		// Stop the run ctx is already cancelled and the trailing partial
		// batch must still be persisted — losing it would be silent data
		// loss on every graceful shutdown.
		appendCtx, cancel := context.WithTimeout(context.Background(), appendTimeout)
		defer cancel()
		if err := c.q.Append(appendCtx, batch); err != nil {
			c.log.Error("queue append failed", "err", err, "dropped", len(batch))
		} else {
			c.wake()
		}
		batch = batch[:0]
	}
	defer flush()

	ingest := func(e *msg.Envelope) {
		c.met.ConnectorMessages(ctx, c.cfg.ID, "received", 1)
		match, err := c.chain.Match(e)
		if err != nil {
			c.met.ConnectorMessages(ctx, c.cfg.ID, "filter_error", 1)
			c.log.Debug("filter eval error", "err", err)
			return
		}
		if !match {
			return
		}
		c.met.ConnectorMessages(ctx, c.cfg.ID, "matched", 1)
		batch = append(batch, e)
		if len(batch) >= batchSize {
			flush()
		}
	}

	for {
		select {
		case <-ctx.Done():
			// Drain envelopes already buffered in the subscription so a
			// graceful stop persists everything the source handed us; the
			// deferred flush writes whatever remains batched.
			for {
				select {
				case e, ok := <-in:
					if !ok {
						return
					}
					ingest(e)
				default:
					return
				}
			}
		case <-flushTimer.C:
			flush()
		case e, ok := <-in:
			if !ok {
				return
			}
			ingest(e)
		}
	}
}

func (c *Connector) deliver(ctx context.Context) {
	defer c.wg.Done()
	cursor, err := c.q.Cursor(ctx)
	if err != nil && ctx.Err() == nil {
		c.log.Error("read checkpoint", "err", err)
	}
	lastAck := time.Now()
	dirty := false
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()

	ack := func(force bool) {
		if !dirty || (!force && time.Since(lastAck) < ackInterval) {
			return
		}
		// use Background: final ack must survive ctx cancellation
		if err := c.q.Ack(context.Background(), cursor); err == nil {
			dirty = false
			lastAck = time.Now()
		}
	}
	defer ack(true)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.notify:
		case <-poll.C:
		}
		for {
			entries, err := c.q.Read(ctx, cursor, readLimit)
			if err != nil {
				if ctx.Err() == nil {
					c.log.Error("queue read failed", "err", err)
				}
				break
			}
			if len(entries) == 0 {
				break
			}
			switch snk := c.snk.(type) {
			case sink.Pusher:
				// pushAll marks dirty per delivered entry so the deferred
				// final ack checkpoints partial progress even when we are
				// cancelled mid-retry — otherwise already-pushed entries
				// would be redelivered to a non-idempotent sink on restart.
				if !c.pushAll(ctx, snk, entries, &cursor, &dirty) {
					return // ctx cancelled mid-retry
				}
			case sink.Broadcaster:
				snk.Broadcast(entries)
				cursor = entries[len(entries)-1].Seq
				dirty = true
				for _, e := range entries {
					c.met.ConnectorMessages(ctx, c.cfg.ID, "delivered", 1)
					c.met.ConnectorBytes(ctx, c.cfg.ID, int64(e.Env.SizeBytes()))
					c.st.Record(c.cfg.ID, 1, int64(e.Env.SizeBytes()))
				}
			default:
				c.log.Error("sink implements neither Pusher nor Broadcaster")
				return
			}
			ack(false)
		}
	}
}

// pushAll delivers entries one-by-one with retry; returns false if ctx ended.
// cursor and dirty advance per delivered entry, not per batch, so partial
// progress survives a cancellation mid-retry via the caller's deferred ack.
func (c *Connector) pushAll(ctx context.Context, p sink.Pusher, entries []queue.Entry, cursor *int64, dirty *bool) bool {
	for _, e := range entries {
		backoff := 250 * time.Millisecond
		for {
			err := p.Push(ctx, e.Env)
			if err == nil || errors.Is(err, sink.ErrSkip) {
				if errors.Is(err, sink.ErrSkip) {
					c.log.Debug("sink skipped message", "pgn", e.Env.PGN)
				}
				c.met.ConnectorMessages(ctx, c.cfg.ID, "delivered", 1)
				c.met.ConnectorBytes(ctx, c.cfg.ID, int64(e.Env.SizeBytes()))
				c.st.Record(c.cfg.ID, 1, int64(e.Env.SizeBytes()))
				*cursor = e.Seq
				*dirty = true
				break
			}
			c.log.Debug("push failed; retrying", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return false
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
		}
	}
	return true
}

func (c *Connector) prune(ctx context.Context) {
	defer c.wg.Done()
	t := time.NewTicker(pruneInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := c.q.Prune(ctx); err == nil && n > 0 {
				c.met.ConnectorMessages(ctx, c.cfg.ID, "pruned", n)
			}
			if st, err := c.q.Stats(ctx); err == nil {
				c.met.SetQueueDepth(c.cfg.ID, st.Depth, st.Bytes)
				c.st.SetQueue(c.cfg.ID, st.Depth, st.Bytes)
			}
		}
	}
}
