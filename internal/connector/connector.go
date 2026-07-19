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
	"github.com/open-ships/beacon/internal/n2kwire"
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

	// lastWarnSeq de-dupes pushAll's retry-failure logging: only the first
	// failure of a given entry's retry sequence logs at Warn, subsequent
	// retries of the same entry stay at Debug. Touched only from the single
	// deliver goroutine, so it needs no synchronization.
	lastWarnSeq   int64
	deliveryClass sink.DeliveryClass

	stateMu sync.Mutex
	issues  map[string]error

	stopOnce sync.Once
}

func New(cfg model.Connector, src source.Runtime, snk sink.Runtime, q queue.Queue,
	chain *filter.Chain, log *slog.Logger, met *metrics.Set, st *stats.Registry) *Connector {
	deliveryClass := sink.DeliveryClassOf(snk)
	if cfg.EffectiveMode() == model.BridgeObserve {
		deliveryClass = sink.DeliveryObserved
	}
	return &Connector{cfg: cfg, src: src, snk: snk, q: q, chain: chain,
		log: log.With("connector", cfg.ID), met: met, st: st, notify: make(chan struct{}, 1),
		lastWarnSeq: -1, deliveryClass: deliveryClass, issues: map[string]error{}}
}

func (c *Connector) ID() string { return c.cfg.ID }

func (c *Connector) setIssue(part string, err error) {
	c.stateMu.Lock()
	if err == nil {
		delete(c.issues, part)
	} else {
		c.issues[part] = err
	}
	state, current := "up", error(nil)
	for _, issue := range c.issues {
		state, current = "degraded", issue
		break
	}
	c.stateMu.Unlock()
	c.st.SetRuntime(c.cfg.ID, string(c.deliveryClass), state, current)
}

func (c *Connector) State() (string, error) {
	c.stateMu.Lock()
	for _, issue := range c.issues {
		c.stateMu.Unlock()
		return "degraded", issue
	}
	c.stateMu.Unlock()
	if c.cfg.EffectiveMode() == model.BridgeObserve {
		c.st.SetRuntime(c.cfg.ID, string(c.deliveryClass), "up", nil)
		return "up", nil
	}
	if state, err := c.snk.State(); state != "up" || err != nil {
		c.st.SetRuntime(c.cfg.ID, string(c.deliveryClass), state, err)
		return state, err
	}
	c.st.SetRuntime(c.cfg.ID, string(c.deliveryClass), "up", nil)
	return "up", nil
}

func (c *Connector) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	if reg, ok := c.snk.(sink.ConnectorRegistrar); ok {
		reg.RegisterConnector(c.cfg.ID, c.q)
	}
	// Register in the stats registry synchronously so a just-created (idle)
	// connector shows up in Registry.All()/Snapshot immediately, rather than
	// waiting for the first prune tick (up to pruneInterval later).
	// Deliberately zero-valued, with NO queue read here: Start runs while the
	// supervisor holds reconcileMu, on a store limited to a single shared
	// SQLite connection — a synchronous q.Stats (COUNT/SUM over this
	// connector's queue rows) would add a large backlog's scan latency to any
	// config write that restarts this connector and stall every other DB op
	// for the duration. The prune goroutine refreshes the real depth/bytes
	// immediately on entry (before its first tick), so a restarted
	// connector's true backlog appears within milliseconds, asynchronously,
	// outside the lock.
	//
	// Touch, not SetQueue: SetQueue appends every call to the depth-history
	// ring behind the UI sparkline, and on a hot-apply restart the registry
	// entry survives (Remove only fires on delete) — a Start-time
	// SetQueue(id, 0, 0) would notch a fake dip-to-zero into mid-history.
	// Touch registers presence and zeroes the gauges without adding a
	// history sample; only prune's periodic measurements feed the sparkline.
	c.st.Touch(c.cfg.ID)
	c.st.SetRuntime(c.cfg.ID, string(c.deliveryClass), "up", nil)

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

	flush := func(flushCtx context.Context) bool {
		if len(batch) == 0 {
			return true
		}
		backoff := 100 * time.Millisecond
		for {
			appendCtx, cancel := context.WithTimeout(flushCtx, appendTimeout)
			err := c.q.Append(appendCtx, batch)
			cancel()
			if err == nil {
				c.setIssue("intake", nil)
				c.st.RecordStage(c.cfg.ID, "queued", int64(len(batch)))
				c.met.ConnectorMessages(context.Background(), c.cfg.ID, "queued", int64(len(batch)))
				batch = batch[:0]
				c.wake()
				return true
			}
			c.setIssue("intake", err)
			c.st.RecordStage(c.cfg.ID, "append_error", 1)
			c.met.ConnectorMessages(context.Background(), c.cfg.ID, "append_error", 1)
			c.log.Warn("queue append failed; retrying", "err", err, "pending_batch", len(batch), "backoff", backoff)
			select {
			case <-flushCtx.Done():
				return false
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
		}
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), appendTimeout)
		defer cancel()
		if !flush(shutdownCtx) && len(batch) > 0 {
			c.st.RecordStage(c.cfg.ID, "intake_loss", int64(len(batch)))
			c.met.ConnectorMessages(context.Background(), c.cfg.ID, "intake_loss", int64(len(batch)))
			c.log.Error("queue append failed during shutdown", "lost", len(batch), "err", shutdownCtx.Err())
		}
	}()

	ingest := func(e *msg.Envelope) {
		c.st.RecordConnectorEvent(c.cfg.ID, "received", e)
		c.met.ConnectorMessages(ctx, c.cfg.ID, "received", 1)
		c.st.RecordStage(c.cfg.ID, "received", 1)
		if c.cfg.EffectiveMode() == model.BridgeTransparent && !c.cfg.ForwardManagement && n2kwire.IsManagementPGN(e.PGN) {
			c.met.ConnectorMessages(ctx, c.cfg.ID, "management_filtered", 1)
			c.st.RecordStage(c.cfg.ID, "management_filtered", 1)
			return
		}
		match, err := c.chain.Match(e)
		if err != nil {
			c.met.ConnectorMessages(ctx, c.cfg.ID, "filter_error", 1)
			c.st.RecordStage(c.cfg.ID, "filter_error", 1)
			c.log.Debug("filter eval error", "err", err)
			return
		}
		if !match {
			c.met.ConnectorMessages(ctx, c.cfg.ID, "filtered", 1)
			c.st.RecordStage(c.cfg.ID, "filtered", 1)
			return
		}
		c.met.ConnectorMessages(ctx, c.cfg.ID, "matched", 1)
		batch = append(batch, e)
		if len(batch) >= batchSize {
			_ = flush(ctx)
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
			_ = flush(ctx)
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
		c.setIssue("checkpoint", err)
	} else {
		c.setIssue("checkpoint", nil)
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
			c.setIssue("checkpoint", nil)
		} else {
			c.setIssue("checkpoint", err)
			c.st.RecordStage(c.cfg.ID, "ack_error", 1)
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
					c.setIssue("queue_read", err)
					c.st.RecordStage(c.cfg.ID, "read_error", 1)
				}
				break
			}
			c.setIssue("queue_read", nil)
			if len(entries) == 0 {
				break
			}
			if c.cfg.EffectiveMode() == model.BridgeObserve {
				cursor = entries[len(entries)-1].Seq
				dirty = true
				for _, e := range entries {
					c.met.ConnectorMessages(ctx, c.cfg.ID, "observed", 1)
					c.st.Record(c.cfg.ID, 1, int64(e.Env.SizeBytes()))
					c.st.RecordStage(c.cfg.ID, "observed", 1)
					c.st.RecordConnectorEvent(c.cfg.ID, "observed", e.Env)
				}
				ack(false)
				continue
			}
			if c.cfg.EffectiveMode() == model.BridgeTransparent {
				wire, ok := c.snk.(sink.WirePusher)
				if !ok {
					err := errors.New("transparent connector requires a wire-capable sink")
					c.setIssue("delivery", err)
					c.log.Error(err.Error())
					return
				}
				if !c.pushWireAll(ctx, wire, entries, &cursor, &dirty) {
					return
				}
				ack(false)
				continue
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
				report := snk.Broadcast(entries)
				cursor = entries[len(entries)-1].Seq
				dirty = true
				acceptedStage := "dispatched"
				if c.deliveryClass == sink.DeliveryResumable {
					acceptedStage = "available"
				}
				for i, e := range entries {
					stage := "dropped"
					if i < len(report.Accepted) && report.Accepted[i] {
						stage = acceptedStage
					}
					c.met.ConnectorMessages(ctx, c.cfg.ID, stage, 1)
					c.st.RecordStage(c.cfg.ID, stage, 1)
					c.st.RecordConnectorEvent(c.cfg.ID, stage, e.Env)
					if stage != "dropped" {
						c.met.ConnectorBytes(ctx, c.cfg.ID, int64(e.Env.SizeBytes()))
						c.st.Record(c.cfg.ID, 1, int64(e.Env.SizeBytes()))
						c.st.RecordSink(c.cfg.SinkID, c.cfg.ID, e.Env)
					}
				}
				if report.RecipientDrops > 0 {
					c.st.RecordStage(c.cfg.ID, "recipient_drop", report.RecipientDrops)
				}
				c.setIssue("delivery", report.Err)
			default:
				err := errors.New("sink implements neither Pusher nor Broadcaster")
				c.setIssue("delivery", err)
				c.log.Error(err.Error())
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
	return c.pushEntries(ctx, p.Push, "confirmed", entries, cursor, dirty)
}

func (c *Connector) pushWireAll(ctx context.Context, p sink.WirePusher, entries []queue.Entry, cursor *int64, dirty *bool) bool {
	return c.pushEntries(ctx, p.PushWire, "forwarded", entries, cursor, dirty)
}

func (c *Connector) pushEntries(ctx context.Context, push func(context.Context, *msg.Envelope) error, successStage string, entries []queue.Entry, cursor *int64, dirty *bool) bool {
	for _, e := range entries {
		backoff := 250 * time.Millisecond
		for {
			err := push(ctx, e.Env)
			if err == nil || errors.Is(err, sink.ErrSkip) {
				if errors.Is(err, sink.ErrSkip) {
					c.log.Debug("sink skipped message", "pgn", e.Env.PGN)
					c.met.ConnectorMessages(ctx, c.cfg.ID, "skipped", 1)
					c.st.RecordStage(c.cfg.ID, "skipped", 1)
				} else {
					c.met.ConnectorMessages(ctx, c.cfg.ID, successStage, 1)
					// Keep the original metric stage as a compatibility alias;
					// route-runtime consumers should prefer "confirmed".
					c.met.ConnectorMessages(ctx, c.cfg.ID, "delivered", 1)
					c.met.ConnectorBytes(ctx, c.cfg.ID, int64(e.Env.SizeBytes()))
					c.st.RecordSink(c.cfg.SinkID, c.cfg.ID, e.Env)
					c.st.RecordConnectorEvent(c.cfg.ID, successStage, e.Env)
					c.st.RecordStage(c.cfg.ID, successStage, 1)
					c.st.Record(c.cfg.ID, 1, int64(e.Env.SizeBytes()))
				}
				c.setIssue("delivery", nil)
				*cursor = e.Seq
				*dirty = true
				break
			}
			// Log the first failure of a retry sequence at Warn (an operator
			// watching logs should see a wedged sink), then drop to Debug for
			// the rest of that entry's retries so a persistently stuck sink
			// doesn't spam the log at Warn every backoff cycle.
			if c.lastWarnSeq != e.Seq {
				c.log.Warn("push failed; retrying", "err", err, "backoff", backoff, "pgn", e.Env.PGN)
				c.lastWarnSeq = e.Seq
			} else {
				c.log.Debug("push failed; retrying", "err", err, "backoff", backoff)
			}
			c.setIssue("delivery", err)
			c.st.RecordStage(c.cfg.ID, "retry", 1)
			c.met.ConnectorMessages(ctx, c.cfg.ID, "retry", 1)
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
	// Refresh queue depth/bytes immediately on entry, not only on the first
	// tick: Start registers the connector with zero values (see Start for why
	// it must not touch the DB itself), so a restarted connector with a
	// backlog would otherwise report depth 0 for up to pruneInterval. This
	// read runs on the prune goroutine — outside the supervisor's
	// reconcileMu — so a slow COUNT/SUM never extends a config write.
	refreshStats := func() {
		if st, err := c.q.Stats(ctx); err == nil {
			c.met.SetQueueDepth(c.cfg.ID, st.Depth, st.Bytes)
			c.st.SetQueueStats(c.cfg.ID, st)
			c.setIssue("queue_stats", nil)
		} else if ctx.Err() == nil {
			c.setIssue("queue_stats", err)
			c.st.RecordStage(c.cfg.ID, "stats_error", 1)
		}
	}
	refreshStats()
	t := time.NewTicker(pruneInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if pruned, err := c.q.Prune(ctx); err == nil {
				c.setIssue("prune", nil)
				if pruned.Total > 0 {
					c.met.ConnectorMessages(ctx, c.cfg.ID, "pruned", pruned.Total)
					c.st.RecordStage(c.cfg.ID, "pruned", pruned.Total)
				}
				if pruned.Pending > 0 {
					c.met.ConnectorMessages(ctx, c.cfg.ID, "pending_pruned", pruned.Pending)
					c.st.RecordStage(c.cfg.ID, "pending_pruned", pruned.Pending)
				}
			} else {
				c.setIssue("prune", err)
				c.st.RecordStage(c.cfg.ID, "prune_error", 1)
			}
			refreshStats()
		}
	}
}
