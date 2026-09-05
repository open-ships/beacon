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
	"github.com/open-ships/beacon/internal/retry"
	"github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/source"
	"github.com/open-ships/beacon/internal/stats"
)

const (
	batchSize = 64
	// Coalesce low-rate bus traffic into ten SQLite transactions/second at
	// most. This halves the prior WAL/fsync churn while keeping bridge latency
	// within a tenth of a second for navigation data.
	batchInterval = 100 * time.Millisecond
	readLimit     = 64
	// Checkpoint coalescing trades at most two seconds of duplicate replay
	// after abrupt power loss for substantially fewer steady-state writes.
	ackInterval   = 2 * time.Second
	pruneInterval = 5 * time.Second
	appendTimeout = 5 * time.Second
	// Retry-After is remote input. Honor a server's pacing request only up to
	// the connector's own maximum retry window so a bad or stale HTTP date
	// cannot park desired delivery for hours without another config change.
	maxRetryAfterDelay = time.Minute
)

// finalAckTimeout is a variable so the shutdown regression test can use a
// short deadline. A checkpoint is important, but it must not make appliance
// shutdown wait forever behind a wedged store connection.
var finalAckTimeout = 2 * time.Second

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
	// SQLite connection. Queue statistics are now a compact aggregate read,
	// but keeping all database work outside that lock also prevents config
	// writes from waiting behind unrelated I/O. The prune goroutine refreshes
	// the real depth/bytes
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
	unsubscribe := sync.OnceFunc(unsub)
	defer unsubscribe()

	batch := make([]*msg.Envelope, 0, batchSize)
	flushTimer := time.NewTicker(batchInterval)
	defer flushTimer.Stop()

	flush := func(flushCtx context.Context) bool {
		if len(batch) == 0 {
			return true
		}
		backoff := retry.NewBackoff(100*time.Millisecond, time.Minute)
		failures := 0
		for {
			appendCtx, cancel := context.WithTimeout(flushCtx, appendTimeout)
			pruned, err := c.q.Append(appendCtx, batch)
			cancel()
			if err == nil {
				c.setIssue("intake", nil)
				c.st.RecordStage(c.cfg.ID, "queued", int64(len(batch)))
				c.met.ConnectorMessages(context.Background(), c.cfg.ID, "queued", int64(len(batch)))
				c.recordPrune(context.Background(), pruned)
				clear(batch)
				batch = batch[:0]
				c.wake()
				return true
			}
			c.setIssue("intake", err)
			c.st.RecordStage(c.cfg.ID, "append_error", 1)
			c.met.ConnectorMessages(context.Background(), c.cfg.ID, "append_error", 1)
			retryIn := backoff.Next()
			failures++
			if failures == 1 || failures%60 == 0 {
				c.log.Warn("queue append failed; retrying", "err", err, "pending_batch", len(batch),
					"retry_in", retryIn, "consecutive_failures", failures)
			} else {
				c.log.Debug("queue append still failing", "err", err, "pending_batch", len(batch),
					"retry_in", retryIn, "consecutive_failures", failures)
			}
			if !retry.Sleep(flushCtx, retryIn) {
				return false
			}
		}
	}
	var interrupted *msg.Envelope
	ingest := func(ingestCtx context.Context, e *msg.Envelope, alreadyReceived bool) bool {
		if !alreadyReceived {
			c.st.RecordConnectorEvent(c.cfg.ID, "received", e)
			c.met.ConnectorMessages(ctx, c.cfg.ID, "received", 1)
			c.st.RecordStage(c.cfg.ID, "received", 1)
		}
		if c.cfg.EffectiveMode() == model.BridgeTransparent && !c.cfg.ForwardManagement && n2kwire.IsManagementPGN(e.PGN) {
			c.met.ConnectorMessages(ctx, c.cfg.ID, "management_filtered", 1)
			c.st.RecordStage(c.cfg.ID, "management_filtered", 1)
			return true
		}
		match, err := c.chain.MatchContext(ingestCtx, e)
		if err != nil {
			if ingestCtx.Err() != nil {
				interrupted = e
				return false
			}
			c.met.ConnectorMessages(ctx, c.cfg.ID, "filter_error", 1)
			c.st.RecordStage(c.cfg.ID, "filter_error", 1)
			c.log.Debug("filter eval error", "err", err)
			return true
		}
		if !match {
			c.met.ConnectorMessages(ctx, c.cfg.ID, "filtered", 1)
			c.st.RecordStage(c.cfg.ID, "filtered", 1)
			return true
		}
		c.met.ConnectorMessages(ctx, c.cfg.ID, "matched", 1)
		batch = append(batch, e)
		if len(batch) >= batchSize {
			return flush(ingestCtx)
		}
		return true
	}

	defer func() {
		// Freeze intake before draining. Hot apply stops the connector while
		// its source may remain active, so draining a live subscription can
		// otherwise grow forever. Even an imperfect Runtime unsubscribe cannot
		// extend this fixed snapshot of outstanding work.
		unsubscribe()
		remaining := len(in)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), appendTimeout)
		defer cancel()
		defer func() {
			lost := len(batch) + remaining
			if interrupted != nil {
				lost++
			}
			if lost > 0 {
				c.st.RecordStage(c.cfg.ID, "intake_loss", int64(lost))
				c.met.ConnectorMessages(context.Background(), c.cfg.ID, "intake_loss", int64(lost))
				c.log.Error("queue intake incomplete during shutdown", "lost", lost, "err", shutdownCtx.Err())
			}
		}()
		// A canceled normal flush may have left a full batch. Persist it before
		// admitting anything else, always within the same shutdown deadline.
		if !flush(shutdownCtx) {
			return
		}
		if interrupted != nil {
			e := interrupted
			interrupted = nil
			if !ingest(shutdownCtx, e, true) {
				return
			}
		}
		for remaining > 0 {
			select {
			case e, ok := <-in:
				if !ok {
					remaining = 0
					break
				}
				remaining--
				if !ingest(shutdownCtx, e, false) {
					return
				}
			default:
				remaining = 0
			}
		}
		flush(shutdownCtx)
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-flushTimer.C:
			if !flush(ctx) {
				return
			}
		case e, ok := <-in:
			if !ok || !ingest(ctx, e, false) {
				return
			}
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
	deliveryReadLimit := readLimit

	ack := func(force bool) bool {
		if !dirty || (!force && time.Since(lastAck) < ackInterval) {
			return true
		}
		ackCtx := ctx
		cancel := func() {}
		if force {
			// The final checkpoint survives pipeline cancellation, but remains
			// bounded so Connector.Stop cannot wedge shutdown indefinitely.
			ackCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), finalAckTimeout)
		}
		defer cancel()
		if err := c.q.Ack(ackCtx, cursor); err == nil {
			dirty = false
			lastAck = time.Now()
			c.setIssue("checkpoint", nil)
			return true
		} else {
			lastAck = time.Now() // throttle retry without an idle queue poll
			c.setIssue("checkpoint", err)
			c.st.RecordStage(c.cfg.ID, "ack_error", 1)
			return false
		}
	}
	defer ack(true)

	for {
		readFailed := false
		for {
			entries, err := c.q.Read(ctx, cursor, deliveryReadLimit)
			if err != nil {
				if ctx.Err() == nil {
					c.log.Error("queue read failed", "err", err)
					c.setIssue("queue_read", err)
					c.st.RecordStage(c.cfg.ID, "read_error", 1)
				}
				readFailed = true
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
			case sink.SelectiveBatchPusher:
				if !c.pushSelectiveBatchAll(ctx, snk, entries, &cursor, &dirty) {
					return
				}
			case sink.BatchPusher:
				if !c.pushBatchAll(ctx, snk, entries, &cursor, &dirty) {
					return // ctx cancelled mid-retry
				}
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

		// Appends wake the loop through c.notify. With no backlog and no dirty
		// checkpoint there is no database polling at all. Timers exist only to
		// retry an actual read failure or flush a short batch's checkpoint.
		var wait time.Duration
		switch {
		case readFailed:
			wait = time.Second
		case dirty:
			wait = ackInterval - time.Since(lastAck)
			if wait <= 0 {
				ack(true)
				continue
			}
		default:
			select {
			case <-ctx.Done():
				return
			case <-c.notify:
				continue
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-c.notify:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
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

// pushBatchAll delivers ordered chunks using the sink's configured maximum.
// Cursor advancement happens only after a whole chunk succeeds, so a failed
// HTTP request leaves every entry in that request pending for the retry.
func (c *Connector) pushBatchAll(ctx context.Context, p sink.BatchPusher, entries []queue.Entry, cursor *int64, dirty *bool) bool {
	size := p.BatchSize()
	if size <= 0 {
		size = len(entries)
	}
	for start := 0; start < len(entries); {
		end := min(start+size, len(entries))
		if limiter, ok := p.(sink.BatchByteLimiter); ok {
			end = batchEndByBytes(entries, start, end, limiter.BatchMaxBytes())
		}
		batch := entries[start:end]
		backoff := retry.NewBackoff(250*time.Millisecond, time.Minute)
		for {
			err := p.PushBatch(ctx, batch)
			if err == nil {
				for _, e := range batch {
					c.recordPushSuccess(ctx, "confirmed", e)
				}
				c.setIssue("delivery", nil)
				*cursor = batch[len(batch)-1].Seq
				*dirty = true
				break
			}
			retryIn := retryDelay(err, backoff.Next())
			lastSeq := batch[len(batch)-1].Seq
			if c.lastWarnSeq != lastSeq {
				c.log.Warn("batch push failed; retrying", "err", err, "retry_in", retryIn,
					"first_seq", batch[0].Seq, "last_seq", lastSeq, "batch_size", len(batch))
				c.lastWarnSeq = lastSeq
			} else {
				c.log.Debug("batch push failed; retrying", "err", err, "retry_in", retryIn,
					"first_seq", batch[0].Seq, "last_seq", lastSeq, "batch_size", len(batch))
			}
			c.setIssue("delivery", err)
			c.st.RecordStage(c.cfg.ID, "retry", 1)
			c.met.ConnectorMessages(ctx, c.cfg.ID, "retry", 1)
			if !retry.Sleep(ctx, retryIn) {
				return false
			}
		}
		start = end
	}
	return true
}

func batchEndByBytes(entries []queue.Entry, start, end int, maxBytes int64) int {
	if maxBytes <= 0 || start >= end {
		return end
	}
	var total int64
	for i := start; i < end; i++ {
		size := int64(entries[i].Env.SizeBytes())
		if i > start && total+size > maxBytes {
			return i
		}
		total += size
	}
	return end
}

func (c *Connector) pushSelectiveBatchAll(ctx context.Context, p sink.SelectiveBatchPusher, entries []queue.Entry, cursor *int64, dirty *bool) bool {
	size := p.BatchSize()
	if size <= 0 {
		size = len(entries)
	}
	for start := 0; start < len(entries); start += size {
		end := min(start+size, len(entries))
		batch := entries[start:end]
		backoff := retry.NewBackoff(250*time.Millisecond, time.Minute)
		for {
			report, err := p.PushBatchSelective(ctx, batch)
			if err == nil && len(report.Skipped) != len(batch) {
				err = errors.New("selective batch sink returned an invalid report")
			}
			if err == nil {
				for i, entry := range batch {
					if report.Skipped[i] {
						c.met.ConnectorMessages(ctx, c.cfg.ID, "skipped", 1)
						c.st.RecordStage(c.cfg.ID, "skipped", 1)
					} else {
						c.recordPushSuccess(ctx, "confirmed", entry)
					}
				}
				c.setIssue("delivery", nil)
				*cursor = batch[len(batch)-1].Seq
				*dirty = true
				break
			}
			retryIn := retryDelay(err, backoff.Next())
			lastSeq := batch[len(batch)-1].Seq
			if c.lastWarnSeq != lastSeq {
				c.log.Warn("batch push failed; retrying", "err", err, "retry_in", retryIn,
					"first_seq", batch[0].Seq, "last_seq", lastSeq, "batch_size", len(batch))
				c.lastWarnSeq = lastSeq
			} else {
				c.log.Debug("batch push failed; retrying", "err", err, "retry_in", retryIn,
					"first_seq", batch[0].Seq, "last_seq", lastSeq, "batch_size", len(batch))
			}
			c.setIssue("delivery", err)
			c.st.RecordStage(c.cfg.ID, "retry", 1)
			c.met.ConnectorMessages(ctx, c.cfg.ID, "retry", 1)
			if !retry.Sleep(ctx, retryIn) {
				return false
			}
		}
	}
	return true
}

func (c *Connector) recordPushSuccess(ctx context.Context, successStage string, e queue.Entry) {
	c.met.ConnectorMessages(ctx, c.cfg.ID, successStage, 1)
	// Keep the original metric stage as a compatibility alias; route-runtime
	// consumers should prefer "confirmed".
	c.met.ConnectorMessages(ctx, c.cfg.ID, "delivered", 1)
	c.met.ConnectorBytes(ctx, c.cfg.ID, int64(e.Env.SizeBytes()))
	c.st.RecordSink(c.cfg.SinkID, c.cfg.ID, e.Env)
	c.st.RecordConnectorEvent(c.cfg.ID, successStage, e.Env)
	c.st.RecordStage(c.cfg.ID, successStage, 1)
	c.st.Record(c.cfg.ID, 1, int64(e.Env.SizeBytes()))
}

func (c *Connector) pushEntries(ctx context.Context, push func(context.Context, *msg.Envelope) error, successStage string, entries []queue.Entry, cursor *int64, dirty *bool) bool {
	for _, e := range entries {
		backoff := retry.NewBackoff(250*time.Millisecond, time.Minute)
		for {
			err := push(ctx, e.Env)
			if err == nil || errors.Is(err, sink.ErrSkip) {
				if errors.Is(err, sink.ErrSkip) {
					c.log.Debug("sink skipped message", "pgn", e.Env.PGN)
					c.met.ConnectorMessages(ctx, c.cfg.ID, "skipped", 1)
					c.st.RecordStage(c.cfg.ID, "skipped", 1)
				} else {
					c.recordPushSuccess(ctx, successStage, e)
				}
				c.setIssue("delivery", nil)
				*cursor = e.Seq
				*dirty = true
				break
			}
			retryIn := retryDelay(err, backoff.Next())
			// Log the first failure of a retry sequence at Warn (an operator
			// watching logs should see a wedged sink), then drop to Debug for
			// the rest of that entry's retries so a persistently stuck sink
			// doesn't spam the log at Warn every backoff cycle.
			if c.lastWarnSeq != e.Seq {
				c.log.Warn("push failed; retrying", "err", err, "retry_in", retryIn, "pgn", e.Env.PGN)
				c.lastWarnSeq = e.Seq
			} else {
				c.log.Debug("push failed; retrying", "err", err, "retry_in", retryIn)
			}
			c.setIssue("delivery", err)
			c.st.RecordStage(c.cfg.ID, "retry", 1)
			c.met.ConnectorMessages(ctx, c.cfg.ID, "retry", 1)
			if !retry.Sleep(ctx, retryIn) {
				return false
			}
		}
	}
	return true
}

func retryDelay(err error, backoff time.Duration) time.Duration {
	if requested, ok := sink.RetryAfter(err); ok {
		requested = min(requested, maxRetryAfterDelay)
		if requested > backoff {
			return requested
		}
	}
	return backoff
}

func (c *Connector) prune(ctx context.Context) {
	defer c.wg.Done()
	// Refresh queue depth/bytes immediately on entry, not only on the first
	// tick: Start registers the connector with zero values (see Start for why
	// it must not touch the DB itself), so a restarted connector with a
	// backlog would otherwise report depth 0 for up to pruneInterval. This
	// read runs on the prune goroutine — outside the supervisor's
	// reconcileMu — so queue I/O never extends a config write.
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
				c.recordPrune(ctx, pruned)
			} else {
				c.setIssue("prune", err)
				c.st.RecordStage(c.cfg.ID, "prune_error", 1)
			}
			refreshStats()
		}
	}
}

func (c *Connector) recordPrune(ctx context.Context, pruned queue.PruneResult) {
	if pruned.Total > 0 {
		c.met.ConnectorMessages(ctx, c.cfg.ID, "pruned", pruned.Total)
		c.st.RecordStage(c.cfg.ID, "pruned", pruned.Total)
	}
	if pruned.Pending > 0 {
		c.met.ConnectorMessages(ctx, c.cfg.ID, "pending_pruned", pruned.Pending)
		c.st.RecordStage(c.cfg.ID, "pending_pruned", pruned.Pending)
		c.log.Error("retention limit removed pending delivery",
			"messages", pruned.Pending, "bytes", pruned.PendingBytes)
	}
}
