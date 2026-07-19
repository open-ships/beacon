// Package stats tracks live per-connector counters and rolling boundary-acceptance
// rates for the config API and the future UI dashboard. It is independent
// of OTel (which serves Prometheus via internal/metrics): this registry
// exists for cheap, synchronous, in-process reads from HTTP handlers.
package stats

import (
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

const (
	bucketWidth = time.Second
	numBuckets  = 10
	window      = bucketWidth * numBuckets

	// depthRingSize caps the queue-depth history ring: with SetQueue driven
	// by the connector's ~5s prune tick (see internal/connector/connector.go),
	// 60 samples is roughly a 5-minute trailing window for the UI's
	// sparkline. It's a plain sample-count cap, not a time window — old
	// samples fall off in FIFO order regardless of any gap between calls.
	depthRingSize = 60
	eventRingSize = 80
)

// Snapshot is the point-in-time view of one connector's counters, returned
// by Snapshot and All. Rates are computed over the trailing 10-second
// window; totals never decay.
type Snapshot struct {
	TotalMessages    int64            `json:"total_messages"`
	TotalBytes       int64            `json:"total_bytes"`
	MsgPerSec        float64          `json:"msg_per_sec"`   // over last 10s window
	BytesPerSec      float64          `json:"bytes_per_sec"` // over last 10s window
	QueueDepth       int64            `json:"queue_depth"`
	QueueBytes       int64            `json:"queue_bytes"`
	RetainedDepth    int64            `json:"retained_depth"`
	RetainedBytes    int64            `json:"retained_bytes"`
	QueueCursor      int64            `json:"queue_cursor"`
	QueueTail        int64            `json:"queue_tail"`
	OldestPending    *time.Time       `json:"oldest_pending,omitempty"`
	OldestRetained   *time.Time       `json:"oldest_retained,omitempty"`
	LimitMessages    int64            `json:"limit_messages,omitempty"`
	LimitBytes       int64            `json:"limit_bytes,omitempty"`
	HeadroomMessages int64            `json:"headroom_messages,omitempty"`
	HeadroomBytes    int64            `json:"headroom_bytes,omitempty"`
	DeliveryClass    string           `json:"delivery_class,omitempty"`
	State            string           `json:"state,omitempty"`
	LastError        string           `json:"last_error,omitempty"`
	Drops            int64            `json:"drops"`
	StageTotals      map[string]int64 `json:"stage_totals,omitempty"`

	// DepthHistory is the last depthRingSize QueueDepth readings, oldest
	// first, as recorded by successive SetQueue calls. Absent (nil/omitted
	// from JSON) for a connector that has never had SetQueue called on it —
	// additive/non-breaking: existing Snapshot consumers (the config API's
	// metrics endpoints, the connector detail/dashboard UI fragments) that
	// don't know this field simply ignore it.
	DepthHistory []int64 `json:"depth_history,omitempty"`
}

type Event struct {
	Time        time.Time `json:"time"`
	Stage       string    `json:"stage"`
	ConnectorID string    `json:"connector_id,omitempty"`
	PGN         uint32    `json:"pgn"`
	PGNName     string    `json:"pgn_name,omitempty"`
	Source      uint8     `json:"source"`
	Dest        uint8     `json:"dest"`
	Priority    uint8     `json:"priority"`
	Timestamp   time.Time `json:"timestamp"`
	Payload     string    `json:"payload,omitempty"`
	SizeBytes   int       `json:"size_bytes"`
}

type eventRing struct {
	items [eventRingSize]Event
	head  int
	len   int
}

func (r *eventRing) add(e Event) {
	r.items[r.head] = e
	r.head = (r.head + 1) % eventRingSize
	if r.len < eventRingSize {
		r.len++
	}
}

func (r *eventRing) recent(limit int) []Event {
	if limit <= 0 || limit > r.len {
		limit = r.len
	}
	out := make([]Event, 0, limit)
	for i := 0; i < limit; i++ {
		idx := (r.head - 1 - i + eventRingSize) % eventRingSize
		out = append(out, r.items[idx])
	}
	return out
}

// bucket accumulates boundary-accepted messages/bytes for one 1-second slot.
type bucket struct {
	start time.Time
	msgs  int64
	bytes int64
}

// counters is one connector's live stats: atomic-free totals plus a ring of
// recent-second buckets, all guarded by mu (contention here is per-connector
// and low-volume compared to the message pipeline itself, so a plain mutex
// is simpler than atomics + a lock-free ring and just as sufficient).
type counters struct {
	mu sync.Mutex

	totalMessages int64
	totalBytes    int64

	buckets [numBuckets]bucket
	head    int // index of the most recently written bucket

	queueDepth       int64
	queueBytes       int64
	retainedDepth    int64
	retainedBytes    int64
	queueCursor      int64
	queueTail        int64
	oldestPending    time.Time
	oldestRetained   time.Time
	limitMessages    int64
	limitBytes       int64
	headroomMessages int64
	headroomBytes    int64
	deliveryClass    string
	state            string
	lastError        string
	drops            int64
	stageTotals      map[string]int64

	// depthRing/depthHead/depthLen implement a fixed-capacity FIFO ring of
	// queue-depth readings, one per SetQueue call, oldest overwritten first
	// once the ring fills. depthHead is the next slot to write; depthLen is
	// the number of valid samples (caps at depthRingSize, unlike buckets'
	// head/numBuckets pair which is always fully populated after the first
	// write — this ring can be partially filled for a connector's first
	// depthRingSize-1 samples).
	depthRing [depthRingSize]int64
	depthHead int
	depthLen  int
}

// record adds a delivery at time t, rolling the ring forward to t's bucket
// and zeroing any buckets skipped since the last write (idle gap).
func (c *counters) record(t time.Time, msgs, bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.totalMessages += msgs
	c.totalBytes += bytes
	c.advanceLocked(t)
	b := &c.buckets[c.head]
	b.msgs += msgs
	b.bytes += bytes
}

// advanceLocked moves head to t's bucket slot, clearing every bucket between
// the last write and now (inclusive of the new slot, exclusive of the old
// head) so stale counts from a reused ring slot never leak into a new
// second. Callers must hold mu.
func (c *counters) advanceLocked(t time.Time) {
	sec := t.Truncate(bucketWidth)
	cur := c.buckets[c.head]
	if cur.start.IsZero() || !sec.Before(cur.start.Add(window)) {
		// Either the first write ever, or the gap since the last write is
		// at least a full window: every existing bucket is already outside
		// the window, so reseed the whole ring at this second directly
		// rather than clamping the loop below. Clamping while still
		// stepping bucketWidth at a time would mislabel the bucket that's
		// about to receive this write with a stale synthetic timestamp
		// (cur.start + numBuckets*bucketWidth) instead of the real "sec" —
		// which then reads as outside the window on the very next
		// Snapshot, hiding traffic that just resumed after an idle gap.
		for i := range c.buckets {
			c.buckets[i] = bucket{start: sec}
		}
		c.head = 0
		return
	}
	if !sec.After(cur.start) {
		return // same second (or clock went backwards) as last write
	}
	// sec is within one window of cur.start, so this is an exact multiple
	// of bucketWidth strictly less than numBuckets — safe to step one
	// bucket at a time without wrapping back onto cur's own slot.
	elapsed := int(sec.Sub(cur.start) / bucketWidth)
	for i := 1; i <= elapsed; i++ {
		c.head = (c.head + 1) % numBuckets
		c.buckets[c.head] = bucket{start: cur.start.Add(time.Duration(i) * bucketWidth)}
	}
}

// snapshot computes a Snapshot as of time now: totals as stored, rates from
// whichever buckets fall within the trailing window.
func (c *counters) snapshot(now time.Time) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := now.Add(-window)
	var msgs, bytes int64
	for _, b := range c.buckets {
		if b.start.IsZero() || b.start.Before(cutoff) {
			continue
		}
		if b.start.After(now) {
			continue
		}
		msgs += b.msgs
		bytes += b.bytes
	}
	return Snapshot{
		TotalMessages:    c.totalMessages,
		TotalBytes:       c.totalBytes,
		MsgPerSec:        float64(msgs) / window.Seconds(),
		BytesPerSec:      float64(bytes) / window.Seconds(),
		QueueDepth:       c.queueDepth,
		QueueBytes:       c.queueBytes,
		RetainedDepth:    c.retainedDepth,
		RetainedBytes:    c.retainedBytes,
		QueueCursor:      c.queueCursor,
		QueueTail:        c.queueTail,
		OldestPending:    timePtr(c.oldestPending),
		OldestRetained:   timePtr(c.oldestRetained),
		LimitMessages:    c.limitMessages,
		LimitBytes:       c.limitBytes,
		HeadroomMessages: c.headroomMessages,
		HeadroomBytes:    c.headroomBytes,
		DeliveryClass:    c.deliveryClass,
		State:            c.state,
		LastError:        c.lastError,
		Drops:            c.drops,
		StageTotals:      cloneTotals(c.stageTotals),
		DepthHistory:     c.depthHistoryLocked(),
	}
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	v := t
	return &v
}

func cloneTotals(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (c *counters) setQueue(depth, bytes int64) {
	c.mu.Lock()
	c.queueDepth = depth
	c.queueBytes = bytes
	c.depthRing[c.depthHead] = depth
	c.depthHead = (c.depthHead + 1) % depthRingSize
	if c.depthLen < depthRingSize {
		c.depthLen++
	}
	c.mu.Unlock()
}

func (c *counters) setQueueStats(s queue.Stats) {
	c.mu.Lock()
	c.queueDepth, c.queueBytes = s.Depth, s.Bytes
	c.retainedDepth, c.retainedBytes = s.RetainedDepth, s.RetainedBytes
	c.queueCursor, c.queueTail = s.Cursor, s.Tail
	c.oldestPending = s.Oldest
	c.oldestRetained = s.OldestRetained
	c.limitMessages, c.limitBytes = s.LimitMessages, s.LimitBytes
	c.headroomMessages, c.headroomBytes = s.HeadroomMessages, s.HeadroomBytes
	c.depthRing[c.depthHead] = s.Depth
	c.depthHead = (c.depthHead + 1) % depthRingSize
	if c.depthLen < depthRingSize {
		c.depthLen++
	}
	c.mu.Unlock()
}

func (c *counters) setRuntime(deliveryClass, state string, err error) {
	c.mu.Lock()
	if deliveryClass != "" {
		c.deliveryClass = deliveryClass
	}
	if state != "" {
		c.state = state
	}
	if err == nil {
		c.lastError = ""
	} else {
		c.lastError = err.Error()
	}
	c.mu.Unlock()
}

func (c *counters) recordStage(stage string, n int64) {
	c.mu.Lock()
	if c.stageTotals == nil {
		c.stageTotals = map[string]int64{}
	}
	c.stageTotals[stage] += n
	c.mu.Unlock()
}

func (c *counters) recordDrops(n int64) {
	c.mu.Lock()
	c.drops += n
	c.mu.Unlock()
}

// zeroQueue resets the depth/bytes gauges without touching the depth ring —
// setQueue minus the history side effect. See Registry.Touch for why the
// two exist separately.
func (c *counters) zeroQueue() {
	c.mu.Lock()
	c.queueDepth = 0
	c.queueBytes = 0
	c.mu.Unlock()
}

// depthHistoryLocked returns the ring's samples oldest-first. Callers must
// hold mu. Returns nil (not an empty non-nil slice) when no sample has ever
// been recorded, so Snapshot's DepthHistory naturally omits the field via
// its omitempty tag instead of serializing "[]".
func (c *counters) depthHistoryLocked() []int64 {
	if c.depthLen == 0 {
		return nil
	}
	out := make([]int64, c.depthLen)
	start := (c.depthHead - c.depthLen + depthRingSize) % depthRingSize
	for i := 0; i < c.depthLen; i++ {
		out[i] = c.depthRing[(start+i)%depthRingSize]
	}
	return out
}

// Registry tracks live per-connector counters and rolling rates for the
// API and UI. It is independent of OTel (which serves Prometheus). A nil
// *Registry no-ops Record/SetQueue/Touch/Remove and returns empty from
// Snapshot/All, the same nil-safe convention as metrics.Set, so callers
// never need a nil check around instrumentation.
type Registry struct {
	now func() time.Time

	mu     sync.Mutex
	conn   map[string]*counters
	source map[string]*counters
	sink   map[string]*counters
	events map[string]*eventRing
}

// NewRegistry returns an empty Registry using the real wall clock.
func NewRegistry() *Registry {
	return newRegistryAt(time.Now)
}

// newRegistryAt is the test constructor: it lets tests inject a fake clock
// (via r.now) to exercise rate decay deterministically.
func newRegistryAt(now func() time.Time) *Registry {
	return &Registry{
		now:    now,
		conn:   map[string]*counters{},
		source: map[string]*counters{},
		sink:   map[string]*counters{},
		events: map[string]*eventRing{},
	}
}

func (r *Registry) get(connector string) *counters {
	return r.getFrom(r.conn, connector)
}

func (r *Registry) getSource(source string) *counters {
	return r.getFrom(r.source, source)
}

func (r *Registry) getSink(sink string) *counters {
	return r.getFrom(r.sink, sink)
}

func (r *Registry) getFrom(m map[string]*counters, id string) *counters {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := m[id]
	if !ok {
		c = &counters{}
		m[id] = c
	}
	return c
}

func eventFromEnvelope(now time.Time, stage, connectorID string, e *msg.Envelope) Event {
	ev := Event{Time: now, Stage: stage, ConnectorID: connectorID}
	if e == nil {
		return ev
	}
	ev.PGN = e.PGN
	ev.PGNName = e.PGNName
	ev.Source = e.Source
	ev.Dest = e.Dest
	ev.Priority = e.Priority
	ev.Timestamp = e.Timestamp
	ev.SizeBytes = e.SizeBytes()
	if len(e.Payload) > 0 {
		ev.Payload = string(e.Payload)
		if len(ev.Payload) > 240 {
			ev.Payload = ev.Payload[:240] + "..."
		}
	}
	return ev
}

func (r *Registry) recordEvent(kind, id string, e Event) {
	if r == nil || id == "" {
		return
	}
	key := kind + ":" + id
	r.mu.Lock()
	ring, ok := r.events[key]
	if !ok {
		ring = &eventRing{}
		r.events[key] = ring
	}
	ring.add(e)
	r.mu.Unlock()
}

func (r *Registry) RecordSource(source string, e *msg.Envelope) {
	if r == nil {
		return
	}
	now := r.now()
	ev := eventFromEnvelope(now, "received", "", e)
	r.getSource(source).record(now, 1, int64(ev.SizeBytes))
	r.recordEvent("source", source, ev)
}

func (r *Registry) RecordSink(sink, connector string, e *msg.Envelope) {
	if r == nil {
		return
	}
	now := r.now()
	ev := eventFromEnvelope(now, "sent", connector, e)
	r.getSink(sink).record(now, 1, int64(ev.SizeBytes))
	r.recordEvent("sink", sink, ev)
}

func (r *Registry) RecordConnectorEvent(connector, stage string, e *msg.Envelope) {
	if r == nil {
		return
	}
	r.recordEvent("connector", connector, eventFromEnvelope(r.now(), stage, connector, e))
}

// RecordStage counts a route-runtime transition without assuming that every
// terminal transition is confirmed delivery.
func (r *Registry) RecordStage(connector, stage string, n int64) {
	if r == nil || n == 0 {
		return
	}
	r.get(connector).recordStage(stage, n)
}

func (r *Registry) RecordSourceDrops(source string, n int64) {
	if r == nil || n == 0 {
		return
	}
	r.getSource(source).recordDrops(n)
}

func (r *Registry) SourceSnapshot(source string) (Snapshot, bool) {
	if r == nil {
		return Snapshot{}, false
	}
	return r.componentSnapshot(r.source, source)
}

func (r *Registry) SinkSnapshot(sink string) (Snapshot, bool) {
	if r == nil {
		return Snapshot{}, false
	}
	return r.componentSnapshot(r.sink, sink)
}

func (r *Registry) componentSnapshot(m map[string]*counters, id string) (Snapshot, bool) {
	if r == nil {
		return Snapshot{}, false
	}
	r.mu.Lock()
	c, ok := m[id]
	r.mu.Unlock()
	if !ok {
		return Snapshot{}, false
	}
	return c.snapshot(r.now()), true
}

func (r *Registry) Recent(kind, id string, limit int) []Event {
	if r == nil {
		return nil
	}
	key := kind + ":" + id
	r.mu.Lock()
	ring := r.events[key]
	if ring == nil {
		r.mu.Unlock()
		return nil
	}
	out := ring.recent(limit)
	r.mu.Unlock()
	return out
}

// Record adds boundary-accepted messages/bytes for a connector at time now.
func (r *Registry) Record(connector string, msgs, bytes int64) {
	if r == nil {
		return
	}
	r.get(connector).record(r.now(), msgs, bytes)
}

// SetQueue records current queue depth/bytes (from the prune loop) and
// appends depth to the connector's depth-history ring (see depthRingSize),
// which Snapshot surfaces as DepthHistory for the UI sparkline. Because
// every call appends a history sample, SetQueue is for genuine periodic
// measurements only — presence registration belongs to Touch.
func (r *Registry) SetQueue(connector string, depth, bytes int64) {
	if r == nil {
		return
	}
	r.get(connector).setQueue(depth, bytes)
}

func (r *Registry) SetQueueStats(connector string, s queue.Stats) {
	if r == nil {
		return
	}
	r.get(connector).setQueueStats(s)
}

func (r *Registry) SetRuntime(connector, deliveryClass, state string, err error) {
	if r == nil {
		return
	}
	r.get(connector).setRuntime(deliveryClass, state, err)
}

// Touch ensures a connector's entry exists (so it shows up in All()/
// Snapshot immediately) and zeroes its depth/bytes gauges, WITHOUT
// appending to the depth-history ring. It exists for Connector.Start's
// synchronous presence registration: on a hot-apply restart (config edit →
// supervisor Stop + new Start) the registry entry survives — Remove only
// fires on delete — so Start seeding via SetQueue(id, 0, 0) would append a
// genuine 0 mid-history, drawing a dip-to-zero notch in the sparkline that
// looks like the queue drained and refilled when it did no such thing.
// History samples must only come from the prune loop's real periodic
// measurements (SetQueue); Touch covers the "make the connector visible
// now, real numbers follow within milliseconds" path.
func (r *Registry) Touch(connector string) {
	if r == nil {
		return
	}
	r.get(connector).zeroQueue()
}

// Remove drops a connector's stats and recent events (deleted connectors),
// including its queue-depth history ring — the whole *counters entry (totals,
// rate buckets, and depth ring alike) is deleted as one unit, so there's no
// separate ring-eviction step to keep in sync with this method.
//
// Ordering contract: callers must ensure the connector's pipeline is fully
// stopped (Connector.Stop is synchronous — it blocks on the connector's
// internal WaitGroup) before calling Remove. Record does not distinguish a
// never-seen id from a just-removed one: get lazily (re)creates a fresh,
// zeroed *counters entry (empty depth ring included) for any id not
// currently in the map. So a Record (or SetQueue) call that lands after
// Remove — from a pipeline that is somehow still running or racing the
// removal — will silently resurrect the entry under the same id, just reset
// to zero rather than holding the pre-removal totals/history. It won't
// reappear with stale data, but it will reappear. Removing only after the
// pipeline has fully stopped is what prevents that resurrection.
func (r *Registry) Remove(connector string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.conn, connector)
	delete(r.events, "connector:"+connector)
	r.mu.Unlock()
}

// RemoveSource and RemoveSink drop process-local counters and recent payload
// events for an entity deleted from config. Disabled or hot-restarted entities
// deliberately keep their history; the supervisor calls these only after it
// observes the configured id disappear entirely.
func (r *Registry) RemoveSource(source string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.source, source)
	delete(r.events, "source:"+source)
	r.mu.Unlock()
}

func (r *Registry) RemoveSink(sink string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.sink, sink)
	delete(r.events, "sink:"+sink)
	r.mu.Unlock()
}

// Snapshot returns a connector's current counters/rates and whether it has
// ever been recorded.
func (r *Registry) Snapshot(connector string) (Snapshot, bool) {
	if r == nil {
		return Snapshot{}, false
	}
	r.mu.Lock()
	c, ok := r.conn[connector]
	r.mu.Unlock()
	if !ok {
		return Snapshot{}, false
	}
	return c.snapshot(r.now()), true
}

// All returns every tracked connector's current snapshot, keyed by id.
func (r *Registry) All() map[string]Snapshot {
	out := map[string]Snapshot{}
	if r == nil {
		return out
	}
	r.mu.Lock()
	conns := make(map[string]*counters, len(r.conn))
	for id, c := range r.conn {
		conns[id] = c
	}
	r.mu.Unlock()
	now := r.now()
	for id, c := range conns {
		out[id] = c.snapshot(now)
	}
	return out
}
