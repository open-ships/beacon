// Package stats tracks live per-connector counters and rolling delivery
// rates for the config API and the future UI dashboard. It is independent
// of OTel (which serves Prometheus via internal/metrics): this registry
// exists for cheap, synchronous, in-process reads from HTTP handlers.
package stats

import (
	"sync"
	"time"
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
)

// Snapshot is the point-in-time view of one connector's counters, returned
// by Snapshot and All. Rates are computed over the trailing 10-second
// window; totals never decay.
type Snapshot struct {
	TotalMessages int64   `json:"total_messages"`
	TotalBytes    int64   `json:"total_bytes"`
	MsgPerSec     float64 `json:"msg_per_sec"`   // over last 10s window
	BytesPerSec   float64 `json:"bytes_per_sec"` // over last 10s window
	QueueDepth    int64   `json:"queue_depth"`
	QueueBytes    int64   `json:"queue_bytes"`

	// DepthHistory is the last depthRingSize QueueDepth readings, oldest
	// first, as recorded by successive SetQueue calls. Absent (nil/omitted
	// from JSON) for a connector that has never had SetQueue called on it —
	// additive/non-breaking: existing Snapshot consumers (the config API's
	// metrics endpoints, the connector detail/dashboard UI fragments) that
	// don't know this field simply ignore it.
	DepthHistory []int64 `json:"depth_history,omitempty"`
}

// bucket accumulates delivered messages/bytes for one 1-second slot.
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

	queueDepth int64
	queueBytes int64

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
		TotalMessages: c.totalMessages,
		TotalBytes:    c.totalBytes,
		MsgPerSec:     float64(msgs) / window.Seconds(),
		BytesPerSec:   float64(bytes) / window.Seconds(),
		QueueDepth:    c.queueDepth,
		QueueBytes:    c.queueBytes,
		DepthHistory:  c.depthHistoryLocked(),
	}
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
// *Registry no-ops Record/SetQueue/Remove and returns empty from
// Snapshot/All, the same nil-safe convention as metrics.Set, so callers
// never need a nil check around instrumentation.
type Registry struct {
	now func() time.Time

	mu   sync.Mutex
	conn map[string]*counters
}

// NewRegistry returns an empty Registry using the real wall clock.
func NewRegistry() *Registry {
	return newRegistryAt(time.Now)
}

// newRegistryAt is the test constructor: it lets tests inject a fake clock
// (via r.now) to exercise rate decay deterministically.
func newRegistryAt(now func() time.Time) *Registry {
	return &Registry{now: now, conn: map[string]*counters{}}
}

func (r *Registry) get(connector string) *counters {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.conn[connector]
	if !ok {
		c = &counters{}
		r.conn[connector] = c
	}
	return c
}

// Record adds delivered messages/bytes for a connector at time now.
func (r *Registry) Record(connector string, msgs, bytes int64) {
	if r == nil {
		return
	}
	r.get(connector).record(r.now(), msgs, bytes)
}

// SetQueue records current queue depth/bytes (from the prune loop) and
// appends depth to the connector's depth-history ring (see depthRingSize),
// which Snapshot surfaces as DepthHistory for the UI sparkline.
func (r *Registry) SetQueue(connector string, depth, bytes int64) {
	if r == nil {
		return
	}
	r.get(connector).setQueue(depth, bytes)
}

// Remove drops a connector's stats (deleted connectors), including its
// queue-depth history ring — the whole *counters entry (totals, rate
// buckets, and depth ring alike) is deleted as one unit, so there's no
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
