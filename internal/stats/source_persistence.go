package stats

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	maxSourceMetricEvents       = 200
	maxPersistedMetricEvents    = 2000
	sourceMetricEventBufferSize = 256
)

type SourceMetricEvent struct {
	ID            int64             `json:"id,omitempty"`
	Time          time.Time         `json:"time"`
	SourceID      string            `json:"source_id"`
	PGN           uint32            `json:"pgn"`
	SourceAddress uint8             `json:"source_address"`
	DeviceNameHex string            `json:"device_name_hex,omitempty"`
	Kind          string            `json:"kind"`
	Severity      string            `json:"severity"`
	Summary       string            `json:"summary"`
	Details       map[string]string `json:"details,omitempty"`
}

type sourceMetricEventRing struct {
	items []SourceMetricEvent
}

func (r *sourceMetricEventRing) add(event SourceMetricEvent) {
	if len(r.items) >= maxSourceMetricEvents {
		copy(r.items, r.items[len(r.items)-maxSourceMetricEvents+1:])
		r.items = r.items[:maxSourceMetricEvents-1]
	}
	r.items = append(r.items, event)
}

type sourceMetricPersistence struct {
	db     *sql.DB
	events chan SourceMetricEvent
	done   chan struct{}
	mu     sync.Mutex
	errors int64
	closed bool
}

// AttachSourceMetricPersistence loads recent source lifecycle events and
// starts their non-blocking persistence writer.
func (r *Registry) AttachSourceMetricPersistence(ctx context.Context, db *sql.DB) error {
	if r == nil || db == nil {
		return nil
	}
	events, err := loadSourceMetricEvents(ctx, db)
	if err != nil {
		return err
	}
	persistence := &sourceMetricPersistence{
		db: db, events: make(chan SourceMetricEvent, sourceMetricEventBufferSize), done: make(chan struct{}),
	}
	r.mu.Lock()
	if r.sourcePersistence != nil {
		r.mu.Unlock()
		return errors.New("source metric persistence already attached")
	}
	for _, event := range events {
		ring := r.sourceMetricEvents[event.SourceID]
		if ring == nil {
			ring = &sourceMetricEventRing{}
			r.sourceMetricEvents[event.SourceID] = ring
		}
		ring.add(event)
	}
	r.sourcePersistence = persistence
	r.mu.Unlock()
	go persistence.run()
	return nil
}

func loadSourceMetricEvents(ctx context.Context, db *sql.DB) ([]SourceMetricEvent, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, doc FROM (
		SELECT id, doc FROM source_metric_events ORDER BY id DESC LIMIT ?
	) ORDER BY id`, maxPersistedMetricEvents)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]SourceMetricEvent, 0)
	for rows.Next() {
		var id int64
		var doc string
		if err := rows.Scan(&id, &doc); err != nil {
			return nil, err
		}
		var event SourceMetricEvent
		if err := json.Unmarshal([]byte(doc), &event); err != nil {
			return nil, err
		}
		event.ID = id
		out = append(out, event)
	}
	return out, rows.Err()
}

func (p *sourceMetricPersistence) run() {
	defer close(p.done)
	for event := range p.events {
		doc, err := json.Marshal(event)
		if err == nil {
			_, err = p.db.Exec(`INSERT INTO source_metric_events
				(ts, source_id, pgn, source_address, kind, severity, doc)
				VALUES (?, ?, ?, ?, ?, ?, ?)`, event.Time.UnixNano(), event.SourceID,
				event.PGN, event.SourceAddress, event.Kind, event.Severity, string(doc))
		}
		if err == nil {
			_, err = p.db.Exec(`DELETE FROM source_metric_events WHERE id NOT IN (
				SELECT id FROM source_metric_events ORDER BY id DESC LIMIT ?
			)`, maxPersistedMetricEvents)
		}
		if err != nil {
			p.mu.Lock()
			p.errors++
			p.mu.Unlock()
		}
	}
}

func (r *Registry) recordSourceMetricEvent(event SourceMetricEvent) {
	if r == nil {
		return
	}
	if event.Time.IsZero() {
		event.Time = r.now().UTC()
	}
	r.mu.Lock()
	ring := r.sourceMetricEvents[event.SourceID]
	if ring == nil {
		ring = &sourceMetricEventRing{}
		r.sourceMetricEvents[event.SourceID] = ring
	}
	ring.add(event)
	persistence := r.sourcePersistence
	r.mu.Unlock()
	if persistence != nil {
		persistence.enqueue(event)
	}
}

func (p *sourceMetricPersistence) enqueue(event SourceMetricEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	select {
	case p.events <- event:
	default:
		p.errors++
	}
}

func (r *Registry) SourceMetricEvents(source string, limit int) []SourceMetricEvent {
	if r == nil {
		return []SourceMetricEvent{}
	}
	r.mu.Lock()
	ring := r.sourceMetricEvents[source]
	if ring == nil {
		r.mu.Unlock()
		return []SourceMetricEvent{}
	}
	items := append([]SourceMetricEvent(nil), ring.items...)
	r.mu.Unlock()
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	out := append([]SourceMetricEvent(nil), items[len(items)-limit:]...)
	sort.Slice(out, func(i, j int) bool { return out[i].Time.After(out[j].Time) })
	return out
}

func (r *Registry) CloseSourceMetricPersistence(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	persistence := r.sourcePersistence
	r.sourcePersistence = nil
	r.mu.Unlock()
	if persistence == nil {
		return nil
	}
	persistence.mu.Lock()
	if !persistence.closed {
		persistence.closed = true
		close(persistence.events)
	}
	persistence.mu.Unlock()
	select {
	case <-persistence.done:
		persistence.mu.Lock()
		errorsCount := persistence.errors
		persistence.mu.Unlock()
		if errorsCount > 0 {
			return fmt.Errorf("source metric persistence encountered %d write errors", errorsCount)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
