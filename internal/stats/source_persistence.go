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
	maxSourceMetricEvents       = 100
	maxPersistedMetricEvents    = 1000
	sourceMetricEventBufferSize = 128
	sourceMetricPersistBatch    = 32
	sourceMetricPersistDelay    = 250 * time.Millisecond

	// Lifecycle events are diagnostic summaries. Bound both persisted input and
	// every retained string/map so a malformed historical row cannot inflate the
	// in-process event rings on restart.
	maxSourceMetricEventDocumentBytes = 4 << 10
	maxSourceMetricEventDetails       = 8
	maxSourceMetricEventKindBytes     = 64
	maxSourceMetricEventSummaryBytes  = 512
	maxSourceMetricEventDetailBytes   = 256
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
	ctx    context.Context
	cancel context.CancelFunc
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
	writerCtx, cancel := context.WithCancel(context.Background())
	persistence := &sourceMetricPersistence{
		db: db, ctx: writerCtx, cancel: cancel,
		events: make(chan SourceMetricEvent, sourceMetricEventBufferSize), done: make(chan struct{}),
	}
	r.mu.Lock()
	if r.sourcePersistence != nil {
		r.mu.Unlock()
		cancel()
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
		SELECT e.id, e.doc FROM source_metric_events e
		WHERE length(CAST(e.doc AS BLOB)) <= ?
		  AND EXISTS (SELECT 1 FROM sources s WHERE s.id = e.source_id)
		ORDER BY e.id DESC LIMIT ?
	) ORDER BY id`, maxSourceMetricEventDocumentBytes, maxPersistedMetricEvents)
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
		event = boundedSourceMetricEvent(event)
		out = append(out, event)
	}
	return out, rows.Err()
}

func (p *sourceMetricPersistence) run() {
	defer close(p.done)
	defer p.cancel()
	for {
		first, ok := <-p.events
		if !ok {
			return
		}
		batch := make([]SourceMetricEvent, 0, sourceMetricPersistBatch)
		batch = append(batch, first)
		timer := time.NewTimer(sourceMetricPersistDelay)
		closed := false
	collect:
		for len(batch) < sourceMetricPersistBatch {
			select {
			case event, ok := <-p.events:
				if !ok {
					closed = true
					break collect
				}
				batch = append(batch, event)
			case <-timer.C:
				break collect
			case <-p.ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if err := p.persistBatch(batch); err != nil {
			p.mu.Lock()
			p.errors += int64(len(batch))
			p.mu.Unlock()
		}
		if closed {
			return
		}
	}
}

func (p *sourceMetricPersistence) persistBatch(events []SourceMetricEvent) error {
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	insert, err := tx.PrepareContext(p.ctx, `INSERT INTO source_metric_events
		(ts, source_id, pgn, source_address, kind, severity, doc)
		VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() { _ = insert.Close() }()
	for _, event := range events {
		doc, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err := insert.ExecContext(p.ctx, event.Time.UnixNano(), event.SourceID,
			event.PGN, event.SourceAddress, event.Kind, event.Severity, string(doc)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(p.ctx, `DELETE FROM source_metric_events WHERE id NOT IN (
		SELECT id FROM source_metric_events ORDER BY id DESC LIMIT ?
	)`, maxPersistedMetricEvents); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *Registry) recordSourceMetricEvent(event SourceMetricEvent) {
	if r == nil {
		return
	}
	if event.Time.IsZero() {
		event.Time = r.now().UTC()
	}
	event = boundedSourceMetricEvent(event)
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
	items := make([]SourceMetricEvent, len(ring.items))
	for i := range ring.items {
		items[i] = cloneSourceMetricEvent(ring.items[i])
	}
	r.mu.Unlock()
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	out := append([]SourceMetricEvent(nil), items[len(items)-limit:]...)
	sort.Slice(out, func(i, j int) bool { return out[i].Time.After(out[j].Time) })
	return out
}

func boundedSourceMetricEvent(event SourceMetricEvent) SourceMetricEvent {
	event.SourceID, _ = boundedDiagnosticText(event.SourceID, maxDiagnosticLabelBytes)
	event.DeviceNameHex, _ = boundedDiagnosticText(event.DeviceNameHex, 32)
	event.Kind, _ = boundedDiagnosticText(event.Kind, maxSourceMetricEventKindBytes)
	event.Severity, _ = boundedDiagnosticText(event.Severity, maxSourceMetricEventKindBytes)
	event.Summary, _ = boundedDiagnosticText(event.Summary, maxSourceMetricEventSummaryBytes)
	if len(event.Details) == 0 {
		event.Details = nil
		return event
	}
	keys := make([]string, 0, len(event.Details))
	for key := range event.Details {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	bounded := make(map[string]string, min(len(keys), maxSourceMetricEventDetails))
	for _, key := range keys {
		if len(bounded) >= maxSourceMetricEventDetails {
			break
		}
		boundedKey, _ := boundedDiagnosticText(key, maxSourceMetricEventKindBytes)
		boundedValue, _ := boundedDiagnosticText(event.Details[key], maxSourceMetricEventDetailBytes)
		bounded[boundedKey] = boundedValue
	}
	event.Details = bounded
	return event
}

func cloneSourceMetricEvent(event SourceMetricEvent) SourceMetricEvent {
	if len(event.Details) == 0 {
		return event
	}
	details := make(map[string]string, len(event.Details))
	for key, value := range event.Details {
		details[key] = value
	}
	event.Details = details
	return event
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
		// Cancel any SQLite work before the Store is closed. modernc.org/sqlite
		// honors ExecContext cancellation, so App.Close cannot strand this
		// writer on a context-free operation while tearing the database down.
		persistence.cancel()
		<-persistence.done
		persistence.mu.Lock()
		errorsCount := persistence.errors
		persistence.mu.Unlock()
		if errorsCount > 0 {
			return errors.Join(ctx.Err(), fmt.Errorf("source metric persistence encountered %d write errors", errorsCount))
		}
		return ctx.Err()
	}
}
