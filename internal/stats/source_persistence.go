package stats

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	maxSourceMetricEvents       = 200
	maxPersistedMetricEvents    = 2000
	sourceMetricEventBufferSize = 256
)

type BaselineField struct {
	Minimum float64 `json:"minimum"`
	Maximum float64 `json:"maximum"`
	Unit    string  `json:"unit,omitempty"`
}

type BaselineRawByte struct {
	Offset            int    `json:"offset"`
	Minimum           uint8  `json:"minimum"`
	Maximum           uint8  `json:"maximum"`
	ChangedBitMaskHex string `json:"changed_bit_mask_hex"`
}

// SourceTrafficBaseline is an operator-approved expectation for one PGN from
// one stable Device NAME (or source address when no NAME is available).
type SourceTrafficBaseline struct {
	SourceID                  string                   `json:"source_id"`
	Identity                  string                   `json:"identity"`
	PGN                       uint32                   `json:"pgn"`
	PGNName                   string                   `json:"pgn_name,omitempty"`
	SourceAddress             uint8                    `json:"source_address"`
	DeviceNameHex             string                   `json:"device_name_hex,omitempty"`
	ExpectedFrequencyHz       float64                  `json:"expected_frequency_hz"`
	FrequencyTolerancePercent float64                  `json:"frequency_tolerance_percent"`
	PayloadLengths            []int                    `json:"payload_lengths"`
	DecodeStatus              string                   `json:"decode_status"`
	Variant                   string                   `json:"variant,omitempty"`
	Transport                 string                   `json:"transport,omitempty"`
	Destinations              []int                    `json:"destinations"`
	Priorities                []int                    `json:"priorities"`
	Fields                    map[string]BaselineField `json:"fields,omitempty"`
	RawBytes                  []BaselineRawByte        `json:"raw_bytes,omitempty"`
	ApprovedAt                time.Time                `json:"approved_at"`
}

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

type sourceBaselineKey struct {
	source, identity string
	pgn              uint32
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

// AttachSourceMetricPersistence loads baselines and recent change events, then
// starts a non-blocking event writer. Call this before serving baseline APIs.
func (r *Registry) AttachSourceMetricPersistence(ctx context.Context, db *sql.DB) error {
	if r == nil || db == nil {
		return nil
	}
	baselines, err := loadSourceBaselines(ctx, db)
	if err != nil {
		return err
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
	r.sourceBaselines = baselines
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

func loadSourceBaselines(ctx context.Context, db *sql.DB) (map[sourceBaselineKey]SourceTrafficBaseline, error) {
	rows, err := db.QueryContext(ctx, `SELECT doc FROM source_metric_baselines`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[sourceBaselineKey]SourceTrafficBaseline)
	for rows.Next() {
		var doc string
		if err := rows.Scan(&doc); err != nil {
			return nil, err
		}
		var baseline SourceTrafficBaseline
		if err := json.Unmarshal([]byte(doc), &baseline); err != nil {
			return nil, err
		}
		out[sourceBaselineKey{baseline.SourceID, baseline.Identity, baseline.PGN}] = baseline
	}
	return out, rows.Err()
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

func sourceBaselineIdentity(deviceNameHex string, address uint8) string {
	if deviceNameHex != "" {
		return "name:" + deviceNameHex
	}
	return fmt.Sprintf("address:%d", address)
}

func baselineFromMetric(metric SourcePGNMetric, approvedAt time.Time) SourceTrafficBaseline {
	lengths := make([]int, 0)
	if metric.Raw != nil {
		for length := range metric.Raw.LengthCounts {
			parsed, err := strconv.Atoi(length)
			if err == nil {
				lengths = append(lengths, parsed)
			}
		}
	}
	if len(lengths) == 0 {
		for value := metric.PayloadBytesMin; value <= metric.PayloadBytesMax && value-metric.PayloadBytesMin < 256; value++ {
			lengths = append(lengths, int(value))
		}
	}
	sort.Ints(lengths)
	destinations := sortedIntKeys(metric.DestinationCounts)
	priorities := sortedIntKeys(metric.PriorityCounts)
	fields := make(map[string]BaselineField)
	for _, field := range metric.Fields {
		if field.P05 == nil || field.P95 == nil {
			continue
		}
		span := *field.P95 - *field.P05
		margin := mathMax(span*0.25, mathMax(mathAbs(*field.P50)*0.02, 1e-9))
		fields[field.Field] = BaselineField{Minimum: *field.P05 - margin, Maximum: *field.P95 + margin, Unit: field.Unit}
	}
	rawBytes := make([]BaselineRawByte, 0)
	if metric.Raw != nil {
		for _, rawByte := range metric.Raw.Bytes {
			rawBytes = append(rawBytes, BaselineRawByte{
				Offset: rawByte.Offset, Minimum: rawByte.Minimum, Maximum: rawByte.Maximum,
				ChangedBitMaskHex: rawByte.ChangedBitMaskHex,
			})
		}
	}
	return SourceTrafficBaseline{
		SourceID: metric.SourceID, Identity: sourceBaselineIdentity(metric.DeviceNameHex, metric.SourceAddress),
		PGN: metric.PGN, PGNName: metric.PGNName, SourceAddress: metric.SourceAddress,
		DeviceNameHex: metric.DeviceNameHex, ExpectedFrequencyHz: metric.FrequencyHz,
		FrequencyTolerancePercent: 25, PayloadLengths: lengths, DecodeStatus: metric.DecodeStatus,
		Variant: metric.Variant, Transport: metric.Transport, Destinations: destinations,
		Priorities: priorities, Fields: fields, RawBytes: rawBytes, ApprovedAt: approvedAt,
	}
}

func sortedIntKeys(values map[string]int64) []int {
	out := make([]int, 0, len(values))
	for value := range values {
		parsed, err := strconv.ParseUint(value, 10, 8)
		if err == nil {
			out = append(out, int(parsed))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func mathAbs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func mathMax(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

func (r *Registry) CommitSourceTrafficBaseline(ctx context.Context, source string) ([]SourceTrafficBaseline, error) {
	if r == nil {
		return nil, errors.New("source metrics unavailable")
	}
	metrics := r.SourcePGNMetrics(source)
	approvedAt := r.now().UTC()
	baselines := make([]SourceTrafficBaseline, 0, len(metrics))
	for _, metric := range metrics {
		if metric.Observed {
			baselines = append(baselines, baselineFromMetric(metric, approvedAt))
		}
	}
	r.mu.Lock()
	persistence := r.sourcePersistence
	r.mu.Unlock()
	if persistence == nil {
		return nil, errors.New("source metric persistence unavailable")
	}
	tx, err := persistence.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM source_metric_baselines WHERE source_id = ?`, source); err != nil {
		return nil, err
	}
	for _, baseline := range baselines {
		doc, err := json.Marshal(baseline)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO source_metric_baselines
			(source_id, identity, pgn, approved_at, doc) VALUES (?, ?, ?, ?, ?)`,
			baseline.SourceID, baseline.Identity, baseline.PGN, baseline.ApprovedAt.UnixNano(), string(doc)); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	for key := range r.sourceBaselines {
		if key.source == source {
			delete(r.sourceBaselines, key)
		}
	}
	for _, baseline := range baselines {
		r.sourceBaselines[sourceBaselineKey{baseline.SourceID, baseline.Identity, baseline.PGN}] = baseline
	}
	r.mu.Unlock()
	r.recordSourceMetricEvent(SourceMetricEvent{Time: approvedAt, SourceID: source,
		Kind: "baseline_committed", Severity: "info",
		Summary: fmt.Sprintf("Approved %d source traffic streams", len(baselines))})
	return baselines, nil
}

func (r *Registry) ClearSourceTrafficBaseline(ctx context.Context, source string) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	persistence := r.sourcePersistence
	r.mu.Unlock()
	if persistence == nil {
		return errors.New("source metric persistence unavailable")
	}
	if _, err := persistence.db.ExecContext(ctx, `DELETE FROM source_metric_baselines WHERE source_id = ?`, source); err != nil {
		return err
	}
	r.mu.Lock()
	for key := range r.sourceBaselines {
		if key.source == source {
			delete(r.sourceBaselines, key)
		}
	}
	r.mu.Unlock()
	r.recordSourceMetricEvent(SourceMetricEvent{Time: r.now().UTC(), SourceID: source,
		Kind: "baseline_cleared", Severity: "info", Summary: "Cleared source traffic baseline"})
	return nil
}

func (r *Registry) SourceTrafficBaselines(source string) []SourceTrafficBaseline {
	if r == nil {
		return []SourceTrafficBaseline{}
	}
	r.mu.Lock()
	out := make([]SourceTrafficBaseline, 0)
	for key, baseline := range r.sourceBaselines {
		if source == "" || key.source == source {
			out = append(out, baseline)
		}
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceID != out[j].SourceID {
			return out[i].SourceID < out[j].SourceID
		}
		if out[i].PGN != out[j].PGN {
			return out[i].PGN < out[j].PGN
		}
		return out[i].Identity < out[j].Identity
	})
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
