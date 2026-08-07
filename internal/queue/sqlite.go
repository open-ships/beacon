package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/store"
)

type sqliteQueue struct {
	db          *sql.DB
	connectorID string
	limits      model.BufferLimits
}

func NewSQLite(st *store.Store, connectorID string, limits model.BufferLimits) Queue {
	return &sqliteQueue{db: st.DB(), connectorID: connectorID, limits: limits.ApplyDefaults()}
}

func (q *sqliteQueue) Append(ctx context.Context, envs []*msg.Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := q.ensureAggregate(ctx, tx, false); err != nil {
		return err
	}
	var cursor int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE((SELECT last_seq FROM checkpoints WHERE connector_id = ?), 0)`,
		q.connectorID).Scan(&cursor); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO queue (connector_id, ts, envelope, bytes) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	var retainedBytes, pendingCount, pendingBytes, tail int64
	var oldestRetained, oldestPending int64
	var oldestRetainedSet, oldestPendingSet bool
	for _, e := range envs {
		doc, err := json.Marshal(e)
		if err != nil {
			return err
		}
		ts := e.Timestamp.UnixNano()
		res, err := stmt.ExecContext(ctx, q.connectorID, ts, string(doc), len(doc))
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		tail = id
		retainedBytes += int64(len(doc))
		if !oldestRetainedSet {
			oldestRetained = ts
			oldestRetainedSet = true
		}
		if id > cursor {
			pendingCount++
			pendingBytes += int64(len(doc))
			if !oldestPendingSet {
				oldestPending = ts
				oldestPendingSet = true
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO queue_aggregates (
		  connector_id, pending_count, pending_bytes, oldest_pending_ts,
		  retained_count, retained_bytes, oldest_retained_ts, tail_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(connector_id) DO UPDATE SET
		  pending_count = queue_aggregates.pending_count + excluded.pending_count,
		  pending_bytes = queue_aggregates.pending_bytes + excluded.pending_bytes,
		  oldest_pending_ts = CASE
		    WHEN excluded.oldest_pending_ts IS NULL THEN queue_aggregates.oldest_pending_ts
		    WHEN queue_aggregates.oldest_pending_ts IS NULL THEN excluded.oldest_pending_ts
		    ELSE queue_aggregates.oldest_pending_ts
		  END,
		  retained_count = queue_aggregates.retained_count + excluded.retained_count,
		  retained_bytes = queue_aggregates.retained_bytes + excluded.retained_bytes,
		  oldest_retained_ts = CASE
		    WHEN queue_aggregates.oldest_retained_ts IS NULL THEN excluded.oldest_retained_ts
		    ELSE queue_aggregates.oldest_retained_ts
		  END,
		  tail_id = MAX(queue_aggregates.tail_id, excluded.tail_id)`,
		q.connectorID, pendingCount, pendingBytes, nullableTimestamp(oldestPending, oldestPendingSet),
		len(envs), retainedBytes, oldestRetained, tail); err != nil {
		return err
	}
	return tx.Commit()
}

func (q *sqliteQueue) Read(ctx context.Context, after int64, limit int) ([]Entry, error) {
	rows, err := q.db.QueryContext(ctx,
		`SELECT id, envelope FROM queue WHERE connector_id = ? AND id > ? ORDER BY id LIMIT ?`,
		q.connectorID, after, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Entry
	for rows.Next() {
		var seq int64
		var doc string
		if err := rows.Scan(&seq, &doc); err != nil {
			return nil, err
		}
		var e msg.Envelope
		if err := json.Unmarshal([]byte(doc), &e); err != nil {
			return nil, err
		}
		e.Seq = seq
		e.ConnectorID = q.connectorID
		if e.ObservedAt.IsZero() {
			e.ObservedAt = e.Timestamp
		}
		if e.Decode.Status == "" {
			e.Decode.Status = "legacy"
		}
		out = append(out, Entry{Seq: seq, Env: &e})
	}
	return out, rows.Err()
}

func (q *sqliteQueue) Cursor(ctx context.Context) (int64, error) {
	var cur int64
	err := q.db.QueryRowContext(ctx,
		`SELECT COALESCE((SELECT last_seq FROM checkpoints WHERE connector_id = ?), 0)`,
		q.connectorID).Scan(&cur)
	return cur, err
}

func (q *sqliteQueue) Ack(ctx context.Context, upTo int64) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := q.ensureAggregate(ctx, tx, false); err != nil {
		return err
	}
	var cursor int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE((SELECT last_seq FROM checkpoints WHERE connector_id = ?), 0)`,
		q.connectorID).Scan(&cursor); err != nil {
		return err
	}
	if upTo <= cursor {
		return tx.Commit()
	}
	var movedCount, movedBytes int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(bytes), 0)
		FROM queue WHERE connector_id = ? AND id > ? AND id <= ?`,
		q.connectorID, cursor, upTo).Scan(&movedCount, &movedBytes); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO checkpoints (connector_id, last_seq) VALUES (?, ?)
		 ON CONFLICT(connector_id) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)`,
		q.connectorID, upTo); err != nil {
		return err
	}
	if err := q.ensureAggregate(ctx, tx, true); err != nil {
		return err
	}
	var oldestPending sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT ts FROM queue
		WHERE connector_id = ? AND id > ?
		ORDER BY id LIMIT 1`, q.connectorID, upTo).Scan(&oldestPending); err != nil && err != sql.ErrNoRows {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE queue_aggregates SET
		  pending_count = MAX(0, pending_count - ?),
		  pending_bytes = MAX(0, pending_bytes - ?),
		  oldest_pending_ts = ?
		WHERE connector_id = ?`,
		movedCount, movedBytes, nullInt64Value(oldestPending), q.connectorID); err != nil {
		return err
	}
	return tx.Commit()
}

func (q *sqliteQueue) Prune(ctx context.Context) (PruneResult, error) {
	var result PruneResult
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := q.ensureAggregate(ctx, tx, true); err != nil {
		return result, err
	}
	var cursor, retainedCount, retainedBytes, pendingCount, pendingBytes int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE((SELECT last_seq FROM checkpoints WHERE connector_id = ?1), 0),
		       retained_count, retained_bytes, pending_count, pending_bytes
		FROM queue_aggregates WHERE connector_id = ?1`, q.connectorID).Scan(
		&cursor, &retainedCount, &retainedBytes, &pendingCount, &pendingBytes); err != nil {
		return result, err
	}

	remove := func(byTimestamp bool, cutoff int64) error {
		params := []any{
			sql.Named("connector", q.connectorID),
			sql.Named("cursor", cursor),
			sql.Named("cutoff", cutoff),
		}
		var removedCount, removedBytes, removedPending, removedPendingBytes int64
		var row *sql.Row
		if byTimestamp {
			row = tx.QueryRowContext(ctx, `
				SELECT COUNT(*), COALESCE(SUM(bytes), 0),
				       COALESCE(SUM(CASE WHEN id > @cursor THEN 1 ELSE 0 END), 0),
				       COALESCE(SUM(CASE WHEN id > @cursor THEN bytes ELSE 0 END), 0)
				FROM queue WHERE connector_id = @connector AND ts < @cutoff`, params...)
		} else {
			row = tx.QueryRowContext(ctx, `
				SELECT COUNT(*), COALESCE(SUM(bytes), 0),
				       COALESCE(SUM(CASE WHEN id > @cursor THEN 1 ELSE 0 END), 0),
				       COALESCE(SUM(CASE WHEN id > @cursor THEN bytes ELSE 0 END), 0)
				FROM queue WHERE connector_id = @connector AND id <= @cutoff`, params...)
		}
		if err := row.Scan(&removedCount, &removedBytes, &removedPending, &removedPendingBytes); err != nil {
			return err
		}
		if removedCount == 0 {
			return nil
		}
		deleteParams := []any{
			sql.Named("connector", q.connectorID),
			sql.Named("cutoff", cutoff),
		}
		var err error
		if byTimestamp {
			_, err = tx.ExecContext(ctx,
				`DELETE FROM queue WHERE connector_id = @connector AND ts < @cutoff`, deleteParams...)
		} else {
			_, err = tx.ExecContext(ctx,
				`DELETE FROM queue WHERE connector_id = @connector AND id <= @cutoff`, deleteParams...)
		}
		if err != nil {
			return err
		}
		retainedCount = max(int64(0), retainedCount-removedCount)
		retainedBytes = max(int64(0), retainedBytes-removedBytes)
		pendingCount = max(int64(0), pendingCount-removedPending)
		pendingBytes = max(int64(0), pendingBytes-removedPendingBytes)
		result.Total += removedCount
		result.Pending += removedPending
		return nil
	}

	if n := q.limits.MaxMessages; n > 0 && retainedCount > n {
		var cutoff int64
		if err := tx.QueryRowContext(ctx,
			`SELECT id FROM queue WHERE connector_id = ? ORDER BY id DESC LIMIT 1 OFFSET ?`,
			q.connectorID, n).Scan(&cutoff); err != nil {
			return result, err
		}
		if err := remove(false, cutoff); err != nil {
			return result, err
		}
	}
	if d := time.Duration(q.limits.MaxAge); d > 0 && retainedCount > 0 {
		cutoff := time.Now().Add(-d).UnixNano()
		var stale bool
		if err := tx.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM queue WHERE connector_id = ? AND ts < ? LIMIT 1)`,
			q.connectorID, cutoff).Scan(&stale); err != nil {
			return result, err
		}
		if stale {
			if err := remove(true, cutoff); err != nil {
				return result, err
			}
		}
	}
	if b := q.limits.MaxBytes; b > 0 && retainedBytes > b {
		rows, err := tx.QueryContext(ctx,
			`SELECT id, bytes FROM queue WHERE connector_id = ? ORDER BY id`, q.connectorID)
		if err != nil {
			return result, err
		}
		need := retainedBytes - b
		var cutoff, reclaimed int64
		for rows.Next() && reclaimed < need {
			var id, size int64
			if err := rows.Scan(&id, &size); err != nil {
				_ = rows.Close()
				return result, err
			}
			cutoff = id
			reclaimed += size
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return result, err
		}
		if err := rows.Close(); err != nil {
			return result, err
		}
		if cutoff > 0 {
			if err := remove(false, cutoff); err != nil {
				return result, err
			}
		}
	}
	if result.Total > 0 {
		var oldestRetained, oldestPending sql.NullInt64
		var tail int64
		if err := tx.QueryRowContext(ctx,
			`SELECT ts FROM queue WHERE connector_id = ? ORDER BY id LIMIT 1`,
			q.connectorID).Scan(&oldestRetained); err != nil && err != sql.ErrNoRows {
			return result, err
		}
		if err := tx.QueryRowContext(ctx,
			`SELECT ts FROM queue WHERE connector_id = ? AND id > ? ORDER BY id LIMIT 1`,
			q.connectorID, cursor).Scan(&oldestPending); err != nil && err != sql.ErrNoRows {
			return result, err
		}
		if err := tx.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(id), 0) FROM queue WHERE connector_id = ?`,
			q.connectorID).Scan(&tail); err != nil {
			return result, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE queue_aggregates SET
			  pending_count = ?, pending_bytes = ?, oldest_pending_ts = ?,
			  retained_count = ?, retained_bytes = ?, oldest_retained_ts = ?, tail_id = ?
			WHERE connector_id = ?`,
			pendingCount, pendingBytes, nullInt64Value(oldestPending),
			retainedCount, retainedBytes, nullInt64Value(oldestRetained), tail, q.connectorID); err != nil {
			return result, err
		}
	}
	if err := tx.Commit(); err != nil {
		return PruneResult{}, err
	}
	return result, nil
}

func (q *sqliteQueue) Purge(ctx context.Context) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM queue WHERE connector_id = ?`, q.connectorID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE connector_id = ?`, q.connectorID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM queue_aggregates WHERE connector_id = ?`, q.connectorID); err != nil {
		return err
	}
	return tx.Commit()
}

func (q *sqliteQueue) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	var oldestPending, oldestRetained sql.NullInt64
	if err := q.ensureAggregate(ctx, q.db, false); err != nil {
		return s, err
	}
	err := q.db.QueryRowContext(ctx,
		`SELECT
		   COALESCE(a.pending_count, 0), COALESCE(a.pending_bytes, 0), a.oldest_pending_ts,
		   COALESCE(a.retained_count, 0), COALESCE(a.retained_bytes, 0), a.oldest_retained_ts,
		   COALESCE(c.last_seq, 0), COALESCE(a.tail_id, 0)
		 FROM (SELECT 1) seed
		 LEFT JOIN queue_aggregates a ON a.connector_id = ?1
		 LEFT JOIN checkpoints c ON c.connector_id = ?1`,
		q.connectorID).Scan(
		&s.Depth, &s.Bytes, &oldestPending,
		&s.RetainedDepth, &s.RetainedBytes, &oldestRetained,
		&s.Cursor, &s.Tail,
	)
	if oldestPending.Valid {
		s.Oldest = time.Unix(0, oldestPending.Int64)
	}
	if oldestRetained.Valid {
		s.OldestRetained = time.Unix(0, oldestRetained.Int64)
	}
	s.LimitMessages, s.LimitBytes = q.limits.MaxMessages, q.limits.MaxBytes
	if s.LimitMessages > 0 {
		s.HeadroomMessages = max(int64(0), s.LimitMessages-s.RetainedDepth)
	}
	if s.LimitBytes > 0 {
		s.HeadroomBytes = max(int64(0), s.LimitBytes-s.RetainedBytes)
	}
	return s, err
}

func nullableTimestamp(v int64, set bool) any {
	if !set {
		return nil
	}
	return v
}

func nullInt64Value(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

type aggregateExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// ensureAggregate rebuilds the statistical read model only when its row is
// absent. Normal traffic never scans retained history: append, acknowledgement,
// and prune maintain this row transactionally. The fallback makes migrations
// and interrupted/manual repairs self-healing without making the aggregate the
// canonical delivery store.
func (q *sqliteQueue) ensureAggregate(ctx context.Context, exec aggregateExecer, force bool) error {
	var exists bool
	if err := exec.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM queue_aggregates WHERE connector_id = ?)`,
		q.connectorID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err := exec.ExecContext(ctx, `
		INSERT INTO queue_aggregates (
		  connector_id, pending_count, pending_bytes, oldest_pending_ts,
		  retained_count, retained_bytes, oldest_retained_ts, tail_id
		)
		SELECT ?1,
		  COALESCE(SUM(CASE WHEN q.id > checkpoint.cursor THEN 1 ELSE 0 END), 0),
		  COALESCE(SUM(CASE WHEN q.id > checkpoint.cursor THEN q.bytes ELSE 0 END), 0),
		  (SELECT pending.ts FROM queue pending
		   WHERE pending.connector_id = ?1 AND pending.id > checkpoint.cursor
		   ORDER BY pending.id LIMIT 1),
		  COUNT(q.id), COALESCE(SUM(q.bytes), 0),
		  (SELECT retained.ts FROM queue retained
		   WHERE retained.connector_id = ?1 ORDER BY retained.id LIMIT 1),
		  COALESCE(MAX(q.id), 0)
		FROM (SELECT COALESCE((SELECT last_seq FROM checkpoints WHERE connector_id = ?1), 0) AS cursor) checkpoint
		LEFT JOIN queue q ON q.connector_id = ?1
		HAVING ?2 OR COUNT(q.id) > 0 OR EXISTS(SELECT 1 FROM checkpoints WHERE connector_id = ?1)
		ON CONFLICT(connector_id) DO NOTHING`, q.connectorID, force)
	return err
}
