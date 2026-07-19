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
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO queue (connector_id, ts, envelope, bytes) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, e := range envs {
		doc, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, q.connectorID, e.Timestamp.UnixNano(), string(doc), len(doc)); err != nil {
			return err
		}
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
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO checkpoints (connector_id, last_seq) VALUES (?, ?)
		 ON CONFLICT(connector_id) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)`,
		q.connectorID, upTo)
	return err
}

func (q *sqliteQueue) Prune(ctx context.Context) (PruneResult, error) {
	var result PruneResult
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	var pendingBefore int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue WHERE connector_id = ?1 AND id > COALESCE((SELECT last_seq FROM checkpoints WHERE connector_id = ?1), 0)`, q.connectorID).Scan(&pendingBefore); err != nil {
		return result, err
	}
	if n := q.limits.MaxMessages; n > 0 {
		res, err := tx.ExecContext(ctx,
			`DELETE FROM queue WHERE connector_id = ?1 AND id <= COALESCE(
			   (SELECT id FROM queue WHERE connector_id = ?1 ORDER BY id DESC LIMIT 1 OFFSET ?2), 0)`,
			q.connectorID, n)
		if err != nil {
			return result, err
		}
		c, _ := res.RowsAffected()
		result.Total += c
	}
	if d := time.Duration(q.limits.MaxAge); d > 0 {
		res, err := tx.ExecContext(ctx,
			`DELETE FROM queue WHERE connector_id = ? AND ts < ?`,
			q.connectorID, time.Now().Add(-d).UnixNano())
		if err != nil {
			return result, err
		}
		c, _ := res.RowsAffected()
		result.Total += c
	}
	if b := q.limits.MaxBytes; b > 0 {
		res, err := tx.ExecContext(ctx,
			`DELETE FROM queue WHERE connector_id = ?1 AND id <= COALESCE(
			   (SELECT id FROM (
			      SELECT id, SUM(bytes) OVER (ORDER BY id DESC) AS running
			      FROM queue WHERE connector_id = ?1
			    ) WHERE running > ?2 ORDER BY id DESC LIMIT 1), 0)`,
			q.connectorID, b)
		if err != nil {
			return result, err
		}
		c, _ := res.RowsAffected()
		result.Total += c
	}
	var pendingAfter int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue WHERE connector_id = ?1 AND id > COALESCE((SELECT last_seq FROM checkpoints WHERE connector_id = ?1), 0)`, q.connectorID).Scan(&pendingAfter); err != nil {
		return result, err
	}
	result.Pending = max(int64(0), pendingBefore-pendingAfter)
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
	return tx.Commit()
}

func (q *sqliteQueue) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	var oldestPending, oldestRetained sql.NullInt64
	err := q.db.QueryRowContext(ctx,
		`WITH checkpoint AS (
		   SELECT COALESCE((SELECT last_seq FROM checkpoints WHERE connector_id = ?1), 0) AS cursor
		 )
		 SELECT
		   COALESCE(SUM(CASE WHEN q.id > checkpoint.cursor THEN 1 ELSE 0 END), 0),
		   COALESCE(SUM(CASE WHEN q.id > checkpoint.cursor THEN q.bytes ELSE 0 END), 0),
		   MIN(CASE WHEN q.id > checkpoint.cursor THEN q.ts END),
		   COUNT(q.id), COALESCE(SUM(q.bytes), 0), MIN(q.ts),
		   checkpoint.cursor, COALESCE(MAX(q.id), 0)
		 FROM checkpoint LEFT JOIN queue q ON q.connector_id = ?1`,
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
