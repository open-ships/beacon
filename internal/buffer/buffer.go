// Package buffer implements a SQLite-backed ring buffer for N2K messages.
package buffer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
	"github.com/open-ships/beacon/internal/admin"
	"github.com/open-ships/beacon/internal/n2k"
)

const schema = `
CREATE TABLE IF NOT EXISTS messages (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    pgn       INTEGER NOT NULL,
    source    INTEGER NOT NULL,
    dest      INTEGER NOT NULL,
    priority  INTEGER NOT NULL,
    ts        INTEGER NOT NULL,
    payload   TEXT    NOT NULL,
    raw       BLOB
);

CREATE TABLE IF NOT EXISTS checkpoint (
    sink_name TEXT PRIMARY KEY,
    last_id   INTEGER NOT NULL DEFAULT 0
);
`

// Buffer is the interface for the SQLite ring buffer.
type Buffer interface {
	Insert(ctx context.Context, msg *n2k.ParsedMessage) error
	Read(ctx context.Context, afterID int64, limit int) ([]*n2k.ParsedMessage, error)
	UpdateCheckpoint(ctx context.Context, sinkName string, lastID int64) error
	GetCheckpoint(ctx context.Context, sinkName string) (int64, error)
	MaxID(ctx context.Context) (int64, error)
	HealthCheck(ctx context.Context) error
	Close() error
}

// SQLiteBuffer implements Buffer using SQLite.
type SQLiteBuffer struct {
	db      *sql.DB
	maxRows int
	metrics *admin.Metrics
}

// Open opens (or creates) the SQLite database at path with WAL mode enabled.
func Open(path string, maxRows int, metrics ...*admin.Metrics) (*SQLiteBuffer, error) {
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_synchronous=NORMAL&cache=shared")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(1) // sqlite3 does not support concurrent writers

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	var m *admin.Metrics
	if len(metrics) > 0 {
		m = metrics[0]
	}

	return &SQLiteBuffer{db: db, maxRows: maxRows, metrics: m}, nil
}

// Insert writes a message to the buffer and prunes old rows if necessary.
func (b *SQLiteBuffer) Insert(ctx context.Context, msg *n2k.ParsedMessage) error {
	payload := msg.Payload
	if payload == nil {
		payload = json.RawMessage("null")
	}

	_, err := b.db.ExecContext(ctx,
		`INSERT INTO messages (pgn, source, dest, priority, ts, payload, raw) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msg.PGN, msg.Source, msg.Dest, msg.Priority,
		msg.Timestamp.UnixNano(), string(payload), msg.Raw,
	)
	if err != nil {
		return fmt.Errorf("insert message: %w", err)
	}

	if b.metrics != nil {
		b.metrics.BufferInsertsTotal.Add(ctx, 1)
	}

	// Ring buffer pruning: if over limit, delete oldest 10%
	if err := b.prune(ctx); err != nil {
		return fmt.Errorf("prune: %w", err)
	}

	// Update buffer size gauge
	if b.metrics != nil {
		if count, err := b.RowCount(ctx); err == nil {
			b.metrics.BufferSizeRows.Record(ctx, count)
		}
	}

	return nil
}

func (b *SQLiteBuffer) prune(ctx context.Context) error {
	var count int
	if err := b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&count); err != nil {
		return err
	}
	if count <= b.maxRows {
		return nil
	}

	deleteCount := b.maxRows / 10
	if deleteCount < 1 {
		deleteCount = 1
	}

	result, err := b.db.ExecContext(ctx,
		`DELETE FROM messages WHERE id IN (SELECT id FROM messages ORDER BY id ASC LIMIT ?)`,
		deleteCount,
	)
	if err != nil {
		return err
	}

	if b.metrics != nil {
		if affected, err := result.RowsAffected(); err == nil {
			b.metrics.BufferPrunedTotal.Add(ctx, affected)
		}
	}

	return nil
}

// Read returns up to limit messages with id > afterID, in ascending order.
func (b *SQLiteBuffer) Read(ctx context.Context, afterID int64, limit int) ([]*n2k.ParsedMessage, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT id, pgn, source, dest, priority, ts, payload, raw FROM messages WHERE id > ? ORDER BY id ASC LIMIT ?`,
		afterID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var msgs []*n2k.ParsedMessage
	for rows.Next() {
		var (
			id, tsNano                 int64
			pgn                        uint32
			source, dest, priority     uint8
			payloadStr                 string
			raw                        []byte
		)
		if err := rows.Scan(&id, &pgn, &source, &dest, &priority, &tsNano, &payloadStr, &raw); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		msg := &n2k.ParsedMessage{
			ID:        id,
			PGN:       pgn,
			Source:    source,
			Dest:      dest,
			Priority:  priority,
			Timestamp: time.Unix(0, tsNano),
			Payload:   json.RawMessage(payloadStr),
			Raw:       raw,
		}
		msgs = append(msgs, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return msgs, nil
}

// UpdateCheckpoint updates or inserts the checkpoint for the given sink.
func (b *SQLiteBuffer) UpdateCheckpoint(ctx context.Context, sinkName string, lastID int64) error {
	_, err := b.db.ExecContext(ctx,
		`INSERT INTO checkpoint (sink_name, last_id) VALUES (?, ?) ON CONFLICT(sink_name) DO UPDATE SET last_id = excluded.last_id`,
		sinkName, lastID,
	)
	return err
}

// GetCheckpoint returns the last checkpointed ID for the given sink, or 0 if none.
func (b *SQLiteBuffer) GetCheckpoint(ctx context.Context, sinkName string) (int64, error) {
	var id int64
	err := b.db.QueryRowContext(ctx,
		`SELECT last_id FROM checkpoint WHERE sink_name = ?`, sinkName,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return id, nil
}

// MaxID returns the maximum message ID in the buffer, or 0 if empty.
func (b *SQLiteBuffer) MaxID(ctx context.Context) (int64, error) {
	var id sql.NullInt64
	err := b.db.QueryRowContext(ctx, `SELECT MAX(id) FROM messages`).Scan(&id)
	if err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

// HealthCheck pings the SQLite database.
func (b *SQLiteBuffer) HealthCheck(ctx context.Context) error {
	return b.db.PingContext(ctx)
}

// Close closes the underlying database connection.
func (b *SQLiteBuffer) Close() error {
	return b.db.Close()
}

// RowCount returns the current number of rows in the messages table.
func (b *SQLiteBuffer) RowCount(ctx context.Context) (int64, error) {
	var count int64
	err := b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&count)
	return count, err
}
