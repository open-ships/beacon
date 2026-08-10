package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/retry"
)

const (
	postgresMaxPGN        = 262143
	postgresMaxBatchBytes = 4 << 20
)

// postgresDB is the small pgxpool surface the sink needs. Keeping this seam
// narrow makes delivery and schema behavior testable without a live server.
type postgresDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Close()
}

type postgresSink struct {
	id          string
	db          postgresDB
	table       string
	batchSize   int
	timeout     time.Duration
	autoCreate  bool
	timescaleDB bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	stop        sync.Once

	// initializeSlot serializes schema verification/creation without making a
	// canceled delivery wait behind the background recovery attempt. A mutex
	// cannot be acquired with a context and previously let Connector.Stop block
	// for the full configured database timeout before the sink itself was
	// canceled later in Supervisor shutdown ordering.
	initializeSlot chan struct{}
	stateMu        sync.Mutex
	ready          bool
	lastErr        error
}

func newPostgresSink(ctx context.Context, cfg model.Sink) (Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("sink %q: parse PostgreSQL URL: %w", cfg.ID, err)
	}
	// A sink may be shared by several connector routes, but delivery is batch
	// oriented and should not consume an appliance-sized default pool.
	poolConfig.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("sink %q: open PostgreSQL pool: %w", cfg.ID, err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	s := &postgresSink{
		id: cfg.ID, db: pool, table: cfg.EffectivePostgresTable(),
		batchSize: cfg.EffectivePostgresBatchSize(), timeout: cfg.EffectivePostgresWriteTimeout(),
		autoCreate: cfg.AutoCreateTable, timescaleDB: cfg.TimescaleDB, cancel: cancel,
		initializeSlot: make(chan struct{}, 1),
		lastErr:        errors.New("PostgreSQL connection has not been verified"),
	}
	s.wg.Add(1)
	go s.initializeLoop(runCtx)
	return s, nil
}

func (s *postgresSink) ID() string                   { return s.id }
func (s *postgresSink) BatchSize() int               { return s.batchSize }
func (s *postgresSink) BatchMaxBytes() int64         { return postgresMaxBatchBytes }
func (s *postgresSink) DeliveryClass() DeliveryClass { return DeliveryConfirmed }

func (s *postgresSink) State() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.lastErr != nil {
		return "degraded", s.lastErr
	}
	return "up", nil
}

func (s *postgresSink) setState(ready bool, err error) {
	s.stateMu.Lock()
	s.ready, s.lastErr = ready, err
	s.stateMu.Unlock()
}

func (s *postgresSink) isReady() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.ready
}

// initializeLoop lets an enabled sink recover when PostgreSQL is unavailable
// at apply time. Manual-table mode also keeps checking until the operator has
// run the DDL shown in the UI.
func (s *postgresSink) initializeLoop(ctx context.Context) {
	defer s.wg.Done()
	backoff := retry.NewBackoff(time.Second, time.Minute)
	for {
		if err := s.ensureReady(ctx); err == nil {
			return
		}
		if !retry.Sleep(ctx, backoff.Next()) {
			return
		}
	}
}

func (s *postgresSink) ensureReady(ctx context.Context) error {
	if s.isReady() {
		return nil
	}
	select {
	case s.initializeSlot <- struct{}{}:
		defer func() { <-s.initializeSlot }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.isReady() {
		return nil
	}

	operationCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	statement := postgresVerifySQL(s.table)
	if s.autoCreate {
		statement = PostgresDDL(s.table, s.timescaleDB)
	}
	if _, err := s.db.Exec(operationCtx, statement); err != nil {
		wrapped := fmt.Errorf("initialize PostgreSQL table %s: %w", s.table, err)
		s.setState(false, wrapped)
		return wrapped
	}
	s.setState(true, nil)
	return nil
}

func (s *postgresSink) PushBatch(ctx context.Context, entries []queue.Entry) error {
	if len(entries) == 0 {
		return ctx.Err()
	}
	if err := s.ensureReady(ctx); err != nil {
		return err
	}
	statement, args, err := postgresInsert(s.table, entries)
	if err != nil {
		s.setState(false, err)
		return err
	}
	operationCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if _, err := s.db.Exec(operationCtx, statement, args...); err != nil {
		wrapped := fmt.Errorf("write PostgreSQL batch: %w", err)
		// Re-run schema verification (or creation) on the next retry. This
		// recovers from both connection replacement and a table dropped while
		// Beacon is running.
		s.setState(false, wrapped)
		return wrapped
	}
	s.setState(true, nil)
	return nil
}

func (s *postgresSink) Stop() {
	s.stop.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.db.Close()
	})
}

// PostgresDDL returns the exact copy/paste schema shown to operators and run
// by auto-create mode. TimescaleDB must already be installed in the target
// database; when enabled, the final statement idempotently converts the table
// into a hypertable using observed_at as its time dimension.
func PostgresDDL(table string, timescaleDB bool) string {
	qualified := postgresIdentifier(table)
	base := strings.Split(table, ".")
	baseName := base[len(base)-1]
	pgnIndex := pgx.Identifier{postgresIndexName(baseName, "_pgn_observed_at_idx")}.Sanitize()
	connectorIndex := pgx.Identifier{postgresIndexName(baseName, "_connector_sequence_idx")}.Sanitize()

	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  observed_at timestamptz NOT NULL,
  message_time timestamptz NOT NULL,
  connector_id text NOT NULL,
  sequence bigint NOT NULL,
  pgn integer NOT NULL CHECK (pgn BETWEEN 0 AND 262143),
  source smallint NOT NULL CHECK (source BETWEEN 0 AND 255),
  destination smallint NOT NULL CHECK (destination BETWEEN 0 AND 255),
  priority smallint NOT NULL CHECK (priority BETWEEN 0 AND 7),
  device_name numeric(20, 0) CHECK (device_name BETWEEN 0 AND 18446744073709551615),
  ingress text,
  payload jsonb NOT NULL,
  envelope jsonb NOT NULL,
  PRIMARY KEY (observed_at, connector_id, sequence)
);

CREATE INDEX IF NOT EXISTS %s ON %s (pgn, observed_at DESC);
CREATE INDEX IF NOT EXISTS %s ON %s (connector_id, sequence);`,
		qualified, pgnIndex, qualified, connectorIndex, qualified)
	if timescaleDB {
		ddl += fmt.Sprintf(`

-- Requires the TimescaleDB extension to be installed in this database.
SELECT create_hypertable(%s::regclass, by_range('observed_at'), if_not_exists => TRUE, migrate_data => TRUE);`,
			quotePostgresLiteral(qualified))
	}
	return ddl
}

func postgresVerifySQL(table string) string {
	return fmt.Sprintf(`SELECT observed_at, message_time, connector_id, sequence, pgn, source,
       destination, priority, device_name, ingress, payload, envelope
FROM %s LIMIT 0`, postgresIdentifier(table))
}

func postgresIdentifier(table string) string {
	return pgx.Identifier(strings.Split(table, ".")).Sanitize()
}

func postgresIndexName(base, suffix string) string {
	if limit := 63 - len(suffix); len(base) > limit {
		base = base[:limit]
	}
	return base + suffix
}

func quotePostgresLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func postgresInsert(table string, entries []queue.Entry) (string, []any, error) {
	const columnsPerRow = 12
	var statement strings.Builder
	fmt.Fprintf(&statement, `INSERT INTO %s (
  observed_at, message_time, connector_id, sequence, pgn, source,
  destination, priority, device_name, ingress, payload, envelope
) VALUES `, postgresIdentifier(table))
	args := make([]any, 0, len(entries)*columnsPerRow)
	for i, entry := range entries {
		if entry.Env == nil {
			return "", nil, fmt.Errorf("encode PostgreSQL batch entry %d: nil envelope", i)
		}
		e := entry.Env
		if e.PGN > postgresMaxPGN {
			return "", nil, fmt.Errorf("encode PostgreSQL batch entry %d: PGN %d exceeds NMEA 2000 maximum %d", i, e.PGN, postgresMaxPGN)
		}
		observedAt := e.ObservedAt
		if observedAt.IsZero() {
			observedAt = e.Timestamp
		}
		if observedAt.IsZero() {
			return "", nil, fmt.Errorf("encode PostgreSQL batch entry %d: observed_at and timestamp are both zero", i)
		}
		messageTime := e.Timestamp
		if messageTime.IsZero() {
			messageTime = observedAt
		}
		payload := "null"
		if len(e.Payload) > 0 {
			if !json.Valid(e.Payload) {
				return "", nil, fmt.Errorf("encode PostgreSQL batch entry %d: payload is not valid JSON", i)
			}
			payload = string(e.Payload)
		}
		envelope, err := e.WireBytes()
		if err != nil {
			return "", nil, fmt.Errorf("encode PostgreSQL batch entry %d: %w", i, err)
		}
		var deviceName any
		if e.DeviceName != nil {
			deviceName = fmt.Sprintf("%d", *e.DeviceName)
		}
		if i > 0 {
			statement.WriteString(", ")
		}
		first := len(args) + 1
		statement.WriteByte('(')
		for column := range columnsPerRow {
			if column > 0 {
				statement.WriteString(", ")
			}
			fmt.Fprintf(&statement, "$%d", first+column)
			// device_name is an unsigned 64-bit ISO NAME. PostgreSQL has no
			// uint64 scalar and pgx's numeric codec does not accept a Go
			// string directly, so bind lossless decimal text and cast it.
			if column == 8 {
				statement.WriteString("::text::numeric")
			}
		}
		statement.WriteByte(')')
		args = append(args,
			observedAt.UTC(), messageTime.UTC(), e.ConnectorID, entry.Seq,
			int64(e.PGN), int16(e.Source), int16(e.Dest), int16(e.Priority),
			deviceName, e.Ingress, payload, string(envelope),
		)
	}
	statement.WriteString(" ON CONFLICT (observed_at, connector_id, sequence) DO NOTHING")
	return statement.String(), args, nil
}
