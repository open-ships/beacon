// Package store owns the SQLite database: schema migrations, config
// entity CRUD, and the connection shared with the queue implementation.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "modernc.org/sqlite"

	"github.com/open-ships/beacon/internal/model"
)

type Store struct{ db *sql.DB }

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
	 CREATE TABLE IF NOT EXISTS sinks (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
	 CREATE TABLE IF NOT EXISTS connectors (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
	 CREATE TABLE IF NOT EXISTS queue (
	   id INTEGER PRIMARY KEY AUTOINCREMENT,
	   connector_id TEXT NOT NULL,
	   ts INTEGER NOT NULL,
	   envelope TEXT NOT NULL,
	   bytes INTEGER NOT NULL
	 );
	 CREATE INDEX IF NOT EXISTS queue_connector ON queue(connector_id, id);
	 CREATE INDEX IF NOT EXISTS queue_connector_ts ON queue(connector_id, ts);
	 CREATE TABLE IF NOT EXISTS checkpoints (
	   connector_id TEXT PRIMARY KEY,
	   last_seq INTEGER NOT NULL DEFAULT 0
	 );`,
	`CREATE TABLE IF NOT EXISTS appliance_identity (
	   id INTEGER PRIMARY KEY CHECK (id = 1),
	   identity_number INTEGER NOT NULL,
	   created_at INTEGER NOT NULL
	 );
	 CREATE TABLE IF NOT EXISTS device_inventory (
	   endpoint TEXT NOT NULL,
	   device_name TEXT NOT NULL,
	   first_seen INTEGER NOT NULL,
	   last_seen INTEGER NOT NULL,
	   expected INTEGER NOT NULL DEFAULT 0,
	   label TEXT NOT NULL DEFAULT '',
	   baseline_doc TEXT NOT NULL DEFAULT '',
	   current_doc TEXT NOT NULL,
	   PRIMARY KEY(endpoint, device_name)
	 );`,
}

func Open(path string) (*Store, error) {
	// _pragma handling is modernc.org/sqlite-specific.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// modernc.org/sqlite serializes writes; a single conn avoids SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)`); err != nil {
		return err
	}
	var current int
	_ = s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current)
	for i := current; i < len(migrations); i++ {
		if _, err := s.db.Exec(migrations[i]); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := s.db.Exec(`INSERT INTO schema_migrations(version) VALUES (?)`, i+1); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DB() *sql.DB  { return s.db }
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT (SELECT COUNT(*) FROM sources) + (SELECT COUNT(*) FROM sinks) + (SELECT COUNT(*) FROM connectors)`).Scan(&n)
	return n == 0, err
}

func put[T any](ctx context.Context, db *sql.DB, table, id string, v T) error {
	doc, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx,
		`INSERT INTO `+table+` (id, doc) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET doc = excluded.doc`, // #nosec G202 -- table is selected only by the typed store methods below.
		id, string(doc))
	return err
}

// list runs a SELECT doc FROM table and unmarshals each row's JSON doc into
// a T. It always returns a non-nil slice — empty rather than nil when the
// table has no rows — so callers built on it (LoadConfig, in turn
// config.Service's ListX/Export) marshal to JSON "[]" rather than "null".
func list[T any](ctx context.Context, db *sql.DB, table string) ([]T, error) {
	rows, err := db.QueryContext(ctx, `SELECT doc FROM `+table+` ORDER BY id`) // #nosec G202 -- table is selected only by the typed store methods below.
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]T, 0)
	for rows.Next() {
		var doc string
		if err := rows.Scan(&doc); err != nil {
			return nil, err
		}
		var v T
		if err := json.Unmarshal([]byte(doc), &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) PutSource(ctx context.Context, v model.Source) error {
	return put(ctx, s.db, "sources", v.ID, v)
}
func (s *Store) PutSink(ctx context.Context, v model.Sink) error {
	return put(ctx, s.db, "sinks", v.ID, v)
}
func (s *Store) PutConnector(ctx context.Context, v model.Connector) error {
	return put(ctx, s.db, "connectors", v.ID, v)
}

func (s *Store) DeleteSource(ctx context.Context, id string) error { return s.del(ctx, "sources", id) }
func (s *Store) DeleteSink(ctx context.Context, id string) error   { return s.del(ctx, "sinks", id) }
func (s *Store) DeleteConnector(ctx context.Context, id string) error {
	return s.del(ctx, "connectors", id)
}

func (s *Store) del(ctx context.Context, table, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM `+table+` WHERE id = ?`, id) // #nosec G202 -- table is selected only by the typed store methods above.
	return err
}

func (s *Store) LoadConfig(ctx context.Context) (model.Config, error) {
	var cfg model.Config
	var err error
	if cfg.Sources, err = list[model.Source](ctx, s.db, "sources"); err != nil {
		return cfg, err
	}
	if cfg.Sinks, err = list[model.Sink](ctx, s.db, "sinks"); err != nil {
		return cfg, err
	}
	if cfg.Connectors, err = list[model.Connector](ctx, s.db, "connectors"); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// KnownConnectorIDs returns every connector id that has queue or checkpoint
// rows — used to purge storage of connectors deleted from config.
func (s *Store) KnownConnectorIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT connector_id FROM queue UNION SELECT connector_id FROM checkpoints`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ReplaceConfig transactionally replaces the whole configuration.
func (s *Store) ReplaceConfig(ctx context.Context, cfg model.Config) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, table := range []string{"sources", "sinks", "connectors"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table); err != nil { // #nosec G202 -- table comes from the fixed literal slice above.
			return err
		}
	}
	ins := func(table, id string, v any) error {
		doc, err := json.Marshal(v)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO `+table+` (id, doc) VALUES (?, ?)`, id, string(doc)) // #nosec G202 -- table is supplied only by fixed literals below.
		return err
	}
	for _, v := range cfg.Sources {
		if err := ins("sources", v.ID, v); err != nil {
			return err
		}
	}
	for _, v := range cfg.Sinks {
		if err := ins("sinks", v.ID, v); err != nil {
			return err
		}
	}
	for _, v := range cfg.Connectors {
		if err := ins("connectors", v.ID, v); err != nil {
			return err
		}
	}
	return tx.Commit()
}
