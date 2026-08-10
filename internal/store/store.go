// Package store owns the SQLite database: schema migrations, config
// entity CRUD, and the connection shared with the queue implementation.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"

	_ "modernc.org/sqlite"

	"github.com/open-ships/beacon/internal/model"
)

type Store struct {
	db   *sql.DB
	path string
}

const (
	maxWALBytes int64 = 16 << 20

	// Migrations run before the persisted appliance budget is reconciled. Give
	// a legacy database bounded room to add schema/index pages even when its
	// previous max_page_count was pinned at the current high-water mark. This is
	// capacity only; SQLite does not preallocate it.
	migrationHeadroomBytes int64 = model.DefaultDatabaseReserve
)

type StorageStats struct {
	PageSize       int64 `json:"page_size"`
	PageCount      int64 `json:"page_count"`
	FreePages      int64 `json:"free_pages"`
	MaxPages       int64 `json:"max_pages"`
	PhysicalBytes  int64 `json:"physical_bytes"`
	AllocatedBytes int64 `json:"allocated_bytes"`
	FreeBytes      int64 `json:"free_bytes"`
	MaxBytes       int64 `json:"max_bytes"`
}

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
	`CREATE TABLE IF NOT EXISTS source_metric_baselines (
	   source_id TEXT NOT NULL,
	   identity TEXT NOT NULL,
	   pgn INTEGER NOT NULL,
	   approved_at INTEGER NOT NULL,
	   doc TEXT NOT NULL,
	   PRIMARY KEY(source_id, identity, pgn)
	 );
	 CREATE INDEX IF NOT EXISTS source_metric_baselines_source ON source_metric_baselines(source_id);
	 CREATE TABLE IF NOT EXISTS source_metric_events (
	   id INTEGER PRIMARY KEY AUTOINCREMENT,
	   ts INTEGER NOT NULL,
	   source_id TEXT NOT NULL,
	   pgn INTEGER NOT NULL,
	   source_address INTEGER NOT NULL,
	   kind TEXT NOT NULL,
	   severity TEXT NOT NULL,
	   doc TEXT NOT NULL
	 );
	 CREATE INDEX IF NOT EXISTS source_metric_events_source_ts ON source_metric_events(source_id, ts DESC);`,
	`DROP TABLE IF EXISTS source_metric_baselines;`,
	`CREATE TABLE IF NOT EXISTS queue_aggregates (
	   connector_id TEXT PRIMARY KEY,
	   pending_count INTEGER NOT NULL DEFAULT 0,
	   pending_bytes INTEGER NOT NULL DEFAULT 0,
	   oldest_pending_ts INTEGER,
	   retained_count INTEGER NOT NULL DEFAULT 0,
	   retained_bytes INTEGER NOT NULL DEFAULT 0,
	   oldest_retained_ts INTEGER,
	   tail_id INTEGER NOT NULL DEFAULT 0
	 );
	 INSERT INTO queue_aggregates (
	   connector_id, pending_count, pending_bytes, oldest_pending_ts,
	   retained_count, retained_bytes, oldest_retained_ts, tail_id
	 )
	 SELECT ids.connector_id,
	   COALESCE(SUM(CASE WHEN q.id > COALESCE(c.last_seq, 0) THEN 1 ELSE 0 END), 0),
	   COALESCE(SUM(CASE WHEN q.id > COALESCE(c.last_seq, 0) THEN q.bytes ELSE 0 END), 0),
	   (SELECT pending.ts FROM queue pending
	    WHERE pending.connector_id = ids.connector_id AND pending.id > COALESCE(c.last_seq, 0)
	    ORDER BY pending.id LIMIT 1),
	   COUNT(q.id), COALESCE(SUM(q.bytes), 0),
	   (SELECT retained.ts FROM queue retained
	    WHERE retained.connector_id = ids.connector_id ORDER BY retained.id LIMIT 1),
	   COALESCE(MAX(q.id), 0)
	 FROM (
	   SELECT connector_id FROM queue
	   UNION
	   SELECT connector_id FROM checkpoints
	 ) AS ids
	 LEFT JOIN checkpoints c ON c.connector_id = ids.connector_id
	 LEFT JOIN queue q ON q.connector_id = ids.connector_id
		 GROUP BY ids.connector_id
		 ON CONFLICT(connector_id) DO NOTHING;`,
	`CREATE TABLE IF NOT EXISTS config_settings (
		   key TEXT PRIMARY KEY,
		   doc TEXT NOT NULL
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
	s := &Store{db: db, path: path}
	// New databases can reclaim deleted pages incrementally. Existing databases
	// created with auto_vacuum=NONE require one explicit offline Compact before
	// incremental maintenance can return pages to the filesystem.
	if _, err := db.Exec(`PRAGMA auto_vacuum=INCREMENTAL`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite auto_vacuum: %w", err)
	}
	// A high-water legacy database may already equal max_page_count. Raise that
	// ceiling by a bounded amount before migrations; the Supervisor applies the
	// persisted desired limit only after the upgraded config can be loaded.
	if err := s.configureMigrationResources(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite migration budget: %w", err)
	}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if path != "" && path != ":memory:" {
		if err := os.Chmod(path, 0o600); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("restrict sqlite permissions: %w", err)
		}
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

func (s *Store) configureMigrationResources(ctx context.Context) error {
	var pageSize, pageCount int64
	if err := s.db.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return fmt.Errorf("read sqlite page size: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount); err != nil {
		return fmt.Errorf("read sqlite page count: %w", err)
	}
	budget, err := migrationBudgetBytes(pageSize, pageCount)
	if err != nil {
		return err
	}
	return s.ConfigureResources(ctx, model.ResourceConfig{MaxDatabaseBytes: budget})
}

func migrationBudgetBytes(pageSize, pageCount int64) (int64, error) {
	if pageSize <= 0 {
		return 0, fmt.Errorf("sqlite reported invalid page size %d", pageSize)
	}
	if pageCount < 0 || (pageCount > 0 && pageSize > math.MaxInt64/pageCount) {
		return 0, fmt.Errorf("sqlite page allocation overflows int64: %d pages of %d bytes", pageCount, pageSize)
	}
	physicalBytes := pageSize * pageCount
	if physicalBytes > math.MaxInt64-migrationHeadroomBytes {
		return 0, fmt.Errorf("sqlite migration budget overflows int64 at %d bytes", physicalBytes)
	}
	return max(model.DefaultMaxDatabaseBytes, physicalBytes+migrationHeadroomBytes), nil
}

// ConfigureResources applies the hard SQLite main-database page ceiling and a
// small WAL retention ceiling. SQLite may transiently exceed the WAL target
// during an active transaction, but the main database cannot grow beyond
// MaxDatabaseBytes. Lowering the limit below an existing file requires Compact.
func (s *Store) ConfigureResources(ctx context.Context, resources model.ResourceConfig) error {
	resources = resources.ApplyDefaults()
	// Keep the preflight and every PRAGMA on the same pooled connection. The
	// store has one connection, so no queue/config writer can grow the database
	// between checking page_count and applying max_page_count.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	var pageSize int64
	if err := conn.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return err
	}
	if pageSize <= 0 {
		return fmt.Errorf("sqlite reported invalid page size %d", pageSize)
	}
	maxPages := resources.MaxDatabaseBytes / pageSize
	if maxPages < 1 {
		return fmt.Errorf("database budget %d is smaller than one SQLite page", resources.MaxDatabaseBytes)
	}
	var pageCount int64
	if err := conn.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount); err != nil {
		return err
	}
	if pageCount > maxPages {
		return fmt.Errorf("database already occupies %d bytes, above configured maximum %d; compact it offline before lowering the limit", pageCount*pageSize, resources.MaxDatabaseBytes)
	}
	var appliedPages int64
	if err := conn.QueryRowContext(ctx, fmt.Sprintf(`PRAGMA max_page_count=%d`, maxPages)).Scan(&appliedPages); err != nil { // #nosec G201 -- maxPages is an integer derived from validated config.
		return err
	}
	if appliedPages > maxPages {
		return fmt.Errorf("database already occupies %d bytes, above configured maximum %d; compact it offline before lowering the limit", appliedPages*pageSize, resources.MaxDatabaseBytes)
	}
	journalLimit := min(maxWALBytes, resources.MaxDatabaseBytes/16)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`PRAGMA journal_size_limit=%d`, journalLimit)); err != nil { // #nosec G201 -- journalLimit is a bounded integer.
		return err
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA wal_autocheckpoint=1000`); err != nil {
		return err
	}
	return nil
}

func (s *Store) StorageStats(ctx context.Context) (StorageStats, error) {
	var out StorageStats
	for query, dst := range map[string]*int64{
		`PRAGMA page_size`:      &out.PageSize,
		`PRAGMA page_count`:     &out.PageCount,
		`PRAGMA freelist_count`: &out.FreePages,
		`PRAGMA max_page_count`: &out.MaxPages,
	} {
		if err := s.db.QueryRowContext(ctx, query).Scan(dst); err != nil {
			return out, err
		}
	}
	out.PhysicalBytes = out.PageCount * out.PageSize
	out.FreeBytes = out.FreePages * out.PageSize
	out.AllocatedBytes = out.PhysicalBytes - out.FreeBytes
	out.MaxBytes = out.MaxPages * out.PageSize
	return out, nil
}

// Maintain performs bounded online maintenance. It never runs a full VACUUM;
// that operation can require another database-sized allocation and belongs to
// the explicit offline Compact command.
func (s *Store) Maintain(ctx context.Context) error {
	var busy, logPages, checkpointed int64
	if err := s.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logPages, &checkpointed); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA incremental_vacuum(1024)`); err != nil {
		return err
	}
	return nil
}

// Compact reclaims the database high-water mark and enables incremental page
// reclamation for legacy databases. Call only while Beacon is offline and with
// enough free space for SQLite's VACUUM operation.
func (s *Store) Compact(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `PRAGMA auto_vacuum=INCREMENTAL`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `VACUUM`); err != nil {
		return err
	}
	return nil
}

func (s *Store) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT (SELECT COUNT(*) FROM sources) + (SELECT COUNT(*) FROM sinks) +
		        (SELECT COUNT(*) FROM connectors) + (SELECT COUNT(*) FROM config_settings)`).Scan(&n)
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

func get[T any](ctx context.Context, db *sql.DB, table, id string) (T, error) {
	var out T
	var doc string
	if err := db.QueryRowContext(ctx,
		`SELECT doc FROM `+table+` WHERE id = ?`, id).Scan(&doc); err != nil { // #nosec G202 -- table is selected only by typed methods below.
		return out, err
	}
	if err := json.Unmarshal([]byte(doc), &out); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) ListSources(ctx context.Context) ([]model.Source, error) {
	return list[model.Source](ctx, s.db, "sources")
}

func (s *Store) ListSinks(ctx context.Context) ([]model.Sink, error) {
	return list[model.Sink](ctx, s.db, "sinks")
}

func (s *Store) ListConnectors(ctx context.Context) ([]model.Connector, error) {
	return list[model.Connector](ctx, s.db, "connectors")
}

func (s *Store) GetSource(ctx context.Context, id string) (model.Source, error) {
	return get[model.Source](ctx, s.db, "sources", id)
}

func (s *Store) GetSink(ctx context.Context, id string) (model.Sink, error) {
	return get[model.Sink](ctx, s.db, "sinks", id)
}

func (s *Store) GetConnector(ctx context.Context, id string) (model.Connector, error) {
	return get[model.Connector](ctx, s.db, "connectors", id)
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
	if cfg.Sources, err = s.ListSources(ctx); err != nil {
		return cfg, err
	}
	if cfg.Sinks, err = s.ListSinks(ctx); err != nil {
		return cfg, err
	}
	if cfg.Connectors, err = s.ListConnectors(ctx); err != nil {
		return cfg, err
	}
	var settingsDoc string
	err = s.db.QueryRowContext(ctx,
		`SELECT doc FROM config_settings WHERE key = 'global'`).Scan(&settingsDoc)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Existing databases and configs have no settings row. Absence is the
		// backward-compatible default, with rich Prometheus details disabled.
	case err != nil:
		return cfg, err
	default:
		var settings model.Settings
		if err := json.Unmarshal([]byte(settingsDoc), &settings); err != nil {
			return cfg, fmt.Errorf("decode global settings: %w", err)
		}
		cfg.Settings = &settings
	}
	return cfg, nil
}

// KnownConnectorIDs returns every connector id that has aggregate or checkpoint
// state. Append maintains the aggregate transactionally, so this lookup never
// scans the retained-envelope table.
func (s *Store) KnownConnectorIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT connector_id FROM queue_aggregates
		 UNION SELECT connector_id FROM checkpoints`)
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
	if cfg.Settings == nil {
		if _, err := tx.ExecContext(ctx, `DELETE FROM config_settings WHERE key = 'global'`); err != nil {
			return err
		}
	} else {
		doc, err := json.Marshal(cfg.Settings)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO config_settings (key, doc) VALUES ('global', ?)
			ON CONFLICT(key) DO UPDATE SET doc = excluded.doc`, string(doc)); err != nil {
			return err
		}
	}
	return tx.Commit()
}
