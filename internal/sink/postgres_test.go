package sink

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

type postgresExecCall struct {
	statement string
	args      []any
}

type fakePostgresDB struct {
	mu     sync.Mutex
	calls  []postgresExecCall
	err    error
	closed bool
}

func testPostgresSink(db postgresDB, table string) *postgresSink {
	return &postgresSink{
		id: "db", db: db, table: table, batchSize: 100, timeout: time.Second,
		initializeSlot: make(chan struct{}, 1),
	}
}

func (db *fakePostgresDB) Exec(_ context.Context, statement string, args ...any) (pgconn.CommandTag, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.calls = append(db.calls, postgresExecCall{statement: statement, args: append([]any(nil), args...)})
	return pgconn.CommandTag{}, db.err
}

func (db *fakePostgresDB) Close() {
	db.mu.Lock()
	db.closed = true
	db.mu.Unlock()
}

func (db *fakePostgresDB) snapshot() []postgresExecCall {
	db.mu.Lock()
	defer db.mu.Unlock()
	return append([]postgresExecCall(nil), db.calls...)
}

type blockingPostgresDB struct {
	started chan struct{}
	once    sync.Once
}

func (db *blockingPostgresDB) Exec(ctx context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	db.once.Do(func() { close(db.started) })
	<-ctx.Done()
	return pgconn.CommandTag{}, ctx.Err()
}

func (*blockingPostgresDB) Close() {}

func TestPostgresDDL(t *testing.T) {
	regular := PostgresDDL("telemetry.envelopes", false)
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "telemetry"."envelopes"`,
		"observed_at timestamptz NOT NULL",
		"payload jsonb NOT NULL",
		"envelope jsonb NOT NULL",
		"PRIMARY KEY (observed_at, connector_id, sequence)",
		`CREATE INDEX IF NOT EXISTS "envelopes_pgn_observed_at_idx"`,
		`CREATE INDEX IF NOT EXISTS "envelopes_connector_sequence_idx"`,
	} {
		if !strings.Contains(regular, want) {
			t.Fatalf("regular PostgreSQL DDL missing %q:\n%s", want, regular)
		}
	}
	if strings.Contains(regular, "create_hypertable") {
		t.Fatalf("regular PostgreSQL DDL contains TimescaleDB setup:\n%s", regular)
	}

	timescale := PostgresDDL("telemetry.envelopes", true)
	for _, want := range []string{
		"Requires the TimescaleDB extension",
		`SELECT create_hypertable('"telemetry"."envelopes"'::regclass, by_range('observed_at')`,
		"if_not_exists => TRUE",
	} {
		if !strings.Contains(timescale, want) {
			t.Fatalf("TimescaleDB DDL missing %q:\n%s", want, timescale)
		}
	}
}

func TestPostgresEnsureReadyCreatesOrVerifiesSchema(t *testing.T) {
	for _, tc := range []struct {
		name       string
		autoCreate bool
		timescale  bool
		want       string
	}{
		{"auto create postgres", true, false, "CREATE TABLE IF NOT EXISTS"},
		{"auto create timescale", true, true, "create_hypertable"},
		{"manual table", false, false, "FROM \"public\".\"beacon_envelopes\" LIMIT 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &fakePostgresDB{}
			s := testPostgresSink(db, "public.beacon_envelopes")
			s.autoCreate, s.timescaleDB = tc.autoCreate, tc.timescale
			if err := s.ensureReady(context.Background()); err != nil {
				t.Fatal(err)
			}
			calls := db.snapshot()
			if len(calls) != 1 || !strings.Contains(calls[0].statement, tc.want) {
				t.Fatalf("schema calls = %+v, want statement containing %q", calls, tc.want)
			}
			if state, err := s.State(); state != "up" || err != nil {
				t.Fatalf("state = %q, %v; want up", state, err)
			}
		})
	}
}

func TestPostgresEnsureReadyCanceledWaiterDoesNotBlockBehindRecovery(t *testing.T) {
	db := &blockingPostgresDB{started: make(chan struct{})}
	s := testPostgresSink(db, "public.beacon_envelopes")
	s.timeout = time.Minute

	recoveryCtx, cancelRecovery := context.WithCancel(context.Background())
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- s.ensureReady(recoveryCtx) }()
	select {
	case <-db.started:
	case <-time.After(time.Second):
		cancelRecovery()
		t.Fatal("background recovery did not acquire initialization admission")
	}

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	cancelWaiter()
	started := time.Now()
	err := s.ensureReady(waiterCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("canceled waiter took %v behind recovery, want prompt return", elapsed)
	}

	cancelRecovery()
	select {
	case <-recoveryDone:
	case <-time.After(time.Second):
		t.Fatal("background recovery did not stop after cancellation")
	}
}

func TestPostgresPushBatchWritesQueryableAndCanonicalData(t *testing.T) {
	db := &fakePostgresDB{}
	s := testPostgresSink(db, "telemetry.envelopes")
	s.setState(true, nil)
	observed := time.Date(2026, 8, 8, 12, 0, 0, 0, time.FixedZone("EDT", -4*60*60))
	deviceName := uint64(0xfedcba9876543210)
	entries := []queue.Entry{
		{Seq: 41, Env: &msg.Envelope{
			ConnectorID: "nav", PGN: 129025, Source: 12, Dest: 255, Priority: 2,
			Timestamp: observed.Add(-time.Second), ObservedAt: observed, Ingress: "socketcan:can0",
			DeviceName: &deviceName, Payload: json.RawMessage(`{"latitude":41.2,"longitude":-71.3}`),
		}},
		{Seq: 42, Env: &msg.Envelope{
			ConnectorID: "nav", PGN: 127250, Source: 12, Dest: 255, Priority: 3,
			Timestamp: observed, ObservedAt: observed.Add(time.Second), Payload: json.RawMessage(`{"heading":1.5}`),
		}},
	}
	if err := s.PushBatch(context.Background(), entries); err != nil {
		t.Fatal(err)
	}
	calls := db.snapshot()
	if len(calls) != 1 {
		t.Fatalf("exec calls = %d, want 1", len(calls))
	}
	call := calls[0]
	for _, want := range []string{
		`INSERT INTO "telemetry"."envelopes"`, "$9::text::numeric", "$24",
		"ON CONFLICT (observed_at, connector_id, sequence) DO NOTHING",
	} {
		if !strings.Contains(call.statement, want) {
			t.Fatalf("insert statement missing %q:\n%s", want, call.statement)
		}
	}
	if len(call.args) != 24 {
		t.Fatalf("insert args = %d, want 24", len(call.args))
	}
	if got := call.args[8]; got != "18364758544493064720" {
		t.Fatalf("device_name arg = %v, want lossless uint64 decimal", got)
	}
	if got := call.args[10]; got != `{"latitude":41.2,"longitude":-71.3}` {
		t.Fatalf("payload arg = %v", got)
	}
	full, ok := call.args[11].(string)
	if !ok || !strings.Contains(full, `"connector":"nav"`) || !strings.Contains(full, `"pgn":129025`) {
		t.Fatalf("canonical envelope arg = %v", call.args[11])
	}
}

func TestPostgresPushBatchRejectsOutOfRangePGN(t *testing.T) {
	db := &fakePostgresDB{}
	s := testPostgresSink(db, "public.beacon_envelopes")
	s.setState(true, nil)
	entry := queue.Entry{Seq: 1, Env: &msg.Envelope{
		ConnectorID: "route", PGN: postgresMaxPGN + 1,
		Timestamp: time.Now(), ObservedAt: time.Now(), Payload: json.RawMessage(`{}`),
	}}

	err := s.PushBatch(context.Background(), []queue.Entry{entry})
	if err == nil || !strings.Contains(err.Error(), "exceeds NMEA 2000 maximum") {
		t.Fatalf("PushBatch() error = %v, want out-of-range PGN error", err)
	}
	if calls := db.snapshot(); len(calls) != 0 {
		t.Fatalf("database calls = %d, want none", len(calls))
	}
}

func TestPostgresPushFailureDegradesAndReinitializes(t *testing.T) {
	db := &fakePostgresDB{err: errors.New("connection reset")}
	s := testPostgresSink(db, "public.beacon_envelopes")
	s.setState(true, nil)
	entry := queue.Entry{Seq: 1, Env: &msg.Envelope{
		ConnectorID: "route", PGN: 127250, Timestamp: time.Now(), ObservedAt: time.Now(),
		Payload: json.RawMessage(`{"heading":1.5}`),
	}}
	if err := s.PushBatch(context.Background(), []queue.Entry{entry}); err == nil {
		t.Fatal("failed PostgreSQL write returned nil")
	}
	if state, err := s.State(); state != "degraded" || err == nil {
		t.Fatalf("state = %q, %v; want degraded", state, err)
	}
	db.mu.Lock()
	db.err = nil
	db.mu.Unlock()
	if err := s.PushBatch(context.Background(), []queue.Entry{entry}); err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	if calls := db.snapshot(); len(calls) != 3 || !strings.HasPrefix(calls[1].statement, "SELECT observed_at") {
		t.Fatalf("calls after retry = %+v, want failed insert, schema verification, insert", calls)
	}
}
