# Beacon Phase 2 — Config API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A JSON REST config API (generated OpenAPI 3.1 + embedded offline reference UI) so agents can manage sources/sinks/connectors at runtime with hot apply, plus the runtime hardening the API depends on (non-blocking health during reconcile, queue purge on delete, per-connector live stats).

**Architecture:** A `config.Service` layer owns validation + store writes + reconcile triggering; the huma-on-chi API in `internal/api` is a thin presentation over it (the Phase 3 UI will be a second thin presentation). Live rates come from a new in-process `internal/stats` registry updated by connector pipelines alongside OTel. Spec: `docs/superpowers/specs/2026-07-12-beacon-gateway-design.md` §4-5, plus the Phase-1 final-review carry-overs.

**Tech Stack:** `github.com/danielgtaylor/huma/v2` + `github.com/go-chi/chi/v5`, vendored Scalar API-reference bundle (offline), existing Phase 1 packages.

## Global Constraints

- API base path `/api/v1`; OpenAPI at `/api/openapi.json`; reference UI at `/api/docs`. All served by the ADMIN server (default :2112) alongside `/health` and `/metrics`.
- **Offline-first: no CDN URLs anywhere.** The API reference UI's JS is vendored into the binary via `go:embed`.
- Errors are RFC-7807 problem documents (huma default). Validation failures (bad CEL, unknown refs, path collisions, negative limits) → 422 with the underlying message; unknown id → 404; id conflict on create → 409.
- Entity JSON shapes are exactly `model.Source`/`model.Sink`/`model.Connector` (field names frozen in Phase 1). IDs are immutable: `id` in the body must match the path on update.
- Every successful config write triggers hot apply (supervisor reconcile) before the response returns; the response includes the resulting component status.
- Wire-format freeze: envelope `payload` must NOT contain the redundant `info` object (strip at envelope creation; `msg.pgn/source/dest/priority/timestamp` remain the only header surface).
- Deleting a connector purges its queue rows and checkpoint.
- `go test ./...` green, `gofmt -l .` empty, `go vet ./...` clean, `CGO_ENABLED=0 go build ./...` green after every task; dependency changes committed with the code.
- KNOWN: `go test -race` ICEs compiling generated n2k/pgn — exempt for packages importing it.

## File Structure (Phase 2 target)

```
internal/msg/envelope.go          MODIFY: strip payload "info" key
internal/queue/queue.go,sqlite.go MODIFY: add Purge (drop all rows + checkpoint)
internal/supervisor/supervisor.go MODIFY: non-blocking Statuses, stops outside state lock,
                                  serialize Reconcile, purge queues of deleted connectors,
                                  component-state metrics on transitions
internal/stats/stats.go           NEW: in-process per-connector counters + rolling rates
internal/connector/connector.go   MODIFY: feed stats registry
internal/config/service.go        NEW: Service (validate+persist+reconcile) used by API & UI
internal/api/api.go               NEW: huma API wiring (routes on chi)
internal/api/entities.go          NEW: CRUD endpoints
internal/api/system.go            NEW: /system, /filters/validate, /config/export|import
internal/api/docsui.go            NEW: /api/docs + embedded Scalar assets
internal/api/assets/scalar.min.js NEW: vendored (downloaded at dev time, committed)
internal/app/app.go               MODIFY: mount API on admin mux; expose Service
cmd/beacon/main.go                MODIFY: export/import CLI verbs
internal/e2e/api_test.go          NEW: end-to-end API → hot apply → data flow
```

---

### Task 1: Supervisor hardening (lock discipline, purge, metrics transitions)

**Files:**
- Modify: `internal/queue/queue.go`, `internal/queue/sqlite.go`, `internal/supervisor/supervisor.go`, `internal/metrics/metrics.go`
- Test: `internal/queue/sqlite_test.go`, `internal/supervisor/supervisor_test.go`

**Interfaces:**
- Consumes: existing supervisor/queue internals.
- Produces:
  - `queue.Queue` gains `Purge(ctx context.Context) error` — deletes ALL queue rows and the checkpoint row for the connector. SQLite impl: two DELETEs in one tx.
  - `metrics.Set` gains `RemoveComponent(kind, id string)` and `RemoveConnector(connector string)` (drop gauge map entries so deleted components stop being exported).
  - Supervisor behavior changes (signatures unchanged):
    1. `Reconcile` is serialized by its own `reconcileMu`; the state maps are guarded by a separate `stateMu` held only for map reads/writes — **component constructors and Stop() calls run outside `stateMu`**, so `Statuses()` never blocks behind a slow teardown.
    2. When a connector leaves the desired set entirely (deleted, not just changed/disabled→wait: purge ONLY when deleted from config; a disabled or changed connector keeps its queue), the supervisor calls `Purge` on its queue and `met.RemoveConnector(id)`.
    3. Healthy transitions now recorded: `SetComponentState(kind, id, 2)` after each successful component start; `RemoveComponent` when a component is stopped and no longer desired; the `errored` map already resets each reconcile (sticky-error fix follows automatically since successful start now overwrites state).

- [ ] **Step 1: Write failing tests**

Add to `internal/queue/sqlite_test.go`:

```go
func TestPurgeRemovesRowsAndCheckpoint(t *testing.T) {
	q := testQueue(t, model.BufferLimits{MaxMessages: 100})
	ctx := context.Background()
	appendN(t, q, 5, time.Now())
	entries, _ := q.Read(ctx, 0, 10)
	_ = q.Ack(ctx, entries[4].Seq)

	if err := q.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := q.Stats(ctx)
	if st.Depth != 0 {
		t.Fatalf("depth = %d after purge", st.Depth)
	}
	cur, _ := q.Cursor(ctx)
	if cur != 0 {
		t.Fatalf("cursor = %d after purge, want 0", cur)
	}
}
```

Add to `internal/supervisor/supervisor_test.go`:

```go
func TestDeletedConnectorQueueIsPurged(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)

	// Seed the connector's queue directly, then delete the connector.
	q := queue.NewSQLite(st, "link", model.BufferLimits{MaxMessages: 10})
	_ = q.Append(ctx, []*msg.Envelope{{PGN: 1, Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}})

	cfg := baseConfig()
	cfg.Connectors = nil
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)

	stats, _ := q.Stats(ctx)
	if stats.Depth != 0 {
		t.Fatalf("deleted connector queue depth = %d, want 0 (purged)", stats.Depth)
	}
}

func TestDisabledConnectorQueueSurvives(t *testing.T) {
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)
	q := queue.NewSQLite(st, "link", model.BufferLimits{MaxMessages: 10})
	_ = q.Append(ctx, []*msg.Envelope{{PGN: 1, Timestamp: time.Now(), Payload: json.RawMessage(`{}`)}})

	cfg := baseConfig()
	cfg.Connectors[0].Enabled = false
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)

	stats, _ := q.Stats(ctx)
	if stats.Depth != 1 {
		t.Fatalf("disabled connector queue depth = %d, want 1 (kept)", stats.Depth)
	}
}

func TestStatusesDoesNotBlockDuringReconcile(t *testing.T) {
	// A sink whose Stop blocks lets us hold Reconcile mid-teardown, then
	// assert Statuses returns promptly. Use a slow-stopping TCP client
	// scenario substitute: block via a connector delivering to a Pusher that
	// hangs — simplest deterministic proxy: call Statuses concurrently with
	// a Reconcile that stops a connector with queued retries, and require
	// Statuses to return within 500ms. (Connector.Stop waits ≤ retry-select
	// wakeups, so pre-fix this exceeds 500ms only under stateMu coupling;
	// post-fix Statuses never touches teardown.)
	st, sup, _ := setup(t)
	ctx := context.Background()
	_ = st.ReplaceConfig(ctx, baseConfig())
	_ = sup.Reconcile(ctx)

	cfg := baseConfig()
	cfg.Connectors = nil
	_ = st.ReplaceConfig(ctx, cfg)

	done := make(chan struct{})
	go func() { _ = sup.Reconcile(ctx); close(done) }()

	start := time.Now()
	_ = sup.Statuses()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Statuses blocked %v during reconcile", elapsed)
	}
	<-done
}

func TestRecoveredComponentStateNotSticky(t *testing.T) {
	// A component that fails to start records error status; after the config
	// is fixed and reconciled, Statuses must no longer report the error.
	st, sup, _ := setup(t)
	ctx := context.Background()
	cfg := baseConfig()
	cfg.Sinks = append(cfg.Sinks, model.Sink{ID: "bad", Name: "Bad", Type: model.SinkTCP,
		Enabled: true, Address: "256.256.256.256:1"})
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if s := find(sup.Statuses(), "sink", "bad"); s == nil || s.State != "error" {
		t.Fatalf("precondition: bad sink should be error, got %+v", s)
	}

	cfg.Sinks[1].Address = "127.0.0.1:0" // now bindable
	_ = st.ReplaceConfig(ctx, cfg)
	_ = sup.Reconcile(ctx)
	if s := find(sup.Statuses(), "sink", "bad"); s == nil || s.State == "error" {
		t.Fatalf("recovered sink still error: %+v", s)
	}
}
```

(Imports for `queue`, `msg`, `json`, `time` as needed.)

- [ ] **Step 2: Run tests, verify failures** — `go test ./internal/queue/ ./internal/supervisor/ -run 'Purge|Deleted|Disabled|Blocks|Sticky' -v` → FAIL (Purge undefined; purge/lock behavior missing).

- [ ] **Step 3: Implement**

`internal/queue`: add `Purge(ctx)` to the interface and SQLite impl:

```go
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
```

`internal/metrics`: add map-entry removal:

```go
func (s *Set) RemoveConnector(connector string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.depths, connector)
	s.mu.Unlock()
}

func (s *Set) RemoveComponent(kind, id string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.states, gaugeKey{kind, id})
	s.mu.Unlock()
}
```

`internal/supervisor` restructure (keep public API identical):
- Split `s.mu` into `reconcileMu sync.Mutex` (serializes Reconcile/Stop bodies) and `stateMu sync.Mutex` (guards `sources`/`sinks`/`connectors`/`errored` maps only).
- Reconcile: take `reconcileMu` for the whole body. Compute the stop set under `stateMu`, remove entries from the maps, release `stateMu`, THEN call `Stop()` on the removed components. Same for starts: construct outside `stateMu`, insert under it.
- Track each running connector's queue (`runningConnector` gains `q queue.Queue`). When a connector id is in the running set or previously known but absent from the FULL desired config (not merely disabled — check against all configured connectors, enabled or not), call `q.Purge(context.Background())` after Stop and `met.RemoveConnector(id)`. For a deleted-but-not-running connector (e.g. was disabled, then deleted) also purge: build the queue on demand with `queue.NewSQLite(s.st, id, model.BufferLimits{})` — Purge doesn't need real limits. Detect "previously known" via a persisted set: simplest correct approach — after loading config, `SELECT DISTINCT connector_id FROM queue`+checkpoints via a small store helper `store.KnownConnectorIDs(ctx) ([]string, error)`; purge ids not present in the loaded config at all. Add that helper to `internal/store/store.go`:

```go
// KnownConnectorIDs returns every connector id that has queue or checkpoint
// rows — used to purge storage of connectors deleted from config.
func (s *Store) KnownConnectorIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT connector_id FROM queue UNION SELECT connector_id FROM checkpoints`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
```

- Metrics transitions: after each successful `source.New`/`sink.New`/`connector Start`, call `s.met.SetComponentState(kind, id, 2)`. In the stop path for components no longer desired at all, call `s.met.RemoveComponent(kind, id)`.

- [ ] **Step 4: Run tests** — the four new tests + full suite green.
- [ ] **Step 5: Commit** — `feat: supervisor hardening - non-blocking health, queue purge, metric hygiene`

---

### Task 2: Freeze the wire format (strip payload `info`) + stats registry

**Files:**
- Modify: `internal/msg/envelope.go`
- Create: `internal/stats/stats.go`
- Modify: `internal/connector/connector.go` (feed stats)
- Test: `internal/msg/envelope_test.go`, `internal/stats/stats_test.go`

**Interfaces:**
- `msg.FromPGN` change: after marshaling a known PGN's payload, remove the top-level `"info"` key (the header data already lives in envelope fields). Implementation: unmarshal to `map[string]json.RawMessage`, `delete(m, "info")`, re-marshal. Do it once at envelope creation (not per read).
- New package `internal/stats`:

```go
// Registry tracks live per-connector counters and rolling rates for the
// API and UI. It is independent of OTel (which serves Prometheus).
type Registry struct{ ... }
func NewRegistry() *Registry
// Record adds delivered messages/bytes for a connector at time now.
func (r *Registry) Record(connector string, msgs, bytes int64)
// SetQueue records current queue depth/bytes (from the prune loop).
func (r *Registry) SetQueue(connector string, depth, bytes int64)
// Remove drops a connector's stats (deleted connectors).
func (r *Registry) Remove(connector string)
type Snapshot struct {
    TotalMessages int64   `json:"total_messages"`
    TotalBytes    int64   `json:"total_bytes"`
    MsgPerSec     float64 `json:"msg_per_sec"`      // over last 10s window
    BytesPerSec   float64 `json:"bytes_per_sec"`    // over last 10s window
    QueueDepth    int64   `json:"queue_depth"`
    QueueBytes    int64   `json:"queue_bytes"`
}
func (r *Registry) Snapshot(connector string) (Snapshot, bool)
func (r *Registry) All() map[string]Snapshot
```

Implementation: per-connector struct with atomic totals plus a mutex-guarded ring of (timestamp, msgs, bytes) buckets at 1-second granularity, 10 buckets; rates computed on read from buckets within the window. A nil `*Registry` no-ops Record/SetQueue/Remove and returns empty from Snapshot/All (same nil-safe convention as metrics.Set).

- `connector.New` gains ONE new parameter: `st *stats.Registry` (nil-safe), placed last: `New(cfg, src, snk, q, chain, log, met, st)`. Delivery paths call `st.Record(id, 1, int64(e.Env.SizeBytes()))` per delivered entry; the prune loop calls `st.SetQueue(id, depth, bytes)`. Supervisor passes its registry through (supervisor.New gains the same last-param `*stats.Registry`; app owns the Registry). Update all existing call sites/tests mechanically (pass nil where stats don't matter).

- [ ] **Step 1: Failing tests**

`internal/msg/envelope_test.go` addition:

```go
func TestPayloadOmitsInfo(t *testing.T) {
	e, err := FromPGN(heading(15708))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.PayloadMap()["info"]; ok {
		t.Fatalf("payload still contains info: %s", e.Payload)
	}
	if e.PayloadMap()["heading"] == nil {
		t.Fatalf("stripping info lost data fields: %s", e.Payload)
	}
}
```

`internal/stats/stats_test.go`:

```go
package stats

import (
	"testing"
	"time"
)

func TestNilRegistrySafe(t *testing.T) {
	var r *Registry
	r.Record("c", 1, 10)
	r.SetQueue("c", 1, 10)
	r.Remove("c")
	if _, ok := r.Snapshot("c"); ok {
		t.Fatal("nil registry returned a snapshot")
	}
	if len(r.All()) != 0 {
		t.Fatal("nil registry All() non-empty")
	}
}

func TestTotalsAndRates(t *testing.T) {
	r := NewRegistry()
	for i := 0; i < 10; i++ {
		r.Record("nav", 1, 100)
	}
	r.SetQueue("nav", 7, 700)
	s, ok := r.Snapshot("nav")
	if !ok {
		t.Fatal("no snapshot")
	}
	if s.TotalMessages != 10 || s.TotalBytes != 1000 || s.QueueDepth != 7 || s.QueueBytes != 700 {
		t.Fatalf("snapshot = %+v", s)
	}
	if s.MsgPerSec <= 0 {
		t.Fatalf("rate should be positive right after recording, got %v", s.MsgPerSec)
	}
}

func TestRateDecaysToZero(t *testing.T) {
	r := newRegistryAt(func() time.Time { return time.Unix(1000, 0) }) // test hook
	r.Record("nav", 5, 500)
	r.now = func() time.Time { return time.Unix(1020, 0) } // 20s later, window is 10s
	s, _ := r.Snapshot("nav")
	if s.MsgPerSec != 0 {
		t.Fatalf("rate after window = %v, want 0", s.MsgPerSec)
	}
	if s.TotalMessages != 5 {
		t.Fatalf("totals must not decay: %+v", s)
	}
}

func TestRemove(t *testing.T) {
	r := NewRegistry()
	r.Record("nav", 1, 1)
	r.Remove("nav")
	if _, ok := r.Snapshot("nav"); ok {
		t.Fatal("removed connector still has snapshot")
	}
}
```

(Expose a `now func() time.Time` field + `newRegistryAt` test constructor for deterministic rate tests.)

- [ ] **Step 2: Verify failures**, **Step 3: Implement** (envelope strip; stats registry; connector + supervisor + app threading; update existing tests' call sites), **Step 4: Full suite green**, **Step 5: Commit** — `feat: freeze payload wire format and add live stats registry`

---

### Task 3: Config service layer

**Files:**
- Create: `internal/config/service.go`
- Test: `internal/config/service_test.go`
- Modify: `internal/app/app.go` (construct Service, expose `(*App).Service() *config.Service`)

**Interfaces:**
- Consumes: `store.Store`, `supervisor.Supervisor` (via a small interface), `filter.Compile`, `model`, `stats.Registry`.
- Produces:

```go
package config

// Reconciler is what the service triggers after committing config.
type Reconciler interface {
    Reconcile(ctx context.Context) error
    Statuses() []supervisor.Status
}

type Service struct{ ... }
func NewService(st *store.Store, rec Reconciler, log *slog.Logger) *Service

// Reads
func (s *Service) ListSources(ctx) ([]model.Source, error)      // and Sinks/Connectors
func (s *Service) GetSource(ctx, id) (model.Source, error)      // ErrNotFound sentinel
func (s *Service) Export(ctx) (model.Config, error)
func (s *Service) Statuses() []supervisor.Status

// Writes — each validates the WHOLE resulting config (model.Config.Validate
// + filter.Compile for every connector), persists, reconciles, returns.
func (s *Service) PutSource(ctx, v model.Source, isCreate bool) error   // ErrExists on create-conflict, ErrNotFound on update-missing
func (s *Service) DeleteSource(ctx, id) error   // ErrInUse if a connector references it
// ... same trio for Sink and Connector (connector delete has no ErrInUse)
func (s *Service) Import(ctx, cfg model.Config, replace bool) error // merge = upsert by id
func (s *Service) ValidateFilters(exprs []string) error

var ErrNotFound = errors.New("not found")
var ErrExists = errors.New("already exists")
var ErrInUse = errors.New("referenced by a connector")
type ValidationError struct{ Msg string } // distinguishes 422 from 500
```

Rules: writes are serialized with an internal mutex (read-modify-write against the store must not interleave); validation happens against the would-be config BEFORE any store write; reconcile failure after a successful write is logged and surfaced in statuses, not rolled back (matches spec §3.6: visible, not fatal).

- [ ] **Step 1: Failing tests** — table-driven over an in-temp-dir store with a fake Reconciler (records Reconcile calls):
  - create source → listed; duplicate create → ErrExists; update missing → ErrNotFound
  - create connector referencing unknown source → ValidationError, store unchanged, no reconcile call
  - connector with invalid CEL → ValidationError naming the expression
  - delete source referenced by connector → ErrInUse; after deleting connector → delete source OK
  - every successful write → exactly one Reconcile call; failed validation → zero
  - Import replace: wholesale swap; Import merge: upserts by id, keeps unmentioned entities; invalid import → ValidationError, store unchanged
  - sink path collision with EXISTING sink caught (create second sink with same path)
- [ ] **Step 2: Verify failures**, **Step 3: Implement** (validation = load current config, apply mutation to a copy, `cfg.Validate()` + compile filters, then persist via store Put/Delete/ReplaceConfig + `rec.Reconcile(ctx)`), **Step 4: green**, **Step 5: Commit** — `feat: config service layer with whole-config validation and hot apply`

---

### Task 4: huma API — entities CRUD

**Files:**
- Create: `internal/api/api.go`, `internal/api/entities.go`
- Test: `internal/api/entities_test.go`
- Modify: `internal/app/app.go` (mount), `go.mod` (huma, chi)

**Interfaces:**
- Produces: `api.New(svc *config.Service, reg *stats.Registry, version string) (http.Handler, huma.API)` — a chi router with huma mounted; `app` mounts it under `/api/` on the admin mux and keeps `/health`+`/metrics` as-is.
- Endpoints (all `/api/v1`, JSON = model shapes):
  - `GET /sources` → `{"sources":[...]}`; `GET /sources/{id}`; `PUT /sources/{id}` (create-or-update; body id must equal path id → 422 otherwise); `DELETE /sources/{id}` (409 if ErrInUse, 404 if missing). Same for `/sinks`, `/connectors`.
  - Every write response body: `{"status": [<supervisor statuses for the affected id>...]}`.
  - Error mapping: `ValidationError`→422, `ErrNotFound`→404, `ErrExists`→409 (POST-style create not offered; PUT is idempotent create-or-update so ErrExists is unused by PUT — service's isCreate=false), `ErrInUse`→409.
- huma specifics: `humago` or `humachi` adapter (`humachi.New(router, huma.DefaultConfig("beacon config API", version))`); operation IDs like `list-sources`, `put-source`, `delete-source` (these become OpenAPI operationIds — stable, agent-friendly); request/response structs with `json` + `doc` tags so the generated schema is documented.

- [ ] **Step 1: Failing tests** — spin the handler with a real Service (temp store, fake reconciler): full CRUD round-trip per entity type over `httptest`; 422 on bad CEL with problem+json content type; 404/409 paths; OpenAPI JSON obtainable from `api.OpenAPI()` (assert it contains `/api/v1/sources` path and `put-source` operationId).
- [ ] **Step 2: Verify failures**, **Step 3: Implement**, **Step 4: green** (commit go.mod/go.sum with the code), **Step 5: Commit** — `feat: huma config API - entity CRUD with hot apply`

---

### Task 5: huma API — system, filters, metrics, export/import

**Files:**
- Create: `internal/api/system.go`
- Test: `internal/api/system_test.go`
- Modify: `cmd/beacon/main.go` (export/import CLI verbs)

**Interfaces / endpoints:**
- `POST /api/v1/filters/validate` body `{"filters":["..."]}` → 200 `{"valid":true}` or 422 problem with the CEL error (uses `svc.ValidateFilters`).
- `GET /api/v1/system` → `{"version":"...","can_interfaces":["can0",...],"serial_ports":["/dev/ttyUSB0",...]}`. Interface discovery best-effort: Linux — `/sys/class/net/*/type` == `280`; serial — glob `/dev/ttyUSB*`, `/dev/ttyACM*`, `/dev/tty.usbserial*`, `/dev/tty.usbmodem*`. Non-Linux returns empty CAN list. Isolate in `func discoverCAN() []string` / `func discoverSerial() []string` with tiny unit tests via a settable root-path variable.
- `GET /api/v1/connectors/{id}/metrics` → `stats.Snapshot` (404 unknown connector — check service GetConnector); `GET /api/v1/metrics` → `{"connectors":{id:Snapshot}}`.
- `GET /api/v1/config/export` → full `model.Config`; `POST /api/v1/config/import?mode=replace|merge` (default replace) → 200 with statuses / 422 on validation.
- `GET /api/v1/health` → same JSON as `/health` (spec §5 lists it under the API; reuse the app's handler logic via the service Statuses).
- CLI verbs in `cmd/beacon/main.go`:
  - `beacon export [--db beacon.db]` → prints export JSON to stdout (opens store directly; no server needed).
  - `beacon import file.json [--db beacon.db] [--merge]` → validates + writes to the store directly (same code path as Service validation — factor `config.ValidateConfig(cfg model.Config) error` as a package function used by both), prints a summary. NOTE: importing into a live beacon's DB while it runs is unsupported (single-conn SQLite) — document that the API endpoint is the live path; the CLI is for offline/bootstrap. Do not add locking heroics.

- [ ] **Step 1: Failing tests** — httptest: filters/validate happy+422; system returns version and arrays (assert JSON shape, not machine specifics); connector metrics 200 after Record + 404 unknown; export→import(replace) round-trip equality; CLI: run export against a seeded temp DB via `exec.Command` on a built binary? NO — factor the CLI logic into testable funcs (`runExport(db string, w io.Writer) error`) and unit-test those; cobra wiring stays thin.
- [ ] **Step 2: Verify failures**, **Step 3: Implement**, **Step 4: green**, **Step 5: Commit** — `feat: system, filter validation, metrics, export/import API + CLI verbs`

---

### Task 6: Embedded API reference UI (offline)

**Files:**
- Create: `internal/api/docsui.go`, `internal/api/assets/scalar.min.js` (vendored), `internal/api/assets/README.md` (provenance note: package name, version, license, upstream URL)
- Test: `internal/api/docsui_test.go`

**Interfaces:**
- `GET /api/docs` → self-contained HTML page loading `/api/assets/scalar.min.js` (embedded via `go:embed assets/*`) and pointing Scalar at `/api/openapi.json`. `GET /api/openapi.json` served by huma config (set `config.OpenAPIPath`/docs path accordingly — huma serves the spec itself; ensure the path is exactly `/api/openapi.json`).
- Vendor step (dev-time, in this task): `npm pack @scalar/api-reference` is NOT needed — download the standalone browser bundle: `curl -L -o internal/api/assets/scalar.min.js https://cdn.jsdelivr.net/npm/@scalar/api-reference@latest/dist/browser/standalone.min.js` and COMMIT it. Record exact version + MIT license in assets/README.md. The page must reference ONLY same-origin URLs.
- Disable/override huma's default docs page (which uses a CDN) — set `huma.Config.DocsPath = ""` and register our own `/api/docs` route.

- [ ] **Step 1: Failing test** — httptest: `GET /api/docs` returns 200 text/html containing `/api/assets/scalar.min.js` and NOT containing `http://` or `https://` external script/link URLs (regex over src=/href= attributes); `GET /api/assets/scalar.min.js` returns 200 with a non-trivial body; `GET /api/openapi.json` returns a spec with `"openapi":"3.1`.
- [ ] **Step 2-4: fail → implement → green**, **Step 5: Commit** — `feat: offline API reference UI with vendored Scalar bundle`

---

### Task 7: e2e — API drives the gateway

**Files:**
- Create: `internal/e2e/api_test.go`

**Scenario (single test, staged):** start `app.Run` with an EMPTY store (no seed) and a fake bus. Via the admin API over HTTP:
1. `PUT /api/v1/sources/can0` (socketcan can0) → 200.
2. `PUT /api/v1/sinks/nav` (http_sse /nav) → 200.
3. `PUT /api/v1/connectors/heading` (filter `msg.pgn == 127250`, buffer 1000) → 200.
4. Connect SSE client to data server `/nav`; inject heading + depth frames via fake bus; assert only 127250 arrives (payload must NOT contain an `info` key — wire-freeze regression).
5. `GET /api/v1/connectors/heading/metrics` → total_messages ≥ 1.
6. `PUT /api/v1/connectors/heading` with filter `msg.pgn == 999` (hot apply) → inject heading frame → assert nothing arrives on a fresh SSE connect within 1s.
7. `DELETE /api/v1/connectors/heading` → assert queue purged (`GET /api/v1/connectors/heading/metrics` → 404) and `/health` shows 2 components.
8. `GET /api/v1/config/export` → contains the source and sink but no connectors.

- [ ] **Steps: fail → green → commit** — `test: end-to-end API-driven gateway lifecycle`

---

## Plan Self-Review Notes

- **Spec coverage (Phase 2 scope):** API CRUD+validate+export/import+system+metrics endpoints (spec §5 table) → Tasks 4-5; OpenAPI + embedded reference UI → Tasks 4, 6; hot apply from API → Tasks 1, 3; CLI export/import + seed already existing → Task 5; per-connector metrics for agents/UI → Task 2, 5. Carry-overs folded: queue purge on delete (T1), non-blocking health (T1), sticky error states + metric hygiene (T1), payload.info freeze (T2), rates for Phase 3 dashboard (T2).
- **Deliberately deferred to Phase 3/4:** admin SSE stream for live UI tiles (Phase 3 builds it with the dashboard), lag_seconds/reconnect-counter metrics (Phase 3 alongside dashboard needs), cel.CostLimit (note added to service docs), README (Phase 4).
- **Type consistency:** `connector.New(cfg, src, snk, q, chain, log, met, st)` — T2 changes the signature; supervisor (T1-modified) is updated in T2 in the same commit. Service Reconciler interface matches `*supervisor.Supervisor` methods exactly. `stats.Snapshot` JSON tags are the API response shape used in T5.
- **Order matters:** T1 → T2 → T3 → T4 → T5 → T6 → T7 strictly (each builds on the previous signatures).
