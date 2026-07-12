# Beacon — Source/Sink/Connector Gateway Design

**Date:** 2026-07-12
**Status:** Approved
**Supersedes:** `specs/spec.md` (the original experimental design)

## 1. Overview

Beacon becomes a general NMEA 2000 gateway built around three user-managed entities:

- **Sources** — where messages come from: CAN (SocketCAN or USB CAN) or HTTP (beacon dials a remote SSE or WebSocket stream).
- **Sinks** — where messages go: CAN (beacon transmits onto the bus), HTTP (beacon serves an SSE or WebSocket endpoint), or TCP (beacon serves an NDJSON listener).
- **Connectors** — named links from one source to one sink, with optional CEL filters, per-connector durable buffering with replay, and per-connector metrics.

Sources and sinks are each defined once and referenced by any number of connectors. All configuration is managed at runtime through a JSON REST API (with generated OpenAPI 3.1 spec and embedded reference UI) and an htmx web UI styled with daisyUI and the OpenBridge design system. The binary also serves an onboard markdown instruction manual at `/docs`.

This is a rewrite of the runtime core (existing code is not preserved), reusing proven patterns from the current implementation: SQLite WAL ring buffering, periodic checkpoint flushing, compiled CEL filter chains, OTel/Prometheus metrics.

### Decisions made during design

| Question | Decision |
|---|---|
| HTTP directionality | Sinks are **served** by beacon (clients connect to beacon); HTTP sources are **dialed** by beacon (beacon connects out) |
| Config storage | SQLite, hot-applied; full export/import to a JSON file; `--seed` for first-boot bootstrap |
| UI stack | htmx + Go templates (no SPA), daisyUI themed to OpenBridge tokens, OpenBridge web components for chrome, all embedded via `go:embed` |
| TCP sink | Kept as a third sink type (serve-mode NDJSON) |
| Auth | None (trusted boat LAN) |
| Architecture | Fresh connector-centric runtime; durable queue behind an interface so an embedded broker (e.g. NATS JetStream) can replace SQLite later |
| n2k version | Latest `open-ships/n2k` (Client read/write, address claiming, typed PGN catalog, `n2k.Bus` seam, `Replay`) |
| Old TOML config | Dies; no automatic migrator (beacon is experimental; manual re-setup via UI, then export) |

## 2. Domain model

### 2.1 Message envelope

The canonical message form used in queues, on HTTP wires, and as CEL input:

```json
{
  "id": 184467,
  "pgn": 127250,
  "source": 12,
  "dest": 255,
  "priority": 2,
  "timestamp": "2026-07-12T10:15:04.123Z",
  "payload": { "heading": 15708, "deviation": null, "variation": null, "reference": 0 },
  "raw": "base64-assembled-payload-bytes"
}
```

- `id` is the connector-queue sequence number; it doubles as the SSE event id for client replay.
- `payload` is the JSON serialization of the typed `pgn.Message` struct from n2k.
- `raw` always carries the assembled payload bytes (base64): for unknown PGNs the original bytes (`UnknownPGN.Data`), for known PGNs the canonical re-encoding via `pgn.EncodeMessage`. This makes the CAN-sink write path uniform — `pgn.DecodeMessage(info, raw)` → `Client.Write` — with no JSON→struct reconstruction needed anywhere.

### 2.2 Source

| Field | Notes |
|---|---|
| `id` | slug, immutable |
| `name` | display name |
| `type` | `socketcan` \| `usbcan` \| `http_sse` \| `http_ws` |
| `enabled` | bool |
| type-specific | `socketcan`: `interface`; `usbcan`: `port` (serial device path); `http_sse`/`http_ws`: `url`, optional `headers` |

Dialed sources (`http_sse`, `http_ws`) reconnect with exponential backoff + jitter and expose health state. The remote stream is expected to emit beacon envelopes (NDJSON over WS, `data:` lines over SSE) — beacon-to-beacon chaining is a first-class use case.

### 2.3 Sink

| Field | Notes |
|---|---|
| `id`, `name`, `enabled` | as above |
| `type` | `socketcan` \| `usbcan` \| `http_sse` \| `http_ws` \| `tcp` |
| type-specific | `socketcan`: `interface`; `usbcan`: `port`; `http_sse`/`http_ws`: `path` served on the data listener; `tcp`: `address` (own listener, NDJSON) |

Sink paths are validated against reserved prefixes (`/api`, `/ui`, `/docs`, `/metrics`, `/health`) and against collision with other sinks.

### 2.4 Connector

| Field | Notes |
|---|---|
| `id`, `name`, `enabled` | as above |
| `source_id`, `sink_id` | references, validated on write |
| `filters` | list of CEL expressions, AND semantics; empty = pass all |
| `buffer.max_messages` | prune-oldest beyond this count |
| `buffer.max_age` | prune entries older than this duration |
| `buffer.max_bytes` | prune-oldest beyond this total payload size |

Whichever buffer limit is hit first triggers pruning. Defaults: `max_messages` 100000, `max_age` and `max_bytes` unset (unlimited); at least one limit must be set, enforced at validation time. Each connector owns: one durable queue, one delivery checkpoint, one compiled filter chain, one metrics set.

### 2.5 CEL environment

Same variable surface as today (`msg.pgn`, `msg.source`, `msg.dest`, `msg.priority`, `msg.timestamp`, `msg.payload.<field>`), evaluated against the envelope. Expressions are compiled when config is applied; compile errors reject the config write (API 422 with the CEL error text). Evaluation errors drop the message and increment a `filter_error` counter.

## 3. Runtime architecture

```
             ┌────────────────────────── beacon process ──────────────────────────┐
 can0 ──▶ BusManager ──▶ Source hub ──┬─▶ Connector A: filter → [Queue] → deliver ─▶ SSE /named-endpoint
 usb  ──▶ (n2k.Client   (broadcast)   └─▶ Connector B: filter → [Queue] → deliver ─▶ BusManager ─▶ can2
 wss://… ─▶ dialer)
                         Supervisor (reconciler): desired state (SQLite) ⇄ running pipelines
```

### 3.1 Bus manager

Owns exactly **one `n2k.Client` per physical CAN endpoint** (SocketCAN interface or USB port). NMEA 2000 address claiming means one bus participant per interface, so a source and a sink on the same interface share the client: the source takes the receive iterator, the sink calls `Client.Write`. Clients are refcounted and started/stopped as sources/sinks come and go.

### 3.2 Source hub

Each running source broadcasts decoded envelopes to its subscribing connectors over channels. No durability at this layer — durability lives in the connector queue. Queue appends are batched SQLite inserts and fast; if a connector's intake stalls, the hub drops for that subscriber and counts it (never blocks other connectors).

### 3.3 Connector pipeline

Two stages per connector:

1. **Intake:** subscribe to source hub → CEL filter → batched append to durable queue → `matched` metrics.
2. **Delivery:** read entries past the checkpoint → deliver to sink → advance checkpoint (flushed to SQLite every ~500 ms, like today's checkpoint flusher).

### 3.4 Queue interface (broker-swappable)

```go
type Queue interface {
    Append(ctx context.Context, entries []Entry) error
    Read(ctx context.Context, after Seq, limit int) ([]Entry, error)
    Ack(ctx context.Context, consumer string, upTo Seq) error
    Prune(ctx context.Context, limits Limits) (pruned int, err error)
    Stats(ctx context.Context) (QueueStats, error) // depth, bytes, oldest timestamp
}
```

Default implementation: SQLite (WAL mode, one shared `queue` table keyed by `connector_id`). The engine depends only on the interface, so an embedded NATS JetStream implementation can replace it later without touching connector logic.

### 3.5 Delivery semantics

- **Push-confirmed** (CAN sinks): delivery blocks until the bus write succeeds. On bus failure the queue accumulates up to its limits and drains on recovery. At-least-once delivery onto the bus.
- **Broadcast + client replay** (SSE/WS sinks): the delivery cursor advances as messages are broadcast to currently connected clients. Each client may replay independently, served directly from the connector queue up to its retention limits. Because a sink may be fed by multiple connectors, event ids are composite: `<connector_id>:<seq>` (SSE `id:` field; also the envelope `id` on WS). SSE clients replay via `Last-Event-ID`; SSE and WS clients can also pass `?after=<connector_id>:<seq>` (repeatable). TCP sinks are live-tail only (their point is `nc`-grade simplicity; no replay). Zero connected clients does not create backpressure.

### 3.6 Hot apply (reconciler)

API/UI writes desired state to SQLite → supervisor diffs desired vs. running → stops/starts only affected pipelines. A connector edit restarts just that connector; its queue and checkpoint persist (keyed by connector id). Component start failures (e.g. missing CAN interface) put the component in an `error` state visible in API/UI instead of rejecting the config — visible, not fatal. Static validation (CEL compile, reference integrity, path collisions) still rejects bad config at write time.

### 3.7 CAN write path (resolved — no spike needed)

n2k exposes `pgn.DecodeMessage(info, payload) (PGN, error)` and `pgn.EncodeMessage(msg) ([]byte, error)`. Since every envelope carries `raw` (§2.1), a CAN sink writes any message — whether it originated on a local bus or arrived as JSON from an HTTP source — by `pgn.DecodeMessage(info, raw)` → `Client.Write`. No JSON→struct registry is required. (Known caveat: 8 of 599 PGNs don't value-round-trip through the codec per n2k's changelog; acceptable for an at-least-once gateway.)

## 4. Persistence

One SQLite database (`beacon.db`, WAL):

| Table | Contents |
|---|---|
| `sources`, `sinks`, `connectors` | config rows; type-specific settings in a JSON column validated against per-type schemas |
| `queue` | durable buffer: `connector_id`, `seq`, envelope fields, `bytes` |
| `checkpoints` | `(connector_id, last_seq)` — the connector's delivery cursor. Client replay is stateless: clients supply their own cursor (`Last-Event-ID` / `after`), so no per-client server state |
| `schema_migrations` | versioned, applied at startup |

Background prune loop enforces each connector's count/age/bytes limits.

### Export / import / seed

- `GET /api/v1/config/export` → `{"sources":[…],"sinks":[…],"connectors":[…]}`; also `beacon export`.
- `POST /api/v1/config/import?mode=replace|merge` → whole-document validation, transactional apply, hot reconcile; also `beacon import file.json`.
- `beacon --seed file.json` seeds an empty database on first boot (git-tracked config bootstrapping).

## 5. HTTP surface

Two listeners (same split as today):

- **Data server** (`:8080`): user-defined SSE/WS sink paths. TCP sinks bind their own listeners.
- **Admin server** (`:2112`):

| Path | What |
|---|---|
| `/api/v1/sources`, `/api/v1/sinks`, `/api/v1/connectors` | REST CRUD |
| `/api/v1/connectors/{id}/metrics` | rates, counts, queue depth |
| `/api/v1/filters/validate` | CEL compile check |
| `/api/v1/config/export`, `/api/v1/config/import` | see §4 |
| `/api/v1/system` | version, available CAN interfaces, serial ports (UI dropdowns) |
| `/api/openapi.json`, `/api/docs` | generated OpenAPI 3.1 + embedded reference UI |
| `/` | htmx web UI |
| `/docs` | rendered markdown manual |
| `/metrics`, `/health` | Prometheus + health |

**API framework:** [huma](https://huma.rocks) over chi. Handlers are typed Go functions; the OpenAPI spec is generated from them and cannot drift; validation errors are RFC-7807 problem documents (agent-friendly).

**Offline constraint:** a boat has no internet. Every asset — API reference UI, OpenBridge web component bundle, Noto Sans fonts, compiled Tailwind/daisyUI CSS, htmx — is embedded in the binary. No CDN references anywhere.

## 6. Web UI

**Stack:** Go `html/template` + htmx, daisyUI (compiled with the standalone Tailwind CLI at build time — no Node at runtime), OpenBridge web components vendored from `@oicl/openbridge-webcomponents`. All assets `go:embed`ded. **No inline CSS** — styling exclusively via Tailwind/daisyUI classes and OpenBridge design tokens.

**Theming:** OpenBridge components provide the app chrome — top bar, navigation menu, brilliance (day/dusk/night/bright) toggle. A custom daisyUI theme maps daisyUI color variables onto OpenBridge CSS custom properties, so daisyUI forms/tables/badges follow the OpenBridge palette switch automatically.

**Pages:**

- **Dashboard** — connector cards with live message-rate and data-rate tiles (htmx SSE extension subscribed to an admin event stream; fragments swap in place), source/sink health chips.
- **Sources / Sinks** — tables + create/edit forms. The type selector swaps in type-specific fields via htmx fragments; interface/port dropdowns populated from `/api/v1/system`.
- **Connectors** — form with source/sink selects, CEL filter textarea with validate-on-blur against `/api/v1/filters/validate`, buffer limit fields. Detail page shows per-connector metrics and a queue-depth sparkline.
- **Docs** — embedded `.md` files rendered server-side with goldmark; sidebar nav generated from the file tree.

**Manual contents** (`/docs`): getting started; CAN interface setup (SocketCAN, USB adapters, vcan); concepts (sources/sinks/connectors, buffering and replay semantics); CEL filter cookbook (carried over from the current README); managing beacon via the API (for agents — points at `/api/docs`); troubleshooting.

The UI is a thin presentation layer: UI handlers call the same internal config service the JSON API uses.

## 7. Observability

OTel → Prometheus at `/metrics`:

- `beacon_connector_messages_total{connector, stage}` — `received` | `matched` | `delivered` | `filter_error` | `pruned`
- `beacon_connector_bytes_total{connector}` (post-filter, delivered)
- `beacon_connector_queue_depth{connector}`, `beacon_connector_queue_bytes{connector}`, `beacon_connector_lag_seconds{connector}`
- `beacon_source_state{source}`, `beacon_sink_state{sink}` (up/degraded/error); source message counters; reconnect and CAN error counters
- Per-sink connected-client gauges (serve-mode sinks)

UI rate tiles come from an in-process rolling-window rate calculator over the same counters, streamed over the admin SSE stream — the UI is useful without Prometheus.

## 8. Error handling

- Dialed sources and CAN buses: exponential backoff + jitter, health state transitions; component failure never crashes the process.
- CEL: compile errors → 422 at config write; eval errors → drop message + `filter_error` counter.
- CAN sink write failures: retry with backoff; queue absorbs up to limits; prune oldest first.
- Serve-mode slow clients: bounded per-client buffer; drop-and-disconnect on overflow (client replays via `Last-Event-ID` / `after`).
- Graceful shutdown: flush checkpoints, close bus clients and listeners, then exit.

## 9. Testing

- **Unit:** queue limit enforcement (count/age/bytes, whichever first), CEL filter behavior, config validation (references, paths, CEL), reconciler diffing, envelope JSON ↔ `pgn.Message` round-trip.
- **Integration:** n2k's `Replay` option and public `n2k.Bus` seam. End-to-end: canned N2K captures → source hub → connector (with filters) → in-test SSE/WS/TCP clients, asserting content and replay behavior. CAN sink: assert exact frames written to a fake bus.
- **e2e:** vcan-based test on Linux CI (as today).
- **UI:** golden-fragment render tests; htmx endpoints tested as plain HTTP handlers.

## 10. Delivery phases

Each phase leaves beacon in a working, releasable state:

1. **Core runtime** — envelope, bus manager, `Queue` + SQLite implementation, source/sink/connector runtimes, supervisor/reconciler; config seeded from a JSON file.
2. **Config API** — huma CRUD, filter validation, export/import, system endpoints; OpenAPI + embedded API reference; hot reconciliation wired to the API.
3. **Web UI** — htmx app: dashboard with live metrics, sources/sinks/connectors CRUD, OpenBridge/daisyUI theming, embedded assets.
4. **Docs & polish** — `/docs` manual content, README rewrite, examples as importable JSON config files, Dockerfile/compose/CI updates, e2e hardening.

## 11. Dependencies (target)

| Package | Purpose |
|---|---|
| `github.com/open-ships/n2k` (latest) | CAN sources/sinks, typed PGNs, encode/decode, address claiming |
| `github.com/google/cel-go` | connector filters |
| `modernc.org/sqlite` | CGO-free SQLite (config + queues) |
| `github.com/danielgtaylor/huma/v2` + `github.com/go-chi/chi/v5` | config API + OpenAPI generation |
| `github.com/yuin/goldmark` | /docs markdown rendering |
| `github.com/coder/websocket` | WS sink serving + WS source dialing |
| OTel + Prometheus exporter | metrics (as today) |
| htmx, daisyUI (Tailwind standalone CLI), `@oicl/openbridge-webcomponents` | UI (build-time / vendored assets) |

Dropped: viper (config file layer), brutella/can (superseded by n2k).
