# Concepts

## Sources, sinks, connectors

- A **source** decodes NMEA 2000 messages onto beacon from one endpoint: a
  CAN interface, USB-CAN adapter, HTTP stream, MQTT topic, capture file, or
  passive TCP/UDP NMEA 2000 gateway stream.
- A **sink** delivers messages somewhere: back onto a CAN bus, out over
  HTTP/TCP/MQTT to clients and brokers, or appended to a local file
  (`ndjson` or `candump` format), or onto a remote bus through a writable
  `tcp_gateway` sink.
- A **connector** is the only thing that actually moves data — it names
  exactly one source and one sink, an optional list of CEL filter
  expressions (see the filters page), a bridge mode, and its own durable buffer. A source
  or sink with no connector referencing it does nothing.

Multiple connectors can share the same source or the same sink (e.g. one
CAN source feeding both an SSE connector for a browser dashboard and a
socketcan connector mirroring onto a second bus), each with its own
filters and its own buffer.

## Bridge modes

- **`semantic`** (default) decodes a message and writes it through Beacon's
  claimed N2K client, so Beacon is the source device on the destination bus.
- **`transparent`** preserves the envelope's original priority, PGN, source,
  destination, and raw payload, including unknown PGNs. It currently requires
  a SocketCAN sink, rejects same-interface loops, and blocks management PGNs
  unless `forward_management` is explicitly enabled.
- **`observe`** filters, retains, checkpoints, and inspects messages without
  calling the configured sink.

## The envelope

Every message sent over SSE, WebSocket, TCP, MQTT, or NDJSON has exactly three
top-level keys:

| JSON field | Type | Meaning |
|---|---|---|
| `payload` | object | the verbatim JSON representation of the decoded `open-ships/n2k` Go struct |
| `metadata` | object | every field Beacon adds for routing, provenance, enrichment, and replay |
| `raw` | string or null | base64-encoded assembled CAN bytes, kept separate because they are data rather than metadata |

Every `payload` includes the n2k struct's complete `info` object. In n2k v1 this
includes timestamp, receive/transport timing, adapter ID, network ID,
direction, priority, PGN, source ID, and target ID. All remaining keys are the
original generated fields for that PGN, using n2k's JSON names and raw wire
values. Beacon does not strip, rename, or relocate any n2k field. A Go consumer
can unmarshal the nested object directly:

```go
var event struct {
    Payload  pgn.VesselHeading `json:"payload"`
    Metadata json.RawMessage   `json:"metadata"`
}
err := json.Unmarshal(document, &event)
```

For an unknown PGN, `payload` is the complete `pgn.UnknownPGN` JSON rather than
`null`.

`metadata` can contain:

| JSON field | Present when | Meaning |
|---|---|---|
| `id` / `connector` | queued/replayed messages | route sequence and connector identity |
| `observed_at` | always | when this Beacon ingress observed the message |
| `ingress` / `origin_ingress` | known | current and first-known ingress provenance |
| `device_name` / `device_name_hex` | address claim known | stable ISO Device NAME in numeric and JavaScript-safe hexadecimal forms |
| `pgn_name` / `variant` / `transport` | cataloged PGNs | catalog identity and N2K transport kind |
| `manufacturer_code` | proprietary PGNs | manufacturer discriminator decoded from the payload |
| `decode` | always | decoded/unknown status and catalog completeness details |
| `physical` | decoded numeric fields | additive unit-scaled values; payload raw ticks stay unchanged |

`id` and `connector` appear only after a message passes through a connector
buffer. Top-level `raw` is original for unknown PGNs and a canonical
re-encoding for decoded PGNs.

## Source and sink stream inspector

Each source and sink overview has a **Stream contents** panel. It starts
stopped and captures only messages that arrive after **Start**:

- A source panel observes envelopes at that source's received boundary.
- A sink panel observes envelopes after successful or accepted delivery to
  that sink.
- **Stop** closes the live preview while keeping the current capture in the
  browser. Start and Stop replace one another so only the currently relevant
  action is shown. **Clear** removes that local capture.

The optional **CEL filter** beside **Start** is applied by Beacon before
messages reach the browser. It uses the same `msg` shape as connector filters,
for example `msg.pgn == 129026` or
`msg.source == 11 && msg.decode_status == "decoded"`.
The filter remains editable while streaming. A valid change reconnects the
best-effort preview with the new expression without clearing captured rows; an
invalid change leaves the current stream filter active.

The inspector is best-effort and non-blocking: it never consumes a connector's
durable queue or slows vessel traffic when the browser cannot keep up. The
captured count continues for the full browser session while the panel keeps the
latest 200 messages in the current browser tab for display, copy, and export.

**JSONL** shows one compact, verbatim nested `payload` per line, ready to
unmarshal into the corresponding n2k Go struct. **CAN bytes** shows one
top-level `raw` payload as hexadecimal per line. **Export JSONL** downloads the
captured payload lines oldest first. **Export CAN** downloads one uppercase
hexadecimal payload per line; the line boundary preserves the message boundary
that would be lost by concatenating the bytes into one binary blob. Click any
JSON key or value to inspect a relevant CEL filter and optionally apply it.
**Copy stream** copies every retained line in arrival order using the active
JSONL or spaced-hex CAN representation.

## Buffering and pruning

Each connector has its own durable, per-connector buffer (backed by SQLite,
survives a beacon restart) that a slow or disconnected sink drains from
independently — a fast source never blocks on a slow sink. Buffer limits
are configured per connector:

| Limit | Meaning |
|---|---|
| `max_messages` | keep at most this many messages |
| `max_age` | keep messages for at most this long after local admission |
| `max_bytes` | keep at most this many logical bytes of canonical Envelope JSON |

Defaults are independent, not an either/or fallback. An omitted or zero
`max_messages` becomes 10,000 and an omitted or zero `max_bytes` becomes
64 MiB, even when another limit such as `max_age` is present. Zero `max_age`
means no age guard. Pruning enforces every effective limit.

Age is based on this Beacon's local `observed_at`/admission clock, not the NMEA
2000 wire timestamp carried inside `payload.info`. Software-facing remote
sources reset `observed_at` when the Envelope reaches this appliance, and a
legacy Envelope without it falls back to its local queue-append time. A stale
or incorrect upstream clock therefore cannot make a newly admitted message
expire immediately.

The live queue totals are maintained separately from retained Envelope rows,
so UI and metrics reads do not continuously aggregate the full history.
Retention pruning can remove pending delivery when a sink falls behind; size
all limits explicitly when the connector route must survive a longer outage.

`queue_depth` and `queue_bytes` mean **pending delivery after the connector
checkpoint**. `retained_depth` and `retained_bytes` include acknowledged rows
kept for replay. The metrics response also exposes checkpoint/tail, oldest
pending time, configured storage headroom, delivery class, route state/error,
drops, and per-stage totals. If retention pruning removes a message that was
still pending delivery, `pending_pruned` records that loss separately from
ordinary retained-history pruning.

Per-source traffic, decode, addressing, and timing counters include every
message. The more expensive decoded-field and raw-byte distributions are
bounded diagnostics sampled at most once per second for each source, PGN, and
address stream; their sample counts and availability percentages use that
diagnostic sample set. Rich diagnostics keep at most 256 PGN/sender streams per
source and 512 across the process. At either capacity, a novel stream still
contributes to exact source totals but its rich state is omitted; streams idle
for more than six hours become eligible for reclamation on a minutely admission
sweep.

Each tracked stream retains at most 32 decoded fields, 32 missing-field names,
and one latest decoded payload up to 8 KiB. An oversized latest payload body
is omitted rather than truncated into invalid JSON; the canonical Envelope is
unchanged. Raw diagnostics retain at most 256 bytes, analyze the first 32, and
bound their distributions to 16 lengths, 16 distinct values per byte offset,
32 fingerprints, and four samples. Truncation and overflow counters make every
omission visible.

Those rich diagnostics remain available in the source overview and through
MCP. Their higher-cardinality per-PGN Prometheus series are disabled by default
to reduce collection work and series count on constrained hosts. Enable them
explicitly when needed:

```json
{
  "settings": {
    "observability": {
      "prometheus_source_details": true
    }
  }
}
```

The ordinary per-source, connector, queue, and component metrics remain enabled.
MCP `get_source_metrics` reports `capacity` for each source with
`tracked_streams`, `stream_limit`, `global_tracked_streams`,
`global_stream_limit`, `messages_omitted`, `streams_expired`, and
`device_names_omitted`, so an operator can distinguish deliberate diagnostic
bounding from lost Envelope traffic.

## Storage budgets and maintenance

`settings.resources` sets process-wide physical storage budgets. Omission uses:

| Setting | Default | Purpose |
|---|---:|---|
| `max_database_bytes` | 1 GiB | hard SQLite main-database page ceiling |
| `database_reserve_bytes` | 128 MiB | headroom within that ceiling for config, indexes, inventory, and SQLite overhead |
| `max_file_store_bytes` | 2 GiB | aggregate configured allocation across file sinks |

Configuration validation adds every connector route's *effective*
`max_bytes`, including the 64 MiB default, and rejects a document when the sum
exceeds `max_database_bytes - database_reserve_bytes` (896 MiB with defaults).
It separately adds `max_file_bytes × max_files` for every file sink and rejects
a sum above `max_file_store_bytes`. These are allocation checks: neither the
128 MiB database reserve nor the 2 GiB file budget is preallocated on disk.
Custom database ceilings must be between 64 MiB and 64 GiB; the effective
reserve must remain below the ceiling, and the file-store budget cannot exceed
64 GiB.

SQLite enforces `max_database_bytes` through `max_page_count`, so the main
database file cannot grow beyond its page-rounded ceiling. The WAL is a
separate file: Beacon targets at most 16 MiB of retained WAL and enables
automatic checkpoints, but SQLite may transiently exceed the target during an
active transaction. Leave additional free filesystem space for the WAL,
filesystem metadata, temporary files, and operational recovery.
Lowering `max_database_bytes` below the database's existing page high-water
mark cannot take effect online; compact offline first, then apply the lower
ceiling.

Every six hours Beacon runs bounded online maintenance: a passive WAL
checkpoint followed by an incremental vacuum of at most 1,024 pages, with a
30-second attempt timeout. It never runs a full `VACUUM` while serving. A
database created before incremental vacuum was enabled needs one explicit
offline compaction before deleted pages can return to the filesystem:

```bash
# Stop Beacon first; VACUUM can require roughly one extra database copy.
beacon compact --db beacon.db
```

### Sizing a disconnected interval

Measure the route's peak admitted Envelope rate and average canonical JSON
size, choose the longest supported outage, then apply a reserve:

```text
factor       = 1 + reserve_percent / 100
max_messages = ceil(rate_per_second × outage_seconds × factor)
max_bytes    = max_messages × average_envelope_bytes
max_age      = outage × factor
```

`beacon size-buffer` calculates all three. For one 512-byte Envelope per
second, seven days disconnected, and 25% reserve:

```bash
beacon size-buffer --rate 1 --average-bytes 512 --outage 168h --reserve-percent 25
# max_messages: 756000
# max_bytes: 387072000
# max_age: 210h0m0s
```

The route hard bounds are 10,000,000 messages, 8 GiB, and 365 days. The CLI
rejects a plan outside them. After sizing every route, make the aggregate
database budget large enough for their logical bytes plus the configured
reserve and physical SQLite/filesystem overhead.

## Capacity and transport limits

These validation and admission caps keep configuration and untrusted network
input bounded:

| Boundary | Limit |
|---|---:|
| Sources / sinks / connector routes | 32 / 32 / 64 |
| One entity ID / display name | 128 bytes / 256 bytes |
| One endpoint URL, address, interface, port, HTTP path, or file path | 8 KiB |
| One MQTT topic | 4 KiB |
| Header name / header value | 256 bytes / 8 KiB |
| Aggregate authored Source/Sink/Connector text | 256 KiB |
| Configured headers per source or sink | 32 |
| CEL filters per connector route | 32 |
| One CEL filter expression | 8 KiB |
| File count per file sink, active file included | 128 |
| Uncommissioned inventory history | 1,024 Device NAMEs |
| Rich diagnostic PGN/sender streams per source / process | 256 / 512 |
| Diagnostic decoded fields / missing names per stream | 32 / 32 |
| Diagnostic latest payload / retained raw / analyzed raw | 8 KiB / 256 bytes / 32 bytes |
| Diagnostic lifecycle events per source / persisted total | 100 / 1,000, with 4 KiB per document |
| Preview subscribers per source or sink / channel buffer | 8 / 8 Envelopes |
| One serialized preview document | 32 KiB |
| Expanded gzip replay / simultaneous expansions | 128 MiB / 2 (256 MiB aggregate) |
| Remote MQTT/SSE/WebSocket encoded Envelope | 256 KiB |
| Remote decoded `payload` JSON / raw NMEA 2000 bytes | 128 KiB / 1,785 bytes |
| Remote `physical` fields / missing-field names | 256 / 256 |
| One remote metadata string or field name/unit/quantity | 1 KiB |

Configuration text limits count UTF-8 bytes, not displayed characters. The
256 KiB aggregate charges every operator-authored string in Source, Sink, and
Connector values—including IDs, names, endpoint fields, headers, MQTT topics,
and CEL expressions—but not fixed JSON syntax or numeric/boolean settings.
The per-field and aggregate limits are both enforced.

The uncommissioned inventory cap evicts the oldest unapproved observations.
Device NAMEs in an operator-approved commissioning baseline are not subject to
that cap.

Source lifecycle diagnostics retain 100 events per source, at most 1,000
persisted events overall, and no more than 4 KiB in one persisted document.
Each source or sink admits at most eight simultaneous preview subscribers,
each with an eight-Envelope channel. Serialized preview documents larger than
32 KiB are omitted. Preview delivery is non-blocking, so a slow browser loses
preview messages instead of applying backpressure to vessel traffic. Source and
sink snapshots expose `preview_documents_omitted`; the source MCP capacity
object exposes the same count. These limits never alter the canonical Envelope
on a Connector route.

The admin listener, shared data HTTP listener, and configured plain-TCP sink
listeners share a maximum of 128 accepted connections, bounding their accepted
socket descriptors across all of those ports.
Both servers allow at most 64 KiB of headers, wait at most five seconds for
headers and 30 seconds for a request read, and close idle HTTP connections
after 90 seconds. Typed REST request bodies and MCP POST bodies are capped at
1 MiB; MCP is stateless and retains no session between requests.

Each configured SSE, WebSocket, or plain TCP sink also admits at most 32
clients of its own. Those per-sink caps are nested inside the shared 128-client
allowance. These are network-connection limits, not a promise that the whole
process uses only 128 descriptors:
listeners, SQLite, CAN/serial endpoints, and outbound connections require
additional file descriptors.

## Replay

A connector's buffer is what makes reconnecting clients able to catch up
instead of just losing whatever happened while they were away. Replay is
identified by a composite `connector:seq` event id — necessary because one
sink can serve more than one connector at once, so a single incrementing
counter wouldn't be enough to say where each connector's client left off.

- **SSE**: the browser's native `EventSource` reconnect sends back whatever
  `id:` value the last event carried as the `Last-Event-ID` header
  automatically. The same cursor can also be supplied explicitly as an
  `?after=` query parameter — useful for a non-browser client. Either form
  accepts a comma-separated list of `connector:seq` pairs when the sink
  serves more than one connector, e.g. `?after=nav:104,engine:57`.
- **WS**: the same `?after=` query parameter (comma-separated for multiple
  connectors); there's no `Last-Event-ID` equivalent for WebSocket, so a
  client that cares about resuming needs to track and resend its own
  cursor.
- **TCP**: live-only. A `tcp` sink has no replay at all — a client that
  disconnects loses whatever was broadcast while it was gone.
- **MQTT**: no Beacon-side consumer replay. The Connector route does retain
  Pending delivery while the broker is unavailable and retries QoS 1 publishes
  until PUBACK; subscribers must tolerate duplicates.

A connection carrying **no** cursor at all gets no replay: it streams live
from the moment it connects. Replay happens only for the connectors a
cursor explicitly names — to pull everything a connector's buffer still
holds, ask for everything after sequence zero: `?after=<connector>:0`.

Each configured SSE, WebSocket, or TCP sink admits at most 32 concurrent
clients. Excess SSE or WebSocket requests receive `503` with
`Retry-After: 5`, and excess TCP connections are closed. TCP sink connections
are receive-only; sending data closes them.

## Delivery guarantees

Delivery guarantees differ by sink kind:

| Delivery class | Sinks / mode | Connector checkpoint advances when... |
|---|---|---|
| Confirmed | SocketCAN, USB-CAN, file, TCP gateway, HTTP POST, MQTT, PostgreSQL, null | The write/batch succeeds, HTTP returns 2xx, MQTT receives broker PUBACK, or null intentionally accepts it |
| Resumable | SSE, WebSocket | The Envelope is available in the route's retained replay stream |
| Best effort | Plain TCP | Dispatch completes; downstream receipt is unknown |
| Observe only | `observe` bridge mode | Local inspection completes without calling the sink |

Confirmed sink delivery, remote-source reconnect, and physical NMEA-endpoint
reconnect retry continuously until success or shutdown. Their exponential
ceiling starts at 250 ms and doubles to one minute; equal jitter selects each
actual delay between half and all of that ceiling. SSE/WebSocket dial, TLS, and
response-header phases are each bounded to 15 seconds while established streams
remain long-lived.
Durable queue-append failures use the same one-minute cap from a 100 ms initial
ceiling. This reduces CPU, log, and network pressure during a multi-day outage
without requiring an operator action to resume delivery. Remote-source,
physical-bus, and MQTT connection history resets only after 30 seconds of
stable connectivity, so handshake/drop flapping continues to back off. Valid HTTP `Retry-After` may
extend a delivery delay but never shortens it and is capped at one minute.

- **CAN sinks** (`socketcan`, `usbcan`, and `tcp_gateway`, which transmits
  onto a remote bus through a Yacht Devices or Actisense TCP WiFi gateway
  with the same semantics) confirm each message that can be
  re-encoded onto the bus: a push failure is retried with exponential
  backoff (250 ms initial ceiling, doubling up to a one-minute cap) until it
  succeeds or the connector stops — the connector's buffer absorbs the
  backlog meanwhile rather than dropping anything. This is at-least-once
  delivery for encodable messages: a message can be redelivered after a
  restart landed between a push succeeding and its checkpoint being
  durably saved, but it is never silently dropped while the sink is
  reachable and the message is encodable. An envelope beacon cannot
  re-encode for CAN transmission (most commonly a PGN with no cataloged
  decoder) is the one exception: it's **skipped**, not retried — counted
  under the connector's `skipped` message stage, with the cursor advancing
  past it exactly like a successful delivery, so one unrecognized PGN
  can't wedge the connector. HTTP POST/streaming, TCP, and MQTT sinks still deliver these messages —
  see the `raw` field note above.
- **HTTP POST sinks** (`http_post`) send JSON arrays of canonical envelopes to
  an HTTP(S) endpoint. `batch_size` is a maximum count: a short batch is sent
  immediately rather than waiting to fill. The entire batch remains pending
  until the endpoint returns 2xx; timeouts, redirects, transport failures, and
  every non-2xx response retry with the connector's bounded backoff. A valid
  `Retry-After` delta-seconds or HTTP-date value becomes the minimum next
  delay. Requests
  carry a deterministic `Idempotency-Key` for receiver-side deduplication,
  making the route at-least-once even when the response is lost after the
  receiver commits the batch. `request_timeout` bounds each attempt, and
  arbitrary headers support API keys, bearer tokens, and other static auth.
  Optional `gzip` compression sets `Content-Encoding: gzip` without changing
  the envelope-count meaning of `batch_size`.
  HTTPS uses the host trust store and verifies certificates normally.
- **PostgreSQL sinks** (`postgres`) insert query-friendly columns and the full
  canonical envelope into PostgreSQL in atomic batches. A successful statement
  confirms the batch. Retries use the primary key
  `(observed_at, connector_id, sequence)` with `ON CONFLICT DO NOTHING`, so a
  lost acknowledgement does not duplicate committed rows. The same schema is
  valid as a TimescaleDB hypertable partitioned on `observed_at`. Beacon can
  create the table and indexes automatically; when automatic creation is off,
  the sink editor shows copy-ready PostgreSQL or TimescaleDB DDL and Beacon
  keeps verifying until the operator-created table appears.
- **HTTP/TCP streaming sinks** (`http_sse`, `http_ws`, `tcp`) broadcast to
  clients connected at the moment. SSE/WS clients can recover anything they
  missed via replay (above), bounded by the Connector route's buffer limits;
  TCP consumers cannot.
- **MQTT sinks** publish at QoS 1 and confirm only after broker PUBACK. That is
  broker acceptance, not subscriber receipt. A PUBACK lost after acceptance
  causes a retry, so delivery is at-least-once and duplicate-tolerant consumers
  are required. MQTT library auto-reconnect is disabled; every broker connection
  owns a fresh client generation. Connection loss atomically invalidates that
  generation and its outstanding Push, and retry uses a newly constructed
  client. Only an actual PUBACK, not a library replay handoff, advances the
  Connector checkpoint.
- **File sinks** (`file`) confirm each write the same way CAN sinks confirm
  a push: a write or flush failure is retried with the connector's backoff
  until it succeeds, so nothing is silently dropped while the disk stays
  writable — at-least-once delivery. In `ndjson` format every envelope is
  written as one JSON object per line; in `candump` format an envelope with
  no raw CAN bytes (the same "undecodable PGN" case CAN sinks skip — see
  the `raw` field note above) is **skipped**, not retried, and a payload too
  large for the fast-packet protocol to re-fragment (over 223 bytes) is
  skipped too. Fast-packet PGNs are re-fragmented into wire-accurate
  NMEA 2000 fast-packet frames — the same frames that would actually appear
  on the CAN bus — so a `candump` log can be replayed with the `can-utils`
  package's `canplayer`: `canplayer -I nav.log vcan0=<connector-id>` maps
  the connector id each line carries in place of an interface name
  (candump's token field) onto a real or virtual interface (`vcan0` here)
  to send the frames on. Fast-packet detection uses the PGN catalog; for a
  PGN the catalog doesn't know, beacon falls back to payload size (over 8
  bytes → fast-packet), so a small-payload message on an uncataloged
  proprietary fast-packet PGN logs as a single frame rather than a
  fast-packet sequence. The active file rotates to `<path>.1` (and
  `.1`→`.2`, etc., oldest dropped) *before* the next complete record would
  exceed `max_file_bytes`; `max_files` counts the active file plus its rotated
  backups and cannot exceed 128. Defaults are 100 MiB and five total files. A
  single encoded record larger than `max_file_bytes` is skipped and
  checkpointed rather than creating an oversized file or retrying forever. On
  startup, Beacon rotates an oversized active file and removes rotated files
  that exceed a newly lowered byte/count limit, logging each removal. The sum
  of configured `max_file_bytes × max_files` allocations across file sinks
  must fit `settings.resources.max_file_store_bytes` (2 GiB by default). A
  short write
  from a full disk (`ENOSPC`) can leave a torn trailing line or, once the
  condition clears and the retried write succeeds, a logically-equivalent
  resend of that one message (for fast-packet lines the resend carries a
  fresh sequence number, so it isn't byte-identical) — both expected under
  this delivery model, not corruption.
- **Null sinks** (`null`) accept every message at the confirmed boundary,
  record the same connector and sink message, byte, rate, and stream-inspector
  statistics as another confirmed sink, and then intentionally discard it.
  They have no endpoint-specific configuration and no external side effects.

## Hot apply

Every configuration write (`PUT`/`DELETE` through the API, and therefore
through the UI too) takes effect immediately: the write is validated,
persisted, and then reconciled against the running system in the same
request — there is no separate "apply" step and no restart of beacon
itself. Reconciliation only stops and restarts whatever actually changed:
a connector also restarts if the source or sink it references changed
(its running instance holds a direct reference to the old one), even
though the connector's own configuration didn't change.

Because a restart is stop-then-start rather than instantaneous, there's a
brief window where an enabled component is momentarily absent from the
supervisor's live status. The dashboard renders that window as a
"restarting" chip rather than treating a momentarily-missing component as
an error; `/health` and `/api/v1/health` don't have this state — a
component there is only ever `up`, `degraded`, or `error` (or absent,
if disabled).

Hot apply is only the fast path. A lifecycle-owned convergence loop also
compares persisted desired configuration with running components on a jittered
30-second cadence. If construction or reconciliation fails, it retries from a
one-second delay up to one minute, with each attempt bounded to 15 seconds. A
committed configuration therefore keeps converging after a request disconnect,
temporary device absence, or startup failure without another write or a
process restart. Already-running components that report a remote-connectivity
outage own their reconnect loop and are not repeatedly restarted.
