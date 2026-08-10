# beacon

[![CI](https://github.com/open-ships/beacon/actions/workflows/test.yaml/badge.svg)](https://github.com/open-ships/beacon/actions/workflows/test.yaml)
[![Go version](https://img.shields.io/github/go-mod/go-version/open-ships/beacon)](go.mod)
[![License](https://img.shields.io/github/license/open-ships/beacon)](LICENSE)

An offline-first NMEA 2000 gateway for moving vessel data between CAN,
USB-CAN, HTTP streams and POST APIs, WebSocket, TCP, MQTT, PostgreSQL/TimescaleDB,
capture files, and marine gateways.
Beacon adds CEL filtering, durable per-route buffering, replay, live diagnostics,
and one REST API without requiring a cloud service.

## Start here

![Beacon dashboard showing a live Yacht Devices gateway feeding an SSE stream and voyage recorder](docs/images/beacon-dashboard-v1.2.0.png)

_A real Beacon instance routing live NMEA 2000 traffic. Sources, connector
routes, sinks, state, throughput, and pending delivery stay visible in one
view._

The quickest local preview needs no CAN hardware. Start Beacon, then open
[http://localhost:2112](http://localhost:2112):

```bash
git clone https://github.com/open-ships/beacon.git
cd beacon
go run ./cmd/beacon \
  --db beacon.db \
  --admin-address 127.0.0.1:2112 \
  --data-address 127.0.0.1:8080
```

This route requires Go 1.25.12 or newer. Prefer a container? The published
image provides the same no-hardware preview:

```bash
mkdir -p data
docker run --rm \
  -p 127.0.0.1:2112:2112 \
  -p 127.0.0.1:8080:8080 \
  -v "$(pwd)/data:/data" \
  ghcr.io/open-ships/beacon:latest
```

Once it is running:

- **Dashboard:** [localhost:2112](http://localhost:2112)
- **Onboard manual:** [localhost:2112/docs](http://localhost:2112/docs)
- **MCP agent reference:** [localhost:2112/mcp/info](http://localhost:2112/mcp/info)
- **Interactive API reference:** [localhost:2112/api/docs](http://localhost:2112/api/docs)
- **Health:** [localhost:2112/health](http://localhost:2112/health)

The UI starts empty on purpose. Add a source, a sink, and a connector route
from the dashboard, or import one of the tested configs in [`examples/`](examples/).
Every change is validated, persisted to SQLite, and applied immediately; there
is no separate configuration file to maintain and no restart step.

### See traffic without hardware

On Linux, a virtual CAN interface gives you a complete local route in a few
commands. Stop the preview above if it is still running, install `can-utils`,
then run:

```bash
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set vcan0 up

go run ./cmd/beacon \
  --db beacon-vcan.db \
  --seed examples/vcan-dev.json
```

In another terminal, connect the SSE consumer first:

```bash
curl -N http://localhost:8080/events
```

Then inject a frame from a third terminal:

```bash
cansend vcan0 18FF0001#0102030405060708
```

You now have a real route from `vcan0` through Beacon's durable connector
buffer to an SSE client. The source, route, counters, and sink state appear on
the dashboard within a couple of seconds.

> [!NOTE]
> `--seed` is applied only when the selected database has no configuration.
> Reusing a populated database safely keeps its current routes.

## Why Beacon

- **Offline by design.** The UI, manual, API reference, PGN catalog, storage,
  and runtime all work with no internet connection.
- **A legible routing model.** Every message moves through an explicit
  source → connector route → sink topology.
- **Independent durability.** Each connector route owns its own SQLite-backed
  buffer, checkpoint, limits, counters, and delivery boundary.
- **Controlled bridging.** Choose semantic re-origination, transparent
  wire-preserving forwarding, or observe-only inspection per route.
- **NMEA 2000 depth.** Beacon keeps raw bytes and wire values while adding PGN
  metadata, decoded fields, physical units, ingress provenance, stable Device
  NAMEs, device inventory, and commissioning reports.
- **Built for integration.** The UI is a thin layer over a typed REST API with
  an embedded OpenAPI 3.1 reference, RFC 7807 errors, health, and Prometheus
  metrics.
- **Hot configuration.** Valid writes are persisted and reconciled against the
  running graph in the same request; only affected components restart.

## The model

```text
source ──▶ connector route ──▶ sink
            │        │
            │        └─ delivery checkpoint + replay retention
            ├─ CEL filters
            ├─ bridge mode
            └─ durable SQLite buffer
```

A **source** reads NMEA 2000 messages onto Beacon. A **sink** makes messages
available somewhere else. A **connector route** is the only component that
moves data: it names exactly one source and one sink and owns the policy and
delivery state between them. Sources and sinks can be shared by any number of
routes, making fan-out and fan-in explicit rather than implicit.

### Supported endpoints

| Kind | Sources | Sinks | Notes |
|---|---|---|---|
| CAN | `socketcan`, `usbcan` | `socketcan`, `usbcan` | Physical NMEA 2000 buses; SocketCAN is Linux-native |
| HTTP | `http_sse`, `http_ws` | `http_sse`, `http_ws`, `http_post` | SSE/WS streaming and replay, or confirmed JSON-batch POST delivery over HTTP(S) |
| Gateways | `tcp`, `udp` | `tcp_gateway` | Yacht Devices RAW or Actisense gateway streams |
| Messaging | `mqtt` | `mqtt` | Topic-based ingestion and QoS 1 broker-confirmed publishing |
| Databases | — | `postgres` | Confirmed batch inserts into PostgreSQL or TimescaleDB |
| Files | `file` | `file` | Replay captures; write rotating `ndjson` or `candump` logs |
| Plain TCP | — | `tcp` | Live NDJSON listener for backend consumers |
| Utility | — | `null` | Accept, count, and intentionally discard routed events |

Compressed file replays are content-detected and expanded to a temporary copy.
Each expansion is capped at 128 MiB and only two copies (256 MiB aggregate)
may exist concurrently; decompress larger captures once and configure the
resulting file directly.

### Bridge modes

| Mode | Behavior | Typical use |
|---|---|---|
| `semantic` | Decodes and re-originates messages through Beacon's persistent N2K appliance identity | Normal routing and protocol conversion |
| `transparent` | Preserves priority, PGN, source, destination, and raw payload, including unknown PGNs | Controlled bridge between two SocketCAN segments |
| `observe` | Filters, retains, checkpoints, and measures without writing to the sink | Inspection, recording policy, and dry-run validation |

`transparent` mode currently requires a SocketCAN sink, rejects a source and
sink on the same interface, and blocks address-claim and network-management
PGNs unless `forward_management` is explicitly enabled.

### Filtering with CEL

Filters are [Common Expression Language](https://cel.dev) expressions evaluated
against the canonical `msg` envelope. A route's expressions use AND semantics;
use `||` inside one expression for OR.

```cel
msg.pgn in [127250, 129025, 129026, 129029]
msg.priority <= 3
has(msg.device_name) && msg.device_name == 123456789
```

![Beacon connector editor with a validated live CEL navigation filter](docs/images/beacon-connector-editor-v1.2.0.png)

The connector editor validates filters as you type and provides completions for
envelope fields, PGNs, decoded payload fields, and CEL helpers. The API exposes
the same compiler without persisting a route:

```bash
curl -X POST http://localhost:2112/api/v1/filters/validate \
  -H 'Content-Type: application/json' \
  -d '{"filters":["msg.pgn == 127250","msg.priority <= 3"]}'
```

See the [filter cookbook](internal/ui/docs/04-filters.md) for payload examples,
null handling, physical values, and reusable policies.

## Delivery, buffering, and replay

Beacon decouples source throughput from sink availability. After filtering, a
message is written to the connector route's SQLite buffer; that route advances
its own checkpoint only when it reaches the sink's declared delivery boundary.
A slow or disconnected sink does not block another route using the same source.

| Delivery class | Sinks / mode | Checkpoint advances when… |
|---|---|---|
| Confirmed | SocketCAN, USB-CAN, file, TCP gateway, HTTP POST, MQTT, PostgreSQL, null | The write or database batch succeeds, HTTP POST returns 2xx, MQTT receives broker PUBACK, or the null sink accepts the message |
| Resumable | SSE, WebSocket | The message is available in the replayable stream |
| Best effort | TCP | Dispatch completes; downstream receipt is not claimed |
| Observe only | `observe` bridge mode | Local inspection completes without a sink write |

Each route bounds retained data with `max_messages`, `max_age`, and
`max_bytes`. Omitted limits are filled independently: every route gets a
10,000-message guard and a 64 MiB logical canonical-Envelope guard unless each
is explicitly set. `max_age` remains optional and measures time held by this
Beacon from local `observed_at`/queue admission, never the upstream wire
timestamp. Metrics distinguish **pending delivery** after the checkpoint from
**retained history** that has already been acknowledged but remains replayable.
Queue totals are maintained in a separate aggregate row, so reading live
metrics does not repeatedly scan or deserialize retained Envelopes. Any cap can
prune pending delivery when a sink falls behind; size all three dimensions for
the longest supported outage.

SSE clients resume with `Last-Event-ID`; SSE and WebSocket clients can also use
`?after=<connector>:<sequence>`. A sink shared by multiple routes accepts a
comma-separated cursor such as `?after=nav:104,engine:57`. TCP is live-only.
MQTT has no Beacon-side consumer replay, but a Connector route keeps delivery
pending and retries until the broker acknowledges its QoS 1 publish.
Each SSE, WebSocket, or TCP sink admits at most 32 concurrent clients. Excess
SSE or WebSocket requests receive `503` with `Retry-After: 5`, and excess TCP
connections are closed. TCP sink connections are receive-only.

Confirmed writes use continuous, jittered exponential retry and at-least-once
semantics. Remote-source, physical NMEA-endpoint, and sink-delivery retry use a
250 ms initial ceiling; durable queue writes use 100 ms. All double to a
one-minute ceiling and continue until success or shutdown, avoiding a tight
loop through a long outage. SSE/WebSocket dial, TLS, and response-header phases
are each bounded to 15 seconds without imposing a lifetime on an established
stream. HTTP POST retries carry a deterministic `Idempotency-Key` derived from
the sink, connector route, and queue range so a receiver can deduplicate an
ambiguous resend. A valid HTTP `Retry-After` response header raises the next
delay when the receiver needs more time, but is capped at one minute so a stale
or hostile value cannot park delivery indefinitely. Source and MQTT reconnect
history resets only after 30 seconds of stable connectivity; handshake/drop
flapping therefore continues backing off. MQTT confirmation is equally precise:
PUBACK proves broker acceptance, not subscriber receipt. A
lost acknowledgement can cause a duplicate publish. Automatic MQTT reconnect
is disabled: every loss invalidates the current client generation, and retry
uses a newly constructed client so an old in-flight token cannot advance the
Connector checkpoint.
An envelope that cannot be encoded for semantic CAN
delivery is counted as skipped and checkpointed so an unknown PGN cannot wedge
a route.
Transparent SocketCAN delivery preserves that unknown PGN losslessly. A
`null` sink accepts each message at this same confirmed boundary, records the
normal connector and sink statistics, and then intentionally discards it.

For the precise retention, pruning, replay, retry, and file-rotation contract,
read [Concepts](internal/ui/docs/03-concepts.md) and
[ADR 0001](docs/adr/0001-route-delivery-boundaries.md).

### Storage budgets and outage sizing

Omitting `settings.resources` applies these appliance-wide defaults:

| Setting | Default | Validation boundary |
|---|---:|---|
| `max_database_bytes` | 1 GiB | Physical SQLite main-database page ceiling |
| `database_reserve_bytes` | 128 MiB | Space inside that ceiling withheld from logical route allocations |
| `max_file_store_bytes` | 2 GiB | Sum of every file sink's `max_file_bytes × max_files` allocation |

The sum of every connector route's effective `max_bytes` must fit within
`max_database_bytes - database_reserve_bytes` (896 MiB by default). The reserve
is validation headroom for configuration, indexes, inventory, and SQLite
overhead; it is not preallocated filesystem space. The 1 GiB page ceiling
applies to the main database file. The separate WAL has a 16 MiB retention
target and automatic checkpoints but can transiently exceed that target during
an active transaction, so leave filesystem headroom beyond the configured
database ceiling.

Beacon performs a passive WAL checkpoint and bounded incremental vacuum every
six hours. It never runs a full `VACUUM` online. To reclaim a legacy or
high-water database, stop Beacon and run `beacon compact --db beacon.db`; allow
temporary free space for approximately one additional database copy. Compact
before lowering `max_database_bytes` below the current page high-water mark.

A file sink defaults to 100 MiB per file and five files including the active
file. `max_files` cannot exceed 128. Beacon rotates *before* a record would
cross `max_file_bytes`, skips and checkpoints a single encoded record larger
than that limit, and on startup rotates/removes existing active or backup files
that no longer fit after limits shrink. Aggregate configured file allocations
must fit the 2 GiB appliance budget even if the files do not yet exist.

Size a route from observed peak Envelope rate and average canonical JSON size:

```text
factor       = 1 + reserve_percent / 100
max_messages = ceil(rate_per_second × outage_seconds × factor)
max_bytes    = max_messages × average_envelope_bytes
max_age      = outage × factor
```

The CLI performs the same calculation. For one 512-byte Envelope per second,
a seven-day outage, and 25% reserve:

```bash
beacon size-buffer --rate 1 --average-bytes 512 --outage 168h --reserve-percent 25
# max_messages: 756000; max_bytes: 387072000; max_age: 210h0m0s
```

These are safety budgets, not measured vessel capacity. Run the
[vessel release gates](docs/vessel-release-gates.md) and then qualify the
actual hardware with representative traffic, outage duration, storage, and
power-loss testing.

### Ingress and connection bounds

Remote MQTT, SSE, and WebSocket sources accept at most a 256 KiB encoded
Envelope. Within it, decoded `payload` JSON is capped at 128 KiB, raw NMEA 2000
data at 1,785 bytes, `physical` and missing-field collections at 256 entries
each, and individual metadata strings at 1 KiB. Invalid remote Envelopes are
discarded before filtering, diagnostics, or durable storage.

Configuration is capped at 32 sources, 32 sinks, and 64 connector routes. A
source or sink may configure at most 32 headers; a connector route may contain
at most 32 CEL filters of 8 KiB each. Names are capped at 256 bytes, endpoint
and header-value text at 8 KiB, MQTT topics at 4 KiB, header names at 256 bytes,
and all authored Source/Sink/Connector strings together at 256 KiB. Beacon
retains at most 1,024 uncommissioned Device NAME records, while operator-approved
commissioning baseline entries are never evicted by that history cap.

The admin listener, data HTTP listener, and configured plain-TCP sink listeners
share a 128 accepted-connection budget. The HTTP servers also enforce a 64 KiB
header limit, five-second header deadline, 30-second read deadline, and
90-second idle timeout. Typed REST request bodies and MCP POST bodies are
capped at 1 MiB, and MCP keeps no cross-request session. Each SSE, WebSocket,
or plain TCP sink also caps clients at 32 inside the shared allowance. These
limits bound accepted network socket descriptors; listeners, SQLite,
CAN/serial endpoints, and outbound connections still use additional file
descriptors.

### HTTP POST sink

An `http_post` sink sends a JSON array of canonical envelopes to an `http://`
or `https://` URL. `batch_size` is the maximum number of envelopes per request;
short batches send immediately. A 2xx response confirms the whole batch and
advances the connector checkpoint. Network errors, timeouts, redirects, and
non-2xx responses leave the batch pending and use the connector's bounded
retry backoff. Valid `Retry-After` delta-seconds and HTTP-date values are
honored as a minimum delay.

```json
{
  "id": "telemetry-api",
  "name": "Telemetry API",
  "type": "http_post",
  "enabled": true,
  "url": "https://api.example.com/v1/envelopes",
  "batch_size": 250,
  "request_timeout": "15s",
  "gzip": true,
  "headers": {
    "Authorization": "Bearer token",
    "X-API-Key": "secret"
  }
}
```

`batch_size` defaults to 100 and is capped at 1,000. `request_timeout`
defaults to 10 seconds. Authentication is expressed as request headers, so API
keys, bearer tokens, and pre-encoded Basic credentials all work. Beacon uses
the host trust store for HTTPS, does not follow redirects (which avoids
forwarding credentials to an unexpected target), and considers every non-2xx
response retryable rather than silently dropping buffered envelopes.
Set `gzip` to send the JSON body compressed with `Content-Encoding: gzip`;
batch size continues to count envelopes rather than bytes.
Header values are stored in Beacon's SQLite configuration and included in
configuration/API exports, so protect the database and exported files as
secrets.

Prometheus request telemetry is labeled by sink id, HTTP status, and payload
encoding. `beacon_sink_http_requests_total` and
`beacon_sink_http_payload_envelopes_total` count attempts (including retries);
payload-size, uncompressed-size, request-latency, and accepted `Retry-After`
values are exported as `beacon_sink_http_*` histograms. Successful connector
delivery counters remain separate, so an operator can distinguish receiver
attempts from confirmed batches.

### PostgreSQL and TimescaleDB sink

A `postgres` sink writes query-friendly envelope columns plus the complete
canonical envelope as `jsonb`. Each batch is one atomic statement; the route
checkpoint advances only after PostgreSQL accepts it. Retries use
`(observed_at, connector_id, sequence)` as an idempotency key, so an ambiguous
reconnect does not duplicate an already committed row.

```json
{
  "id": "telemetry-db",
  "name": "Telemetry database",
  "type": "postgres",
  "enabled": true,
  "url": "postgresql://beacon:replace-me@db.local:5432/vessel?sslmode=require",
  "table": "telemetry.envelopes",
  "batch_size": 250,
  "write_timeout": "15s",
  "auto_create_table": true,
  "timescaledb": true
}
```

`table` defaults to `public.beacon_envelopes`, `batch_size` to 100, and
`write_timeout` to 10 seconds. Enable `timescaledb` to convert the table into
a hypertable partitioned on `observed_at`; the TimescaleDB extension must
already be installed in the target database.

The sink editor includes an **Automatically create and verify the table**
checkbox. Clear it for operator-managed migrations: the editor reveals the
exact PostgreSQL or TimescaleDB DDL with a **Copy DDL** action. Beacon verifies
that schema in the background and recovers without a restart after the DDL is
applied. Connection URLs can contain credentials and are included in
configuration/API exports, so protect the SQLite configuration database and
exports as secrets; overview pages redact the password.

## The envelope

MQTT, SSE, WebSocket, TCP, NDJSON, each element in an HTTP POST batch, and the
PostgreSQL sink's `envelope` column receive exactly three top-level keys:

```json
{
  "payload": {
    "info": {
      "timestamp": "2026-07-25T12:00:00Z",
      "receivedAt": "2026-07-25T12:00:00Z",
      "adapterId": "socketcan:can0",
      "networkId": "can0",
      "direction": "received",
      "priority": 2,
      "pgn": 127250,
      "sourceId": 12,
      "targetId": null
    },
    "heading": 15708
  },
  "metadata": {
    "id": 42,
    "connector": "heading",
    "observed_at": "2026-07-25T12:00:00Z",
    "ingress": "can0",
    "pgn_name": "Vessel Heading",
    "decode": {"status": "decoded", "complete": true}
  },
  "raw": "XC9///////8="
}
```

`payload` is the verbatim JSON representation of the decoded
[`open-ships/n2k`](https://github.com/open-ships/n2k) Go struct, including every
exported `MessageInfo` field such as receive time, transport timing, adapter,
network, and direction. A consumer that knows the PGN can unmarshal `payload`
directly into the corresponding `pgn` type. The raw-tick values are unchanged.

`metadata` holds only what Beacon adds: queue and connector identity, ingress
provenance, stable Device NAME, catalog/decode details, and physical values.

`raw` is the assembled CAN payload as base64 bytes. It is top-level data, not
metadata.

Unknown PGNs still move through HTTP, TCP, MQTT, PostgreSQL, file, observe, and
transparent routes. Their `payload` is the complete `pgn.UnknownPGN` JSON and their original
bytes remain available at top-level `raw`. See
[ADR 0004](docs/adr/0004-keep-wire-values-canonical.md) for the compatibility
rationale and [Concepts](internal/ui/docs/03-concepts.md#the-envelope) for the
complete field contract.

## Configuration

The entire desired state is one JSON document:

```json
{
  "sources": [
    {
      "id": "can0",
      "name": "Engine room bus",
      "type": "socketcan",
      "enabled": true,
      "interface": "can0"
    }
  ],
  "sinks": [
    {
      "id": "events",
      "name": "Browser feed",
      "type": "http_sse",
      "enabled": true,
      "path": "/events"
    }
  ],
  "connectors": [
    {
      "id": "navigation",
      "name": "Navigation data",
      "source_id": "can0",
      "sink_id": "events",
      "mode": "semantic",
      "filters": ["msg.pgn in [127250, 129025, 129026, 129029]"],
      "buffer": {"max_messages": 10000},
      "enabled": true
    }
  ],
  "settings": {
    "observability": {
      "prometheus_source_details": false
    },
    "resources": {
      "max_database_bytes": 1073741824,
      "database_reserve_bytes": 134217728,
      "max_file_store_bytes": 2147483648
    }
  }
}
```

`settings` is optional. Per-source and queue health metrics are always
available, while the higher-cardinality per-PGN Prometheus series are disabled
by default to keep collection inexpensive on constrained hosts. Set
`settings.observability.prometheus_source_details` to `true` only when that
diagnostic detail is needed. The `resources` values shown are also the defaults;
omit that block to use them unchanged.

Use it in whichever workflow fits:

```bash
# Seed an empty database at boot
beacon --db beacon.db --seed examples/navigation.json

# Replace or merge config on a running Beacon
curl -X POST 'http://localhost:2112/api/v1/config/import?mode=replace' \
  -H 'Content-Type: application/json' \
  --data-binary @examples/navigation.json

curl -X POST 'http://localhost:2112/api/v1/config/import?mode=merge' \
  -H 'Content-Type: application/json' \
  --data-binary @examples/engine-room.json

# Export a running Beacon
curl http://localhost:2112/api/v1/config/export > beacon-config.json
```

Entity-level CRUD is available at
`/api/v1/{sources,sinks,connectors}/{id}`. Deleting a source or sink still in
use returns `409`; structural and CEL errors return an RFC 7807 `422` without
changing the running configuration.

The CLI also provides offline `beacon export` and `beacon import` commands.
Do not use them against a database held open by a running Beacon; use the live
HTTP endpoints instead. Full examples and caveats are in
[`examples/README.md`](examples/README.md).

## API and operational surfaces

![Beacon's embedded OpenAPI 3.1 reference running locally](docs/images/beacon-api-reference-v1.2.0.png)

Beacon runs separate admin and data servers so a sink configuration cannot
take away the control surface used to repair it.

| Surface | Default location | Purpose |
|---|---|---|
| Dashboard | `:2112/dashboard` | Route graph, pending/retained state, bus diagnostics, device commissioning |
| Manual | `:2112/docs` | Offline getting started, CAN setup, concepts, filters, API, troubleshooting |
| MCP endpoint | `:2112/mcp` | Streamable HTTP tools for agent configuration, health, delivery, and source PGN metrics |
| MCP reference | `:2112/mcp/info` | Embedded connection guide, tool catalog, and call examples |
| REST API | `:2112/api/v1/...` | Configuration, live state, PGN catalog, inventory, commissioning |
| API reference | `:2112/api/docs` | Interactive, embedded OpenAPI 3.1 documentation |
| OpenAPI document | `:2112/api/openapi.json` | Machine-readable discovery for SDKs, scripts, and agents |
| Health | `:2112/health` | Rolled-up component health; mirrored at `/api/v1/health` |
| Metrics | `:2112/metrics` | Prometheus exposition, including per-source/sender PGN traffic, HTTP sink requests, and value distributions |
| SSE / WebSocket sinks | `:8080/<configured-path>` | Data and replay endpoints |
| TCP sinks | Configured listener address | Live-only NDJSON stream |

An MCP client can connect without a cloud relay or companion process:

```json
{
  "mcpServers": {
    "beacon": {"url": "http://127.0.0.1:2112/mcp"}
  }
}
```

The MCP server exposes tools to read the complete configuration, create or
update sources, sinks, and connector routes, delete each entity type, and read
health, delivery metrics, per-source PGN traffic metrics, or the exact latest
decoded payloads from a bounded, filterable set of sensor/PGN streams.
Rich-diagnostic calls return 16 matches by default, cap explicit requests at
32, and report `truncated` when filters should be narrowed. Connector responses expose both
authored and effective retention limits. It uses the same validation, SQLite
persistence, and hot reconciliation as the UI and REST API.
The server, tool schemas, and reference page are all embedded in the Beacon
binary; no internet connection, remote schema, CDN, or hosted MCP service is
required. The endpoint retains no cross-request MCP session state and caps each
POST body at 1 MiB.

Each source overview groups traffic by source address and PGN, including PGNs
Beacon cannot decode. It reports learned frequency and jitter, gaps and bursts,
last-seen age, addressing and decode outcomes, traffic share and estimated CAN
load, payload lengths, decoded-field quantiles/availability/rate-of-change, and
bounded raw-wire fingerprints, entropy, byte ranges, and change masks. Approved
baselines make missing streams, frequency drift, payload/decode changes, address
moves, and out-of-range values visible after a restart. The scrape-safe subset
is exported as `beacon_source_pgn_*` at `/metrics`; raw payloads and fingerprint
identifiers stay in the UI/MCP response to avoid unbounded Prometheus labels.
Traffic, decode, addressing, and timing counters are exact for every message;
decoded-field and raw-byte distributions are diagnostic samples taken at most
once per second per source/PGN/address stream. Rich state is capped at 256
streams per source and 512 process-wide; novel streams omitted at capacity
still contribute to exact source totals, and state idle for six hours is
reclaimed. Per stream, Beacon keeps at most 32 decoded fields, 32 missing-field
names, an 8 KiB latest decoded payload, 256 retained raw bytes (32 analyzed),
16 raw lengths, 16 distinct values per analyzed byte, 32 fingerprints, and four
raw samples. MCP reports local/global capacity, omitted/expired, and truncation
counters. The canonical Envelope is never truncated by diagnostic limits.
Source lifecycle diagnostics retain 100 events per source, at most 1,000
persisted events overall, and no more than 4 KiB in one persisted document.

Every source and sink overview also has a stopped-by-default stream inspector.
Start captures future source-received or sink-sent messages without consuming
connector queues or blocking routing; Stop freezes the current browser-local
capture. An optional CEL expression beside Start filters the server-side
preview using the same `msg` fields as connector filters and can be changed
while streaming without clearing captured rows. Clicking a JSON key or value
shows a usable CEL expression for that field. The inspector switches between
the verbatim decoded n2k JSON and assembled CAN payload bytes in hexadecimal,
with exactly one message per line in either view. Captures can be copied,
exported as n2k JSONL, or exported as one hexadecimal CAN payload per line,
preserving message boundaries. The captured counter tracks the entire browser
session even though only its latest 200 messages remain available for display,
copy, and export. Each source or sink admits at most eight simultaneous preview
subscribers, each with an eight-Envelope channel. Serialized preview documents
larger than 32 KiB are omitted. A slow inspector loses preview messages rather
than applying backpressure to vessel traffic; these limits never alter the
canonical Envelope on a Connector route.

The admin API also exposes the complete PGN and field catalog, best-effort
CAN/USB hardware discovery, stable Device NAME inventory, commissioning
baselines, and a machine-readable commissioning report.

Beacon does not currently provide built-in authentication or TLS. Bind the
admin server to loopback for local use, or place it behind the access controls
appropriate for the vessel network.

## Running in production

Prebuilt archives for Linux, macOS, and Windows on amd64 and arm64 are published
on [GitHub Releases](https://github.com/open-ships/beacon/releases). Physical
SocketCAN operation requires Linux; the other builds remain useful for USB-CAN,
remote streams, development, and administration.

Before qualifying a build for constrained or intermittently connected vessel
hardware, run the repeatable [vessel release gates](docs/vessel-release-gates.md).
They enforce an idle resource budget and exercise SQLite full-database and
abrupt-process recovery paths; the guide also identifies the hardware tests
that those automated proxies cannot replace.

| Flag | Default | Purpose |
|---|---|---|
| `--db` | `beacon.db` | SQLite configuration, appliance identity, inventory, and connector buffers |
| `--data-address` | `0.0.0.0:8080` | Bind address for HTTP sink endpoints |
| `--admin-address` | `0.0.0.0:2112` | Bind address for UI, API, MCP, health, and metrics |
| `--seed` | none | Config loaded into an empty database; ignored after configuration exists |
| `--log-level` | `info` | `debug`, `info`, `warn`, or `error` JSON logs |

For a Linux host with a configured CAN interface, the repository Compose file
uses host networking so the container can see that interface:

```bash
docker compose up
```

Or run the published image with persistent storage:

```bash
docker run --rm --network host \
  -v "$(pwd)/data:/data" \
  ghcr.io/open-ships/beacon:latest \
  --db /data/beacon.db
```

Host networking exposes host network interfaces but not USB device nodes. Pass
the appropriate serial device explicitly when using USB-CAN in a container.
The onboard [CAN setup guide](internal/ui/docs/02-can-setup.md) covers real and
virtual interfaces, permissions, bitrate, Docker, and troubleshooting.

## Architecture

Beacon is a single static Go binary with embedded UI/docs assets and a SQLite
database. Configuration follows one write path whether it begins in the UI,
REST API, seed file, or offline import:

```text
UI / REST / import
        │
        ▼
 validate structure + compile CEL
        │
        ▼
 persist desired config in SQLite
        │
        ▼
 supervisor reconciles changed runtimes

source runtime ─▶ filter ─▶ connector queue ─▶ delivery policy ─▶ sink runtime
                         └──── metrics / replay / checkpoint ────┘
```

Important design invariants:

- One persistent appliance NAME is reused across restarts and independently
  connected bus endpoints.
- Raw wire values remain canonical; physical values and catalog metadata are
  additive.
- Every connector route declares one bridge mode and one delivery class.
- Pending delivery and retained replay history are reported separately.
- The admin and data listeners fail independently.
- All configuration mutations pass through validation, persistence, and
  reconciliation in that order.

The vocabulary in [`CONTEXT.md`](CONTEXT.md) is the shared domain model. Design
decisions are recorded in [`docs/adr/`](docs/adr/), including
[delivery boundaries](docs/adr/0001-route-delivery-boundaries.md),
[appliance identity](docs/adr/0002-persist-one-appliance-identity.md),
[bridge modes](docs/adr/0003-separate-semantic-and-transparent-bridges.md),
and [canonical wire values](docs/adr/0004-keep-wire-values-canonical.md).

### Project layout

```text
cmd/beacon/       CLI entrypoint: serve, export, import
internal/
  app/            composition root and HTTP servers
  api/            typed REST API and embedded OpenAPI reference
  bus/            shared N2K clients per physical endpoint
  config/         validate → persist → reconcile write path
  connector/      subscribe → filter → queue → deliver route runtime
  filter/         CEL environment, compile, and evaluation
  identity/       persistent N2K appliance identity
  inventory/      Device NAME history and commissioning baseline
  model/          sources, sinks, connector routes, validation
  msg/            canonical NMEA 2000 envelope
  queue/          SQLite-backed per-route buffer and checkpoints
  sink/           CAN, HTTP, TCP, MQTT, PostgreSQL, file, gateway, and null delivery
  source/         CAN, HTTP, MQTT, file, and gateway ingestion
  stats/          live route counters
  supervisor/     desired-state runtime reconciliation
  ui/             embedded htmx UI and onboard manual
examples/         validated, importable starter configurations
tests/browser/    Playwright end-to-end tests
```

## Development

Common tasks use [just](https://just.systems):

```bash
just build          # local static binary
just test           # all Go tests
just test-race      # full suite with the race detector
just test-browser   # Playwright end-to-end tests
just vessel-gate    # Linux idle-resource and SQLite recovery gates
just fmt            # gofmt
just vet            # go vet
just lint           # golangci-lint
just secure         # govulncheck + gosec
just docker-build   # local container image
```

The direct path has no task-runner dependency:

```bash
go test ./...
go build ./cmd/beacon
```

Browser tests start an isolated in-memory Beacon automatically:

```bash
npm ci
npx playwright install chromium
npm test
```

Before changing a domain boundary, read [`CONTEXT.md`](CONTEXT.md) and the
relevant ADR. Before changing an endpoint or configuration shape, update the
API/UI tests and keep the files in [`examples/`](examples/) valid; CI validates
them through the same code path used by real configuration writes.

Issues and pull requests are welcome. The most useful contributions preserve
offline operation, wire fidelity, explicit delivery semantics, and the
source → connector route → sink model.

## License

Beacon is available under the [Apache License 2.0](LICENSE).
