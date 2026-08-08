# Getting started

beacon is an offline NMEA 2000 gateway. It reads frames from CAN sources,
decodes them, optionally filters them, and delivers them to sinks — HTTP
streams, MQTT topics, another CAN bus, or a plain TCP feed. Everything is
configured through one HTTP API (and this UI, which is a thin layer over that same
API), stored in a single SQLite file, and served with no dependency on
internet access.

## The pipeline model

```
source --> [connector: CEL filters] --> sink
```

- **Sources** read decoded NMEA 2000 messages onto beacon: a physical CAN
  interface (`socketcan`), USB-CAN adapter (`usbcan`), HTTP stream
  (`http_sse` / `http_ws`), MQTT topic (`mqtt`), capture file (`file`), or a
  passive Yacht Devices/Actisense gateway stream (`tcp` / `udp`).
- **Sinks** deliver messages somewhere: back onto a CAN bus (`socketcan` /
  `usbcan`, confirmed at-least-once delivery), or out over HTTP
  (`http_sse` / `http_ws`, with replay for reconnecting clients), a remote
  HTTP(S) API (`http_post`, confirmed JSON batches with optional gzip, custom
  authentication headers, and receiver-directed retry timing), a plain
  `tcp` NDJSON feed (live-only, no replay), an MQTT topic (`mqtt`, live-only),
  a PostgreSQL/TimescaleDB table (`postgres`, confirmed batches), a remote
  NMEA 2000 bus through a claiming TCP gateway client (`tcp_gateway`), or
  intentionally nowhere (`null`, with normal statistics).
- **Connectors** wire one source to one sink, with an optional list of CEL
  filter expressions (see the filters page) and a durable per-connector
  buffer (see the concepts page) that survives a restart and absorbs a slow
  or disconnected sink.

Nothing flows until all three exist: a source and a sink alone deliver
nothing — a connector has to name both.

Gateway streams use an `address` in `host:port` form and a `format` of
`ydraw` or `actisense`. A `tcp`/`udp` source is passive and never claims an
NMEA 2000 address; a `tcp_gateway` sink is writable, claims an address, and
participates on the remote bus. A `file` source takes an absolute
`file_path`, replays its capture once at the recorded timing, then remains
up and idle. Gzip-compressed captures are detected from their contents, so
the filename does not need a `.gz` suffix. When the configured path is
missing, Beacon also tries the same path with `.gz` appended; an existing
exact path always takes precedence.

## Running beacon

### Binary

```
go build ./cmd/beacon
./beacon --db beacon.db --data-address 0.0.0.0:8080 --admin-address 0.0.0.0:2112
```

Flags (all optional; shown with their defaults):

| Flag | Default | Purpose |
|---|---|---|
| `--db` | `beacon.db` | SQLite database path — configuration and every connector's message buffer live here |
| `--data-address` | `0.0.0.0:8080` | Bind address for sink endpoints (SSE/WS; TCP sinks bind their own address) |
| `--admin-address` | `0.0.0.0:2112` | Bind address for this UI, the config API, `/mcp`, `/health`, and `/metrics` |
| `--seed` | (none) | JSON config file to load into an empty database on first boot |
| `--log-level` | `info` | `debug`, `info`, `warn`, or `error` |

Two separate HTTP servers are running: one for sink traffic (the data you
configure sinks to serve), one for everything administrative (this UI, the
API, MCP, health, and metrics). They're kept apart so a sink misconfiguration
never risks the surface you use to fix it.

### Docker

```
docker compose up
```

`docker-compose.yml` builds the image from the repo `Dockerfile`, mounts
`./data` onto `/data` for the SQLite file, and runs with `network_mode:
host` — required so the container can see a CAN interface that already
exists on the host (see the CAN setup page). The image's healthcheck polls
`/health`.

## Your first pipeline, via the UI

1. Open `/` (redirects to `/dashboard`).
2. **Sources** → Add source. For a quick test without hardware, use a
   `socketcan` source pointed at `vcan0` (see the CAN setup page for
   bringing up a virtual interface).
3. **Sinks** → Add sink. An `http_sse` sink with path `/events` is the
   easiest to watch — any browser or `curl` can consume it.
4. **Connectors** → Add connector, pick the source and sink you just made,
   leave filters empty (everything passes), and enable it.

The dashboard's source -> connector -> sink graph updates within a couple of
seconds and shows live throughput once messages start flowing.

## Your first pipeline, via curl

The same three writes, directly against the config API (all on the admin
port, `2112` by default). Every field shown is required by the API's
schema — in particular `enabled` and, for connectors, `buffer` must be
present even when empty:

```
curl -X PUT localhost:2112/api/v1/sources/can0 \
  -H 'Content-Type: application/json' \
  -d '{"id":"can0","name":"Engine room CAN bus","type":"socketcan","enabled":true,"interface":"vcan0"}'

curl -X PUT localhost:2112/api/v1/sinks/sse \
  -H 'Content-Type: application/json' \
  -d '{"id":"sse","name":"Browser feed","type":"http_sse","enabled":true,"path":"/events"}'

curl -X PUT localhost:2112/api/v1/connectors/all \
  -H 'Content-Type: application/json' \
  -d '{"id":"all","name":"Everything","source_id":"can0","sink_id":"sse","enabled":true,"buffer":{}}'
```

Each write reconciles immediately: the response body's `status` array
reports the live state of the entity you just wrote, so you can see the
effect without a second round trip. See the API page for the full CRUD
surface, validation errors, and export/import.

## Where the data comes out

Sink endpoints are served on the data port (`8080` by default), at the
path you gave the sink:

```
curl -N localhost:8080/events
```

Each line is a `data:` frame carrying one JSON-encoded envelope (see the
concepts page for its exact shape) and an `id:` you can hand back as
`Last-Event-ID` on reconnect to resume where you left off. A `tcp` sink
instead accepts a plain socket connection (`nc localhost <port>`) streaming
one JSON envelope per line, live only. An `mqtt` sink publishes the same
envelope JSON to its configured broker topic, live only.
