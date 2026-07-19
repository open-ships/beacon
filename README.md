# beacon

[![Tests](https://github.com/open-ships/beacon/actions/workflows/test.yml/badge.svg)](https://github.com/open-ships/beacon/actions/workflows/test.yml)
[![Go version](https://img.shields.io/github/go-mod/go-version/open-ships/beacon)](go.mod)

An offline NMEA 2000 gateway: read CAN/USB-CAN/HTTP/MQTT sources, filter with CEL
expressions, deliver to CAN/HTTP/TCP/MQTT sinks — all configured through one REST
API and web UI, with durable buffering and replay for reconnecting clients.

## How it fits together

```
source --> [connector: CEL filters + durable buffer] --> sink
```

- **Sources** decode NMEA 2000 messages onto beacon: SocketCAN (`socketcan`),
  USB-CAN (`usbcan`), HTTP (`http_sse` / `http_ws`), MQTT (`mqtt`), a capture
  replay (`file`), or a passive Yacht Devices/Actisense gateway stream
  (`tcp` / `udp`).
- **Sinks** deliver messages somewhere: back onto a CAN bus (`socketcan` /
  `usbcan`, push-confirmed with retry), or out over HTTP (`http_sse` /
  `http_ws`, with replay for reconnecting clients), a plain `tcp` NDJSON
  feed (live-only), an MQTT topic (`mqtt`, live-only), or onto a remote NMEA
  2000 bus through a TCP gateway (`tcp_gateway`).
- **Connectors** are the only thing that moves data — each names exactly
  one source and one sink, an optional list of CEL filter expressions, and
  its own durable SQLite-backed buffer. Each connector explicitly chooses
  `semantic`, `transparent`, or `observe` bridge mode and reports a
  confirmed, resumable, best-effort, or observe-only delivery boundary.

Beacon also persists one stable N2K appliance NAME, exposes the complete PGN
and field catalog with unit-scaled physical values, inventories devices by
their stable NAME, and generates commissioning reports with SocketCAN health.

Every configuration write (via the API or the UI) applies immediately —
validated, persisted, and reconciled against the running system in one
step. There's no separate config file and no restart.

## Quick start

### Binary

```bash
go build ./cmd/beacon
./beacon --db beacon.db --seed examples/vcan-dev.json
```

| Flag | Default | Purpose |
|---|---|---|
| `--db` | `beacon.db` | SQLite database path — configuration and every connector's message buffer live here |
| `--data-address` | `0.0.0.0:8080` | Bind address for sink endpoints (SSE/WS; TCP sinks bind their own address) |
| `--admin-address` | `0.0.0.0:2112` | Bind address for the web UI, config API, `/health`, and `/metrics` |
| `--seed` | (none) | JSON config to load into an empty database on first boot; ignored once the database already holds a configuration |
| `--log-level` | `info` | `debug`, `info`, `warn`, or `error` |

`beacon` also has two offline CLI subcommands, `export` and `import`, for
reading/writing a database file directly without a running process — see
[`examples/README.md`](examples/README.md) and `/ui/docs/api` for usage
and the offline caveat (never run them against a database a live beacon
process has open).

### Docker

```bash
docker compose up
```

`docker-compose.yml` builds from the repo `Dockerfile`, mounts `./data`
onto `/data` for the SQLite file, and runs with `network_mode: host` —
required so the container can see a CAN interface that already exists on
the host. Or run the published image directly:

```bash
docker run --rm --network host \
  -v $(pwd)/data:/data \
  ghcr.io/open-ships/beacon:latest --db /data/beacon.db
```

### First pipeline

No CAN hardware needed to try it — bring up a virtual interface and seed
[`examples/vcan-dev.json`](examples/vcan-dev.json):

```bash
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set vcan0 up

./beacon --db beacon.db --seed examples/vcan-dev.json &
cansend vcan0 18FF0001#0102030405060708   # from can-utils
curl -N http://localhost:8080/events      # watch it arrive as SSE
```

See [`examples/`](examples/) for more starter configs (navigation PGN
filtering, engine-room fan-out to SSE + TCP, chaining two beacons), and the
onboard manual (`/ui/docs`, or [`internal/ui/docs/`](internal/ui/docs/) in
source form) for everything else, including real CAN interface bring-up.

## Surfaces

Two HTTP servers run side by side, kept apart so a sink misconfiguration
never risks the surface you use to fix it:

| Surface | Where | Notes |
|---|---|---|
| Web UI | admin address, `/ui` (`/` and `/ui` both redirect to `/ui/dashboard`) | routing, pending/retained state, bus diagnostics, device commissioning |
| Manual | admin address, `/ui/docs` (`/docs` 301s there) | getting started, CAN setup, concepts, filters, API, troubleshooting |
| Config API | admin address, `/api/v1/...` | REST CRUD, live route state, PGN catalog, device inventory, commissioning report |
| API reference | admin address, `/api/docs` (interactive, offline) and `/api/openapi.json` (OpenAPI 3.1) | self-describing — start here for scripting/agents |
| Health | admin address, `/health` (mirrored at `/api/v1/health`) | JSON status, rolled up from every component |
| Metrics | admin address, `/metrics` | Prometheus exposition |
| Sink endpoints (SSE/WS) | data address, at each sink's configured `path` | e.g. `/events`; supports replay via `Last-Event-ID` / `?after=` |
| Sink endpoints (TCP) | each `tcp` sink's own configured `address` | its own listener, independent of the admin/data servers; live-only, no replay |
| Sink endpoints (MQTT) | each `mqtt` sink's configured broker URL and topic | publishes live envelope JSON to the broker; no replay |

Admin address defaults to `0.0.0.0:2112`, data address to `0.0.0.0:8080`.

## Configuration

The whole configuration is one JSON document — `{"sources": [...], "sinks":
[...], "connectors": [...]}` (`internal/model/model.go`) — read/written
wholesale via `GET`/`POST /api/v1/config/{export,import}` (or the offline
`beacon export`/`import` CLI verbs), or entity-by-entity via
`PUT`/`DELETE /api/v1/{sources,sinks,connectors}/{id}`. A connector's
`filters` is a list of [CEL](https://cel.dev) expressions (AND semantics;
`||` within one expression for OR), validated with the rest of the config
on every write. For the full field reference, the envelope shape, buffering
and replay semantics, and a CEL cookbook, see `/ui/docs` (or
[`internal/ui/docs/`](internal/ui/docs/)) and `/api/docs`; for ready-to-use
starting points, see [`examples/`](examples/).

Bridge mode is selected on each connector. Omitted mode means `semantic`:

```json
{"id":"bridge","name":"CAN 0 to CAN 1","source_id":"can0","sink_id":"can1","mode":"transparent","forward_management":false,"filters":[],"buffer":{},"enabled":true}
```

Transparent mode currently requires a SocketCAN sink, rejects a source and
sink on the same interface, preserves source/destination/priority/raw payload
including unknown PGNs, and blocks address-claim and other management PGNs
unless `forward_management` is deliberately enabled.

Gateway and replay source examples:

```json
{"id":"replay","name":"Capture replay","type":"file","enabled":true,"file_path":"/data/capture.log"}
{"id":"yd-in","name":"YD gateway input","type":"tcp","enabled":true,"address":"192.168.4.1:1457","format":"ydraw"}
{"id":"actisense-in","name":"Actisense broadcast","type":"udp","enabled":true,"address":"0.0.0.0:2000","format":"actisense"}
```

A writable gateway is a sink instead:

```json
{"id":"yd-out","name":"YD gateway output","type":"tcp_gateway","enabled":true,"address":"192.168.4.1:1457","format":"ydraw"}
```

When upgrading from the pre-v0.2 n2k dependency, note that repeating PGN
groups now decode as arrays and NMEA null sentinels decode as JSON `null`.
Existing CEL filters that referenced flattened repeating-group fields may
need updating.

## Development

Common tasks are managed with [just](https://just.systems):

```bash
just build        # compile binary locally (CGO_ENABLED=0)
just test         # run all Go tests
just test-browser # run browser end-to-end tests with Playwright
just test-browser-ui # run browser tests in Playwright's interactive UI
just test-race    # run the full suite with the race detector
just run          # go run (pass args after --)
just fmt          # gofmt
just vet          # go vet
just lint         # golangci-lint
just clean        # remove build artifacts
just build-arm64  # cross-compile for Raspberry Pi (linux/arm64)
just build-amd64  # cross-compile for linux/amd64
just docker-build # build Docker image
just docker-run   # docker compose up
just tidy         # go mod tidy
just version      # print current version
```

Or run directly:

```bash
git clone git@github.com:open-ships/beacon.git
cd beacon
go build ./cmd/beacon
go test ./...
```

Browser end-to-end tests use Playwright and start beacon automatically with
an in-memory database. Install the Node dependencies and Chromium once, then
run the suite:

```bash
npm ci
npx playwright install chromium
just test-browser
```

`just test-race` and the CI `race` job both run `go test -race ./...`.
n2k v0.2.0's chunked generated PGN definitions keep the full repository
within the compiler's race-build limits.

### Virtual CAN (no hardware needed)

```bash
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set vcan0 up
```

Use [`examples/vcan-dev.json`](examples/vcan-dev.json) as a starting
config (via `--seed` or `beacon import`), then inject test frames:

```bash
cansend vcan0 18FF0001#0102030405060708
```

### Project layout

```
cmd/beacon/     CLI entrypoint: serve, export, import
internal/
  model/        config entities (sources, sinks, connectors) + structural validation
  store/        SQLite: schema, config CRUD, connection shared with queue
  queue/        durable per-connector buffer (SQLite-backed)
  filter/       CEL compile + evaluate against message envelopes
  msg/          canonical message envelope shape
  bus/          one n2k.Client per physical CAN endpoint, refcounted
  source/       runs configured sources, fans envelopes out to connectors
  connector/    per-connector pipeline: subscribe -> filter -> queue -> deliver
  sink/         runs configured sinks (CAN push-confirm; HTTP/TCP/MQTT broadcast, replay for SSE/WS only)
  supervisor/   reconciles desired config against running components
  config/       validate + persist + reconcile choke point (API and UI both sit on this)
  api/          REST config API (huma-on-chi) + offline API reference UI
  ui/           offline server-rendered web UI (htmx + lightweight Open Ships CSS) + onboard docs manual
  app/          composition root: store, bus manager, data server, supervisor, admin HTTP server
  stats/        live per-connector counters for the API/UI
  metrics/      OTel instrument set (Prometheus exposition)
  sysinfo/      best-effort CAN/USB-serial hardware discovery
examples/       importable starter configs
```

### Release

The `Release` workflow runs after `Test` succeeds on `main` (i.e. on every
merge) and:

1. Tags the commit `YYYY.MM.DD` (incrementing to `YYYY.MM.DD-2` etc. if
   multiple merges land the same day)
2. Creates a GitHub Release with pre-built `linux/amd64` and `linux/arm64`
   binaries (`CGO_ENABLED=0`, version-stamped via `-ldflags`)
3. Publishes `ghcr.io/open-ships/beacon:<tag>` and `:latest`
