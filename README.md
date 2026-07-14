# beacon

[![Tests](https://github.com/open-ships/beacon/actions/workflows/test.yml/badge.svg)](https://github.com/open-ships/beacon/actions/workflows/test.yml)
[![Go version](https://img.shields.io/github/go-mod/go-version/open-ships/beacon)](go.mod)

An offline NMEA 2000 gateway: read CAN/USB-CAN/HTTP sources, filter with CEL
expressions, deliver to CAN/HTTP/TCP sinks — all configured through one REST
API and web UI, with durable buffering and replay for reconnecting clients.

## How it fits together

```
source --> [connector: CEL filters + durable buffer] --> sink
```

- **Sources** decode NMEA 2000 messages onto beacon: a SocketCAN interface
  (`socketcan`), a USB-CAN adapter (`usbcan`), or an HTTP stream from
  somewhere else (`http_sse` / `http_ws` — including another beacon's own
  sink, for chaining gateways).
- **Sinks** deliver messages somewhere: back onto a CAN bus (`socketcan` /
  `usbcan`, push-confirmed with retry), or out over HTTP (`http_sse` /
  `http_ws`, with replay for reconnecting clients) or a plain `tcp` NDJSON
  feed (live-only).
- **Connectors** are the only thing that moves data — each names exactly
  one source and one sink, an optional list of CEL filter expressions, and
  its own durable SQLite-backed buffer that absorbs a slow or disconnected
  sink without blocking the source. A source or sink with no connector
  naming it does nothing.

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
| `--data-address` | `0.0.0.0:8080` | Bind address for sink endpoints (SSE/WS/TCP) |
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
| Web UI | admin address, `/ui` (`/` and `/ui` both redirect to `/ui/dashboard`) | dashboard, sources/sinks/connectors CRUD, live stats |
| Manual | admin address, `/ui/docs` (`/docs` 301s there) | getting started, CAN setup, concepts, filters, API, troubleshooting |
| Config API | admin address, `/api/v1/...` | REST CRUD, filter validation, export/import, live metrics, health, system info |
| API reference | admin address, `/api/docs` (interactive, offline) and `/api/openapi.json` (OpenAPI 3.1) | self-describing — start here for scripting/agents |
| Health | admin address, `/health` (mirrored at `/api/v1/health`) | JSON status, rolled up from every component |
| Metrics | admin address, `/metrics` | Prometheus exposition |
| Sink endpoints (SSE/WS) | data address, at each sink's configured `path` | e.g. `/events`; supports replay via `Last-Event-ID` / `?after=` |
| Sink endpoints (TCP) | each `tcp` sink's own configured `address` | its own listener, independent of the admin/data servers; live-only, no replay |

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

## Development

Common tasks are managed with [just](https://just.systems):

```bash
just build        # compile binary locally (CGO_ENABLED=0)
just test         # run all tests
just test-race    # race detector, for the packages not blocked by an upstream n2k/pgn compiler bug
just run          # go run (pass args after --)
just ui-css       # rebuild internal/ui/assets/app.css from internal/ui/uisrc/input.css
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

`just test-race` (and the CI `race` job) runs only
`internal/bus/busfake`, `internal/metrics`, `internal/model`,
`internal/stats`, `internal/store`, and `internal/sysinfo`: every other
package pulls in `n2k/pgn` transitively, which ICEs the Go compiler under
`-race` (an upstream bug, not beacon's). Those packages are still covered
by the regular (non-race) test suite.

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
  sink/         runs configured sinks (CAN push-confirm; HTTP/TCP broadcast, replay for SSE/WS only)
  supervisor/   reconciles desired config against running components
  config/       validate + persist + reconcile choke point (API and UI both sit on this)
  api/          REST config API (huma-on-chi) + offline API reference UI
  ui/           offline server-rendered web UI (htmx + OpenBridge + daisyUI) + onboard docs manual
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
