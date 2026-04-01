# beacon

[![Tests](https://github.com/open-ships/beacon/actions/workflows/test.yml/badge.svg)](https://github.com/open-ships/beacon/actions/workflows/test.yml)
[![Go version](https://img.shields.io/github/go-mod/go-version/open-ships/beacon)](go.mod)

Bridge NMEA 2000 CAN bus data to TCP and SSE consumers.

## TL;DR

beacon reads raw frames from a SocketCAN interface, decodes them into structured NMEA 2000 messages, buffers them in SQLite, and streams them to any number of TCP or HTTP/SSE clients — with per-client replay, CEL-based filtering, and Prometheus metrics. Run it on a Raspberry Pi (or any Linux box with a USB-CAN adapter) and your boat's instrument data becomes a live JSON feed.

---

## Features

- **Two output formats** — raw TCP NDJSON for backend consumers; HTTP Server-Sent Events for browsers and dashboards
- **Per-client replay** — clients reconnecting with `Last-Event-ID` receive missed messages from the SQLite ring buffer
- **CEL filter expressions** — filter by PGN, source, priority, or any decoded payload field with Google's Common Expression Language
- **Zero CGO** — pure Go binary, no C toolchain required; cross-compiles cleanly for `linux/amd64` and `linux/arm64`
- **Tiny footprint** — single static binary, minimal runtime dependencies (`iproute2` + `ca-certificates`)
- **Prometheus metrics** — frame counts, sink lag, client counts, filter error rates, buffer utilization
- **Graceful shutdown** — SIGTERM/SIGINT flushes all checkpoints before exit; at-least-once delivery semantics
- **TOML config with env overrides** — every option overridable via `BEACON_*` environment variables

---

## Getting started

### Docker (fastest)

```bash
docker run --rm \
  --network host \
  --cap-add NET_ADMIN \
  -v $(pwd)/config.toml:/app/config.toml \
  -v beacon-data:/data \
  ghcr.io/open-ships/beacon:latest
```

### Binary

Download the latest release for your architecture from the [releases page](https://github.com/open-ships/beacon/releases):

```bash
chmod +x beacon-linux-arm64
./beacon-linux-arm64 --config config.toml
```

### Minimal config

Save this as `config.toml` and set `interface` to match your adapter:

```toml
[app]
log_level = "info"

[can]
interface = "can0"
bitrate   = 250000
auto_up   = true     # runs `ip link set can0 up` — requires NET_ADMIN

[buffer]
path     = "beacon.db"
max_rows = 100000

[admin]
address = "0.0.0.0:2112"

[sinks.sse]
enabled = true
address = "0.0.0.0:8080"
path    = "/events"

[sinks.tcp]
enabled = true
address = "0.0.0.0:9090"
```

Once running:

```bash
curl -N http://localhost:8080/events   # SSE stream
nc localhost 9090                      # TCP NDJSON stream
curl http://localhost:2112/health     # health check
curl http://localhost:2112/metrics     # Prometheus metrics
```

See [`examples/`](examples/) for annotated configs covering navigation filtering, engine monitoring, high-volume tuning, and virtual CAN development.

---

## Configuration reference

### `[app]`

| Key | Default | Description |
|-----|---------|-------------|
| `log_level` | `"info"` | `"debug"`, `"info"`, `"warn"`, or `"error"` |

### `[can]`

| Key | Default | Description |
|-----|---------|-------------|
| `interface` | — | SocketCAN interface name (e.g. `"can0"`, `"vcan0"`) |
| `bitrate` | `250000` | Bus bitrate in bps (ignored for virtual interfaces) |
| `auto_up` | `false` | Run `ip link set <iface> up` on startup (requires `NET_ADMIN`) |
| `restart_ms` | `100` | Delay in ms before reconnecting after a socket error |

### `[buffer]`

| Key | Default | Description |
|-----|---------|-------------|
| `path` | `"/data/beacon.db"` | SQLite database path |
| `max_rows` | `100000` | Ring buffer capacity; oldest rows pruned when exceeded |
| `checkpoint_ms` | `500` | How often (ms) to flush per-sink delivery checkpoints to disk |

### `[sinks.sse]` and `[sinks.tcp]`

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `false` | Enable this sink |
| `address` | `"0.0.0.0:8080"` / `"0.0.0.0:9090"` | Bind address |
| `path` | `"/events"` | HTTP path (SSE only) |
| `filters` | `[]` | CEL filter expressions (AND semantics — all must match) |

### `[admin]`

| Key | Default | Description |
|-----|---------|-------------|
| `address` | `"0.0.0.0:2112"` | Bind address for `/health` and `/metrics` |

### Environment variable overrides

Every config key is accessible as `BEACON_<SECTION>_<KEY>`:

```bash
BEACON_CAN_INTERFACE=can0
BEACON_APP_LOG_LEVEL=debug
BEACON_BUFFER_MAX_ROWS=500000
```

---

## Filters

Filters use [CEL (Common Expression Language)](https://cel.dev). All expressions in the array must pass (AND). Use `||` within a single expression for OR logic. Empty `filters = []` passes everything.

### Message fields

| Field | Type | Description |
|-------|------|-------------|
| `msg.pgn` | `int` | NMEA 2000 PGN number |
| `msg.source` | `int` | Source device address (0–253) |
| `msg.dest` | `int` | Destination address (255 = broadcast) |
| `msg.priority` | `int` | Priority (0 = highest, 7 = lowest) |
| `msg.timestamp` | `string` | RFC3339 timestamp |
| `msg.payload` | `map` | Decoded PGN fields |

### Examples

```toml
# Single PGN
filters = ["msg.pgn == 127250"]

# Allow-list of PGNs
filters = ["msg.pgn in [127250, 128259, 129026]"]

# High-priority navigation messages only
filters = [
  "msg.pgn in [129025, 129026, 129029]",
  "msg.priority <= 3",
]

# Payload field threshold
filters = ["double(msg.payload.speed) > 2.0"]

# Exclude a noisy device
filters = ["msg.source != 42"]

# OR across PGNs
filters = ["msg.pgn == 127250 || msg.pgn == 128259"]

# Only accept specific PGNs from a trusted GPS source (source 7)
filters = ["msg.source != 7 || msg.pgn in [129025, 129026, 129029]"]

# Per-source PGN allowlist: engine data from source 3, GPS from source 7, anything from source 1
filters = [
  "msg.source == 1 || (msg.source == 3 && msg.pgn in [127488, 127489, 127493]) || (msg.source == 7 && msg.pgn in [129025, 129026, 129029])",
]

# Reject all messages from untrusted sources except heading (e.g. a backup compass on source 12)
filters = [
  "msg.source in [1, 3, 7] || (msg.source == 12 && msg.pgn == 127250)",
]

# Combine source allowlist with priority gating: only low-latency nav PGNs from known sources
filters = [
  "msg.source in [1, 3, 7]",
  "msg.pgn in [129025, 129026, 127250, 128259]",
  "msg.priority <= 3",
]
```

---

## Endpoints

| Endpoint | Default port | Description |
|----------|-------------|-------------|
| `GET /events` | `8080` | SSE stream of decoded N2K messages |
| TCP | `9090` | NDJSON stream of decoded N2K messages |
| `GET /health` | `2112` | JSON health status of all subsystems |
| `GET /metrics` | `2112` | Prometheus-format metrics |
| `GET /config` | `2112` | JSON dump of the active configuration |

### SSE reconnect

Each SSE event carries an `id:` field. Clients reconnecting with the `Last-Event-ID` header receive missed messages from the buffer (up to the buffer's retention limit), enabling seamless recovery from network interruptions.

### Docker compose

```yaml
services:
  beacon:
    image: ghcr.io/open-ships/beacon:latest
    network_mode: host
    cap_add:
      - NET_ADMIN
    volumes:
      - ./config.toml:/app/config.toml:ro
      - beacon-data:/data
    restart: unless-stopped

volumes:
  beacon-data:
```

`network_mode: host` gives the container access to the host's CAN interfaces. `NET_ADMIN` is only required if `auto_up = true`.

---

## Development

### Prerequisites

- Go 1.24+
- Linux (for live CAN testing; a virtual interface works on any Linux machine)

### Build and test

Common tasks are managed with [just](https://just.systems):

```bash
just build        # compile binary locally
just test         # run all tests
just test-race    # run tests with race detector
just run          # go run (pass args after --)
just fmt          # gofmt
just vet          # go vet
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

Cross-compile for Raspberry Pi:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./cmd/beacon
```

### Virtual CAN (no hardware needed)

```bash
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set vcan0 up
```

Use `examples/vcan-dev.toml` as your config, then inject test frames:

```bash
cansend vcan0 18FF0001#0102030405060708
```

### Project layout

```
cmd/beacon/      main entrypoint
internal/
  can/              SocketCAN reader (linux) + no-op stub (other platforms)
  n2k/              NMEA 2000 parser (wraps boatkit-io/n2k)
  buffer/           SQLite ring buffer + checkpoint flusher
  filter/           CEL filter chain
  sink/
    sse/            HTTP Server-Sent Events sink
    tcp/            TCP NDJSON sink
  config/           TOML config loading
  admin/            /health, /metrics, OTel setup
examples/           Annotated configs for common use cases
```

### Release

Merging to `main` automatically:
1. Tags the commit `YYYY.MM.DD` (incrementing to `YYYY.MM.DD-2` etc. if multiple merges happen the same day)
2. Creates a GitHub Release with pre-built `linux/amd64` and `linux/arm64` binaries
3. Publishes `ghcr.io/open-ships/beacon:<tag>` and `:latest`
