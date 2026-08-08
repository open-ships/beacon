# Example configs

Each file here is a complete, importable `model.Config` document —
`{"sources": [...], "sinks": [...], "connectors": [...]}` — validated by
`internal/config/examples_test.go` against the same rules every API write,
CLI import, and `--seed` boot uses (`config.ValidateConfig`: structural
checks plus a CEL compile of every connector's filters). If an example
stops validating, that test fails.

## Files

- **`minimal.json`** — one CAN source (`can0`), one SSE sink (`/events`),
  one connector with no filters (everything passes). The smallest config
  that actually moves data. Start here.
- **`navigation.json`** — the same shape as `minimal.json`, but the
  connector's filter allow-lists navigation PGNs only (heading, rapid
  position, COG/SOG, GNSS position: `127250`, `129025`, `129026`,
  `129029`), served on `/nav`. See the filters page (`/docs/filters`)
  for how the filter list and CEL expressions work.
- **`engine-room.json`** — one CAN source feeding *two* connectors, both
  filtered to engine PGNs (`127488`, `127489`, `127493`): one to an SSE
  sink (`/engine`, for a browser dashboard) and one to a `tcp` sink
  (`0.0.0.0:9090`, for a backend NDJSON consumer). It also includes disabled
  MQTT source/sink starter endpoints (`mqtt://broker.local:1883`) for broker
  integration. Demonstrates fan-out — multiple connectors sharing one
  source, each with its own filters and buffer.
- **`http-post.json`** — a disabled-by-default confirmed HTTP(S) POST route
  showing batch size, timeout, gzip, bearer/API-key headers, and durable
  buffering.
- **`postgres.json`** — a disabled-by-default confirmed TimescaleDB route
  showing connection URL, table, batch size, write timeout, and
  operator-managed schema creation. Import it, edit the sink, and use
  **Copy DDL** before enabling it; clear `timescaledb` for ordinary PostgreSQL.
- **`beacon-chain.json`** — an `http_sse` *source* pointed at another
  beacon's SSE sink (`http://upstream-beacon.local:8080/events`), feeding
  a local `socketcan` sink. Chains two beacons together: this instance
  mirrors everything the upstream one publishes onto a local CAN bus.
  Change the `url` to your upstream beacon's actual data-address and sink
  path before using this.
- **`logging.json`** — one CAN source (`can0`) feeding *two* connectors into
  two local `file` sinks: one `ndjson` (`/data/log/nav.ndjson`) and one
  `candump` (`/data/log/nav.candump`), both capped at 50 MiB &times; 3 files
  (`max_file_bytes: 52428800`, `max_files: 3` — the active file plus two
  rotated backups). No filters, so everything the source decodes is logged.
  `file_path` must be an absolute path that exists on disk (the sink opens
  it but does not create missing parent directories) — create `/data/log`
  first, or change both paths to a directory you control. See the concepts
  page (`/docs/concepts`) for file sink delivery semantics, rotation, and
  replaying a `candump` log with `canplayer`.
- **`vcan-dev.json`** — identical to `minimal.json` but pointed at
  `vcan0` instead of a real interface, for developing and testing beacon
  with no CAN hardware attached. Bring the virtual interface up first (see
  `/docs/can-setup`):

  ```
  sudo modprobe vcan
  sudo ip link add dev vcan0 type vcan
  sudo ip link set vcan0 up
  ```

  Then inject test frames with `cansend vcan0 18FF0001#0102030405060708`
  (from `can-utils`).

Replace `can0` / `vcan0` with your actual interface name, and adjust sink
paths/addresses and connector filters as needed — these are starting
points, not fixed configurations. All buffer limits shown are optional; if
a connector's `buffer` object sets nothing at all, `max_messages` defaults
to 10000 (see `/docs/concepts`).

## Using an example

**First boot (no database yet):** pass the file to `--seed`. It only
applies when the database is empty — a database that already holds a
configuration ignores `--seed` and logs that it did so, so this is safe to
leave on the command line permanently.

```
./beacon --db beacon.db --seed examples/minimal.json
```

**Offline, against an existing database file** (the file must not be held
open by a running beacon process — see `/docs/api` for why):

```
beacon import --db beacon.db examples/navigation.json           # replaces the whole config
beacon import --db beacon.db --merge examples/engine-room.json  # upserts onto the existing config
```

**Live, against a running beacon** — `POST` the file to the config API:

```
curl -X POST 'localhost:2112/api/v1/config/import?mode=replace' \
  --data-binary @examples/navigation.json

curl -X POST 'localhost:2112/api/v1/config/import?mode=merge' \
  --data-binary @examples/engine-room.json
```

Either way, an invalid file is rejected before anything is written — the
existing configuration (or empty database) is left untouched.
