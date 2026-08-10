# Example configs

Each file here is a complete, importable `model.Config` document —
`{"sources": [...], "sinks": [...], "connectors": [...], "settings": {...}}`
— validated by
`internal/config/examples_test.go` against the same rules every API write,
CLI import, and `--seed` boot uses (`config.ValidateConfig`: structural
checks plus a CEL compile of every connector's filters). If an example
stops validating, that test fails. `settings` is optional; omitted
observability settings retain the low-resource default. Omitted resource
settings apply a 1 GiB SQLite main-database ceiling, 128 MiB database reserve,
and 2 GiB aggregate file-sink allocation budget.

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
  replaying a `candump` log with `canplayer`. Rotation happens before a record
  would cross its per-file ceiling; a single oversized record is skipped, and
  startup removes rotations outside a newly lowered limit. All file-sink
  `max_file_bytes × max_files` allocations must fit the appliance-wide budget,
  and `max_files` cannot exceed 128.
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
a connector omits `max_messages`, it independently defaults to 10,000; omitted
`max_bytes` independently defaults to 64 MiB, even if an age or count limit is
present. `max_age` measures local `observed_at`/queue-admission time rather than
the upstream wire clock (see `/docs/concepts`). The aggregate of every route's
effective byte limit must fit the database ceiling minus its reserve—896 MiB
with default resource settings.

## Sizing for an outage

Use measured peak Envelope rate and average canonical JSON size rather than
guessing from PGN count:

```
beacon size-buffer --rate 1 --average-bytes 512 --outage 168h --reserve-percent 25
```

That example plans one Envelope per second for seven disconnected days plus
25% reserve: `max_messages: 756000`, `max_bytes: 387072000`, and
`max_age: 210h0m0s`. The formula and SQLite/WAL headroom caveats are on the
concepts page. Before shipping, run the repository's
[vessel release gates](../docs/vessel-release-gates.md), then repeat load and
power-loss qualification on the target vessel hardware; the CI thresholds are
regression guards, not hardware measurements.

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
