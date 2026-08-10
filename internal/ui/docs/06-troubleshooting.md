# Troubleshooting

## No frames arriving

Work from the source outward:

1. **Is the interface actually up, at the right bitrate?** (see the CAN
   setup page) — `ip -details link show can0` for SocketCAN; a `usbcan`
   source needs its serial `port` field pointing at the right device.
2. **Physical bus termination.** Missing or mid-bus (rather than end-of-bus)
   120 ohm termination causes exactly this symptom on real hardware — it
   won't affect `vcan`.
3. **Check `GET /health`** (admin port). The source's component entry
   should read `"up"`. `"degraded"` means it's actively reconnecting — read
   its `err` field. Absent entirely means it isn't configured as `enabled`
   at all.
4. **Check `beacon_source_messages_total` in `/metrics`** (admin port),
   labeled by `source`. If this counter is incrementing, frames are
   arriving and decoding — the problem is downstream (a filter dropping
   everything, or no connector wired to this source at all). If it's flat
   at zero, nothing is reaching beacon from the interface.

For one missing sensor or PGN on an otherwise busy bus, open that source's
overview and check **PGN traffic**. Gap rows are sorted first and show last-seen
age versus the stream's learned period. The equivalent Prometheus signal is
`beacon_source_pgn_gap_active` when
`settings.observability.prometheus_source_details` is enabled; agents can query
the same store with the MCP `get_source_metrics` tool without enabling that
Prometheus surface.

## CAN write failures

A `socketcan`/`usbcan` sink confirms every push; on failure the connector
runs continuous equal-jitter exponential retry (250 ms initial ceiling,
doubling to a one-minute ceiling)
rather than dropping the message — see the concepts page. This means a
stuck or disconnected CAN sink doesn't lose data, it **queues** it: the
connector's buffer grows for as long as the sink stays unreachable. Watch
that connector's `queue_depth` (`GET /api/v1/connectors/{id}/metrics`, JSON
field `queue_depth`) or the `beacon_connector_queue_depth` gauge in
`/metrics` — steady growth with no delivery means the sink side needs
attention (bus down, adapter unplugged, wrong interface), not the source
side.

This retry-forever behavior only applies to messages the sink can actually
write. A message beacon cannot re-encode for CAN transmission — most
commonly an unrecognized PGN with no cataloged decoder — is **skipped**
instead: the connector counts it under the `skipped` message stage
(`beacon_connector_messages_total{connector="...",stage="skipped"}` in
`/metrics`) and moves on rather than retrying forever. A connector logs
each new stuck-entry retry sequence at `warn` (`push failed; retrying`,
then drops to `debug` for that same entry's subsequent retries) — check
its logs if `queue_depth` is climbing but nothing looks skipped.

## HTTP POST delivery failures

An `http_post` sink confirms a whole batch only after a 2xx response. A
timeout, redirect, transport failure, or non-2xx response leaves the batch
pending and marks the route degraded while Beacon retries. Check the sink URL,
certificate trust, authentication headers, and the response status/body in the
route error. If the receiver committed the batch but the response was lost,
use Beacon's deterministic `Idempotency-Key` to deduplicate the retry.
For rate limits and planned outages, the receiver can return `Retry-After` as
delta-seconds or an HTTP date; Beacon waits for the larger of that value and
its current connector backoff. Inspect `beacon_sink_http_requests_total` by
`status`, plus the `beacon_sink_http_request_latency_seconds` and payload-size
histograms, to separate receiver rejection, network slowness, and oversized
batches. With gzip enabled, compare on-wire and uncompressed size histograms.

## PostgreSQL sink not writing

A `postgres` sink stays degraded while it cannot connect to or verify its
destination table, and its connector keeps the batch pending for retry. Check
the connection URL, TLS mode, database permissions, and the sink's live error.
If **Automatically create and verify the table** is off, edit the sink and use
**Copy DDL**, run that exact schema in the target database, and leave Beacon
running; its background verification recovers automatically. For TimescaleDB,
install the extension in that database before running the generated
`create_hypertable` statement. A table created with different columns is not
silently accepted: schema verification or the first insert reports the
database error while preserving the pending batch.

## File sink not writing

1. **Is `file_path` absolute?** A file sink rejects a relative path at
   validation time (`file_path must be an absolute path`) — it never even
   attempts to open one, so this fails the write/import immediately rather
   than showing up here later.
2. **Does the parent directory exist and is it writable by the beacon
   process?** The sink opens the file with `O_CREATE` but does not create
   missing parent directories, and a permissions problem on either the
   directory or an existing file surfaces the same way: the sink fails to
   start.
3. **Check `GET /health`** (admin port). A file sink that failed to open (or
   whose last write/rotation failed) reports `"error"` there with an `err`
   field naming the underlying OS error — read it before guessing.
4. **Disk full?** Like CAN sinks, a file sink's write failure is retried
   with the connector's backoff rather than dropping the message — see CAN
   write failures above. Watch that connector's `queue_depth` climb while
   nothing is being written; once space frees up, the sink catches up
   automatically with no manual intervention.
5. **Did a limit shrink?** On startup or hot restart, Beacon rotates an active
   file larger than `max_file_bytes` and removes rotations that exceed the new
   byte/count limits, logging each action. `max_files` includes the active file
   and cannot exceed 128. A new record larger than `max_file_bytes` is counted
   as skipped and checkpointed; it is not written oversized or retried forever.

For file sinks, **confirmed** means the buffered `Flush` reached the operating
system's write path. It does not mean `fsync` or stable-media persistence.
Beacon avoids a media sync per record or batch to prevent flash write and wear
amplification, so qualify recent-record durability under real power cuts on the
target host, filesystem, controller, and storage device.

## Queue growth

Every connector's buffer is bounded by its effective `max_messages` /
`max_age` / `max_bytes` (see the concepts page); pruning runs periodically and
enforces all effective limits. Check a connector's
configured limits with `GET /api/v1/connectors/{id}` and its current depth
with `GET /api/v1/connectors/{id}/metrics` — sustained growth up against
`max_messages` (or the age/byte equivalent) means deliveries aren't
keeping up with intake and old data is now being pruned to make room, not
retained indefinitely. Every omitted `max_messages` becomes 10,000 and every
omitted `max_bytes` becomes 64 MiB independently, even if `max_age` or the other
limit is present. Those caps apply to retained rows whether acknowledged or
still pending. Age uses local `observed_at`/admission time, not the source wire
timestamp. If a sink may be unavailable long enough to exceed either default,
use `beacon size-buffer` and configure a larger count, age, and byte budget.

Durable queue-write failures themselves retry with equal jitter from a 100 ms
initial ceiling to a one-minute ceiling. Sink delivery starts at 250 ms and has
the same one-minute ceiling. Neither path requires a restart after a long
storage or connectivity outage, but retention limits can prune pending delivery
before recovery if the route was undersized.

## Configuration or client rejected at a limit

One configuration supports at most 32 sources, 32 sinks, and 64 connector
routes. A source or sink can carry 32 configured headers; a route can carry 32
CEL filters, each at most 8 KiB. Text caps count UTF-8 bytes: IDs are limited to
128 bytes; names and header names to 256 bytes; endpoint URLs, addresses,
interfaces, ports, HTTP paths, file paths, and header values to 8 KiB; and MQTT
topics to 4 KiB. All operator-authored strings in Source, Sink, and Connector
values—including filters—also share a 256 KiB aggregate cap; fixed JSON syntax
and numeric/boolean settings are not charged. The per-field and aggregate
limits both apply, and validation rejects the whole write before reconciliation.

Each SSE, WebSocket, or plain TCP sink admits at most 32 clients. SSE/WS excess
requests receive `503` and `Retry-After: 5`; a TCP sink closes excess clients.
The admin listener, data HTTP listener, and configured plain-TCP sinks also
share one 128 accepted-connection budget; the per-sink limits are nested in it.
Their headers are capped at 64 KiB, typed REST and MCP request bodies at 1 MiB,
and slow reads by five-second header, 30-second request, and 90-second idle
timeouts. These limits bound accepted network sockets, not SQLite, CAN/serial,
listener, or outbound file descriptors.

MQTT, SSE, and WebSocket source Envelopes over 256 KiB, or with a decoded
payload over 128 KiB, more than 1,785 raw bytes, more than 256 physical or
missing fields, or metadata text over 1 KiB, are discarded at admission. This
does not put an error into a connector route because the Envelope never reaches
one; inspect source traffic and upstream payload shape.

## Health states

`GET /health` (and `GET /api/v1/health`) report each component as `up`,
`degraded`, or `error`; overall status is `"ok"` only when every reporting
component is `up`. A disabled component is simply absent from the list —
that's expected, not degraded.

The dashboard additionally shows a `restarting` state for an *enabled*
component that's momentarily missing from live status — the window
between a hot-apply's stop of the old instance and the new one landing
(see the concepts page). This is transient and UI-only; if a component
sits in `restarting` for more than a few seconds, check its logs — that
usually means the new instance is failing to start rather than just
starting slowly.

## SSE client missed data

A browser's native `EventSource` resumes automatically via
`Last-Event-ID` on reconnect. A connection with no cursor at all gets
**no** replay — it streams live from the moment it connects — so a fresh
script that wants buffered history must ask for it explicitly:
`?after=<connector>:<seq>` to resume from a known point, or
`?after=<connector>:0` to pull everything the buffer still holds. See the
concepts page for the full replay semantics, including the comma-separated
multi-connector form. Data pruned beyond the buffer's limits is gone; there
is no unbounded history.

## Where the data lives

Configuration (sources, sinks, connector routes, settings, identity, and
inventory) and every connector route's durable queue live in one SQLite main
database, the `--db` path (`beacon.db` by default, relative to the working
directory unless given an absolute path). File-sink output lives at each
configured `file_path` and is not part of that database backup.

By default the SQLite main file has a 1 GiB physical page ceiling and route
allocations share 896 MiB after the 128 MiB reserve. The WAL is separate and
can temporarily exceed its 16 MiB retention target, so a filesystem needs
headroom beyond 1 GiB. Beacon performs a passive checkpoint and bounded
incremental vacuum every six hours. It does not run full `VACUUM` online; stop
Beacon and run `beacon compact --db beacon.db` with room for approximately one
extra database copy to reclaim a legacy/high-water file. The same offline step
is required before lowering `max_database_bytes` below the existing page
high-water mark.

### Upgrade rejected before Beacon becomes ready

A candidate may migrate an old schema and then refuse startup when current
validation or the persisted resource budget is applied. Two deliberate checks
commonly expose legacy state:

- source, sink, and connector IDs are lowercase slugs of at most 128 bytes; and
- the SQLite main database's page high-water mark must not exceed the effective
  `max_database_bytes` (1 GiB when omitted).

Do not troubleshoot this by repeatedly opening the only database copy. Stop
Beacon, preserve the main database plus any `-wal`/`-shm` sidecars, and validate
the exported configuration with the candidate against a throwaway database.
Use `beacon compact --db beacon.db` only while stopped and only with temporary
space for approximately one additional database copy. Compaction can remove
free pages; it cannot make live retained data fit a smaller budget. Renaming a
connector also changes its durable route identity, so drain its queue first or
explicitly accept that its old queue/checkpoint will not transfer.

Store opening temporarily permits the larger of a 1 GiB page ceiling or the
current high-water mark plus 128 MiB so schema/index migrations are not blocked
by a legacy `max_page_count`. That allowance is not preallocated disk, does not
include WAL growth, and does not override the configured budget applied after
migration. Follow the complete backup, disposable-copy, ID-repair, and compact
procedure in the repository's `docs/vessel-release-gates.md`.

Deleting the main database and restarting Beacon gives you a completely empty
system (no sources, sinks, connector routes, or retained history). Take a
configuration backup first if you might want it back (`GET
/api/v1/config/export`, or `beacon export --db beacon.db`, see the API page) —
deleting the file does not export it for you.
