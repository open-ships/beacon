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
`beacon_source_pgn_gap_active`; agents can query the same store with the MCP
`get_source_metrics` tool.

## CAN write failures

A `socketcan`/`usbcan` sink confirms every push; on failure the connector
retries with exponential backoff (250ms, doubling, capped at 5 seconds)
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

## Queue growth

Every connector's buffer is bounded by its own `max_messages` / `max_age`
/ `max_bytes` (see the concepts page); pruning runs periodically and
enforces whichever of those are actually set. Check a connector's
configured limits with `GET /api/v1/connectors/{id}` and its current depth
with `GET /api/v1/connectors/{id}/metrics` — sustained growth up against
`max_messages` (or the age/byte equivalent) means deliveries aren't
keeping up with intake and old data is now being pruned to make room, not
retained indefinitely. If no limit at all is configured, `max_messages`
defaults to 10000.
That cap applies to retained rows whether acknowledged or still pending. If a
sink may be unavailable long enough to exceed 10000 messages, configure a
larger explicit count, age, or byte budget for that connector.

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

Everything — configuration (sources, sinks, connectors) and every
connector's message buffer — lives in one SQLite file, the `--db` path
(`beacon.db` by default, relative to the working directory unless given an
absolute path). There's nothing else to back up or reset: deleting that
file and restarting beacon gives you a completely empty system (no
sources, sinks, connectors, or buffered history) — the same as a first
boot. Take a configuration backup first if you might want it back (`GET
/api/v1/config/export`, or `beacon export --db beacon.db`, see the API
page) — deleting the file does not export it for you.
