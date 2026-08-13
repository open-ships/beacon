# Troubleshoot Beacon

Start at the source. Then, check the connector and the sink. Use the component
state, counters, queue measurements, and logs to identify the boundary that
does not operate correctly.

[![Decision flow for tracing missing messages from source reception through connector filtering and queueing, sink delivery, and the client cursor](/assets/manual/route-triage.svg)](/assets/manual/route-triage.svg)

_Follow the route in message order. The first boundary without evidence of
progress is usually where the useful error or counter lives._

## No messages arrive

1. For SocketCAN, run `ip -details link show can0`.
2. Make sure that the interface is up at 250 kbit/s.
3. For USB-CAN, make sure that `port` identifies the correct serial device.
4. On a physical bus, check the two 120 ohm end-of-bus termination resistors.
5. Read `GET /health` on the admin port.
6. Find the source component.
7. Read `beacon_source_messages_total` at `/metrics`.

An `up` source operates. A `degraded` source is reconnecting; read its `err`
value. An absent source is disabled or is not in the configuration.

If `beacon_source_messages_total` increases, the source receives and decodes
messages. Check for a missing connector or a filter that rejects all messages.
If the value remains zero, correct the source or physical network.

For one missing sensor or PGN on a busy bus, open the source overview. Examine
**PGN traffic**. Gap rows show the last-seen age and learned message period.

Agents can read the same data with MCP `get_source_metrics`. Prometheus exports
`beacon_source_pgn_gap_active` when
`settings.observability.prometheus_source_details` is true.

## A CAN sink does not write

A `socketcan` or `usbcan` sink retries a failed write. It does not discard an
encodable message. The connector queue grows while the sink is unavailable.

Check these values:

```http
GET /api/v1/connectors/{id}/metrics
GET /metrics
```

Read `queue_depth`, or read the `beacon_connector_queue_depth` Prometheus
gauge. If the queue grows and delivery does not increase, check the sink bus,
adapter, and interface.

Beacon skips a message that it cannot encode for CAN. Check this metric:

```text
beacon_connector_messages_total{connector="...",stage="skipped"}
```

An unknown PGN without a decoder is a common cause. Beacon advances the
checkpoint so that one unencodable message does not stop the route.

For a new pending entry, Beacon logs `push failed; retrying` at warning level.
It logs later retries for the same entry at debug level.

## An HTTP POST sink does not deliver

An `http_post` sink confirms a batch only after a 2xx response. A timeout,
redirect, transport failure, or non-2xx response keeps the batch pending.

Check these items:

1. Check the sink URL.
2. Check certificate trust.
3. Check authentication headers.
4. Read the response status and body in the route error.
5. Check the request timeout.

If the receiver stored a batch but lost the response, use Beacon's
deterministic `Idempotency-Key` to remove the duplicate retry.

For a rate limit or planned outage, return `Retry-After` as delta-seconds or an
HTTP date. Beacon uses the larger of that delay and its current backoff.

Use `beacon_sink_http_requests_total` to separate response statuses. Use
`beacon_sink_http_request_latency_seconds` to identify a slow receiver. Use the
payload-size histograms to identify a large batch. If gzip is active, compare
the on-wire and uncompressed sizes.

## A PostgreSQL sink does not write

A `postgres` sink remains degraded until it can connect and verify the table.
The connector keeps the batch pending.

1. Check the connection URL and TLS mode.
2. Check the database permissions.
3. Read the live sink error.
4. Verify that the destination table has the required columns.

If automatic table creation is off, edit the sink and select **Copy DDL**. Run
that exact DDL in the target database. Beacon verifies the table again and
recovers without a restart.

For TimescaleDB, install the extension before you run the generated
`create_hypertable` statement.

Beacon does not accept a table with incompatible columns. It reports the
schema or insert error and keeps the batch pending.

## A file sink does not write

Check these items in sequence:

1. Make sure that `file_path` is absolute.
2. Make sure that the parent directory exists.
3. Make sure that the Beacon process can write to the directory and file.
4. Read the file-sink `err` value in `GET /health`.
5. Check available disk space.
6. Check `max_file_bytes` and `max_files`.

Beacon creates the output file, but it does not create parent directories. A
relative path fails configuration validation.

A write failure remains pending and uses the connector retry backoff. After
space becomes available, Beacon catches up without an operator action.

At startup or hot restart, Beacon rotates an active file that exceeds
`max_file_bytes`. It removes rotations that exceed lower byte or count limits.
`max_files` includes the active file and cannot exceed 128.

Beacon skips a new record that is larger than `max_file_bytes`. It checkpoints
the record and does not create an oversized file.

File-sink confirmation means that buffered data reached the operating-system
write path. It does not mean that the data reached stable media. Beacon does
not perform `fsync` for each record or batch because that can increase flash
wear. Test recent-record durability with the actual computer, filesystem,
controller, storage device, and power-loss conditions.

## A connector queue grows

Each connector buffer is bounded. Beacon applies `max_messages`, `max_age`,
and `max_bytes` independently.

Read the configured connector:

```http
GET /api/v1/connectors/{id}
```

Read its live measurements:

```http
GET /api/v1/connectors/{id}/metrics
```

Sustained growth near any limit means that the sink cannot keep pace with the
source. Beacon prunes old retained data to enforce the limits.

An omitted `max_messages` becomes 10,000. An omitted `max_bytes` becomes
64 MiB. These limits apply to acknowledged replay data and pending delivery.
Age uses the local observation or admission time, not the wire timestamp.

Use `beacon size-buffer` if the sink outage can exceed a default limit. Set
the message, byte, and age values explicitly.

Queue-write retries start with a 100 ms ceiling. Sink-delivery retries start
with a 250 ms ceiling. Both increase to a one-minute maximum. Neither requires
a restart after a long outage. However, retention can prune a pending message
if the route is too small.

## Configuration exceeds a limit

Beacon rejects the complete write before reconciliation when a value exceeds
a configuration limit.

Common limits are:

- 32 sources
- 32 sinks
- 64 connectors
- 32 headers for one source or sink
- 32 filters for one connector
- 8 KiB for one filter
- 128 bytes for an ID
- 256 bytes for a name or header name
- 8 KiB for an endpoint or header value
- 4 KiB for an MQTT topic
- 256 KiB for all authored Source, Sink, and Connector text

These limits count UTF-8 bytes. The aggregate includes IDs, names, endpoints,
headers, topics, and filters.

See the Storage and resource limits page for the complete list.

## A client connection is rejected

Each SSE, WebSocket, or TCP sink permits 32 clients. An additional SSE or
WebSocket request gets `503` and `Retry-After: 5`. An additional TCP connection
closes.

The admin listener, data HTTP listener, and plain TCP sink listeners also
share 128 accepted connections. The 32-client sink limits are part of this
shared limit.

Request headers cannot exceed 64 KiB. Typed REST and MCP bodies cannot exceed
1 MiB. Beacon applies a five-second header timeout, a 30-second request-read
timeout, and a 90-second idle timeout.

## A remote Envelope is rejected

Beacon discards an MQTT, SSE, or WebSocket source Envelope at admission if it
exceeds one of these limits:

- 256 KiB for the encoded Envelope
- 128 KiB for decoded `payload` JSON
- 1,785 bytes for raw data
- 256 physical fields
- 256 missing-field names
- 1 KiB for a metadata string, field name, unit, or quantity

The Envelope does not enter a connector, so the connector cannot report the
error. Examine source traffic and the upstream Envelope.

## Interpret health states

`GET /health` and `GET /api/v1/health` report `up`, `degraded`, or `error` for
each component. The overall status is `ok` only when all reported components
are `up`.

A disabled component is absent. This is normal.

The dashboard can also show `restarting` for an enabled component that is
temporarily absent during a hot update. This state exists only in the UI. If
it remains for more than a few seconds, read the component logs. The new
instance probably cannot start.

## An SSE client missed data

A browser `EventSource` sends `Last-Event-ID` after a reconnect. A new client
without a cursor receives live data only.

Resume from a known cursor:

```text
?after=<connector>:<sequence>
```

Request all retained data for one connector:

```text
?after=<connector>:0
```

Use commas between cursors when a sink serves multiple connectors. Beacon
cannot replay a message that retention pruning removed.

## Find stored data

The SQLite main database contains these items:

- Sources, sinks, connectors, and settings
- Appliance identity and inventory
- Durable connector queues

The `--db` option selects the file. The default path is `beacon.db` relative
to the working directory. A file sink writes to its configured `file_path`.
That file is not part of a database backup.

The default SQLite main-file limit is 1 GiB. Route allocations share 896 MiB
after the 128 MiB reserve. The WAL is separate and can temporarily exceed its
16 MiB target. Keep filesystem headroom above the configured database limit.

Beacon performs a passive checkpoint and bounded incremental vacuum every six
hours. It does not run a full vacuum while it operates.

**CAUTION:** Stop Beacon before you run an offline compaction. Keep temporary
space for approximately one additional database copy.

```bash
beacon compact --db beacon.db
```

Use compaction to reclaim a legacy or high water file. You must also compact
before you lower `max_database_bytes` below the current page high water mark.

## An upgrade fails before Beacon is ready

A new version can migrate an old schema and then reject legacy configuration
or storage state. Check these common causes:

- Each source, sink, and connector ID must be a lowercase slug of not more
  than 128 bytes.
- The database page high water mark must not exceed the effective
  `max_database_bytes`.

**CAUTION:** Do not repeatedly open the only database copy during diagnosis.

1. Stop Beacon.
2. Preserve the main database and its `-wal` and `-shm` sidecar files.
3. Export the configuration.
4. Test the candidate against a disposable database.
5. Correct invalid IDs in the disposable copy.
6. Compact only a stopped copy with sufficient free space.

Compaction can remove free pages. It cannot make live retained data fit a
smaller limit.

Renaming a connector changes its durable route identity. Drain its queue
before you rename it, or accept that the old queue and checkpoint do not
transfer.

During migration, Beacon temporarily permits the larger of a 1 GiB page limit
or the current high water mark plus 128 MiB. This allows schema and index
migrations. It does not preallocate disk space, include WAL growth, or replace
the configured budget that Beacon applies after migration.

Use the full backup, disposable-copy, ID-repair, and compaction procedure in
`docs/vessel-release-gates.md`.

## Reset the database

Deleting the main database and restarting Beacon creates an empty system. It
removes the configuration, identity, inventory, connector queues, and retained
history.

**WARNING:** Export the configuration before you delete the database if you
need to keep it. Deleting the database does not create an export.

For a running Beacon process, use `GET /api/v1/config/export`. For a stopped
process, use `beacon export --db beacon.db`.
