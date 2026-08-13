# Manage storage and limits

Beacon bounds stored data and untrusted input. Configure these limits for the
traffic rate, outage duration, storage device, and computer on the vessel.

## Connector buffer

Each connector has an independent buffer in SQLite. The buffer lets a source
continue to receive messages when its sink is slow or disconnected. The
buffer survives a Beacon restart.

Use these settings for each connector:

| Setting | Function |
|---|---|
| `max_messages` | Sets the maximum number of retained messages |
| `max_age` | Sets the maximum time after local admission |
| `max_bytes` | Sets the maximum logical size of retained canonical Envelope JSON |

Beacon applies all effective limits. It does not select only one limit.

If `max_messages` is omitted or zero, Beacon uses 10,000 messages. If
`max_bytes` is omitted or zero, Beacon uses 64 MiB. These defaults apply even
when you set `max_age`. A zero `max_age` disables only the age limit.

Retention pruning can remove a message that is still pending. Configure all
limits explicitly when a route must tolerate a long sink outage.

## Retention time

Beacon calculates message age from the local `observed_at` or queue-admission
time. It does not use the timestamp in `payload.info`.

A software-facing remote source resets `observed_at` when the Envelope reaches
this Beacon instance. If a legacy Envelope does not contain `observed_at`,
Beacon uses the local queue-append time. Thus, a wrong upstream clock cannot
cause a new local message to expire immediately.

## Queue measurements

Beacon maintains live totals separately from retained Envelope rows. Metrics
reads do not scan the full retained history.

Use these values to examine a route:

| Value | Meaning |
|---|---|
| `queue_depth` | Messages that are pending after the connector checkpoint |
| `queue_bytes` | Logical bytes that are pending after the checkpoint |
| `retained_depth` | All retained messages, including acknowledged replay data |
| `retained_bytes` | Logical bytes of all retained messages |
| `pending_pruned` | Pending messages that retention pruning removed |

The metrics also show the checkpoint, tail, oldest pending time, storage
headroom, delivery class, route state, route error, drop count, and per-stage
totals.

## Size a buffer for an outage

Measure these route values:

1. Measure the peak admitted Envelope rate.
2. Measure the average canonical JSON Envelope size.
3. Select the longest sink outage that the route must support.
4. Select a reserve percentage.

Use this calculation:

```text
factor       = 1 + reserve_percent / 100
max_messages = ceil(rate_per_second x outage_seconds x factor)
max_bytes    = max_messages x average_envelope_bytes
max_age      = outage x factor
```

Use the CLI to calculate the settings. For example, calculate a seven-day
outage at one 512-byte Envelope each second with a 25-percent reserve:

```bash
beacon size-buffer --rate 1 --average-bytes 512 --outage 168h --reserve-percent 25
# max_messages: 756000
# max_bytes: 387072000
# max_age: 210h0m0s
```

One route cannot exceed these hard limits:

- 10,000,000 messages
- 8 GiB
- 365 days

The CLI rejects a result that exceeds a hard limit.

After you size all routes, set the database budget. Include the logical route
bytes, the database reserve, SQLite overhead, and filesystem overhead.

## Process storage budgets

The `settings.resources` object controls process-wide storage.

| Setting | Default | Function |
|---|---:|---|
| `max_database_bytes` | 1 GiB | Sets the physical page limit of the SQLite main database |
| `database_reserve_bytes` | 128 MiB | Reserves space for configuration, indexes, inventory, and SQLite overhead |
| `max_file_store_bytes` | 2 GiB | Sets the aggregate configured allocation for file sinks |

Beacon adds the effective `max_bytes` of all connector routes. This includes
the 64 MiB default for a route that omits `max_bytes`. The total must not
exceed `max_database_bytes - database_reserve_bytes`. With the defaults, the
route allocation limit is 896 MiB.

Beacon also adds `max_file_bytes x max_files` for all file sinks. This total
must not exceed `max_file_store_bytes`.

These checks validate configured allocations. Beacon does not preallocate the
database reserve or the file-store budget.

Use a database limit from 64 MiB through 64 GiB. Keep the effective reserve
below the database limit. Do not set a file-store budget above 64 GiB.

## SQLite file growth

SQLite uses `max_page_count` to enforce `max_database_bytes`. The main database
cannot grow above the page-rounded limit.

The write-ahead log is a separate file. Beacon targets a retained WAL size of
16 MiB and enables automatic checkpoints. An active transaction can cause the
WAL to temporarily exceed this target.

Keep free space for these items:

- The WAL
- Filesystem metadata
- Temporary files
- Recovery operations

You cannot lower `max_database_bytes` below the current database page high
water mark while Beacon operates. Stop Beacon and compact the database first.

## Database maintenance

Every six hours, Beacon does this bounded maintenance:

1. It requests a passive WAL checkpoint.
2. It runs an incremental vacuum of not more than 1,024 pages.

The attempt has a 30-second time limit. Beacon does not run a full `VACUUM`
while it serves traffic.

If an earlier Beacon version created the database without incremental vacuum,
perform one offline compaction. Also use this procedure to reclaim a high
water mark.

**CAUTION:** Stop Beacon before you compact the database. A full vacuum can
require temporary space for approximately one additional database copy.

```bash
beacon compact --db beacon.db
```

## Diagnostic storage limits

Beacon counts all source traffic, decode results, addressing data, and timing
data. It samples expensive decoded-field and raw-byte distributions at most
one time each second for each source, PGN, and address stream.

Beacon keeps rich diagnostics for a maximum of 256 streams for each source and
512 streams for the process. A new stream still contributes to exact source
totals when these limits are full, but Beacon omits its rich state. A minutely
admission sweep can reclaim a stream that is idle for more than six hours.

Each tracked stream keeps these bounded values:

- 32 decoded fields
- 32 missing-field names
- One latest decoded payload of not more than 8 KiB
- 256 raw bytes, with analysis of the first 32 bytes
- 16 raw lengths
- 16 distinct values for each byte position
- 32 fingerprints
- Four raw samples

Beacon omits an oversized latest payload. It does not truncate it into invalid
JSON. It does not change the canonical Envelope. Truncation and overflow
counters report each omission.

The Metrics and monitoring page explains how to enable the detailed
`beacon_source_pgn_*` Prometheus families. Core source, connector, queue, and
component metrics remain active when the detailed families are off. The MCP
`get_source_metrics` tool also reports diagnostic-capacity information.

## Configuration limits

| Item | Limit |
|---|---:|
| Sources | 32 |
| Sinks | 32 |
| Connector routes | 64 |
| Entity ID | 128 bytes |
| Display name | 256 bytes |
| Endpoint URL, address, interface, port, HTTP path, or file path | 8 KiB |
| MQTT topic | 4 KiB |
| Header name | 256 bytes |
| Header value | 8 KiB |
| Authored Source, Sink, and Connector text | 256 KiB aggregate |
| Headers for one source or sink | 32 |
| Filters for one connector | 32 |
| One CEL expression | 8 KiB |
| Files for one file sink, including the active file | 128 |
| Uncommissioned inventory entries | 1,024 Device NAMEs |

Text limits count UTF-8 bytes. The 256 KiB aggregate includes all
operator-authored strings in Source, Sink, and Connector values. It includes
IDs, names, endpoint fields, headers, MQTT topics, and CEL expressions. It does
not include fixed JSON syntax, numbers, or Boolean settings.

The inventory limit removes the oldest unapproved observations first. It does
not apply to Device NAMEs in an approved commissioning baseline.

## Preview and network limits

| Item | Limit |
|---|---:|
| Preview subscribers for each source or sink | 8 |
| Preview channel for each subscriber | 8 Envelopes |
| One serialized preview document | 32 KiB |
| Expanded gzip replay | 128 MiB |
| Simultaneous expanded replays | 2, or 256 MiB total |
| Remote MQTT, SSE, or WebSocket Envelope | 256 KiB |
| Remote decoded `payload` JSON | 128 KiB |
| Raw NMEA 2000 data in a remote Envelope | 1,785 bytes |
| Remote physical fields | 256 |
| Remote missing-field names | 256 |
| Remote metadata string, field name, unit, or quantity | 1 KiB |

Preview delivery does not block vessel traffic. A slow browser loses preview
messages. Source and sink snapshots report `preview_documents_omitted`.

Each source keeps 100 lifecycle events. Beacon persists a maximum of 1,000
lifecycle events for the process. One persisted event document cannot exceed
4 KiB.

The admin listener, shared data HTTP listener, and configured plain TCP sink
listeners share 128 accepted connections. Each configured SSE, WebSocket, or
plain TCP sink also has a 32-client limit. The per-sink limit is part of the
shared 128-connection limit.

The HTTP servers apply these limits:

- 64 KiB of request headers
- Five seconds to read headers
- 30 seconds to read a request
- 90 seconds before an idle connection closes
- 1 MiB for a typed REST request body or an MCP POST body

These connection limits do not include listeners, SQLite files, CAN or serial
endpoints, or outbound connections. Plan additional file descriptors for
these resources.
