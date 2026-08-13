# Control delivery and replay

The delivery guarantee depends on the sink type and bridge mode. Select a sink
that gives the guarantee that your application needs.

## Delivery classes

| Class | Sink or mode | Beacon advances the connector checkpoint when... |
|---|---|---|
| Confirmed | SocketCAN, USB-CAN, file, TCP gateway, HTTP POST, MQTT, PostgreSQL, and null | The write or batch succeeds, HTTP returns 2xx, MQTT receives PUBACK, or null accepts the message |
| Resumable | SSE and WebSocket | The Envelope is available in the retained replay stream |
| Best effort | Plain TCP | Dispatch completes; Beacon does not know if the client received the message |
| Observe only | `observe` bridge mode | Local inspection completes without a sink call |

At-least-once delivery can produce duplicates. A consumer of a confirmed sink
must tolerate duplicate messages.

## Retry behavior

Beacon retries confirmed sink delivery, remote-source connections, and
physical NMEA endpoint connections until the operation succeeds or the
component stops.

The retry ceiling starts at 250 ms and doubles to one minute. Equal jitter
selects the actual delay from one-half of the current ceiling through the full
ceiling.

Durable queue-append retries start with a 100 ms ceiling and use the same
one-minute maximum.

The connection history for a remote source, physical bus, or MQTT connection
resets after 30 seconds of stable connectivity. Short connect-and-disconnect
cycles continue to increase the backoff.

For SSE and WebSocket sources, Beacon limits each dial, TLS, and response-header
phase to 15 seconds. An established stream can remain open.

A valid HTTP `Retry-After` value can increase the next delivery delay. It
cannot reduce the current backoff, and Beacon limits it to one minute.

## CAN and TCP gateway sinks

The `socketcan`, `usbcan`, and `tcp_gateway` sinks confirm each encodable
message. Beacon retries a failed push. The connector buffer holds the pending
message during the retry.

The result is at-least-once delivery. Beacon can send a duplicate if the
process stops after the push succeeds but before it saves the checkpoint.

Beacon skips a message that it cannot encode for CAN transmission. An unknown
PGN without a catalog decoder is a common cause. Beacon increments the
connector `skipped` stage and advances the checkpoint. It does not let one
message stop the route.

HTTP streaming, HTTP POST, plain TCP, and MQTT sinks can deliver an unknown
message because the Envelope contains the raw data.

## HTTP POST sinks

An `http_post` sink sends an array of canonical Envelopes to an HTTP or HTTPS
endpoint. `batch_size` is a maximum. Beacon sends a short batch immediately
when no additional message is ready.

Beacon keeps the complete batch pending until the endpoint returns a 2xx
status. It retries these results:

- Request timeout
- Redirect
- Transport failure
- Any non-2xx status

Each request contains a deterministic `Idempotency-Key`. The receiving service
can use this key to remove a duplicate retry after a lost response.

Use `request_timeout` to limit one attempt. Use headers for static API keys,
bearer tokens, or other authentication values.

If you enable `gzip`, Beacon sets `Content-Encoding: gzip`. Compression does
not change the Envelope-count meaning of `batch_size`.

HTTPS uses the host trust store and performs normal certificate verification.

## PostgreSQL sinks

A `postgres` sink inserts query fields and the complete canonical Envelope in
an atomic batch. A successful statement confirms the batch.

Beacon uses this primary key for retry safety:

```text
(observed_at, connector_id, sequence)
```

It uses `ON CONFLICT DO NOTHING`. Therefore, a retry after a lost database
acknowledgement does not create an additional row.

The schema is also valid for a TimescaleDB hypertable partitioned on
`observed_at`.

Beacon can create the table and indexes. If automatic creation is off, the
sink editor supplies PostgreSQL or TimescaleDB DDL. Beacon continues to verify
the schema until the table is available.

## MQTT sinks

An `mqtt` sink publishes with QoS 1. Beacon confirms the message only after
the broker sends PUBACK.

PUBACK confirms broker acceptance. It does not confirm that a subscriber
received the message. If PUBACK is lost, Beacon publishes the message again.

Beacon disables automatic reconnect in the MQTT library. Each connection uses
a new client generation. A connection loss invalidates the generation and its
pending push. Beacon retries with a new client. Only an actual PUBACK advances
the connector checkpoint.

## File sinks

A `file` sink confirms a write after its buffered flush reaches the operating
system write path. This confirmation does not mean `fsync` or stable-media
persistence.

Beacon retries a write or flush failure. This gives at-least-once delivery
while the disk remains writable.

### NDJSON format

The `ndjson` format writes one canonical Envelope on each line.

### Candump format

The `candump` format writes CAN frames. Beacon skips an Envelope that has no
raw CAN data. It also skips a payload that is larger than the 223-byte
fast-packet limit.

Beacon re-fragments a cataloged fast-packet PGN into NMEA 2000 fast-packet
frames. You can replay the file with `canplayer`. For example:

```bash
canplayer -I nav.log vcan0=nav-route
```

In this example, `nav-route` is the connector ID that the candump line uses in
place of an interface name.

For an unknown PGN, Beacon uses payload size to identify a fast packet. A
payload larger than eight bytes becomes a fast packet. An unknown proprietary
fast-packet PGN with a small payload can therefore be written as one frame.

### File rotation

The defaults are 100 MiB for `max_file_bytes` and five files for `max_files`.
`max_files` includes the active file and cannot exceed 128.

Before a complete record would exceed `max_file_bytes`, Beacon rotates the
active file to `<path>.1`. It moves the existing rotated files to the next
number and removes the oldest file.

Beacon skips and checkpoints one record that is larger than
`max_file_bytes`. It does not create an oversized file or retry the record
without a limit.

At startup, Beacon rotates an oversized active file. It also removes rotated
files that exceed new, lower limits. Beacon logs each removal.

The configured allocation for a file sink is `max_file_bytes x max_files`.
The sum for all file sinks must fit `settings.resources.max_file_store_bytes`.
The default process limit is 2 GiB.

A full disk can cause a short write and a partial last line. After space is
available, the retry can write a logically equivalent duplicate. A retried
fast-packet record uses a new sequence number, so its bytes can differ.

## Streaming sinks

The `http_sse`, `http_ws`, and `tcp` sinks broadcast to connected clients.

SSE and WebSocket clients can replay retained messages. The connector buffer
limits the replay history. A plain TCP client receives only live messages.

Each configured streaming sink permits 32 clients. If an SSE or WebSocket sink
is full, Beacon returns `503` and `Retry-After: 5`. If a TCP sink is full,
Beacon closes the additional connection. TCP sink connections are receive
only. Beacon closes a client that sends data.

## Null sinks

A `null` sink accepts and discards each message. It records the normal
connector, sink, byte, rate, and inspector statistics. It has no
endpoint-specific configuration and no external effect.

## Replay cursor

Beacon identifies a replay position with `connector:sequence`. The connector
ID is necessary because one sink can serve multiple connectors.

### Replay an SSE stream

A browser `EventSource` automatically sends the last event `id:` value in the
`Last-Event-ID` header after a reconnect.

A client can also set the cursor in an `after` query parameter:

```text
/events?after=nav:104
```

For a sink that serves multiple connectors, separate cursor values with
commas:

```text
/events?after=nav:104,engine:57
```

### Replay a WebSocket stream

Use the same `after` query parameter. WebSocket does not have an equivalent of
`Last-Event-ID`, so the client must save and send its cursor.

### Request all retained data

A connection without a cursor receives only live messages. It does not replay
old messages.

To request all retained messages for one connector, start after sequence zero:

```text
/events?after=nav:0
```

Beacon cannot replay data that retention pruning removed.

### MQTT and TCP behavior

MQTT has no Beacon consumer-replay feature. The connector does retain pending
delivery while the broker is unavailable. Subscribers must tolerate duplicate
QoS 1 messages.

Plain TCP is live only. A disconnected client loses messages that Beacon
broadcasts during the disconnection.
