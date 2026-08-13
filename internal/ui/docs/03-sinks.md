# Sinks

A sink sends, serves, stores, or discards messages from a connector. The sink
type determines the delivery guarantee and the required settings.

A sink does not receive source data by itself. At least one enabled connector
must refer to the sink.

## Sink types

| Type | Sends to | Required or primary settings | Delivery class |
|---|---|---|---|
| `socketcan` | A Linux SocketCAN interface | `interface` | Confirmed |
| `usbcan` | A serial USB-CAN adapter | `port` | Confirmed |
| `http_sse` | Connected SSE clients | `path` | Resumable |
| `http_ws` | Connected WebSocket clients | `path` | Resumable |
| `http_post` | A remote HTTP or HTTPS API | `url`; optional batch, timeout, gzip, and headers | Confirmed |
| `tcp` | Connected NDJSON clients | `address` | Best effort |
| `mqtt` | An MQTT broker topic | `url`, `topic` | Confirmed at QoS 1 |
| `postgres` | PostgreSQL or TimescaleDB | `url`; optional table and batch settings | Confirmed |
| `file` | A local NDJSON or candump file | `file_path`, `format` | Confirmed |
| `tcp_gateway` | A remote NMEA 2000 bus | `address`, `format` | Confirmed |
| `null` | No external destination | None | Confirmed discard |

All sinks also have `id`, `name`, `type`, and `enabled` settings.

## SocketCAN sink

Set `interface` to a Linux CAN network-interface name. Prepare the interface
before you enable the sink.

Beacon writes each encodable message to the bus. It retries a failed write. It
skips a message that it cannot encode for CAN.

## USB-CAN sink

Set `port` to the serial device path. Beacon opens the adapter directly and
uses the same confirmed write behavior as a SocketCAN sink.

## SSE and WebSocket sinks

Set `path` to a path on the data server, such as `/events`. The default data
server address is `0.0.0.0:8080`.

The path cannot use a Beacon administrative path, such as `/api`, `/docs`,
`/health`, or `/metrics`.

SSE and WebSocket clients receive canonical Envelopes. A client can use a
connector cursor to replay retained messages. Each sink permits 32 connected
clients.

## HTTP POST sink

Set `url` to the remote HTTP or HTTPS endpoint. Beacon sends a JSON array of
canonical Envelopes.

Use these optional settings:

| Setting | Default | Function |
|---|---:|---|
| `batch_size` | 100 | Sets the maximum Envelopes in one request; maximum 1,000 |
| `request_timeout` | `10s` | Limits one request attempt |
| `gzip` | false | Compresses the request body and sets `Content-Encoding: gzip` |
| `headers` | None | Adds static authentication or application headers |

A 2xx response confirms the complete batch. Beacon retries a timeout,
redirect, transport failure, or non-2xx response. Each request has a
deterministic `Idempotency-Key` for receiver-side duplicate removal.

Beacon stores header values in SQLite and includes them in configuration
exports. Protect the database and exported files as secrets.

## Plain TCP sink

Set `address` to the listen address, such as `0.0.0.0:9090`.

Each client receives one canonical Envelope on each NDJSON line. Delivery is
live and best effort. The sink does not replay missed messages.

## MQTT sink

Set `url` to the broker URL. Set `topic` to the publish topic.

Beacon publishes one canonical Envelope at QoS 1. It advances the connector
checkpoint after broker PUBACK. PUBACK confirms broker acceptance, not
subscriber receipt. Subscribers must tolerate duplicate messages.

## PostgreSQL sink

Set `url` to the PostgreSQL connection URL.

Use these optional settings:

| Setting | Default | Function |
|---|---:|---|
| `table` | `public.beacon_envelopes` | Sets the schema-qualified destination table |
| `batch_size` | 100 | Sets the maximum Envelopes in one statement; maximum 1,000 |
| `write_timeout` | `10s` | Limits schema and write operations |
| `auto_create_table` | false | Creates and verifies the table and indexes |
| `timescaledb` | false | Converts the table to a hypertable on `observed_at` |

Install the TimescaleDB extension before you enable `timescaledb`. If automatic
creation is off, use **Copy DDL** in the sink editor and run the supplied DDL.

Beacon writes each batch atomically. Its conflict key prevents an ambiguous
retry from inserting the same Envelope twice.

## File sink

Set `file_path` to an absolute path. The parent directory must exist. Set
`format` to one of these values:

| Format | Output |
|---|---|
| `ndjson` | One canonical Envelope on each line |
| `candump` | CAN frames that `canplayer` can replay |

`max_file_bytes` defaults to 100 MiB. `max_files` defaults to five and includes
the active file. Beacon rotates before the next complete record would exceed
the byte limit.

A file confirmation means that a buffered flush reached the operating-system
write path. It does not mean `fsync` or stable-media persistence.

## TCP gateway sink

Set `address` to `host:port`. Set `format` to `ydraw` or `actisense`.

This sink connects to a writable Yacht Devices or Actisense gateway. It claims
an NMEA 2000 address on the remote bus and uses confirmed CAN delivery.

## Null sink

The `null` sink needs no endpoint settings. It accepts and discards each
message. It keeps the normal route, rate, byte, and stream-inspector
statistics.

Use this sink to measure or inspect a route without an external side effect.

## Sink state

An enabled sink reports `up`, `degraded`, or `error`. A disabled sink does not
run and is absent from health results.

The process supports a maximum of 32 configured sinks.
