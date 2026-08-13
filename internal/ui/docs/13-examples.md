# Examples

Use these examples as starting points. Change interface names, addresses,
credentials, paths, and capacity limits for your installation.

## Create a test route without CAN hardware

Create a virtual CAN interface:

```bash
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set vcan0 up
```

Start Beacon:

```bash
go run ./cmd/beacon \
  --db beacon.db \
  --admin-address 127.0.0.1:2112 \
  --data-address 127.0.0.1:8080
```

In the UI, create these components:

| Component | ID | Type | Important setting |
|---|---|---|---|
| Source | `can0` | `socketcan` | `interface: vcan0` |
| Sink | `sse` | `http_sse` | `path: /events` |
| Connector | `all` | Source `can0` to sink `sse` | Empty filter list |

Connect an SSE client:

```bash
curl -N localhost:8080/events
```

Send a CAN frame from another terminal:

```bash
cansend vcan0 18FF0001#0102030405060708
```

The SSE client receives the canonical Envelope.

## Create the same route with the REST API

Create the source:

```bash
curl -X PUT localhost:2112/api/v1/sources/can0 \
  -H 'Content-Type: application/json' \
  -d '{"id":"can0","name":"Test CAN bus","type":"socketcan","enabled":true,"interface":"vcan0"}'
```

Create the sink:

```bash
curl -X PUT localhost:2112/api/v1/sinks/sse \
  -H 'Content-Type: application/json' \
  -d '{"id":"sse","name":"Browser feed","type":"http_sse","enabled":true,"path":"/events"}'
```

Create the connector:

```bash
curl -X PUT localhost:2112/api/v1/connectors/all \
  -H 'Content-Type: application/json' \
  -d '{"id":"all","name":"All messages","source_id":"can0","sink_id":"sse","enabled":true,"filters":[],"buffer":{}}'
```

The empty `buffer` uses the effective defaults of 10,000 messages and 64 MiB.

## Seed an empty database

The repository contains ready-to-edit configurations in `examples/`.

Start an empty database with the minimal example:

```bash
beacon --db beacon.db --seed examples/minimal.json
```

The seed loads only when the database is empty.

Start with a navigation-only route:

```bash
beacon --db beacon.db --seed examples/navigation.json
```

Other supplied examples show engine routes, MQTT, HTTP POST, PostgreSQL,
files, remote gateways, and a chain between Beacon instances. Read
`examples/README.md` before you use credentials or production endpoints.

## Import a configuration into a running process

Replace the full configuration:

```bash
curl -X POST 'localhost:2112/api/v1/config/import?mode=replace' \
  -H 'Content-Type: application/json' \
  --data-binary @examples/navigation.json
```

Merge the engine-room resources by ID:

```bash
curl -X POST 'localhost:2112/api/v1/config/import?mode=merge' \
  -H 'Content-Type: application/json' \
  --data-binary @examples/engine-room.json
```

## Select one PGN

Select Vessel Heading, PGN 127250:

```cel
msg.pgn == 127250
```

## Select a list of PGNs

Select common navigation PGNs:

```cel
msg.pgn in [127250, 128259, 129025, 129026, 129029]
```

## Apply a priority gate

Use two filter items. Beacon requires both expressions to be true:

```json
"filters": [
  "msg.pgn in [129025, 129026, 129029]",
  "msg.priority <= 3"
]
```

## Exclude one source address

Exclude source address 42:

```cel
msg.source != 42
```

## Restrict PGNs from one source

Permit only position PGNs from source address 7. Leave messages from other
source addresses unchanged:

```cel
msg.source != 7 || msg.pgn in [129025, 129026, 129029]
```

## Use different PGNs for different sources

Accept all messages from source 1. Accept engine PGNs from source 3. Accept
position PGNs from source 7:

```cel
msg.source == 1 ||
(msg.source == 3 && msg.pgn in [127488, 127489, 127493]) ||
(msg.source == 7 && msg.pgn in [129025, 129026, 129029])
```

## Permit a backup compass

Accept trusted sources 1, 3, and 7. Also accept Vessel Heading from backup
source 12:

```cel
msg.source in [1, 3, 7] ||
(msg.source == 12 && msg.pgn == 127250)
```

## Filter a physical value

Select water-referenced speed greater than 2 m/s. Guard the optional field
before you read it:

```cel
has(msg.physical.speedWaterReferenced) &&
msg.physical.speedWaterReferenced.value > 2.0
```

To keep messages that do not contain the field and apply the threshold only
when the field exists, use this expression:

```cel
!has(msg.physical.speedWaterReferenced) ||
msg.physical.speedWaterReferenced.value > 2.0
```

## Resume an SSE stream

Resume connector `nav` after sequence 104:

```bash
curl -N 'localhost:8080/nav?after=nav:104'
```

Resume two connectors that share one sink:

```bash
curl -N 'localhost:8080/events?after=nav:104,engine:57'
```

Request all retained messages for connector `nav`:

```bash
curl -N 'localhost:8080/nav?after=nav:0'
```

A request without `after` receives live messages only.

## Size a connector buffer

Calculate limits for this condition:

- Peak rate: one Envelope each second
- Average Envelope size: 512 bytes
- Outage: seven days
- Reserve: 25 percent

Run:

```bash
beacon size-buffer \
  --rate 1 \
  --average-bytes 512 \
  --outage 168h \
  --reserve-percent 25
```

The result is:

```text
max_messages: 756000
max_bytes: 387072000
max_age: 210h0m0s
```

Apply all three values to the connector buffer:

```json
"buffer": {
  "max_messages": 756000,
  "max_bytes": 387072000,
  "max_age": "210h0m0s"
}
```

Make sure that the sum of all route byte limits fits the process database
budget.

## Send confirmed HTTP batches

Create an HTTP POST sink:

```json
{
  "id": "telemetry-api",
  "name": "Telemetry API",
  "type": "http_post",
  "enabled": true,
  "url": "https://api.example.com/v1/envelopes",
  "batch_size": 250,
  "request_timeout": "15s",
  "gzip": true,
  "headers": {
    "Authorization": "Bearer replace-me"
  }
}
```

Beacon stores header values in SQLite and includes them in configuration
exports. Protect the database and exported files as secrets.

The receiving service must return a 2xx status to confirm the batch. Use the
deterministic `Idempotency-Key` header to remove duplicate retries.

## Write to PostgreSQL or TimescaleDB

Create a PostgreSQL sink:

```json
{
  "id": "telemetry-db",
  "name": "Telemetry database",
  "type": "postgres",
  "enabled": true,
  "url": "postgresql://beacon:replace-me@db.local:5432/vessel?sslmode=require",
  "table": "telemetry.envelopes",
  "batch_size": 250,
  "write_timeout": "15s",
  "auto_create_table": true,
  "timescaledb": true
}
```

Install the TimescaleDB extension before Beacon tries to create a hypertable.
Set `timescaledb` to false for standard PostgreSQL.

The connection URL can contain credentials. Beacon includes the URL in
configuration exports. Protect these files as secrets.

## Chain two Beacon instances

On the downstream Beacon, use an upstream SSE endpoint as the source. Then,
send the messages to a local CAN sink.

```json
{
  "sources": [{
    "id": "upstream",
    "name": "Upstream Beacon",
    "type": "http_sse",
    "enabled": true,
    "url": "http://upstream-beacon.local:8080/events"
  }],
  "sinks": [{
    "id": "local-can",
    "name": "Local CAN bus",
    "type": "socketcan",
    "enabled": true,
    "interface": "can0"
  }],
  "connectors": [{
    "id": "chain",
    "name": "Upstream to local CAN",
    "source_id": "upstream",
    "sink_id": "local-can",
    "enabled": true,
    "filters": [],
    "buffer": {"max_messages": 50000}
  }]
}
```

Use the semantic bridge mode unless you have a specific requirement to
preserve original CAN addressing. The Delivery and replay page describes the
CAN encoding and retry behavior.
