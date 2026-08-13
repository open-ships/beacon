# Connectors

A connector moves messages from one source to one sink. It controls filtering,
bridge behavior, buffering, and delivery state for that route.

## Connector settings

| Setting | Required | Function |
|---|---|---|
| `id` | Yes | Gives the route a stable lowercase identifier |
| `name` | Yes | Gives the route an operator-facing name |
| `source_id` | Yes | Selects one configured source |
| `sink_id` | Yes | Selects one configured sink |
| `enabled` | Yes | Starts or stops the route |
| `filters` | No | Selects messages with CEL expressions |
| `buffer` | Yes | Sets durable retention limits; `{}` uses defaults |
| `mode` | No | Sets `semantic`, `transparent`, or `observe` behavior |
| `forward_management` | No | Permits management PGNs in transparent mode |

The process supports a maximum of 64 configured connectors.

## Route behavior

A source and sink do not exchange messages until an enabled connector refers
to both components.

One connector has one source and one sink. Create more connectors when you
must do one of these tasks:

- Send one source to multiple sinks.
- Combine multiple sources at one sink.
- Apply different filters or buffer limits to the same source data.

Each connector has an independent checkpoint and durable buffer.

## Bridge modes

| Mode | Behavior | Restrictions |
|---|---|---|
| `semantic` | Decodes and writes the message through Beacon's claimed NMEA 2000 client | Default mode |
| `transparent` | Preserves priority, PGN, source, destination, and raw payload | Requires SocketCAN; prevents a same-interface loop |
| `observe` | Filters, buffers, checkpoints, and inspects the message without a sink call | The configured sink receives no message |

In semantic mode, Beacon is the source device on the destination bus.

Transparent mode also preserves an unknown PGN. It blocks NMEA 2000
network-management PGNs by default. Set `forward_management` to true only when
the destination bus must receive those PGNs.

## Filters

Beacon applies every expression in the `filters` list. All expressions must
return `true`. An empty list accepts all messages.

For example, accept Vessel Heading, PGN 127250:

```cel
msg.pgn == 127250
```

Use the Filters page for variables, optional fields, raw values, and more
examples.

## Buffer

The connector buffer is in SQLite and survives a Beacon restart. It lets the
source continue when the sink is slow or unavailable.

| Setting | Default | Function |
|---|---:|---|
| `max_messages` | 10,000 | Limits retained message count |
| `max_bytes` | 64 MiB | Limits retained canonical Envelope bytes |
| `max_age` | No age limit | Limits time after local admission |

Beacon applies all effective limits independently. Retention can remove a
message that is still pending if a route is too small for an outage.

Use the Storage and limits page to calculate explicit route limits.

## Checkpoint and state

The checkpoint identifies the last message that crossed the connector's
delivery boundary. The exact boundary depends on the sink type.

The connector reports queue depth, retained depth, checkpoint, delivery totals,
skipped messages, pruned pending messages, and the current route error. Use the
Delivery and replay page for delivery guarantees and cursor behavior.

## Apply a connector change

A UI, REST, or MCP write takes effect immediately. Beacon validates and stores
the connector before it reconciles the running route.

Beacon restarts the connector when you change its configuration. It also
restarts the connector when its source or sink changes.

The dashboard can briefly show `restarting`. The health APIs report `up`,
`degraded`, or `error`.

Beacon also compares stored configuration with running components approximately
every 30 seconds. It retries a failed start from a one-second delay to a
one-minute delay. Each attempt has a 15-second limit.
