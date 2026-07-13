# Concepts

## Sources, sinks, connectors

- A **source** decodes NMEA 2000 messages onto beacon from one endpoint: a
  CAN interface, a USB-CAN adapter, or an HTTP stream.
- A **sink** delivers messages somewhere: back onto a CAN bus, or out over
  HTTP/TCP to clients.
- A **connector** is the only thing that actually moves data — it names
  exactly one source and one sink, an optional list of CEL filter
  expressions (see the filters page), and its own durable buffer. A source
  or sink with no connector referencing it does nothing.

Multiple connectors can share the same source or the same sink (e.g. one
CAN source feeding both an SSE connector for a browser dashboard and a
socketcan connector mirroring onto a second bus), each with its own
filters and its own buffer.

## The envelope

Every message flowing through beacon — in a connector's buffer, over SSE/WS/TCP,
and as the value a CEL filter's `msg` variable is bound to — is a JSON
object with this shape:

| JSON field | Type | Present when | Meaning |
|---|---|---|---|
| `id` | integer | queued/replayed messages only | the message's sequence number within its connector's buffer |
| `connector` | string | queued/replayed messages only | the id of the connector that delivered this message |
| `pgn` | integer | always | the NMEA 2000 PGN number |
| `source` | integer | always | source device address (0-253) |
| `dest` | integer | always | destination address (255 = broadcast) |
| `priority` | integer | always | 0 (highest) to 7 (lowest) |
| `timestamp` | string | always | RFC 3339 timestamp |
| `payload` | object or `null` | always | the decoded PGN fields, one JSON key per field, `null` for a PGN beacon doesn't know how to decode |
| `raw` | string (base64) | usually | the CAN payload bytes: for an undecodable PGN, the original bytes as received; for a decoded PGN, the canonical re-encoding (omitted in the rare case re-encoding fails). Undecodable PGNs still carry these bytes here, but a CAN sink cannot write them back out (see Delivery guarantees below) — they're skipped by `socketcan`/`usbcan` sinks and delivered as-is to HTTP/TCP sinks |

`id` and `connector` are only populated once a message has passed through a
connector's buffer — a freshly-decoded message from a source doesn't have
them yet.

## Buffering and pruning

Each connector has its own durable, per-connector buffer (backed by SQLite,
survives a beacon restart) that a slow or disconnected sink drains from
independently — a fast source never blocks on a slow sink. Buffer limits
are configured per connector:

| Limit | Meaning |
|---|---|
| `max_messages` | keep at most this many messages |
| `max_age` | drop messages older than this duration |
| `max_bytes` | keep at most this many bytes of payload+raw data |

Any combination can be set; if none are set at all, `max_messages` defaults
to 100000. Pruning runs periodically per connector, deleting whatever each
configured limit says to drop; if more than one limit is set, all of them
are enforced.

## Replay

A connector's buffer is what makes reconnecting clients able to catch up
instead of just losing whatever happened while they were away. Replay is
identified by a composite `connector:seq` event id — necessary because one
sink can serve more than one connector at once, so a single incrementing
counter wouldn't be enough to say where each connector's client left off.

- **SSE**: the browser's native `EventSource` reconnect sends back whatever
  `id:` value the last event carried as the `Last-Event-ID` header
  automatically. The same cursor can also be supplied explicitly as an
  `?after=` query parameter — useful for a non-browser client. Either form
  accepts a comma-separated list of `connector:seq` pairs when the sink
  serves more than one connector, e.g. `?after=nav:104,engine:57`.
- **WS**: the same `?after=` query parameter (comma-separated for multiple
  connectors); there's no `Last-Event-ID` equivalent for WebSocket, so a
  client that cares about resuming needs to track and resend its own
  cursor.
- **TCP**: live-only. A `tcp` sink has no replay at all — a client that
  disconnects loses whatever was broadcast while it was gone.

A connection carrying **no** cursor at all gets no replay: it streams live
from the moment it connects. Replay happens only for the connectors a
cursor explicitly names — to pull everything a connector's buffer still
holds, ask for everything after sequence zero: `?after=<connector>:0`.

## Delivery guarantees

Delivery guarantees differ by sink kind:

- **CAN sinks** (`socketcan`, `usbcan`) confirm each message that can be
  re-encoded onto the bus: a push failure is retried with exponential
  backoff (starting at 250ms, doubling up to a 5 second cap) until it
  succeeds or the connector stops — the connector's buffer absorbs the
  backlog meanwhile rather than dropping anything. This is at-least-once
  delivery for encodable messages: a message can be redelivered after a
  restart landed between a push succeeding and its checkpoint being
  durably saved, but it is never silently dropped while the sink is
  reachable and the message is encodable. An envelope beacon cannot
  re-encode for CAN transmission (most commonly a PGN with no cataloged
  decoder) is the one exception: it's **skipped**, not retried — counted
  under the connector's `skipped` message stage, with the cursor advancing
  past it exactly like a successful delivery, so one unrecognized PGN
  can't wedge the connector. HTTP/TCP sinks still deliver these messages —
  see the `raw` field note above.
- **HTTP/TCP sinks** (`http_sse`, `http_ws`, `tcp`) broadcast to whichever
  clients happen to be connected at the moment, with no per-message
  confirmation. SSE/WS clients can recover anything they missed via replay
  (above), bounded by the connector's buffer limits; a `tcp` client cannot.

## Hot apply

Every configuration write (`PUT`/`DELETE` through the API, and therefore
through the UI too) takes effect immediately: the write is validated,
persisted, and then reconciled against the running system in the same
request — there is no separate "apply" step and no restart of beacon
itself. Reconciliation only stops and restarts whatever actually changed:
a connector also restarts if the source or sink it references changed
(its running instance holds a direct reference to the old one), even
though the connector's own configuration didn't change.

Because a restart is stop-then-start rather than instantaneous, there's a
brief window where an enabled component is momentarily absent from the
supervisor's live status. The dashboard renders that window as a
"restarting" chip rather than treating a momentarily-missing component as
an error; `/health` and `/api/v1/health` don't have this state — a
component there is only ever `up`, `degraded`, or `error` (or absent,
if disabled).
