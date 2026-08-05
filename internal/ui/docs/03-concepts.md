# Concepts

## Sources, sinks, connectors

- A **source** decodes NMEA 2000 messages onto beacon from one endpoint: a
  CAN interface, USB-CAN adapter, HTTP stream, MQTT topic, capture file, or
  passive TCP/UDP NMEA 2000 gateway stream.
- A **sink** delivers messages somewhere: back onto a CAN bus, out over
  HTTP/TCP/MQTT to clients and brokers, or appended to a local file
  (`ndjson` or `candump` format), or onto a remote bus through a writable
  `tcp_gateway` sink.
- A **connector** is the only thing that actually moves data — it names
  exactly one source and one sink, an optional list of CEL filter
  expressions (see the filters page), a bridge mode, and its own durable buffer. A source
  or sink with no connector referencing it does nothing.

Multiple connectors can share the same source or the same sink (e.g. one
CAN source feeding both an SSE connector for a browser dashboard and a
socketcan connector mirroring onto a second bus), each with its own
filters and its own buffer.

## Bridge modes

- **`semantic`** (default) decodes a message and writes it through Beacon's
  claimed N2K client, so Beacon is the source device on the destination bus.
- **`transparent`** preserves the envelope's original priority, PGN, source,
  destination, and raw payload, including unknown PGNs. It currently requires
  a SocketCAN sink, rejects same-interface loops, and blocks management PGNs
  unless `forward_management` is explicitly enabled.
- **`observe`** filters, retains, checkpoints, and inspects messages without
  calling the configured sink.

## The envelope

Every message sent over SSE, WebSocket, TCP, MQTT, or NDJSON has exactly three
top-level keys:

| JSON field | Type | Meaning |
|---|---|---|
| `payload` | object | the verbatim JSON representation of the decoded `open-ships/n2k` Go struct |
| `metadata` | object | every field Beacon adds for routing, provenance, enrichment, and replay |
| `raw` | string or null | base64-encoded assembled CAN bytes, kept separate because they are data rather than metadata |

Every `payload` includes the n2k struct's complete `info` object. In n2k v1 this
includes timestamp, receive/transport timing, adapter ID, network ID,
direction, priority, PGN, source ID, and target ID. All remaining keys are the
original generated fields for that PGN, using n2k's JSON names and raw wire
values. Beacon does not strip, rename, or relocate any n2k field. A Go consumer
can unmarshal the nested object directly:

```go
var event struct {
    Payload  pgn.VesselHeading `json:"payload"`
    Metadata json.RawMessage   `json:"metadata"`
}
err := json.Unmarshal(document, &event)
```

For an unknown PGN, `payload` is the complete `pgn.UnknownPGN` JSON rather than
`null`.

`metadata` can contain:

| JSON field | Present when | Meaning |
|---|---|---|
| `id` / `connector` | queued/replayed messages | route sequence and connector identity |
| `observed_at` | always | when this Beacon ingress observed the message |
| `ingress` / `origin_ingress` | known | current and first-known ingress provenance |
| `device_name` / `device_name_hex` | address claim known | stable ISO Device NAME in numeric and JavaScript-safe hexadecimal forms |
| `pgn_name` / `variant` / `transport` | cataloged PGNs | catalog identity and N2K transport kind |
| `manufacturer_code` | proprietary PGNs | manufacturer discriminator decoded from the payload |
| `decode` | always | decoded/unknown status and catalog completeness details |
| `physical` | decoded numeric fields | additive unit-scaled values; payload raw ticks stay unchanged |

`id` and `connector` appear only after a message passes through a connector
buffer. Top-level `raw` is original for unknown PGNs and a canonical
re-encoding for decoded PGNs.

## Source and sink stream inspector

Each source and sink overview has a **Stream contents** panel. It starts
stopped and captures only messages that arrive after **Start**:

- A source panel observes envelopes at that source's received boundary.
- A sink panel observes envelopes after successful or accepted delivery to
  that sink.
- **Stop** closes the live preview while keeping the current capture in the
  browser. Start and Stop replace one another so only the currently relevant
  action is shown. **Clear** removes that local capture.

The optional **CEL filter** beside **Start** is applied by Beacon before
messages reach the browser. It uses the same `msg` shape as connector filters,
for example `msg.pgn == 129026` or
`msg.source == 11 && msg.decode_status == "decoded"`.
The filter remains editable while streaming. A valid change reconnects the
best-effort preview with the new expression without clearing captured rows; an
invalid change leaves the current stream filter active.

The inspector is best-effort and non-blocking: it never consumes a connector's
durable queue or slows vessel traffic when the browser cannot keep up. The
captured count continues for the full browser session while the panel keeps the
latest 200 messages in the current browser tab for display, copy, and export.

**JSONL** shows one compact, verbatim nested `payload` per line, ready to
unmarshal into the corresponding n2k Go struct. **CAN bytes** shows one
top-level `raw` payload as hexadecimal per line. **Export JSONL** downloads the
captured payload lines oldest first. **Export CAN** downloads one uppercase
hexadecimal payload per line; the line boundary preserves the message boundary
that would be lost by concatenating the bytes into one binary blob. Click any
JSON key or value to inspect a relevant CEL filter and optionally apply it.
**Copy stream** copies every retained line in arrival order using the active
JSONL or spaced-hex CAN representation.

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

`queue_depth` and `queue_bytes` mean **pending delivery after the connector
checkpoint**. `retained_depth` and `retained_bytes` include acknowledged rows
kept for replay. The metrics response also exposes checkpoint/tail, oldest
pending time, configured storage headroom, delivery class, route state/error,
drops, and per-stage totals. If retention pruning removes a message that was
still pending delivery, `pending_pruned` records that loss separately from
ordinary retained-history pruning.

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

- **Confirmed** routes advance after a successful CAN/file/raw-wire write or
  acceptance by a `null` sink.
- **Resumable** SSE/WS routes advance after the entry is available through
  their retained replay stream.
- **Best-effort** TCP/MQTT routes advance after dispatch; downstream receipt
  is unknown and is never labeled confirmed delivery.
- **Observe-only** routes advance after local inspection without a sink write.

- **CAN sinks** (`socketcan`, `usbcan`, and `tcp_gateway`, which transmits
  onto a remote bus through a Yacht Devices or Actisense TCP WiFi gateway
  with the same semantics) confirm each message that can be
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
  can't wedge the connector. HTTP/TCP/MQTT sinks still deliver these messages —
  see the `raw` field note above.
- **HTTP/TCP/MQTT sinks** (`http_sse`, `http_ws`, `tcp`, `mqtt`) broadcast to whichever
  clients happen to be connected at the moment, with no per-message
  confirmation. SSE/WS clients can recover anything they missed via replay
  (above), bounded by the connector's buffer limits; `tcp` and `mqtt`
  consumers cannot.
- **File sinks** (`file`) confirm each write the same way CAN sinks confirm
  a push: a write or flush failure is retried with the connector's backoff
  until it succeeds, so nothing is silently dropped while the disk stays
  writable — at-least-once delivery. In `ndjson` format every envelope is
  written as one JSON object per line; in `candump` format an envelope with
  no raw CAN bytes (the same "undecodable PGN" case CAN sinks skip — see
  the `raw` field note above) is **skipped**, not retried, and a payload too
  large for the fast-packet protocol to re-fragment (over 223 bytes) is
  skipped too. Fast-packet PGNs are re-fragmented into wire-accurate
  NMEA 2000 fast-packet frames — the same frames that would actually appear
  on the CAN bus — so a `candump` log can be replayed with the `can-utils`
  package's `canplayer`: `canplayer -I nav.log vcan0=<connector-id>` maps
  the connector id each line carries in place of an interface name
  (candump's token field) onto a real or virtual interface (`vcan0` here)
  to send the frames on. Fast-packet detection uses the PGN catalog; for a
  PGN the catalog doesn't know, beacon falls back to payload size (over 8
  bytes → fast-packet), so a small-payload message on an uncataloged
  proprietary fast-packet PGN logs as a single frame rather than a
  fast-packet sequence. The active file rotates to `<path>.1` (and
  `.1`→`.2`, etc., oldest dropped) once it exceeds `max_file_bytes`;
  `max_files` counts the active file plus its rotated backups. A short write
  from a full disk (`ENOSPC`) can leave a torn trailing line or, once the
  condition clears and the retried write succeeds, a logically-equivalent
  resend of that one message (for fast-packet lines the resend carries a
  fresh sequence number, so it isn't byte-identical) — both expected under
  this delivery model, not corruption.
- **Null sinks** (`null`) accept every message at the confirmed boundary,
  record the same connector and sink message, byte, rate, and stream-inspector
  statistics as another confirmed sink, and then intentionally discard it.
  They have no endpoint-specific configuration and no external side effects.

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
