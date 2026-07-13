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

## Queue growth

Every connector's buffer is bounded by its own `max_messages` / `max_age`
/ `max_bytes` (see the concepts page); pruning runs periodically and
enforces whichever of those are actually set. Check a connector's
configured limits with `GET /api/v1/connectors/{id}` and its current depth
with `GET /api/v1/connectors/{id}/metrics` — sustained growth up against
`max_messages` (or the age/byte equivalent) means deliveries aren't
keeping up with intake and old data is now being pruned to make room, not
retained indefinitely. If no limit at all is configured, `max_messages`
defaults to 100000.

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
