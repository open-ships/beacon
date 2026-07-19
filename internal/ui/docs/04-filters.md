# Filters

A connector's `filters` field is a list of CEL (Common Expression Language)
expressions. Each one is evaluated against every message the connector's
source hands it; a message is delivered only if **every** expression in the
list evaluates to `true`. An empty list passes everything.

## Variables

Every expression sees a single variable, `msg`, matching the envelope shape
(see the concepts page):

| Variable | CEL type | Notes |
|---|---|---|
| `msg.pgn` | int | NMEA 2000 PGN number |
| `msg.source` | int | source device address (0-253) |
| `msg.dest` | int | destination address (255 = broadcast) |
| `msg.priority` | int | 0 (highest) to 7 (lowest) |
| `msg.timestamp` | string | RFC 3339 |
| `msg.payload` | map | decoded PGN fields, keyed by their JSON field name (camelCase, e.g. `heading`, `speedWaterReferenced`) |

`msg.payload`'s keys depend entirely on which PGN the message is — check
the specific PGN's decoded field names (the same names its JSON `payload`
uses on the wire; see the concepts page) rather than assuming a name. A
payload field is only present for messages of the PGN(s) that define it;
referencing a key absent from the current message's payload is a runtime
evaluation error, not `false` — see "Missing payload fields" below.

**Units: payload numbers are raw wire values, not scaled physical units.**
Each NMEA 2000 field has a per-field resolution the decoder does *not*
apply — e.g. PGN 128259's `speedWaterReferenced` has a resolution of
0.01 m/s, so a boat doing 2 m/s reports a raw value of 200; PGN 127250's
`heading` has a resolution of 0.0001 radians, so 90 degrees is roughly
15708. Write thresholds against the raw value (physical value divided by
the field's resolution), not the physical one.

## List semantics: AND across entries, OR within one

The list is ANDed: `filters: ["a", "b"]` requires both `a` and `b` to be
true. Use `||` inside a single expression for OR:

```json
"filters": ["msg.pgn == 127250 || msg.pgn == 128259"]
```

is one condition ("either PGN"), while

```json
"filters": ["msg.pgn == 127250", "msg.source != 42"]
```

is two conditions, both required ("this PGN, and not from that source").

## Cookbook

Single PGN:

```
msg.pgn == 127250
```

Allow-list of PGNs:

```
msg.pgn in [127250, 128259, 129026]
```

Priority gate (two list entries — both required):

```json
"filters": [
  "msg.pgn in [129025, 129026, 129029]",
  "msg.priority <= 3"
]
```

Payload field threshold — PGN 128259 (Speed) decodes a
`speedWaterReferenced` field with 0.01 m/s resolution, so "faster than
2 m/s" is a raw value above 200 (see the units note above). JSON numbers
evaluate as CEL doubles, so integer and float literals both compare
cleanly against them:

```
msg.payload.speedWaterReferenced > 200
```

Exclude a noisy device:

```
msg.source != 42
```

OR across PGNs:

```
msg.pgn == 127250 || msg.pgn == 128259
```

Only accept specific PGNs from a trusted GPS source (source 7), everything
else from source 7 dropped, every other source untouched by this rule:

```
msg.source != 7 || msg.pgn in [129025, 129026, 129029]
```

Per-source PGN allow-list — engine data (127488, 127489, 127493) only from
source 3, GPS data (129025, 129026, 129029) only from source 7, anything
from source 1:

```
msg.source == 1 || (msg.source == 3 && msg.pgn in [127488, 127489, 127493]) || (msg.source == 7 && msg.pgn in [129025, 129026, 129029])
```

Reject all untrusted sources except heading (PGN 127250) from a backup
compass on source 12:

```
msg.source in [1, 3, 7] || (msg.source == 12 && msg.pgn == 127250)
```

Combine a source allow-list with a priority gate — only low-latency nav
PGNs from known sources (three list entries, all required):

```json
"filters": [
  "msg.source in [1, 3, 7]",
  "msg.pgn in [129025, 129026, 127250, 128259]",
  "msg.priority <= 3"
]
```

## Missing payload fields

`msg.payload.someField` errors at evaluation time (not `false`) when the
current message's decoded payload doesn't have that key — most PGN fields
are optional and simply absent from a given message rather than present
with a zero value. An evaluation error drops that message and is counted
as a filter error (visible per-connector via the metrics endpoints — see
the API page), which is easy to mistake for "nothing matches" when it's
really "this PGN doesn't always carry that field". Guard an optional field
with `has()`:

```
!has(msg.payload.speedWaterReferenced) || msg.payload.speedWaterReferenced > 200
```

(An explicit `double(...)` cast is harmless but unnecessary — JSON numbers
already arrive as CEL doubles.)

## Validating before you save

`POST /api/v1/filters/validate` CEL-compiles a list of expressions without
persisting anything (see the API page) — the same check the UI's connector
form runs automatically as you type. Compile errors are shown below the
editor and the offending token is underlined in red. The same check is also
embedded in a connector `PUT` (which 422s), so an invalid expression cannot
reach a running connector even if client-side validation is unavailable.
