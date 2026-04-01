# Configuration Examples

| File | Use case |
|---|---|
| `minimal.toml` | Bare minimum — all messages, both sinks, no filtering |
| `navigation.toml` | Navigation dashboard — heading, GPS, speed, depth, XTE |
| `engine-room.toml` | Engine monitoring — RPM, temps, pressures, battery |
| `vcan-dev.toml` | Local development with virtual CAN (`vcan0`), debug logging |
| `high-volume.toml` | Busy bus (200+ msg/s) — large buffer, aggressive PGN + priority filtering |

## Usage

```sh
beacon --config examples/navigation.toml
```

## CEL filter reference

Filters are [CEL](https://github.com/google/cel-spec) expressions. Every expression
receives a `msg` variable with these fields:

| Field | Type | Example |
|---|---|---|
| `msg.pgn` | `int` | `129029` |
| `msg.source` | `int` | `1` |
| `msg.dest` | `int` | `255` (broadcast) |
| `msg.priority` | `int` | `2` (0 = highest) |
| `msg.timestamp` | `string` | `"2024-07-01T12:00:00Z"` |
| `msg.payload.<field>` | `dyn` | `msg.payload.SpeedWaterReferenced` |

Multiple expressions in the `filters` list are ANDed together. Use `||` inside
a single expression for OR logic:

```toml
# AND — both conditions must be true
filters = [
  "msg.pgn in [129025, 129026]",
  "msg.priority <= 3",
]

# OR — expressed in one CEL string
filters = [
  "msg.pgn == 127250 || msg.pgn == 128259",
]

# Only accept specific PGNs from a trusted GPS source (source 7)
filters = ["msg.source != 7 || msg.pgn in [129025, 129026, 129029]"]

# Per-source PGN allowlist: engine data from source 3, GPS from source 7, anything from source 1
filters = [
  "msg.source == 1 || (msg.source == 3 && msg.pgn in [127488, 127489, 127493]) || (msg.source == 7 && msg.pgn in [129025, 129026, 129029])",
]

# Reject all messages from untrusted sources except heading from a backup compass on source 12
filters = [
  "msg.source in [1, 3, 7] || (msg.source == 12 && msg.pgn == 127250)",
]

# Source allowlist combined with PGN and priority gating
filters = [
  "msg.source in [1, 3, 7]",
  "msg.pgn in [129025, 129026, 127250, 128259]",
  "msg.priority <= 3",
]
```

## Common PGN quick reference

| PGN | Name |
|---|---|
| 127250 | Vessel Heading |
| 127251 | Rate of Turn |
| 127257 | Attitude |
| 127488 | Engine Parameters, Rapid Update |
| 127489 | Engine Parameters, Dynamic |
| 127493 | Transmission Parameters |
| 127505 | Fluid Level |
| 127508 | Battery Status |
| 128259 | Speed Through Water |
| 128267 | Water Depth |
| 129025 | Position, Rapid Update |
| 129026 | COG & SOG, Rapid Update |
| 129029 | GNSS Position Data |
| 129283 | Cross Track Error |
| 129284 | Navigation Data |
