# Filter messages

A connector uses CEL, the Common Expression Language, to select messages. The
connector `filters` field is a list of CEL expressions.

Beacon evaluates each expression for each message from the connector source.
All expressions must return `true`. An empty list accepts all messages.

## Use the `msg` variable

Each expression uses one variable named `msg`.

| Variable | CEL type | Content |
|---|---|---|
| `msg.pgn` | int | NMEA 2000 PGN number |
| `msg.source` | int | Source device address from 0 through 253 |
| `msg.dest` | int | Destination address; 255 is broadcast |
| `msg.priority` | int | Priority from 0, highest, through 7, lowest |
| `msg.timestamp` | string | NMEA timestamp in RFC 3339 format |
| `msg.observed_at` | string | Time when this Beacon ingress observed the message |
| `msg.ingress` | string | Current ingress |
| `msg.origin_ingress` | string | First known ingress |
| `msg.device_name` | uint | Stable ISO Device NAME, or zero when unknown |
| `msg.device_name_hex` | string | Device NAME as 16 uppercase hexadecimal digits, or empty when unknown |
| `msg.pgn_name` | string | Cataloged PGN name |
| `msg.variant` | string | Decoded PGN variant |
| `msg.manufacturer_code` | int | Proprietary manufacturer code, or zero |
| `msg.decode_status` | string | `decoded` or `unknown` |
| `msg.physical` | map | Scaled fields with `value`, `unit`, and `physical_quantity` |
| `msg.payload` | map | Complete native N2K struct JSON, including `info` |

The external Envelope has a different shape. It puts native N2K data in
`payload`, Beacon data in `metadata`, and assembled CAN bytes in `raw`. The
flat `msg` object exists only to make filters easier to write.

## Use payload fields correctly

The keys in `msg.payload` depend on the PGN. Use the JSON field names from the
applicable PGN. Do not assume that one payload field exists for all messages.

Payload numbers are raw wire values. Beacon does not apply the field
resolution to these values.

For example, PGN 128259 field `speedWaterReferenced` has a resolution of
0.01 m/s. A speed of 2 m/s has the raw value 200. This filter selects a raw
value greater than 200:

```cel
msg.payload.speedWaterReferenced > 200
```

You can instead use the scaled value in `msg.physical`:

```cel
has(msg.physical.speedWaterReferenced) &&
msg.physical.speedWaterReferenced.value > 2.0
```

## Combine filter expressions

Beacon applies AND logic between items in the `filters` list.

This connector accepts PGN 127250 only when the source address is not 42:

```json
"filters": [
  "msg.pgn == 127250",
  "msg.source != 42"
]
```

Use `||` in one expression for OR logic.

This connector accepts either PGN:

```json
"filters": [
  "msg.pgn == 127250 || msg.pgn == 128259"
]
```

## Guard an optional field

If a key is absent, `msg.payload.someField` causes an evaluation error. The
result is not `false`. Beacon drops the message and increments the connector
filter-error count.

Use `has()` before you read an optional field:

```cel
!has(msg.payload.speedWaterReferenced) ||
msg.payload.speedWaterReferenced > 200
```

JSON numbers enter CEL as double values. You do not need an explicit
`double(...)` conversion.

## Validate filters

The connector editor compiles each expression while you type. It identifies a
compile error and underlines the related token.

Scripts can validate a list without changing the configuration:

```http
POST /api/v1/filters/validate
Content-Type: application/json

{"filters":["msg.pgn == 127250","msg.priority <= 3"]}
```

A connector `PUT` performs the same validation. Beacon returns `422` for an
invalid expression and does not start the connector with that expression.

See the Examples page for PGN allow lists, source restrictions, priority
gates, and physical-value filters.
