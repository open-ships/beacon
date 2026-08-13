# Messages and inspection

Beacon carries each NMEA 2000 message in a canonical Envelope. Network and
file sinks use this Envelope unless their output format is CAN-specific.

## Envelope structure

The Envelope has three top-level fields.

| Field | Type | Content |
|---|---|---|
| `payload` | object | The complete JSON representation of the decoded `open-ships/n2k` Go struct |
| `metadata` | object | Routing, origin, enrichment, and replay data that Beacon adds |
| `raw` | string or null | Base64-encoded assembled CAN bytes |

The `payload` object contains the complete `info` object from the N2K struct.
In n2k v1, this includes the timestamp, transport timing, adapter ID, network
ID, direction, priority, PGN, source ID, and target ID.

The other payload keys are the generated fields for the applicable PGN. Beacon
does not remove, rename, or move an N2K field. It keeps raw wire values in
`payload`.

A Go client can unmarshal the nested object directly:

```go
var event struct {
    Payload  pgn.VesselHeading `json:"payload"`
    Metadata json.RawMessage   `json:"metadata"`
}
err := json.Unmarshal(document, &event)
```

For an unknown PGN, `payload` contains the complete `pgn.UnknownPGN` JSON
object. It is not `null`.

## Metadata fields

The `metadata` object can contain these fields.

| Field | Condition | Content |
|---|---|---|
| `id` and `connector` | The message entered a connector buffer | Route sequence and connector ID |
| `observed_at` | Always | Time when this Beacon ingress observed the message |
| `ingress` and `origin_ingress` | The origin is known | Current and first known ingress |
| `device_name` and `device_name_hex` | Beacon knows the address claim | Stable ISO Device NAME in numeric and JavaScript-safe hexadecimal forms |
| `pgn_name`, `variant`, and `transport` | The PGN is in the catalog | Catalog identity and NMEA 2000 transport type |
| `manufacturer_code` | The PGN is proprietary | Manufacturer discriminator from the payload |
| `decode` | Always | Decode state and catalog-completeness information |
| `physical` | The payload has decoded numeric fields | Unit-scaled values that do not change the raw payload values |

The `id` and `connector` fields appear after the message enters a connector
buffer.

Top-level `raw` contains the original bytes for an unknown PGN. For a decoded
PGN, it contains a canonical re-encoding.

## Example Envelope

```json
{
  "payload": {
    "info": {
      "timestamp": "2026-07-25T12:00:00Z",
      "adapterId": "socketcan:can0",
      "priority": 2,
      "pgn": 127250,
      "sourceId": 12,
      "targetId": null
    },
    "heading": 15708
  },
  "metadata": {
    "id": 42,
    "connector": "heading",
    "observed_at": "2026-07-25T12:00:00Z",
    "ingress": "can0",
    "pgn_name": "Vessel Heading",
    "decode": {
      "status": "decoded",
      "complete": true
    }
  },
  "raw": "XC9///////8="
}
```

## Stream inspector

Each source and sink overview has a **Stream contents** panel. The panel starts
in the stopped state. It captures only messages that arrive after you select
**Start**.

A source inspector shows messages at the receive boundary. A sink inspector
shows messages after the sink accepts or successfully delivers them.

Use the controls as follows:

1. Enter an optional CEL filter.
2. Select **Start**.
3. Select **Stop** to stop the preview and keep the captured data.
4. Select **Clear** to remove the captured data from the browser.

The filter uses the same `msg` object as a connector filter. You can change a
valid filter while the stream is active. Beacon reconnects the preview and
keeps the captured rows. If the new filter is not valid, Beacon continues to
use the last valid filter.

The inspector is best effort and does not block a connector queue. The browser
keeps the latest 200 messages in the current tab. The captured count continues
for the full browser session.

[![Beacon source stream inspector actively capturing JSONL messages with filter, display, export, and copy controls](/assets/manual/stream-inspector.png)](/assets/manual/stream-inspector.png)

_The inspector observes future messages at one component boundary. It is a
browser-local diagnostic view, not another connector or delivery queue._

## Display and export formats

- **JSONL** shows one compact `payload` object on each line.
- **CAN bytes** shows one top-level `raw` value as hexadecimal on each line.
- **Export JSONL** downloads the retained payloads in arrival order.
- **Export CAN** downloads uppercase hexadecimal payloads in arrival order.
- **Copy stream** copies all retained lines in the selected format.

Select a JSON key or value to see a relevant CEL filter. You can then apply
that filter to the preview.
