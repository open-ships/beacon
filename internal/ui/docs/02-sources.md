# Sources

A source receives messages from one endpoint. It makes the messages available
to connectors.

A source does not send data to a sink by itself. At least one enabled connector
must refer to the source.

## Source types

| Type | Receives from | Required settings |
|---|---|---|
| `socketcan` | A Linux SocketCAN interface | `interface` |
| `usbcan` | A serial USB-CAN adapter | `port` |
| `http_sse` | An upstream SSE Envelope stream | `url` |
| `http_ws` | An upstream WebSocket Envelope stream | `url` |
| `mqtt` | An MQTT topic that contains Envelopes | `url`, `topic` |
| `file` | An NMEA 2000 capture file | `file_path` |
| `tcp` | A passive NMEA 2000 TCP gateway stream | `address`, `format` |
| `udp` | A passive NMEA 2000 UDP gateway stream | `address`, `format` |

All sources also have `id`, `name`, `type`, and `enabled` settings.

## SocketCAN source

Set `interface` to a Linux CAN network-interface name, such as `can0` or
`vcan0`.

Prepare the interface before you enable the source. Beacon does not set the
bitrate or bring up the interface. NMEA 2000 uses 250 kbit/s.

The source reads all frames that the interface receives. It assembles
fast-packet messages and decodes cataloged PGNs.

## USB-CAN source

Set `port` to the serial device path, such as `/dev/ttyUSB0`. Do not set it to
a SocketCAN interface name.

Beacon opens the serial device directly. Adapter framing, baud rate, and
detection behavior depend on the supported hardware.

Use a stable device path when possible. A udev rule can prevent a device name
from changing after a restart.

## SSE and WebSocket sources

Set `url` to the complete upstream endpoint. Use `headers` for static request
headers when the upstream service requires them.

The upstream service must publish Beacon Envelopes. Beacon reconnects after a
connection failure. It limits each dial, TLS, and response-header phase to
15 seconds. An established stream can remain open.

Beacon discards an upstream Envelope at admission when the Envelope exceeds a
remote-input limit. See the Storage and limits page for those limits.

## MQTT source

Set `url` to the MQTT broker URL. Set `topic` to the subscription topic.

The topic payload must contain one Beacon Envelope. The source reconnects to
the broker after a connection failure.

## File source

Set `file_path` to an absolute capture-file path. Beacon supports NMEA 2000
captures in candump, canboat, Yacht Devices, and Actisense formats.

Beacon replays the capture one time at the recorded timing. The source then
remains enabled and idle.

Beacon detects gzip compression from the file contents. The path does not need
a `.gz` suffix. If the specified path does not exist, Beacon also tries the
same path with `.gz` appended. The specified path has priority when both files
exist.

One compressed replay can expand to a maximum of 128 MiB. Beacon permits two
expanded replays at the same time. Decompress a larger file on the data volume
and configure the uncompressed file.

## TCP and UDP gateway sources

Set `address` to `host:port`. Set `format` to one of these values:

| Format | Gateway data |
|---|---|
| `ydraw` | Yacht Devices RAW ASCII records |
| `actisense` | Actisense binary records |

These sources are passive. They do not claim an NMEA 2000 address on the
remote bus.

## Source state and diagnostics

An enabled source reports `up`, `degraded`, or `error`. A disabled source does
not run and is absent from health results.

The source overview shows total traffic, PGN and sender streams, decode state,
timing, gaps, bursts, bus-load estimates, and recent lifecycle events. It also
provides the live stream inspector.

The process supports a maximum of 32 configured sources.
