# Use MCP

Beacon provides Model Context Protocol tools for agents. The MCP service uses
Streamable HTTP on the admin server. Its default URL is
`http://127.0.0.1:2112/mcp`.

The onboard `/mcp/info` page provides client installation commands, the current
tool catalog, and call examples.

## Connect an MCP client

Configure a client with the local Beacon URL:

```json
{
  "mcpServers": {
    "beacon": {
      "url": "http://127.0.0.1:2112/mcp"
    }
  }
}
```

Beacon supports normal Streamable HTTP initialization. Its synchronous tools
use stateless requests. Beacon does not keep an MCP client session between
calls.

## Use the tools

The MCP service provides these tools:

| Tool | Function |
|---|---|
| `get_config` | Reads the complete configuration |
| `put_source` | Creates or updates a source |
| `put_sink` | Creates or updates a sink |
| `put_connector` | Creates or updates a connector |
| `delete_source` | Deletes an unused source |
| `delete_sink` | Deletes an unused sink |
| `delete_connector` | Deletes a connector |
| `get_health` | Reads component and process health |
| `get_delivery_metrics` | Reads connector delivery and queue measurements |
| `get_source_metrics` | Reads source, PGN, sender, timing, and field diagnostics |
| `get_latest_payloads` | Reads the latest decoded payloads for matching streams |

Call MCP `tools/list` to get the current input and output JSON Schemas. Use
these schemas as the source of truth for tool arguments and results.

Configuration writes use the same validation, SQLite persistence, and hot
reconciliation as the UI and REST API.

`get_config` and `put_connector` return the authored `buffer` and the
`effective_buffer`. The effective value shows the independent defaults of
10,000 messages and 64 MiB.

## Read source diagnostics

`get_source_metrics` can filter by source ID, PGN, NMEA 2000 source address,
and stable Device NAME. It returns timing, jitter, rates, estimated bus load,
addressing, decode quality, payload sizes, raw-byte distributions, last-seen
age, gaps, bursts, field statistics, availability, and lifecycle events.

Core traffic counters include all messages. Expensive field and raw-byte
distributions use diagnostic samples. Beacon takes at most one such sample
each second for one source, PGN, and address stream.

Rich diagnostic calls return 16 streams by default. You can set a limit
through 32. A result sets `truncated` when more streams match. Narrow a later
call by source, PGN, address, or Device NAME. Lifecycle events default to 10
for each source and have a maximum of 20.

## Read the latest payloads

`get_latest_payloads` returns a bounded set of exact latest decoded payloads.
The `sensor_id` is the hexadecimal Device NAME when Beacon knows it. Otherwise,
it is `address:<source_address>`.

Use the returned `sensor_id` as a filter in a later call. Beacon keeps one
latest payload for each configured-source, sensor, and PGN stream. It does not
build a payload history. These process-local values reset when Beacon restarts.

The call returns 16 payloads by default and permits a limit through 32. Use
filters when a result sets `truncated`.

## Protect the MCP service

MCP tools can change the live configuration. Bind the admin server to localhost
or a trusted onboard network.

Beacon rejects cross-origin browser requests to MCP. It limits each MCP POST
body to 1 MiB. The service does not need a cloud relay, a companion process, a
remote schema, or internet access.

The admin service, data HTTP service, and plain TCP sink listeners share a
128-connection budget. See the Storage and limits page for the complete
network limits.
