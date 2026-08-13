# Metrics and monitoring

Beacon provides live status in the UI, JSON measurements for scripts, health
endpoints for service checks, and a Prometheus exposition for monitoring.

All monitoring endpoints use the admin server. The default admin address is
`0.0.0.0:2112`.

## Choose a monitoring surface

| Surface | Endpoint | Use |
|---|---|---|
| Dashboard | `/dashboard` | Inspect routes, queues, components, CAN status, and devices |
| Component overview | `/sources/{id}/`, `/sinks/{id}/`, `/connectors/{id}/` | Inspect one component and its live stream |
| Health | `/health` or `/api/v1/health` | Check the rolled-up process and component state |
| Connector JSON | `/api/v1/connectors/{id}/metrics` | Read one connector snapshot from a script |
| All connector JSON | `/api/v1/metrics` | Read all connector snapshots, keyed by ID |
| Prometheus | `/metrics` | Scrape counters, gauges, and histograms |

A known connector with no activity returns a zero JSON snapshot. An unknown
connector returns `404`.

## Scrape Prometheus metrics

Add Beacon to the Prometheus configuration:

```yaml
scrape_configs:
  - job_name: beacon
    static_configs:
      - targets: ["beacon.local:2112"]
```

Prometheus requests `/metrics` by default. Bind the admin server to a trusted
onboard network. Beacon does not provide built-in authentication or TLS.

Verify the exposition directly:

```bash
curl http://localhost:2112/metrics
```

## Core metric families

Core metrics are active without additional settings.

| Metric | Type | Important labels | Meaning |
|---|---|---|---|
| `beacon_source_messages_total` | Counter | `source` | Messages received by a source |
| `beacon_subscriber_dropped_total` | Counter | `component` | Messages dropped by a full non-blocking subscriber channel |
| `beacon_connector_messages_total` | Counter | `connector`, `stage` | Messages at connector processing and delivery stages |
| `beacon_connector_bytes_total` | Counter | `connector` | Envelope bytes delivered by a connector |
| `beacon_connector_queue_depth` | Gauge | `connector` | Messages pending after the connector checkpoint |
| `beacon_connector_queue_bytes` | Gauge | `connector` | Logical bytes pending after the checkpoint |
| `beacon_component_state` | Gauge | `kind`, `id` | Component state: 0 error, 1 degraded, 2 up |
| `beacon_sink_clients` | Up-down counter | `sink` | Clients connected to an SSE, WebSocket, or TCP sink |

The `stage` label separates received, matched, filtered, queued, retried,
delivered, skipped, pruned, and other connector outcomes. Inspect the current
exposition before you create an alert for a stage.

## Monitor HTTP POST sinks

HTTP POST request metrics use the `beacon_sink_http_*` prefix.

| Metric family | Meaning |
|---|---|
| `beacon_sink_http_requests_total` | Request attempts, including retries |
| `beacon_sink_http_payload_envelopes_total` | Envelopes in attempted requests |
| `beacon_sink_http_payload_size_bytes` | Compressed or uncompressed on-wire request size |
| `beacon_sink_http_payload_uncompressed_size_bytes` | Request size before optional gzip compression |
| `beacon_sink_http_request_latency_seconds` | Request latency through response-body read |
| `beacon_sink_http_retry_after_seconds` | Valid receiver-supplied retry delay |

Labels include the sink ID, HTTP status, and payload encoding. A transport
failure uses `transport_error` as the status.

These metrics count attempts. Connector delivery metrics count only confirmed
2xx batches. Use both surfaces to distinguish repeated attempts from successful
delivery.

## Enable detailed source metrics

High-cardinality per-PGN, sender, field, and raw-byte Prometheus metrics are
off by default. Enable them only when you need this detail:

```json
{
  "settings": {
    "observability": {
      "prometheus_source_details": true
    }
  }
}
```

This setting enables the `beacon_source_pgn_*` metric families. They include
message frequency, expected period, last-seen time, gaps, timing, traffic,
decode outcomes, payload lengths, destination and priority counts, field
values, field state, field quality, and bounded raw-byte statistics.

Common labels include source ID, PGN, source address, Device NAME, PGN name,
variant, transport, and manufacturer code. Field metrics can add field, unit,
statistic, or quality labels.

The source overview and MCP provide richer diagnostic documents without
turning raw payloads or fingerprint identifiers into Prometheus labels.

Detailed metrics use bounded diagnostic samples. Beacon samples expensive
decoded-field and raw-byte distributions at most one time each second for each
source, PGN, and address stream. The Storage and limits page describes the
stream, field, payload, and raw-data bounds.

Disable detailed metrics when you do not use them. Core source, connector,
queue, component, client, and HTTP sink metrics remain active.

## Query common conditions

Calculate the receive rate for each source:

```promql
sum by (source) (rate(beacon_source_messages_total[5m]))
```

Show a connector queue that contains pending messages:

```promql
beacon_connector_queue_depth > 0
```

Find a component that is not up:

```promql
beacon_component_state < 2
```

Find pending messages that retention removed in the last 15 minutes:

```promql
increase(beacon_connector_messages_total{stage="pending_pruned"}[15m]) > 0
```

Find HTTP POST attempts that did not receive a 2xx response:

```promql
sum by (sink, status) (
  rate(beacon_sink_http_requests_total{status!~"2.."}[5m])
)
```

When detailed source metrics are active, find a missing PGN stream:

```promql
beacon_source_pgn_gap_active == 1
```

Tune query windows and thresholds from representative vessel traffic. Do not
use one set of rate or gap thresholds for all PGNs.

## Check health

Use either health endpoint:

```http
GET /health
GET /api/v1/health
```

They return the same result. Each component reports `up`, `degraded`, or
`error`. The overall status is `ok` only when all reported components are up.
A disabled component is absent.

Use the health endpoint for readiness and service checks. Use Prometheus
metrics for trends, rates, queues, and alert history.

## Inspect system information

Read the system endpoint:

```http
GET /api/v1/system
```

It returns the Beacon version, NMEA 2000 identity, client queue health, live
devices, discovered CAN interfaces, serial devices, and diagnostics. Hardware
discovery is best effort.
