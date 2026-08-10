# Keep wire values canonical

The Envelope retains raw bytes and raw-tick JSON payloads as its canonical
lossless representation, then adds PGN catalog identity, decode status, ingress
provenance, stable Device NAME, and unit-scaled physical values. This keeps
replay and filters backward compatible while giving operators and integrations
ergonomic engineering values without destructive conversion.

Software-facing MQTT, SSE, and WebSocket ingress applies a separate bounded
admission policy before an untrusted Envelope reaches filters, diagnostics, or
durable storage: 256 KiB encoded Envelope, 128 KiB decoded payload JSON, 1,785
raw bytes, 256 physical fields, 256 missing-field names, and 1 KiB per metadata
string/name/unit/quantity. These bounds reject an oversized remote document;
they do not truncate or redefine an admitted canonical Envelope.

Diagnostic projections are also bounded independently of the canonical
Envelope. Exact traffic counters continue while rich state may omit a novel
stream at its per-source capacity, and retained latest-payload/raw diagnostic
copies may be omitted or truncated with explicit counters. Delivery and replay
continue to use the complete admitted Envelope, so observability limits never
become wire-data loss.
