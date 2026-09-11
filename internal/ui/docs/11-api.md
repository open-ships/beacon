# Use the API

The web UI and REST API share the same configuration validation and storage.
The default admin address is `127.0.0.1:2112`. Scripts and agents can use the
API locally or through the SSH tunnel described in Getting started.

Use these onboard references for the current Beacon version:

- `/api/docs` provides an interactive offline API reference.
- `/api/openapi.json` provides the OpenAPI 3.1 document.

Use the onboard OpenAPI document as the source of truth for request and
response schemas.

## Configuration contract

The schema enumerates transport types, bridge modes, type-dependent required
fields, and limits. Each property describes its meaning and default behavior.
Text limits use UTF-8 bytes; `x-max-bytes` records that budget because JSON
Schema's `maxLength` counts Unicode characters. Cross-entity constraints, CEL
compilation, URL validity, and duration ranges are also checked on each write.

Duration fields (`request_timeout`, `write_timeout`, and `buffer.max_age`) are
strings such as `"10s"`, `"1m30s"`, and `"24h"`, not numeric nanoseconds. Omitted
or `"0s"` values use the default described for that field. An exported document
uses the same representation and can be imported without conversion.

`PUT /api/v1/{sources|sinks|connectors}/{id}` upserts a complete entity; its
body ID must match the path. Read it first when retaining existing fields.
Invalid configuration returns a structured HTTP 422 error and is not persisted.
Protect exports: they contain configured headers and connection credentials.

Unsafe requests from foreign browser origins are rejected with HTTP 403,
including bodyless commissioning operations. CLI clients without browser
Origin/Fetch Metadata headers remain supported. Origin checks do not provide
authentication; protect any listener exposed beyond loopback.

[![Beacon's embedded interactive API reference showing resource operations, the configuration export endpoint, a curl example, and response schema](/assets/manual/api-reference.png)](/assets/manual/api-reference.png)

_The interactive reference is served by the running Beacon process and matches
that binary's API schema. It remains available without internet access._

## Manage REST resources

Sources, sinks, and connectors use these resource paths:

```http
GET    /api/v1/sources
GET    /api/v1/sources/{id}
PUT    /api/v1/sources/{id}
DELETE /api/v1/sources/{id}
```

Replace `sources` with `sinks` or `connectors` for the other resource types.

`PUT` creates or updates a resource. The `id` in the request body must match
the path ID. If the values differ, Beacon returns `422`.

Include all fields that the schema marks as required. In particular, include
`enabled` for each resource type. Include `buffer` for a connector, even when
its value is an empty object.

For example:

```bash
curl -i -X PUT localhost:2112/api/v1/sources/can0 \
  -H 'Content-Type: application/json' \
  -d '{"id":"can0","name":"Engine room CAN bus","type":"socketcan","enabled":true,"interface":"can0"}'
```

A successful write returns `200` and the affected live state:

```json
{"status":[{"kind":"source","id":"can0","state":"up"}]}
```

Beacon validates, stores, and reconciles the write before it sends the
response.

If a connector refers to a source or sink, Beacon does not delete that source
or sink. It returns `409` with `ErrInUse`. Delete the connector first.

## Handle validation errors

A schema error or CEL compile error returns `422` with an
`application/problem+json` body:

```json
{
  "title": "Unprocessable Entity",
  "status": 422,
  "detail": "connector \"c1\": filter \"msg.pgn ==\": ...",
  "errors": []
}
```

Show or log the human-readable `detail` value. An unrelated storage or I/O
failure returns a sanitized `500`. Beacon does not put internal details in
that response body.

## Validate a filter

Compile filters without storing them:

```http
POST /api/v1/filters/validate
Content-Type: application/json

{"filters":["msg.pgn == 127250","msg.priority <= 3"]}
```

The endpoint uses the same compiler as a connector write.

## Export a live configuration

Export the complete configuration from a running Beacon instance:

```bash
curl localhost:2112/api/v1/config/export > beacon-config.json
```

## Import a live configuration

Replace the complete running configuration:

```bash
curl -X POST 'localhost:2112/api/v1/config/import?mode=replace' \
  -H 'Content-Type: application/json' \
  --data-binary @beacon-config.json
```

Merge resources by ID and keep the other resources:

```bash
curl -X POST 'localhost:2112/api/v1/config/import?mode=merge' \
  -H 'Content-Type: application/json' \
  --data-binary @patch.json
```

The default mode is `replace`. Beacon validates an import atomically. It does
not apply part of a document that fails validation.

## Use offline import and export

When Beacon is stopped, you can access the SQLite database directly:

```bash
beacon export --db beacon.db > backup.json
beacon import --db beacon.db backup.json
beacon import --db beacon.db --merge patch.json
```

**CAUTION:** Do not run an offline import or export against a database that a
Beacon process has open. Stop Beacon first. For a running process, use the HTTP
endpoints.

## Set process resources

The optional `settings.resources` object controls storage budgets:

```json
{
  "settings": {
    "resources": {
      "max_database_bytes": 1073741824,
      "database_reserve_bytes": 134217728,
      "max_file_store_bytes": 2147483648
    }
  }
}
```

The values shown are the defaults: 1 GiB, 128 MiB, and 2 GiB. Beacon validates
the sum of effective route buffer allocations and the sum of file-sink
allocations. See the Storage and resource limits page for the calculations.

## Use the NMEA 2000 catalog and inventory

Use these endpoints:

```http
GET  /api/v1/n2k/pgns
GET  /api/v1/n2k/pgns/{pgn}
GET  /api/v1/n2k/inventory
POST /api/v1/n2k/inventory/baseline
PUT  /api/v1/n2k/inventory/{hex-name}/label?endpoint=socketcan:can0
GET  /api/v1/n2k/commissioning-report
```

An inventory key contains the endpoint and the stable Device NAME. It does not
use the dynamic source address. Inventory states are `new`, `online`,
`changed`, `missing`, and `historical`.

Responses keep numeric NAME values for compatibility. They also supply
`name_hex` or `device_name_hex`. A JavaScript client must use the 16-digit
hexadecimal value to prevent IEEE-754 precision loss.
