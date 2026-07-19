# API (for agents and scripts)

Everything this UI does goes through one REST API on the admin port
(`2112` by default). It's self-describing — an agent driving beacon should
start from its own documentation rather than this page:

- `/api/docs` — an interactive, offline reference (no CDN dependency)
- `/api/openapi.json` — the machine-readable OpenAPI 3.1 document backing it

What follows is a curl-first walkthrough of the shapes and gotchas that
aren't obvious from the schema alone.

## Entities: sources, sinks, connectors

Sources, sinks, and connectors are each a small REST resource under
`/api/v1/{sources,sinks,connectors}`:

```
GET    /api/v1/sources           list all
GET    /api/v1/sources/{id}      get one       (404 if unknown)
PUT    /api/v1/sources/{id}      create or update
DELETE /api/v1/sources/{id}      delete
```

(`sinks` and `connectors` are identical in shape.)

`PUT` is create-or-update: it succeeds whether or not `{id}` already
exists. The request body's `id` field must equal the path `{id}` or the
request 422s. Every field the entity's schema marks required must be
present in the body — in particular `enabled` (all three kinds) and
`buffer` (connectors, can be `{}`) are required even though they might seem
optional; omitting one 422s before your write is even evaluated,
independent of any of beacon's own validation:

```
curl -i -X PUT localhost:2112/api/v1/sources/can0 \
  -H 'Content-Type: application/json' \
  -d '{"id":"can0","name":"Engine room CAN bus","type":"socketcan","enabled":true,"interface":"can0"}'
```

A successful write answers `200` with the affected entity's live
supervisor status, so you can see the effect of a hot apply without a
second request:

```json
{"status":[{"kind":"source","id":"can0","state":"up"}]}
```

Deleting a source or sink still referenced by a connector 409s
(`ErrInUse`) rather than silently leaving the connector dangling; delete
the connector first.

## Validation errors

A structural or CEL-compile problem answers `422` with an
`application/problem+json` body (RFC 7807):

```json
{
  "title": "Unprocessable Entity",
  "status": 422,
  "detail": "connector \"c1\": filter \"msg.pgn ==\": ...",
  "errors": [...]
}
```

`detail` carries the human-readable reason; treat it as the message to
show or log. A store/IO failure (unrelated to the request body) answers a
sanitized `500` instead, with no internal detail in the body.

## Filter validation

```
POST /api/v1/filters/validate
{"filters": ["msg.pgn == 127250", "msg.priority <= 3"]}
```

CEL-compiles the list without persisting anything — check a connector's
filters before submitting the `PUT` that would otherwise 422 on a typo.

## Export / import

The whole configuration (every source, sink, and connector) can be pulled
or pushed as one JSON document:

```
GET  /api/v1/config/export
POST /api/v1/config/import?mode=replace   (default) — body becomes the whole configuration
POST /api/v1/config/import?mode=merge     — body's entities upserted by id; everything else untouched
```

The same two operations exist offline as CLI verbs, reading/writing the
SQLite file directly instead of talking HTTP:

```
beacon export --db beacon.db > backup.json
beacon import --db beacon.db backup.json
beacon import --db beacon.db --merge patch.json
```

**Offline caveat**: the CLI verbs open the database file directly and must
never be run against a database file a live beacon process currently has
open — beacon's SQLite driver holds a single connection per process, so a
second process opening the same file races the running one rather than
sharing it safely. Against a *running* beacon, use the HTTP
export/import endpoints above instead.

## Live metrics

```
GET /api/v1/connectors/{id}/metrics   one connector's live counters (404 if the connector itself is unknown)
GET /api/v1/metrics                   every connector's live counters, keyed by id
```

A known-but-idle connector reports a zero snapshot rather than 404 —
404 means the connector id itself doesn't exist. See the Prometheus
exposition at `/metrics` (admin port) for the same data in a form built
for scraping/alerting rather than point queries.

## Health and system info

```
GET /api/v1/health   {"status": "ok"|"degraded", "components": [...]}
GET /api/v1/system   version, persistent N2K identity, live devices, CAN/serial discovery and CAN diagnostics
```

`/api/v1/health` mirrors the admin server's top-level `GET /health`
exactly — use whichever is more convenient. `/api/v1/system` is a
best-effort hardware inventory (detected SocketCAN interfaces and
USB-serial device paths) — the same data this UI's add-source/add-sink
forms use to offer choices instead of a blank text field.

## NMEA 2000 catalog and commissioning

```
GET  /api/v1/n2k/pgns                         complete machine-readable PGN/variant/field catalog
GET  /api/v1/n2k/pgns/{pgn}                   one PGN and every variant
GET  /api/v1/n2k/inventory                    persistent devices and baseline state
POST /api/v1/n2k/inventory/baseline           accept all online devices as expected
PUT  /api/v1/n2k/inventory/{hex-name}/label?endpoint=socketcan:can0
GET  /api/v1/n2k/commissioning-report          identity, bus health, inventory, routes, and components
```

Inventory keys are endpoint plus stable Device NAME, never the dynamic source
address. Status is `new`, `online`, `changed`, `missing`, or `historical`.
Device and envelope responses retain numeric NAME fields for compatibility and
also expose `name_hex` / `device_name_hex`; JavaScript clients should use the
16-digit hex form to avoid IEEE-754 integer precision loss.
