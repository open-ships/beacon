# Vendored assets

Everything in this directory is served by `internal/ui.Handler` under
`/ui/assets/` via `go:embed`, so beacon's web UI (`/ui/`) is fully
self-contained and works with no network access beyond the browser talking
to beacon itself.

## htmx.min.js

- Package: `htmx.org`
- Version: **2.0.10** (confirmed via the `x-jsd-version` response header;
  the requested `@2` tag resolves to the latest 2.x release)
- License: MIT (auto-detected by jsDelivr as `htmx.org`'s license: MIT)
- Upstream URL: `https://cdn.jsdelivr.net/npm/htmx.org@2/dist/htmx.min.js`
- Downloaded with:
  ```
  curl -L -o internal/ui/assets/htmx.min.js \
    https://cdn.jsdelivr.net/npm/htmx.org@2/dist/htmx.min.js
  ```

Used for progressive-enhancement navigation (`hx-boost`) and form fragments.
Plain links and server-rendered pages still work without it; htmx only avoids
full page reloads and handles the fragment swaps.

## app.js

Hand-authored progressive enhancement for the connector form's CEL filter
textarea. It supplies accessible, keyboard-driven typeahead for envelope
fields, CEL helpers, PGN numbers, and decoded payload keys. The schema-backed
items are fetched from `/ui/cel-completions`; a small built-in fallback keeps
the core interaction useful if that request fails. It also debounces CEL
compilation while the operator types and overlays compiler error ranges as red
wavy underlines. No configuration write depends on this script, and filters
are still validated server-side on Save.

## app.css

`app.css` is hand-authored source, not a compiled Tailwind/daisyUI artifact.
It implements the lightweight Open Ships product theme used by Beacon's
server-rendered templates: white surface, gray copy, black navigation, blue
accent, 8px radii, forms, tables, badges, docs typography, and the home DAG.

The stylesheet intentionally avoids OpenBridge, web components, remote fonts,
and inline CSS so embedded products carry the same basic visual language as
`openships.ai` without adding a heavy first-load payload.

## favicon.svg

Copied from `../site/public/favicon.svg` so Beacon uses the same browser icon
as the Open Ships site while still serving all UI assets from the embedded
binary.
