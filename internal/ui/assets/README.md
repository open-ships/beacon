# Vendored assets

Everything in this directory is served by `internal/ui.Handler` under
`/assets/` via `go:embed`, so beacon's web UI (`/`) is fully
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
items are fetched from `/cel-completions`; a small built-in fallback keeps
the core interaction useful if that request fails. It also debounces CEL
compilation while the operator types and overlays compiler error ranges as red
wavy underlines. No configuration write depends on this script, and filters
are still validated server-side on Save. The same file progressively enhances
source and sink overview pages with their stopped-by-default stream inspector,
live CEL filtering and field suggestions, bounded browser-local capture,
JSON/CAN view switching, clipboard copy, and export.

## app.css

`app.css` is the compiled, minified Tailwind CSS v4 and Basecoat v1 stylesheet
embedded in release binaries. Its source is `internal/ui/styles/app.css`,
which imports Basecoat's Vega component bundle and applies Beacon's Open Ships
tokens, operational topology, dense data surfaces, docs typography, and
responsive behavior.

Rebuild it after changing templates or UI styles:

```
npm run build:css
```

Use `npm run dev:css` (or `just css-watch`) while iterating. The compiled file
is committed so Go and Docker builds remain self-contained and do not need
Node at runtime. The UI still avoids web components, remote fonts, and runtime
CDN dependencies.

## favicon.svg

Copied from `../site/public/favicon.svg` so Beacon uses the same browser icon
as the Open Ships site while still serving all UI assets from the embedded
binary.

## manual/

Screenshots and explanatory SVG diagrams used by the onboard manual. The PNGs
are captures of a running Beacon instance, not static UI mockups. The SVGs use
the product tokens from `DESIGN.md` and keep routing, checkpoint, replay, and
troubleshooting concepts legible at any display density. All manual visuals
are embedded and served locally with the rest of the UI.
