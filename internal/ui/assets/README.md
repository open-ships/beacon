# Vendored assets

Everything in this directory is served by `internal/ui.Handler` under
`/ui/assets/` via `go:embed`, so beacon's web UI (`/ui/`) is fully
self-contained and works with no network access beyond the browser talking
to beacon itself (beacon is an offline gateway appliance). Files here are
either vendored dev-time downloads (this section) or the compiled Tailwind/
daisyUI stylesheet (see `../uisrc/README.md`) — `app.css` is the one file
in this directory generated from source under version control rather than
downloaded verbatim.

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

Used for progressive-enhancement navigation (`hx-boost`) in
`templates/layout.html`'s sidebar nav — plain `<a href>` links still work
without it, htmx just avoids a full page reload.

## openbridge.bundle.js / palettes.css / NotoSans.ttf

- Package: `@oicl/openbridge-webcomponents`
- Version: **1.0.1** (confirmed via the `x-jsd-version` response header)
- License: Apache-2.0 (per the package's own `package.json`)
- Upstream URLs:
  ```
  https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@1.0.1/bundle/openbridge-webcomponents.bundle.js
  https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@1.0.1/src/palettes/variables.css
  https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@1.0.1/bundle/NotoSans.ttf
  ```
- Downloaded with:
  ```
  curl -L -o internal/ui/assets/openbridge.bundle.js \
    "https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@1.0.1/bundle/openbridge-webcomponents.bundle.js"
  curl -L -o internal/ui/assets/palettes.css \
    "https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@1.0.1/src/palettes/variables.css"
  curl -L -o internal/ui/assets/NotoSans.ttf \
    "https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@1.0.1/bundle/NotoSans.ttf"
  ```

`openbridge.bundle.js` is OpenBridge's standalone custom-elements bundle:
loading it via `<script type="module">` registers every `obc-*` /
`obi-*` custom element (top bar, brilliance menu, icons, ...) globally, no
bundler required. `templates/layout.html` uses `<obc-top-bar>` and
`<obc-brilliance-menu>` directly.

`palettes.css` defines OpenBridge's design-token custom properties, scoped
per `:root[data-obc-theme="bright|day|dusk|night"]` block. `layout.html`
sets `data-obc-theme` on `<html>` (default `"day"`, switched at runtime by
the brilliance menu — see `templates/layout.html`'s inline script) so these
tokens flip with it. `internal/ui/uisrc/input.css` maps daisyUI's own theme
tokens onto a subset of these — see that file's header comment and
`../uisrc/README.md` for exactly which OpenBridge variables were chosen and
why.

`NotoSans.ttf` is the typeface OpenBridge's design tokens name via
`--font-family-main: "Noto Sans"` (see `palettes.css`); vendoring it means
that font actually renders instead of silently falling back to a system
sans-serif on a machine that doesn't happen to have Noto Sans installed.
`input.css` declares the `@font-face` that points at it
(`/ui/assets/NotoSans.ttf?v=1.0.1` — the `?v=` hand-pins this package's
version as the cache-buster, since it's baked into the compiled `app.css`
and can't carry beacon's runtime version like `layout.html`'s asset URLs
do; see `../uisrc/README.md`).

To upgrade: re-run the curl commands above with a pinned
`@<new-version>` instead of `@1.0.1`, update the version noted here, bump
the `?v=` in `../uisrc/input.css`'s `@font-face` and rerun `just ui-css`,
then re-run `internal/ui/ui_test.go` plus the visual smoke check in
`../uisrc/README.md`.

## app.css

Compiled by the Tailwind v4 standalone CLI + daisyUI v5 from
`../uisrc/input.css`. Not a raw vendored download — see `../uisrc/README.md`
for how to regenerate it (`just ui-css`) and for the OpenBridge-palette
variable mapping that makes daisyUI follow the brilliance/theme switch.
