# internal/ui/uisrc — Tailwind/daisyUI build pipeline

`../assets/app.css` (committed, embedded, served at `/ui/assets/app.css`) is
compiled from `input.css` here by the Tailwind v4 standalone CLI plus the
daisyUI v5 plugin, following daisyUI's [standalone-CLI
docs](https://daisyui.com/docs/install/standalone/). Nothing under
`uisrc/` other than `input.css` and this README is committed — `build/`
(the CLI binary and plugin JS) is git-ignored dev tooling, not source.

## input.css — the OpenBridge <-> daisyUI bridge

`input.css` is the core of beacon's theming story: it maps daisyUI's
color tokens (`--color-base-100`, `--color-primary`, ...) onto OpenBridge's
own palette custom properties (vendored at `../assets/palettes.css`, which
redefines those properties per `:root[data-obc-theme="bright|day|dusk|night"]`).
Because the mapping is a `var()` indirection rather than a static color
copy, daisyUI-styled elements (`.btn`, `.card`, `.menu`, ...) restyle
automatically whenever `templates/layout.html`'s brilliance menu changes
`data-obc-theme` on `<html>` — no daisyUI theme switch is involved, only
OpenBridge's own palette swap underneath a single daisyUI theme named
`"openbridge"`.

The real OpenBridge variable names were found by reading the vendored
`palettes.css` (same content as
`~/code/openships/openbridge-webcomponents/packages/openbridge-webcomponents/src/palettes/variables.css`
in the local OpenBridge checkout), not guessed from a naming convention —
several plausible names turned out not to exist (`--container-border-color`,
`--on-container-color`, `--on-active-color`, `--alert-ok-color`). Each
variable actually mapped was confirmed present, with a distinct sane value,
in all four `:root[data-obc-theme="..."]` blocks:

| daisyUI token           | OpenBridge variable                  | Why                                                                                   |
|--------------------------|----------------------------------------|----------------------------------------------------------------------------------------|
| `--color-base-100`       | `--container-background-color`        | page/app background                                                                    |
| `--color-base-200`       | `--container-section-color`           | recessed section background                                                            |
| `--color-base-300`       | `--border-divider-color`              | divider/border color                                                                   |
| `--color-base-content`   | `--element-active-color`              | primary text/icon color on a container (no `on-container-color` token exists)          |
| `--color-primary`        | `--raised-enabled-background-color`   | fill of `obc-button`'s `raised` variant — OpenBridge's elevated/emphasis button style, the closest analog to daisyUI "primary" (the `normal` variant is unstyled/near-white, not a brand color) |
| `--color-primary-content`| `--on-raised-active-color`            | text/icon color on a raised button                                                     |
| `--color-error`          | `--alert-alarm-color`                 | highest-severity alert color                                                           |
| `--color-error-content`  | `--on-alarm-color`                    | text/icon color on alert-alarm                                                         |
| `--color-warning`        | `--alert-warning-color`               | mid-severity alert color                                                               |
| `--color-warning-content`| `--on-warning-color`                  | text/icon color on alert-warning                                                       |
| `--color-success`        | `--alert-running-color`               | "system running normally" green (no `alert-ok-color` token exists)                     |
| `--color-success-content`| `--on-running-color`                  | text/icon color on alert-running                                                       |

`--color-secondary`/`--color-accent`/`--color-neutral`/`--color-info` and
their `-content` pairs are left unset (daisyUI's own defaults apply);
beacon's templates don't use them yet. Radius/size tokens are also left at
daisyUI's defaults — OpenBridge's own components carry their own shapes,
and daisyUI is only styling the plain HTML elements beacon's templates
render (nav, cards, buttons).

`input.css` also declares the `@font-face` for the vendored Noto Sans
(`../assets/NotoSans.ttf`) and sets it as `body`'s font, following
OpenBridge's own `--global-typography-font-family: "Noto Sans"` token.

### The font URL's hand-pinned cache-buster

Every `/ui/assets/` response is served with a one-year **immutable**
Cache-Control, so every asset URL needs a `?v=` cache-buster to survive
upgrades. `templates/layout.html`'s asset URLs get beacon's runtime build
version templated in, but the `@font-face` src is baked into the compiled
`app.css` at build time and can't carry a runtime value. `input.css`
therefore hand-pins the **vendored `@oicl/openbridge-webcomponents`
package version** (the package the font ships in — `1.0.1` as of this
writing, see `../assets/README.md`) as the font URL's `?v=`, the same
convention as `internal/api/docsui.go`'s `scalarVersion` constant.

**When re-vendoring OpenBridge (which replaces `NotoSans.ttf`): bump the
`?v=` in `input.css`'s `@font-face` and rerun `just ui-css`.**
`ui_test.go`'s `TestAppCSSFontURLHasCacheBuster` fails if the compiled
`app.css` ever loses the query parameter entirely (it cannot check the
pinned value is *current* — keeping it current is on the upgrade procedure
above).

This was chosen over the two alternatives considered: a version-segmented
asset route (e.g. `/ui/assets/fonts/{ver}/NotoSans.ttf`) adds routing
machinery and an embed-path rename per upgrade for a single file, and
exempting `*.ttf` from immutable caching gives the 610KB font weaker cache
semantics than every sibling asset forever.

## Rebuilding app.css

```
just ui-css
```

Or by hand:

```
cd internal/ui/uisrc
./build/tailwindcss-macos-arm64 -i input.css -o ../assets/app.css --minify
```

### First-time / re-fetching `build/`

`build/` doesn't exist until you fetch it (it's git-ignored). Pick the
Tailwind CLI binary for your host platform and download it plus the
daisyUI plugin bundles:

```
mkdir -p internal/ui/uisrc/build
cd internal/ui/uisrc/build
curl -sLO https://github.com/tailwindlabs/tailwindcss/releases/latest/download/tailwindcss-macos-arm64   # pick your platform
chmod +x tailwindcss-macos-arm64
curl -sLO https://github.com/saadeghi/daisyui/releases/latest/download/daisyui.js
curl -sLO https://github.com/saadeghi/daisyui/releases/latest/download/daisyui-theme.js
```

As vendored here: Tailwind CLI **v4.3.2** (macOS arm64 binary,
`tailwindcss-macos-arm64`), daisyUI **v5.6.18** (`daisyui.js` +
`daisyui-theme.js`), both released under their respective upstream
licenses (Tailwind CSS: MIT; daisyUI: MIT).

## Verifying the theme switch actually works

After rebuilding, confirm daisyUI is still following OpenBridge's palette
rather than a copied color literal:

```
grep -o '\[data-theme=openbridge\][^}]*}' internal/ui/assets/app.css
```

The `--color-base-100`/`--color-primary`/etc. values in that block must be
`var(--container-background-color)`-style references, never resolved
literal colors — a literal would mean the mapping got baked in at build
time instead of tracking the live `data-obc-theme` attribute.

Then run beacon and check in a browser: open `/ui/dashboard`, click the top
bar's dimming button to open the brilliance menu, and switch palettes — the
page background, card, and button colors should all shift to match
(bright/day are light palettes, dusk/night are progressively darker), since
they're driven by the same `--container-*`/`--element-active-color`/etc.
tokens the OpenBridge components themselves use.
