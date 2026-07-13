# Beacon Phase 3 — Web UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An htmx web UI (server-rendered Go templates) for managing sources/sinks/connectors and watching live data rates, styled with daisyUI mapped onto the OpenBridge design system, fully offline, embedded in the binary.

**Architecture:** New `internal/ui` package: chi-free plain handlers on the admin mux at `/ui/...` (pages) and `/ui/frag/...` (htmx fragments), all calling the same `config.Service` + `stats.Registry` the API uses. OpenBridge web components (vendored bundle) provide the chrome: top bar, brilliance (palette) menu, navigation. daisyUI provides forms/tables/cards, themed via CSS custom properties that follow OpenBridge's `data-obc-theme` palette switching. Live tiles poll fragments every 2s (htmx `hx-trigger="every 2s"`). **Documented deviation from spec §6:** the spec sketches an SSE-based live stream for dashboard tiles; Phase 3 ships polling (identical UX at boat scale, far less machinery); an SSE stream can be added later without UI rework.

**Tech Stack:** Go html/template + go:embed; htmx (vendored); `@oicl/openbridge-webcomponents@1.0.1` bundle + palettes CSS + NotoSans.ttf (vendored); Tailwind CSS v4 standalone CLI + daisyUI v5 plugin (build-time only, compiled CSS committed).

## Global Constraints

- **Offline absolute:** every page must reference only same-origin URLs. No CDN, no webfont fetches. (Test-enforced, same regex approach as the API docs page.)
- **NO inline CSS** — no `style=` attributes, no `<style>` blocks in templates. Styling exclusively via classes (daisyUI/Tailwind utilities) and the compiled stylesheet. (Test-enforced: grep templates for `style=` and `<style`.)
- UI routes: `GET /` → 302 `/ui/dashboard`; pages under `/ui/{dashboard,sources,sinks,connectors}`; fragments under `/ui/frag/...`; static under `/ui/assets/...`. All on the ADMIN server. `/api`, `/health`, `/metrics` untouched.
- UI handlers go through `config.Service` and `stats.Registry` only — never the store or supervisor directly.
- Entity forms submit as regular form-encoded POSTs to UI endpoints (htmx `hx-post`), handlers translate to Service calls; validation errors render inline (daisyUI alert / field errors), never a bare 500 page.
- Vendored assets carry provenance in `internal/ui/assets/README.md` (package, version, license, upstream URL) and are committed. Cache-busted URLs (`?v=` + version const) with long immutable cache, same pattern as Scalar.
- Gates after every task: `go test ./... -timeout 300s`, `gofmt -l .` empty, `go vet ./...`, `CGO_ENABLED=0 go build ./...`, `git status` clean post-commit. Dependency/asset changes committed with the code. KNOWN: `-race` ICEs on n2k/pgn — exempt.

## File Structure (Phase 3 target)

```
internal/store/store.go             MODIFY: LoadConfig returns empty slices not nil (carry-over)
internal/config/service.go          MODIFY: ListX return []model.X{} not nil (carry-over)
internal/connector/connector.go     MODIFY: seed stats registry at Start (carry-over: idle-metrics gap)
internal/app/app.go                 MODIFY: shared health rollup helper; mount UI
internal/api/system.go              MODIFY: reuse rollup helper (dedup carry-over)
internal/ui/ui.go                   NEW: Handler(svc, reg, statuses func, version) http.Handler; routes
internal/ui/render.go               NEW: template parsing/execution helpers, page vs fragment
internal/ui/pages.go                NEW: page handlers (dashboard, sources, sinks, connectors, connector detail)
internal/ui/forms.go                NEW: form decode/validate/submit handlers + fragment handlers
internal/ui/templates/layout.html   NEW: base layout (OpenBridge top bar, brilliance menu, nav)
internal/ui/templates/dashboard.html, sources.html, sinks.html, connectors.html, connector_detail.html
internal/ui/templates/frag_*.html   NEW: fragments (tables, forms, type-fields, stats tiles, health chips, filter-validate)
internal/ui/assets/htmx.min.js, openbridge.bundle.js, palettes.css, NotoSans.ttf, app.css (compiled), README.md
internal/ui/uisrc/input.css         NEW: Tailwind/daisyUI source (theme mapping) — build-time input
internal/ui/uisrc/README.md         NEW: how to rebuild app.css (tailwind standalone + daisyui plugin)
justfile                            MODIFY: `just ui-css` rebuild target
internal/e2e/ui_test.go             NEW: UI-driven lifecycle test
```

---

### Task 1: Phase-2 carry-overs the UI depends on

**Files:**
- Modify: `internal/store/store.go` (LoadConfig empty slices), `internal/config/service.go` (ListX empty slices), `internal/connector/connector.go` (stats seed at Start), `internal/app/app.go` + `internal/api/system.go` (shared health rollup)
- Test: extend `internal/store/store_test.go`, `internal/config/service_test.go`, `internal/connector/connector_test.go`, `internal/api/system_test.go`

**Interfaces:**
- `store.LoadConfig` returns `model.Config` whose Sources/Sinks/Connectors are non-nil empty slices when empty (JSON `[]` not `null`); same for `Service.ListSources/ListSinks/ListConnectors` and therefore `Export`.
- `connector.Connector.Start` immediately calls `c.st.SetQueue(c.cfg.ID, depth, bytes)` from a synchronous initial `q.Stats` read (or zero values if the read errors), so a just-created connector appears in `stats.Registry.All()` without waiting for the first 5s prune tick.
- New exported helper (in `internal/supervisor` or a tiny shared spot — implementer's choice, document it): `RollupHealth(statuses []supervisor.Status) (status string)` returning "ok"/"degraded"; both `app.handleHealth` and the API `get-health` use it (logic today: any state != "up" → degraded).

- [ ] Steps: failing tests for each behavior (empty-config export JSON contains `"sources":[]`; API list-metrics includes a just-created idle connector; both health endpoints share rollup — assert equal outputs for a crafted status set), implement, full gates, commit `fix: empty-slice JSON bodies, immediate stats registration, shared health rollup`.

---

### Task 2: Asset pipeline + UI skeleton

**Files:**
- Create: `internal/ui/assets/*` (vendored), `internal/ui/uisrc/input.css`, `internal/ui/uisrc/README.md`, `internal/ui/ui.go`, `internal/ui/render.go`, `internal/ui/templates/layout.html`, `internal/ui/templates/dashboard.html` (shell only this task)
- Modify: `internal/app/app.go` (mount `/` redirect + `/ui/` handler), `justfile`
- Test: `internal/ui/ui_test.go`

**Vendor (dev-time downloads, commit results; record versions+licenses in assets/README.md):**
```bash
curl -L -o internal/ui/assets/htmx.min.js https://cdn.jsdelivr.net/npm/htmx.org@2/dist/htmx.min.js
curl -L -o internal/ui/assets/openbridge.bundle.js "https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@1.0.1/bundle/openbridge-webcomponents.bundle.js"
curl -L -o internal/ui/assets/palettes.css "https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@1.0.1/src/palettes/variables.css"
curl -L -o internal/ui/assets/NotoSans.ttf "https://cdn.jsdelivr.net/npm/@oicl/openbridge-webcomponents@1.0.1/bundle/NotoSans.ttf"
```
(htmx.org@2: MIT. openbridge: Apache-2.0. Record exact resolved htmx version from the bundle header comment.)

**app.css build (dev-time, commit result; document in uisrc/README.md + `just ui-css`):** download the Tailwind v4 standalone CLI for the host platform and daisyUI v5 plugin bundle per daisyUI's standalone-CLI docs (https://daisyui.com/docs/install/standalone/): `curl -sLO https://github.com/tailwindlabs/tailwindcss/releases/latest/download/tailwindcss-macos-arm64` (pick platform), `curl -sLO https://github.com/saadeghi/daisyui/releases/latest/download/daisyui.js` (and `daisyui-theme.js` if the docs require), then `./tailwindcss -i internal/ui/uisrc/input.css -o internal/ui/assets/app.css --minify`. The CLI binary and plugin js live in a git-ignored `internal/ui/uisrc/build/` dir (add to .gitignore) — only input.css and the compiled app.css are committed.

**`internal/ui/uisrc/input.css` — the OpenBridge↔daisyUI bridge (core of the theming story):**
```css
@import "tailwindcss" source("../templates");
@plugin "./build/daisyui.js";

/* Map daisyUI's theme tokens onto OpenBridge palette custom properties.
   OpenBridge palettes.css defines these vars per data-obc-theme value
   (bright/day/dusk/night); daisyUI components then follow the brilliance
   switch automatically. Consult palettes.css for canonical var names —
   the ones below are the load-bearing surfaces:
   --container-background-color, --container-section-color,
   --element-active-color, --on-container-color, --border-divider-color,
   --alert-alarm-color, --alert-warning-color, --alert-caution-color */
@plugin "./build/daisyui-theme.js" {
  name: "openbridge";
  default: true;
  --color-base-100: var(--container-background-color);
  --color-base-200: var(--container-section-color);
  --color-base-300: var(--container-border-color);
  --color-base-content: var(--on-container-color);
  --color-primary: var(--element-active-color);
  --color-primary-content: var(--on-active-color);
  --color-error: var(--alert-alarm-color);
  --color-warning: var(--alert-warning-color);
  --color-success: var(--alert-ok-color);
  /* radius/size tokens: keep daisyUI defaults */
}

@font-face {
  font-family: "Noto Sans";
  src: url("/ui/assets/NotoSans.ttf") format("truetype");
  font-display: swap;
}
```
**The exact OpenBridge variable names above are ILLUSTRATIVE** — the implementer MUST open the vendored `palettes.css`, find the real palette-level variable names (they're long design-token names like `--instrument-enhanced-secondary-color`; the local checkout `~/code/openships/openbridge-webcomponents/packages/openbridge-webcomponents/src/palettes/variables.css` and its storybook usage show which ones are backgrounds/text/accents), and map daisyUI tokens to those. Verify visually via the smoke step below. If daisyUI's standalone theme-plugin syntax differs from the sketch, follow daisyUI's current docs — the deliverable is: daisyUI components restyle when `data-obc-theme` changes.

**`layout.html` sketch (bindings the other templates rely on):**
```html
<!doctype html>
<html lang="en" data-obc-theme="day">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}} — beacon</title>
<link rel="stylesheet" href="/ui/assets/palettes.css?v={{.AssetVersion}}">
<link rel="stylesheet" href="/ui/assets/app.css?v={{.AssetVersion}}">
<script src="/ui/assets/htmx.min.js?v={{.AssetVersion}}" defer></script>
<script src="/ui/assets/openbridge.bundle.js?v={{.AssetVersion}}" type="module"></script>
</head>
<body class="min-h-screen bg-base-100 text-base-content">
<obc-top-bar apptitle="beacon" pagename="{{.Title}}" showappsbutton showdimmingbutton>
</obc-top-bar>
<!-- brilliance menu toggled from the top bar's dimming button; persists
     choice to localStorage and sets document.documentElement.dataset.obcTheme.
     Wire with a SMALL inline <script> (allowed; the no-inline rule is CSS only)
     reading the top bar's dimming-button-clicked event; consult the storybook
     stories in ~/code/openships/openbridge-webcomponents for the exact event
     name and obc-brilliance-menu attribute/event contract. -->
<div class="drawer lg:drawer-open"> ... daisyUI sidebar with nav links
     (Dashboard / Sources / Sinks / Connectors), hx-boosted ... 
  <main class="p-4">{{template "content" .}}</main>
</div>
</body>
</html>
```

**`ui.Handler(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string) http.Handler`** — plain `http.ServeMux` inside; app mounts: `mux.Handle("/ui/", uiHandler)` + `mux.HandleFunc("GET /{$}", redirect to /ui/dashboard)`. Assets served from go:embed with immutable cache (Scalar pattern).

- [ ] Steps: failing tests (GET / → 302 /ui/dashboard; GET /ui/dashboard → 200 html referencing only same-origin URLs — reuse the API docs regex test approach; GET each asset → 200 with sane content-type and >1KB except htmx; templates contain no `style=`/`<style`), vendor assets, build app.css, implement skeleton, **visual smoke:** run the binary, screenshot-free check `curl /ui/dashboard | grep obc-top-bar`, gates, commit `feat: offline UI skeleton - vendored OpenBridge/htmx/daisyUI assets, themed layout`.

---

### Task 3: Sources & Sinks pages

**Files:**
- Create: `internal/ui/templates/sources.html`, `sinks.html`, `frag_source_table.html`, `frag_sink_table.html`, `frag_source_form.html`, `frag_sink_form.html`, `frag_source_type_fields.html`, `frag_sink_type_fields.html`, `internal/ui/forms.go`
- Modify: `internal/ui/ui.go` (routes), `internal/ui/pages.go`
- Test: `internal/ui/forms_test.go`

**Behavior contract:**
- `/ui/sources`: daisyUI table (name, id, type, enabled badge, per-row state chip from statuses, edit/delete buttons) + "Add source" button opening the form (htmx swap into a `<dialog class="modal">` or inline panel).
- Form fields: id (immutable when editing — rendered disabled + hidden input), name, type select, enabled toggle, then type-specific fields swapped via `GET /ui/frag/source-type-fields?type=socketcan&...` (htmx `hx-get` on the select's change): socketcan→interface (datalist populated from `/api/v1/system` data passed by the handler — call the same discovery code via a small exported func or an internal HTTP call; simplest: expose `api.DiscoverSystem()` — implementer picks, documents), usbcan→port (datalist from serial ports), http_sse/http_ws→url + headers (key/value repeatable rows are overkill — a single textarea "Header: value per line" parsed by the handler is fine, document the format in the form).
- Submit: `POST /ui/sources` (create) / `POST /ui/sources/{id}` (update) form-encoded → handler builds `model.Source` → `svc.PutSource(ctx, v, isCreate)` → on success re-render the table fragment (htmx target) + a success toast (daisyUI alert, auto-dismiss); on `*config.ValidationError` re-render the FORM fragment with the error in a daisyUI alert and field values preserved; ErrExists → same inline treatment.
- Delete: `POST /ui/sources/{id}/delete` (HTML forms can't DELETE; keep it simple) with `hx-confirm`; ErrInUse renders the table fragment with an error alert naming the referencing connectors (service returns ErrInUse — handler lists referencing connectors via svc.ListConnectors to compose the message).
- Sinks: exactly parallel (type-specific: socketcan/usbcan same; http_sse/http_ws→path; tcp→address).
- Handlers never panic on malformed form input — parse errors render as validation alerts.

- [ ] Steps: failing handler tests (table renders configured entities; type-fields fragment per type; create round-trip via form POST persists through Service and re-renders table; validation error → 200 with alert markup + preserved values, NOT a 500; delete in-use → alert), implement, gates, commit `feat: sources and sinks CRUD pages`.

---

### Task 4: Connectors pages

**Files:**
- Create: `internal/ui/templates/connectors.html`, `connector_detail.html`, `frag_connector_table.html`, `frag_connector_form.html`, `frag_filter_validate.html`, `frag_connector_stats.html`
- Modify: `internal/ui/ui.go`, `internal/ui/pages.go`, `internal/ui/forms.go`
- Test: extend `internal/ui/forms_test.go`

**Behavior contract:**
- `/ui/connectors`: table (name, source→sink, filter count, enabled, queue depth from reg.Snapshot, msg/s) + add/edit form.
- Form: id/name/enabled; source & sink `<select>`s from svc lists; filters as a textarea (one CEL expression per line — split on newlines, trim, drop empties); buffer limits (max_messages number, max_age as Go duration string with placeholder "e.g. 24h", max_bytes number).
- CEL validate-on-blur: textarea `hx-post="/ui/frag/validate-filters" hx-trigger="blur changed" hx-target="#filter-feedback"` → handler runs `svc.ValidateFilters` → fragment renders ✓ or the CEL error text in an alert. This is advisory UX only — submit revalidates authoritatively.
- `/ui/connectors/{id}`: detail page — config summary, live stats tile block (`frag_connector_stats.html` with `hx-get="/ui/frag/connectors/{id}/stats" hx-trigger="every 2s"`): total msgs, msg/s, bytes/s (humanized), queue depth/bytes. 404 page for unknown id.

- [ ] Steps: failing tests (form create persists incl. filters+buffer parsing; duration parse error → inline alert; validate-filters fragment happy/error; stats fragment renders snapshot numbers; detail 404), implement, gates, commit `feat: connectors CRUD and detail pages with live stats`.

---

### Task 5: Dashboard + UI e2e

**Files:**
- Create: `internal/ui/templates/frag_dashboard.html`, `internal/e2e/ui_test.go`
- Modify: `internal/ui/templates/dashboard.html`, `internal/ui/pages.go`, `internal/ui/ui.go`
- Test: `internal/e2e/ui_test.go`

**Behavior contract:**
- Dashboard content = one fragment refreshed `every 2s`: grid of connector cards (daisyUI card per connector: name, source→sink line, msg/s + bytes/s rate tiles, queue depth, enabled/error badge from statuses) + a components health strip (chip per source/sink: name + state color: up=success, degraded=warning, error=error; **tolerate transient absence** — a component missing from statuses during hot apply renders as neutral "restarting" chip, never crashes the template) + empty-state hero when no connectors configured ("Add your first source" CTA linking /ui/sources).
- e2e (`internal/e2e/ui_test.go`, reuse api_test.go plumbing): start app with empty store + fake bus; drive the UI's OWN form endpoints (form-encoded POSTs to /ui/sources, /ui/sinks, /ui/connectors) to build source→connector→SSE sink; inject frames via busfake; assert: dashboard fragment HTML contains the connector card with nonzero total after delivery; SSE data flows on the data server (proves UI writes hot-apply); delete via UI endpoint; dashboard shows empty state again.

- [ ] Steps: failing tests, implement, gates, **manual smoke:** `just run` + open browser (report what you see textually via curl checks), commit `feat: live dashboard; UI end-to-end lifecycle test`.

---

## Plan Self-Review Notes

- **Spec §6 coverage:** stack (htmx+daisyUI+OpenBridge, embedded, no inline CSS) → T2; theming/brilliance → T2; Dashboard → T5; Sources/Sinks pages with type-swapping + system-populated dropdowns → T3; Connectors + CEL validate-on-blur + detail metrics → T4; thin-presentation-over-Service → all. Docs page is Phase 4 (per spec §6 it's part of the UI nav — Phase 4 adds the nav item with the /docs section).
- **Documented deviations:** polling instead of SSE for live tiles (rationale in header); form POSTs instead of REST verbs for UI endpoints (HTML form reality; the JSON API remains the agent surface).
- **Carry-overs folded:** null→[] (T1), idle-stats gap (T1), health rollup dedup (T1), transient-absence tolerance (T5 dashboard contract).
- **Risk noted:** exact OpenBridge custom-property names and component attribute/event contracts must be read from the vendored palettes.css + local storybook checkout at implementation time — the plan deliberately marks its sketches as illustrative and the acceptance bar as behavioral (daisyUI follows data-obc-theme; brilliance menu switches it).
