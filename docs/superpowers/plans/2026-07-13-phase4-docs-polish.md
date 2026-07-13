# Beacon Phase 4 — Docs & Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The onboard `/docs` manual (goldmark-rendered markdown, embedded), the queue-depth sparkline the spec promised, README rewrite + importable JSON examples, Docker/CI updates, and the security/UX polish items from the Phase 3 final review — leaving the branch fully merge-ready.

**Architecture:** `/ui/docs` joins the existing UI (same layout/nav, goldmark renders embedded `.md` at request time with a small cache); sparkline = server-side depth-history ring in `stats.Registry` rendered as inline SVG in the stats fragment; everything else is targeted polish on existing packages. Spec: docs/superpowers/specs/2026-07-12-beacon-gateway-design.md §5 (docs route), §6 (manual contents, sparkline).

**Tech Stack:** `github.com/yuin/goldmark`, existing UI machinery. No new vendored assets.

## Global Constraints

- Offline + no-inline-CSS rules apply to docs pages exactly as to the rest of the UI (extend the existing test nets to cover them). Inline SVG elements/attributes (`<svg>`, `<polyline points=...>`) are NOT inline CSS — allowed.
- Docs live at `/ui/docs` (nav item "Docs") with the spec's `/docs` path 301-redirecting to `/ui/docs` (reserved prefix already). Sidebar built from the embedded file tree order.
- Manual content files: `internal/ui/docs/*.md` — plain CommonMark + tables; no HTML blocks (goldmark with raw HTML DISABLED — XSS hygiene for future doc edits).
- All gates after every task: `go test ./... -timeout 300s`, `gofmt -l .` empty, `go vet ./...`, `CGO_ENABLED=0 go build ./...`, clean `git status`. KNOWN: -race ICEs on n2k/pgn — exempt.

---

### Task 1: Security & UX polish (Phase 3 review carry-overs)

**Files:** `internal/ui/ui.go`, `internal/ui/forms.go`, `internal/ui/dashboard.go`, templates, `internal/sysinfo/sysinfo.go` (new), `internal/api/system.go`, tests alongside.

Items (each with a test):
1. **Same-origin guard on UI writes:** middleware on every `POST /ui/*`: if an `Origin` header is present and its host ≠ request Host → 403; else if `Sec-Fetch-Site` header present and not `same-origin`/`none` → 403; otherwise allow (curl/htmx both pass). Tests: cross-origin POST → 403; same-origin and no-header POSTs → pass.
2. **`GET /ui/` and `GET /ui` → 302 `/ui/dashboard`.**
3. **Deleted-connector poll notice:** stats fragment handler already 404s; change the detail page's polling container to `hx-swap="innerHTML" hx-target="this"` with `hx-on::response-error` swapping a static "connector no longer exists" notice and halting polling (set `hx-trigger` via a wrapper the notice replaces). Simplest robust approach: on 404, htmx doesn't swap — instead return 200 with a "deleted" notice fragment and an `HX-Retarget`-free body that OMITS the polling attributes (the swap replaces the poller, stopping the loop). Test: fragment for unknown id returns the notice (200) and the notice contains no hx-trigger.
4. **Names not raw IDs:** connectors table + dashboard cards show source/sink NAMES (fall back to id when name empty) — handler passes a lookup map. Tests updated.
5. **`internal/sysinfo`:** move discoverCAN/discoverSerial (+ settable roots + tests) from internal/api to a new leaf package; api and ui both import it; delete the api→ui coupling comment.
6. **Small test debt:** ParseForm-error branch test; validation-error select/filters preservation test.
7. **Version stamping:** check `.github/workflows/release.yml` — ensure `-ldflags "-X main.version=..."` is set at build so the UI/docs `?v=` cache-buster isn't permanently "dev" in releases; add if missing (keep tag format).

- [ ] Steps: tests first per item → implement → gates → ONE commit: `fix: UI write origin guard, sysinfo extraction, dashboard polish`.

---

### Task 2: Queue-depth sparkline

**Files:** `internal/stats/stats.go`, `internal/connector/connector.go` (no change expected — SetQueue already called per prune tick), `internal/ui/dashboard.go` or `forms.go`, `internal/ui/templates/frag_connector_stats.html`, tests.

- `stats.Registry` gains a per-connector depth-history ring: every `SetQueue` appends `(now, depth)` to a 60-sample ring (5s prune cadence ⇒ ~5 min window). `Snapshot` gains `DepthHistory []int64` (oldest→newest, absent when empty; JSON `depth_history,omitempty` — additive, non-breaking).
- Stats fragment renders an inline SVG sparkline (`<svg viewBox="0 0 120 32" class="...">` + `<polyline>` with points computed in the handler — template receives a precomputed points string; stroke via `class="stroke-primary fill-none"` Tailwind classes, rebuild app.css if new classes). Flat-zero history renders a flat line, not an error.
- Detail page keeps tiles + sparkline. Test: fragment HTML contains `<polyline` with >1 point after two SetQueue calls with different depths; JSON API connector metrics includes depth_history.

- [ ] Steps: tests → implement → gates → commit: `feat: queue-depth sparkline on connector detail`.

---

### Task 3: /docs manual — engine + content

**Files:** `internal/ui/docspages.go` (new), `internal/ui/templates/docs.html` (new), `internal/ui/docs/*.md` (new content), `internal/ui/ui.go` (routes + nav), `go.mod` (goldmark), tests.

**Engine:** `//go:embed docs/*.md`; goldmark (GFM tables ON, raw HTML OFF, auto-heading-ids ON); rendered once per file at first request then cached (`sync.Map` — content is embedded/immutable); docs.html template = layout + sidebar (ordered file list with titles from first `# ` heading) + rendered body inside `<article class="prose">` (typography via Tailwind's prose classes if available in daisyUI build — else minimal utility classes; rebuild app.css). Routes: `GET /ui/docs` → first page; `GET /ui/docs/{slug}`; `GET /docs` + `GET /docs/{slug}` → 301 to /ui/docs equivalents. Nav gains "Docs" item. Unknown slug → 404.

**Content files (order-prefixed):** `01-getting-started.md` (what beacon is; run binary/Docker; first source→sink→connector via UI and via curl API; where data comes out), `02-can-setup.md` (SocketCAN bring-up, bitrate 250k, USB adapters incl. slcan note, vcan for testing, Docker host-network/NET_ADMIN notes), `03-concepts.md` (sources/sinks/connectors; envelope JSON shape with field table; durable buffering + replay semantics incl. `connector:seq` ids, Last-Event-ID, ?after=; delivery guarantees per sink type; hot apply behavior), `04-filters.md` (CEL surface table msg.pgn/source/dest/priority/timestamp/payload; the cookbook examples carried from the old README — port them, updating payload field names to the new decoded-JSON shape), `05-api.md` (for agents: point at /api/docs + /api/openapi.json; CRUD walkthrough with curl; export/import incl. CLI verbs; validation errors), `06-troubleshooting.md` (no frames arriving checklist; bus write failures; queue growth/pruning; health states incl. transient restarting; -race/goldmark notes NOT needed — keep it operator-focused). Content must be accurate to THIS codebase — verify every command/path/JSON shape against the source before writing it.

- [ ] Steps: engine tests (routes, sidebar, 404, offline/no-inline nets extended, raw-HTML-in-md stays escaped) → engine → content (accuracy-checked) → gates → commit: `feat: onboard /docs manual`.

---

### Task 4: README, examples, Docker, CI

**Files:** `README.md` (rewrite), `examples/*.json` (new), `examples/README.md`, `Dockerfile`, `docker-compose.yml`, `.github/workflows/*.yml`, `.golangci.yml` (verify), `justfile`.

- README: what beacon is (diagram-level description matching the user's original sketch: sources → optional CEL filters → connectors → sinks), quick start (binary + Docker), UI screenshot placeholder omitted (no binary assets) — describe UI + API surface with the port table, config model summary, link to /docs and /api/docs for depth, development section (just targets incl. ui-css, vcan), release/versioning note. Everything accurate to the current flags (--db/--data-address/--admin-address/--seed/--log-level) and endpoints.
- `examples/`: importable JSON configs (minimal.json: can0→SSE all; navigation.json: nav PGN allowlist; engine-room.json; beacon-chain.json: http_sse source from another beacon → local CAN sink; vcan-dev.json) each with a comment-equivalent README section (JSON has no comments). Validate each via `beacon import --dry`... no dry flag exists — validate in a TEST: iterate examples/*.json, config.ValidateConfig each.
- Dockerfile: verify it builds the current binary with version ldflags, CGO_ENABLED=0, and exposes 8080/2112 + /data volume for beacon.db; compose: healthcheck against /health.
- CI: test.yml runs gates + the examples-validate test; lint.yml — run golangci-lint locally IF config is valid, fix config if broken (reviewer noted a config-version mismatch earlier); add a race job: `go test -race` for the packages that don't import n2k/pgn (list them explicitly; comment why).
- justfile: targets current (build/test/run/ui-css/docker); remove stale ones.

- [ ] Steps: examples-validation test first → content/config work → gates (+ `docker build` if docker available locally — if not, note it) → commit: `docs: README rewrite, importable examples, Docker/CI updates`.

---

## Plan Self-Review Notes

- Spec coverage: §5 /docs route → T3 (301 from /docs); §6 manual contents list → T3 content files map 1:1 (getting started, CAN setup, concepts, CEL cookbook, agents/API, troubleshooting); §6 sparkline → T2 (closing the Phase 3 gap); examples as importable configs (spec §10 phase 4 line) → T4.
- Carry-overs folded: all Phase 3 final-review must-dos → T1; version stamping → T1.7.
- Deviations to document in the spec afterward: none new (T2 closes the sparkline one).
- After T4: final whole-BRANCH review (all 4 phases, merge-base 32a9d52), then finishing-a-development-branch.
