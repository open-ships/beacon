**Beacon codebase review — 10 September 2026**

Reviewed commit `7fcac7586bd819567a47652bd064e799c7d808df`, including code, API contracts, configuration flows, security boundaries, tests, and the rendered UI. The findings below record that pre-fix baseline. Runtime probes used a separate in-memory instance on loopback, synthetic configuration, and test credentials.

**Remediation in this change**

| Finding | Implemented correction | Regression evidence |
| --- | --- | --- |
| 1 | Duration schemas use the actual string wire format | REST sink writes and configuration export/import with nonzero durations |
| 2 | Admin defaults to loopback; explicit network override and SSH access documented | CLI default/override test |
| 3 | REST and UI use Go's cross-origin protection | Assembled-app tests reject foreign-origin baseline writes without changing inventory, and retain same-origin/CLI access |
| 4 | SSE and WebSocket sources reject redirects | Two-server tests cover 301, 302, 307, and 308 without forwarding credentials |
| 5 | Controls have labels and associated help | Browser accessible-name checks across all source/sink types, connector and config fields |
| 6 | Type changes use POST; DDL requests include only their three inputs; overview URL credentials and query values are redacted | Synthetic-secret browser request capture and read-only page tests |
| 7 | Modal IDs are unique and swaps target the enclosing form container | Invalid source/sink/connector submissions from lists and dashboard |
| 8 | Close actions delegate from the stable dialog | Cancel, Close, and Escape after validation swaps |
| 9 | Failed requests show accessible feedback and retain entries; stale polls recover visibly | HTTP 500, network error, timeout, and polling recovery browser tests |
| 10 | Manual dismissal and htmx removal share subtree cleanup | Five connector-dialog cycles retain the baseline listener count |
| 11 | Native POST, full-page errors, redirects, and type selection work without JavaScript | Browser create/edit/validation flow for all entity kinds and native HTTP tests |
| 12 | `just secure` uses the module-compatible toolchain | The documented command completes successfully |
| 13 | Configuration schemas describe types, choices, conditional fields, bounds, defaults, and byte budgets | OpenAPI assertions and existing configuration API validation tests |

Desktop and mobile Chromium coverage is included. Form spacing stays compact
after replacing legends with labels; the four token advisories recorded below
now use existing palette and label-size values. The implementation preserves
the current layout and shared configuration service. Hardware, real database
integration, and other browser engines remain outside this change's validation.

Direct edit pages also redirect to the saved overview with JavaScript enabled;
the obsolete form-clearing/table-swap path and its unused templates are removed.

Post-fix verification: all Go race tests pass (79.5% aggregate statement
coverage), all 19 Chromium desktop/mobile tests pass, `go vet` and
`golangci-lint` pass, and the documented `just secure` command completes with
zero reachable known vulnerabilities or static security findings. Linux/arm64
`govulncheck` also reports no reachable known vulnerabilities. `npm audit`
reports zero dependency vulnerabilities. The bundled UI detector reports no
findings across templates, application JavaScript, and stylesheet source.

| Post-fix UI dimension | Score | Evidence and limit |
| --- | ---: | --- |
| Accessibility | 3/4 | Named controls, keyboard submission/dismissal, associated help, and visible request errors verified; no full WCAG certification |
| Performance | 3/4 | Bounded listeners and polling recovery verified; existing reduced-motion and stable diagram tests retained |
| Responsive design | 3/4 | Desktop/mobile rendering and touch interaction verified; broader devices and text scaling remain unqualified |
| Theming | 3/4 | Existing palette retained and reported token drift corrected; light theme only |
| Implementation integrity | 4/4 | Coherent routing interface, unique form IDs, shared cleanup, consistent save destinations, zero detector findings |
| **Total** | **16/20** | **Good within the tested scope** |

The review's verification and scores below describe the original baseline;
they are retained as the audit record rather than claims about the fixed UI.

The architecture has a useful foundation: shared configuration validation, explicit delivery boundaries, durable queues, bounded ingestion, and an embedded UI. The interface expresses a coherent operational product. However, there are release-relevant security and API defects, and several reproducible UI failure cases. The visual direction needs less attention than form behavior and lifecycle management.

**13 findings: 5 P1, 8 P2.** P1 means address before release; P2 means a concrete defect or integration gap with a narrower trigger or workaround. No P0 finding was established.

**1. [P1] REST duration fields reject every usable representation.**

Location: [internal/model/model.go](../internal/model/model.go), lines 108–124, 155, 164, and 204; schema setup in [internal/api/api.go](../internal/api/api.go), lines 76–90.

`Duration` serializes to a string, but Huma generates an integer schema from its underlying Go type. On the running application, PUT of an HTTP POST sink with `request_timeout: "10s"` returned 422, `expected integer`. Sending `10000000000` returned 422 because the custom decoder expects a string. This also affects `write_timeout` and `buffer.max_age`. A sink created through the UI exported `"10s"`; posting that exported configuration to the REST import endpoint failed with 422. Backups containing these values therefore cannot round-trip through the API.

Register a string schema for this wire type, preserving the same representation across REST, MCP, UI, and CLI. Huma provides a schema customization mechanism for types with custom serialization ([official documentation](https://huma.rocks/features/schema-customization/)). Add HTTP round-trip tests for each duration field and an export/import test containing nonzero durations; assert that actual responses satisfy their published schemas.

**2. [P1] The default installation exposes unauthenticated administration to the network.**

Location: [cmd/beacon/main.go](../cmd/beacon/main.go), line 41; [internal/app/app.go](../internal/app/app.go), admin handler composition; [docker-compose.yml](../docker-compose.yml), host networking.

The default admin address is `0.0.0.0:2112`. No authentication boundary wraps the API, UI, or MCP handler. Any client that can reach that port can read configuration, including configured secrets, and change sources, sinks, connector routes, and forwarding policy. The compose deployment exposes the same listener through host networking. The README acknowledges this operating model, but the default does not implement its recommendation to use loopback or external access controls.

Default administration to loopback and make LAN exposure an explicit configuration choice. Provide a concrete authenticated deployment configuration for remote operators and agents. Keep the trust assumption prominent in installation instructions. Test default binding and access behavior. This finding concerns the default exposure; an installation already protected by network access controls has a different risk profile.

**3. [P1] The commissioning API bypasses cross-origin write protection.**

Location: [internal/api/system.go](../internal/api/system.go), lines 146–158; [internal/api/api.go](../internal/api/api.go), router construction; [internal/app/app.go](../internal/app/app.go), separate API/UI mounts.

POST `/api/v1/n2k/inventory/baseline` accepts a bodyless form request without checking Origin or Fetch Metadata. A direct request carrying a foreign Origin and `Sec-Fetch-Site: cross-site` returned 200 with `status: "committed"`; the equivalent `/n2k/inventory/baseline` UI request returned 403. The API can therefore accept a baseline change from a browser origin that the UI explicitly rejects. Unlike JSON configuration writes, this endpoint does not require a content type that forces a CORS preflight. Browser local-network restrictions can affect delivery of such requests; they do not supply a server-side authorization boundary.

Apply a common cross-origin policy to unsafe methods across the admin surface, including API operations with no request body. Go supplies [CrossOriginProtection](https://pkg.go.dev/net/http#CrossOriginProtection). Add integration tests against the assembled application, verifying rejection and unchanged inventory for foreign-origin form POSTs while retaining supported non-browser clients.

**4. [P1] SSE redirects forward custom credentials to another host.**

Location: [internal/source/http.go](../internal/source/http.go), lines 37–49 and the header copy in `runSSE`.

The source HTTP client follows redirects using the default policy. Configured custom authentication headers are copied into the initial request and can follow redirects to another host. A local test source redirected from `127.0.0.1` to `localhost`; the receiving server obtained the synthetic `X-API-Key` unchanged. An upstream endpoint or redirect target can consequently redirect a credential-bearing source request to a destination the operator did not authorize. The HTTP POST sink already refuses redirects, so source and sink behavior differs in a security-sensitive way.

Define a consistent redirect policy: reject redirects by default, or explicitly constrain destination origins and prevent HTTPS downgrade and credential forwarding. Add two-server redirect tests using custom header credentials. Review the WebSocket source, which uses the same HTTP client construction.

**5. [P1] Form inputs do not have individual accessible names.**

Location: [internal/ui/templates/frag_source_form.html](../internal/ui/templates/frag_source_form.html), lines 34–42; the same pattern appears in sink, connector, type-specific, and config forms.

Fieldset legends name the groups, but the input/select inside each group has neither an associated label nor an ARIA naming reference. Chromium's accessibility tree reported `group "Name"` containing an unnamed `textbox`, and `group "Type"` containing an unnamed `combobox`. This prevents reliable label-based identification by assistive technology and browser agents. The browser tests locate these fields primarily by CSS IDs, so they do not detect the omission.

Give each control an explicit label or `aria-labelledby` reference; connect help and errors with `aria-describedby` where applicable. The [W3C control-labeling guidance](https://www.w3.org/WAI/tutorials/forms/labels/) explains the individual naming requirement. This affects WCAG 4.1.2, Name, Role, Value. Verify accessible names and use role/name or label locators in browser tests. Suggested UI command: `$impeccable harden`.

**6. [P2] Type-switch requests put credentials into URL query strings.**

Location: [internal/ui/templates/frag_sink_form.html](../internal/ui/templates/frag_sink_form.html), lines 37–39; [internal/ui/templates/frag_source_form.html](../internal/ui/templates/frag_source_form.html), lines 40–42.

The type selects use `hx-get` with `hx-include="closest form"`. Changing a sink type after entering an API key produced a request ending in `&headers=X-API-Key: review-canary-only`. Source headers and credential-bearing connection URLs use the same mechanism. Request URLs can be retained in reverse-proxy/access logs and monitoring systems even though the operator has not saved the configuration.

Send only the fields needed to render the selected type, or use a protected POST body when current secret values must be preserved. Avoid secret values in fragment URLs and redact credential-bearing overview URLs consistently. Add a request-capture test with synthetic secrets. Suggested UI command: `$impeccable harden`.

**7. [P2] Validation errors render behind the modal on list pages.**

Location: [internal/ui/templates/sources.html](../internal/ui/templates/sources.html), line 10; [internal/ui/templates/frag_source_form.html](../internal/ui/templates/frag_source_form.html), lines 9 and 71. Sinks and connectors repeat the same structure.

The list page and its modal both contain `source-form-container`. The form's global `hx-target` resolves to the first matching element, outside the dialog. Reproduction: open Add source from `/sources`, enter a 257-byte name and an interface, then Save. The response contains the validation error, but there are zero alerts inside the modal and one in a duplicated form behind it. The visible editor provides no explanation for the failed save.

Use a form-relative target or unique modal/page target IDs. Exercise invalid submissions from each list and detail entry point, asserting that errors remain inside the active dialog and IDs stay unique. Suggested UI command: `$impeccable harden`.

**8. [P2] Cancel and Close stop working after a form validation refresh.**

Location: [internal/ui/assets/app.js](../internal/ui/assets/app.js), lines 1374–1395.

Close listeners are attached directly to the initial buttons and the dialog is marked initialized. Validation replaces the form and its buttons, while leaving the initialized dialog. The replacement buttons never receive listeners. On the dashboard, where the duplicate-target problem above is absent, an invalid source submission followed by either Cancel or Close left the dialog open. Escape still worked.

Delegate close-button handling from the dialog or initialize replacement controls independently of the dialog's lifecycle flag. Add invalid-submit → Cancel/Close/Escape tests. Suggested UI command: `$impeccable harden`.

**9. [P2] HTTP failures during saves give no visible feedback.**

Location: [internal/ui/forms.go](../internal/ui/forms.go), lines 575–579; [internal/ui/assets/app.js](../internal/ui/assets/app.js), lines 1652–1655.

Unexpected persistence failures return HTTP 500, which htmx does not swap into the form by default. There is no global failed-request handler that presents an error. A browser test that returned 500 for Save left the unchanged form visible with zero alerts. Operators cannot distinguish a rejected write from an action that is still running or did nothing.

Add a shared accessible request-error presentation that preserves the user's entries and explains retry behavior. Cover server errors, network failure, and timeouts. Apply an equivalent stale-state indication when live status refreshes fail. Suggested UI command: `$impeccable harden`.

**10. [P2] Closing connector dialogs leaks CEL editor listeners.**

Location: [internal/ui/assets/app.js](../internal/ui/assets/app.js), lines 237–246, 1381–1386, and 1637–1645.

CEL editors install a document-level pointer listener and expose `cleanupCEL`. That cleanup runs through htmx's element cleanup event, but dialog dismissal removes the subtree with native `replaceChildren()`/`remove()`, bypassing the event. Instrumentation counted one document pointer listener initially and six after five connector-dialog open/close cycles, with zero dialogs left. The retained callbacks hold detached editor state and execute on later pointer events.

Use a shared subtree-disposal function for both htmx removal and manual dialog dismissal, cancelling validation timers/requests as well as listeners. Test repeated open/close cycles for bounded listeners and detached state. Suggested UI command: `$impeccable optimize`.

**11. [P2] The advertised progressive form fallback does not save.**

Location: [internal/ui/templates/frag_source_form.html](../internal/ui/templates/frag_source_form.html), lines 7–9; equivalent sink and connector forms; [internal/ui/assets/README.md](../internal/ui/assets/README.md).

Entity forms have `hx-post` but no native `method` or `action`. With JavaScript disabled, Save submits a GET to the current page. A direct `/sinks/new` submission navigated to `/sinks/new?id=…&name=Review+no+JS&type=socketcan&interface=can0` and saved nothing. Typed values are placed in the URL. The existing no-JavaScript browser test checks only dashboard rendering, while asset documentation claims configuration writes do not depend on the enhancement script.

Implement native POST actions and full-page success/error responses, including a usable type selection flow, or explicitly change the product contract to require JavaScript for configuration. Test a complete disabled-JavaScript create/edit flow if the fallback remains supported. Suggested UI command: `$impeccable harden`.

**12. [P2] The documented local security command cannot run with this module.**

Location: [justfile](../justfile), lines 6 and 100–102; [go.mod](../go.mod), line 3.

`just secure` forces Go 1.25.12, while the module requires Go 1.26.6. Executing its first command returned `go.mod requires go >= 1.26.6 (running go 1.25.12; GOTOOLCHAIN=go1.25.12)`. The recipe stops before either security check completes. Running the scanners directly with the current project toolchain succeeded; the defect is the documented developer/agent entry point, not a claim that CI scanning is broken.

Use the module-compatible toolchain for this recipe and derive toolchain policy from one maintained source. Verify the actual documented command after updating it.

**13. [P2] Generated API schemas omit configuration choices and rules.**

Location: [internal/model/model.go](../internal/model/model.go), source/sink/connector definitions; [internal/api/entities_test.go](../internal/api/entities_test.go), `TestOpenAPI` around line 349.

Beyond the duration defect, the generated schemas expose fields such as source `type`, sink `type`, connector `mode`, and `format` as unrestricted strings without their allowed values. Most configuration properties lack descriptions, limits, defaults, and type-dependent requirements despite these rules existing in model validation and prose. For example, the served Source schema does not tell a client that socketcan requires `interface`, or enumerate source types. The existing schema test checks a route and operation ID; it does not check the domain contract.

Document the actual wire contract in generated schemas: descriptions, enumerations, bounds, default semantics, and conditional fields. Keep this definition aligned with validation rather than duplicating rules across transports. Add representative documented-payload tests and schema assertions. This directly improves generated clients, API exploration, and agent use.

**Verification and coverage**

| Check | Result |
| --- | --- |
| `go test -race -coverprofile=… ./...` | Passed; 78.8% aggregate statement coverage |
| `go vet ./...` | Passed |
| `golangci-lint run ./...` | Passed; zero reported issues |
| `gosec -exclude-generated -exclude-dir=.claude ./...` | Passed; zero reported issues, 24 suppressions |
| `govulncheck ./...` | No reachable known vulnerabilities on the local Darwin/arm64 target |
| Linux/arm64 `govulncheck` | No reachable known vulnerabilities |
| `npm audit --json` | Zero reported dependency vulnerabilities |
| `npm test` | All 7 Chromium browser tests passed |
| Additional HTTP and browser probes | Reproduced findings above on an isolated instance |
| Responsive checks | No document-level horizontal overflow on 7 routes at 1440px and 390px |
| Impeccable static detector | Four advisories; no blocking design findings |

The Go vulnerability scans also identified eight advisories in required modules whose affected packages/symbols were not reached: `github.com/google/cel-go` and `golang.org/x/net`. These are dependency maintenance items, not eight confirmed exploitable Beacon vulnerabilities. The npm result does not cover manually vendored htmx/Scalar bundles. A clean scan is limited to its database, target, and analysis coverage.

Package coverage included config 88.0%, supervisor 91.1%, API 77.8%, UI 78.2%, store 69.6%, sink 73.3%, and CLI 35.7%. Coverage was collected using ordinary per-package instrumentation; code called only from another package's tests or an uninstrumented subprocess can show zero locally. Do not interpret every zero-covered wrapper as untested behavior.

The most valuable missing tests are boundary and failure tests, rather than a higher percentage target:

1. Schema/JSON agreement, duration writes, and exported-config restore through the real API mount.
2. Cross-origin behavior for every unsafe admin operation, plus source redirect credential handling.
3. Modal validation, close behavior after swaps, failed saves, and repeated mount/unmount cleanup.
4. Accessible-name assertions, a mobile browser project, and actual non-JavaScript writes if supported. Existing browser coverage uses one desktop Chromium project.
5. A real PostgreSQL/TimescaleDB integration job. Current PostgreSQL tests use a fake `Exec` implementation; the production pool constructor and initialization loop show 0% local package coverage. Those fakes cannot prove the emitted SQL works against supported server versions, TLS/authentication works, or actual outages recover.
6. Smoke tests for documented CLI/developer commands, including `just secure`.

Physical CAN/USB devices, live PostgreSQL/TimescaleDB services, production network controls, Linux vessel resource/recovery gates, and other browser engines were not exercised in this review. The existing tests include simulations and local transport fixtures; those are useful evidence, not hardware or deployment certification.

**UI health and implementation integrity**

The visual implementation passes the coherence check: a restrained palette, readable tables, explicit routing topology, locally embedded assets, responsive stacking, and purposeful status treatment. Preserve that direction. The behavioral implementation has significant form and lifecycle gaps.

| Dimension | Score | Evidence |
| --- | ---: | --- |
| Accessibility | 2/4 | Named dialogs and keyboard support exist; many individual form controls remain unnamed |
| Performance | 2/4 | Bounded capture and hidden-tab polling suppression help; dialog listeners accumulate |
| Responsive design | 3/4 | Tested pages fit desktop/mobile widths; wider device and touch testing remains |
| Theming | 3/4 | Consistent incumbent palette; four minor token/documentation advisories |
| Implementation integrity | 2/4 | Coherent product surfaces, but duplicated IDs and inconsistent disposal break interactions |
| **Total** | **12/20** | **Acceptable visual foundation; significant behavioral work needed** |

The detector's four advisories were `#666` at stylesheet lines 671 and 2108, `#444` at line 2112, and `0.72rem` at line 698 being outside the documented tokens. These do not establish contrast failures or justify a redesign. No dark-mode defect is asserted for a product that currently presents a light theme.

For UI remediation, use `$impeccable harden` for form semantics and failure handling, `$impeccable optimize` for lifecycle cleanup, and `$impeccable polish` as the final step after behavioral regression tests pass. These can be addressed individually or together; re-run `$impeccable audit` after the fixes.

**Maintainability direction**

Keep `config.Service` as the validation/persistence boundary and preserve the explicit queue/delivery abstractions. Concentrate reusable code around demonstrated seams: a shared admin origin policy, a deliberate outbound credential/redirect policy, one documented configuration wire contract, and one UI subtree initialization/disposal lifecycle. Split the 1,664-line enhancement script and 1,980-line form handler file along those responsibilities when applying the fixes. File size alone is not a reason for a rewrite.

Preserve the existing strengths: contextual cancellation, CEL evaluation budgets, bounded remote envelopes, parameterized database access, transactional configuration validation, sanitized internal API errors, immutable embedded assets, reduced-motion handling, and race-tested recovery behavior. The priority is to make those protections consistent at the boundaries where humans and agents interact with the system.
