# Vendored assets

## scalar.min.js

The [Scalar](https://github.com/scalar/scalar) API reference standalone
browser bundle, vendored so `GET /api/docs` is fully self-contained and
works with no network access (beacon is an offline gateway appliance and
must not fetch its docs UI from a CDN at request time).

- Package: `@scalar/api-reference`
- Version: **1.62.5** (confirmed via the `x-jsd-version` response header and
  the file's own leading comment when downloaded)
- License: MIT
- Upstream URL:
  `https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.62.5/dist/browser/standalone.min.js`
- Downloaded with:
  ```
  curl -L -o internal/api/assets/scalar.min.js \
    https://cdn.jsdelivr.net/npm/@scalar/api-reference@latest/dist/browser/standalone.min.js
  ```

This is the "standalone" build: including it via `<script src=...>` reads
an element with `id="api-reference"` and a `data-url` attribute (or a
`data-configuration` JSON blob) off the page and mounts the reference UI
into it — no bundler or build step required. `internal/api/docsui.go`'s
`/api/docs` handler follows that convention, pointing `data-url` at the
same-origin `/api/openapi.json`.

To upgrade: re-run the curl command above with a pinned `@<version>`
instead of `@latest`, update the version noted here, and re-run
`internal/api/docsui_test.go`.
