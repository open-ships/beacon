package api

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// assetsFS embeds the offline API reference UI's static files: the vendored
// Scalar standalone bundle and its provenance note. See assets/README.md
// for exactly which upstream version is vendored and why.
//
//go:embed assets/*
var assetsFS embed.FS

// docsPage is GET /api/docs's entire response body: a minimal, fully
// self-contained HTML shell that loads the vendored Scalar bundle from
// /api/assets/scalar.min.js and points it at beacon's own OpenAPI document
// (/api/openapi.json, served by huma per the OpenAPIPath set in api.go).
// Every URL here is same-origin and path-absolute — beacon is an offline
// gateway appliance, so this page must never depend on a CDN or any other
// network access to render.
//
// The <script id="api-reference" data-url="..."> element and its
// data-configuration attribute follow the standalone bundle's own
// conventions (confirmed by reading the vendored bundle itself: it does
// `document.getElementById("api-reference")` and reads `data-url` /
// `data-configuration` off that element). withDefaultFonts is turned off
// in the configuration because Scalar's default theme otherwise fetches
// webfonts from fonts.scalar.com at render time — harmless if a browser
// happens to have internet access, but not "offline" if it doesn't.
const docsPage = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>beacon API reference</title>
</head>
<body>
<script
  id="api-reference"
  data-url="/api/openapi.json"
  data-configuration='{"withDefaultFonts":false}'
></script>
<script src="/api/assets/scalar.min.js"></script>
</body>
</html>
`

// registerDocsRoutes registers the offline API reference UI on router:
// GET /api/docs (the HTML shell above) and GET /api/assets/* (the embedded
// files it loads, notably scalar.min.js). Neither is a huma operation —
// they serve static content, not part of the config API's OpenAPI surface
// — so they're registered directly on the chi router api.New builds,
// alongside huma's own routes, with the same full "/api/..." paths huma's
// operations use (this handler is mounted un-stripped, so a route
// registered as just "/docs" would 404 in the real deployment).
func registerDocsRoutes(router chi.Router) {
	router.Get("/api/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(docsPage))
	})

	assets, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		// Unreachable: "assets" is a literal path component of the go:embed
		// directive above, so it always exists in assetsFS.
		panic(err)
	}
	fileServer := http.StripPrefix("/api/assets/", http.FileServer(http.FS(assets)))
	router.Get("/api/assets/*", func(w http.ResponseWriter, r *http.Request) {
		// Content is embedded in the binary and versioned by the binary
		// itself, so it's safe (and, for a multi-megabyte JS bundle,
		// valuable) to let clients and any intermediary cache it for a long
		// time. http.FileServer/ServeContent only sets Content-Type when
		// this handler hasn't already, so it still infers the right type
		// (application/javascript, text/markdown, ...) per file extension.
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		fileServer.ServeHTTP(w, r)
	})
}
