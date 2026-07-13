// Package ui is beacon's offline, server-rendered web UI: an OpenBridge-
// themed dashboard mounted at /ui/. Every asset it serves (htmx, the
// OpenBridge web components bundle, the OpenBridge palette CSS, Noto Sans,
// and the compiled Tailwind/daisyUI stylesheet) is embedded in the binary
// via go:embed — beacon is an offline gateway appliance, so the UI must
// render with no network access beyond the browser talking to beacon
// itself. See assets/README.md for exactly what's vendored and
// uisrc/README.md for how internal/ui/assets/app.css is built from
// uisrc/input.css.
package ui

import (
	"embed"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
)

// assetsFS embeds every vendored/compiled static file this package serves
// under /ui/assets/ — see the package doc comment and assets/README.md.
//
//go:embed assets/*
var assetsFS embed.FS

// Handler returns beacon's web UI. svc, reg, and statuses are threaded
// through for the pages that use them: the Sources and Sinks pages below
// read and write through svc and read live component state through
// statuses; reg and the rest of statuses are for Task 5's live dashboard
// and Task 4's connectors page, which don't exist yet — the dashboard
// below is still a shell.
//
// version is beacon's own build version (see internal/app.Options.Version).
// It doubles as every vendored asset URL's "?v=" cache-busting query
// parameter: assets are served with a one-year immutable Cache-Control (see
// withImmutableCache), so without a cache-buster a binary upgrade that
// re-vendors an asset would keep serving browsers their stale cached copy.
// Tying the cache-buster to the binary version rather than a hand-maintained
// per-asset constant (contrast internal/api/docsui.go's scalarVersion) means
// every asset invalidates together on every release, with nothing to
// remember to bump when re-vendoring.
//
// A nil log defaults to slog.Default(), the same convention as api.New and
// config.NewService; render failures are logged through it (see render.go).
//
// The returned handler is a plain *http.ServeMux serving "GET /ui/<page>"
// routes plus "GET /ui/assets/". It is mounted at internal/app/app.go as
// mux.Handle("/ui/", handler); routes below are registered with the full
// "/ui/..." path since the mux they're added to isn't stripped.
func Handler(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /ui/dashboard", func(w http.ResponseWriter, r *http.Request) {
		render(w, log, "dashboard", "Dashboard", version)
	})

	// Sources: full page, its "add/edit" form + type-fields fragments, and
	// its create/update/delete write endpoints. See forms.go for every
	// handler constructor below and the behavior contract in its package
	// doc comment.
	mux.HandleFunc("GET /ui/sources", handleSourcesPage(svc, statuses, version, log))
	mux.HandleFunc("GET /ui/frag/source-form", handleSourceFormFrag(svc, log))
	mux.HandleFunc("GET /ui/frag/source-type-fields", handleSourceTypeFieldsFrag(log))
	mux.HandleFunc("POST /ui/sources", handleSourceCreate(svc, log))
	mux.HandleFunc("POST /ui/sources/{id}", handleSourceUpdate(svc, log))
	mux.HandleFunc("POST /ui/sources/{id}/delete", handleSourceDelete(svc, log))

	// Sinks: exactly parallel to sources above.
	mux.HandleFunc("GET /ui/sinks", handleSinksPage(svc, statuses, version, log))
	mux.HandleFunc("GET /ui/frag/sink-form", handleSinkFormFrag(svc, log))
	mux.HandleFunc("GET /ui/frag/sink-type-fields", handleSinkTypeFieldsFrag(log))
	mux.HandleFunc("POST /ui/sinks", handleSinkCreate(svc, log))
	mux.HandleFunc("POST /ui/sinks/{id}", handleSinkUpdate(svc, log))
	mux.HandleFunc("POST /ui/sinks/{id}/delete", handleSinkDelete(svc, log))

	assets, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		// Unreachable: "assets" is a literal path component of the go:embed
		// directive below, so it always exists in assetsFS.
		panic(err)
	}
	fileServer := http.StripPrefix("/ui/assets/", http.FileServer(http.FS(assets)))
	mux.Handle("GET /ui/assets/", withImmutableCache(fileServer))

	return mux
}

// withImmutableCache marks every response next serves as safe for clients
// and intermediaries to cache for a year without revalidating. Safe because
// content under /ui/assets/ is embedded in the binary and versioned by the
// binary itself (see Handler's "?v=" doc comment above) — the same pattern
// internal/api/docsui.go uses for /api/assets/.
func withImmutableCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		next.ServeHTTP(w, r)
	})
}
