// Package ui is beacon's offline, server-rendered web UI mounted at /ui/.
// Every asset it serves (htmx, the CEL autocomplete enhancement, and the
// lightweight Open Ships stylesheet) is embedded in the binary via go:embed —
// beacon is an offline gateway appliance, so the UI must render with no
// network access beyond the browser talking to beacon itself. See
// assets/README.md for the asset inventory and internal/ui/assets/app.css for
// the theme source.
package ui

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/inventory"
	"github.com/open-ships/beacon/internal/mcpserver"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
	"github.com/open-ships/beacon/internal/sysinfo"
)

type RuntimeInfo struct {
	Inventory  *inventory.Registry
	CANDetails func() []sysinfo.CANInterface
}

// assetsFS embeds every vendored/compiled static file this package serves
// under /ui/assets/ — see the package doc comment and assets/README.md.
//
//go:embed assets/*
var assetsFS embed.FS

// Handler returns beacon's web UI. svc, reg, and statuses are threaded
// through for the pages that use them: the Sources and Sinks pages read and
// write through svc and read live component state through statuses; the
// Connectors pages read and write through svc and read live per-connector
// counters/rates through reg (see forms.go's "--- Connectors ---" section);
// the dashboard (see dashboard.go) reads through svc, reg, AND statuses
// together — its DAG uses reg for rates/queue depth the same way the
// Connectors page does, its source/sink nodes use statuses the same way
// Sources/Sinks do, and a connector node's error badge uses statuses too
// (a connector has no reg-based error signal of its own).
//
// version is beacon's own build version (see internal/app.Options.Version).
// Release builds use it as every vendored asset URL's "?v=" cache-busting
// query parameter: assets are served with a one-year immutable Cache-Control
// (see withImmutableCache), so without a cache-buster a binary upgrade that
// re-vendors an asset would keep serving browsers their stale cached copy.
// Development builds usually pass "dev", so assetCacheVersion replaces that
// non-unique value with a hash of the embedded UI assets; otherwise `go run`
// iterations would keep reusing /ui/assets/app.css?v=dev while the browser
// quite correctly holds onto its immutable cached copy.
//
// A nil log defaults to slog.Default(), the same convention as api.New and
// config.NewService; render failures are logged through it (see render.go).
//
// The returned handler is an *http.ServeMux serving "GET /ui/<page>" routes
// plus "GET /ui/assets/", wrapped in sameOriginGuard (see its doc comment).
// It is mounted at internal/app/app.go as mux.Handle("/ui/", handler) AND
// mux.Handle("/ui", handler) — the second, exact-path mount is required
// too: without it, a bare "GET /ui" request never reaches this handler's
// own "GET /ui" redirect route below, because net/http.ServeMux intercepts
// it first with its own 301-to-"/ui/" redirect (a subtree pattern's
// implicit behavior for the path with the trailing slash removed) unless an
// exact-path registration for "/ui" already exists at that outer mux.
// Routes below are registered with the full "/ui/..." path since the mux
// they're added to isn't stripped.
func Handler(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, devices func() []bus.DeviceInfo, version string, log *slog.Logger, runtimeInfo ...RuntimeInfo) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	assetVersion := assetCacheVersion(version)
	var runtime RuntimeInfo
	if len(runtimeInfo) > 0 {
		runtime = runtimeInfo[0]
	}
	mux := http.NewServeMux()

	// GET /ui and GET /ui/ both land the operator on the dashboard. Both
	// patterns are registered here (rather than relying on http.ServeMux's
	// built-in "add the trailing slash" redirect for the bare "/ui") so
	// each is a single-hop 302 straight to /ui/dashboard — see the
	// package-level mounting note above: the parent mux this Handler is
	// mounted under must forward exact "/ui" requests here unmodified (in
	// addition to its "/ui/" subtree mount) for the "GET /ui" pattern below
	// to ever be reached.
	redirectToDashboard := func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/dashboard", http.StatusFound)
	}
	mux.HandleFunc("GET /ui", redirectToDashboard)
	mux.HandleFunc("GET /ui/{$}", redirectToDashboard)

	mux.HandleFunc("GET /ui/dashboard", func(w http.ResponseWriter, r *http.Request) {
		// The dashboard is where create handlers land the operator via
		// HX-Redirect, so it consumes the one-shot flash (see flash.go).
		data := newPageData("Home", assetVersion, "dashboard")
		data.Flash = takeFlash(w, r)
		renderPage(w, log, "dashboard", data)
	})
	mux.HandleFunc("GET /ui/frag/dashboard", handleDashboardFrag(svc, reg, statuses, devices, runtime, log))
	mux.HandleFunc("POST /ui/n2k/inventory/baseline", func(w http.ResponseWriter, r *http.Request) {
		if runtime.Inventory == nil {
			http.Error(w, "inventory unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := runtime.Inventory.CommitBaseline(r.Context()); err != nil {
			log.Error("commit N2K baseline", "err", err)
			http.Error(w, "baseline failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("HX-Redirect", "/ui/dashboard")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /ui/n2k/inventory/{name}/label", func(w http.ResponseWriter, r *http.Request) {
		if runtime.Inventory == nil {
			http.Error(w, "inventory unavailable", http.StatusServiceUnavailable)
			return
		}
		name, err := strconv.ParseUint(r.PathValue("name"), 16, 64)
		if err != nil {
			http.Error(w, "invalid Device NAME", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "invalid form", http.StatusBadRequest)
			return
		}
		if err := runtime.Inventory.SetLabel(r.Context(), r.PostFormValue("endpoint"), name, r.PostFormValue("label")); err != nil {
			log.Error("label N2K device", "err", err)
			http.Error(w, "label failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("HX-Redirect", "/ui/dashboard")
		w.WriteHeader(http.StatusNoContent)
	})

	// Sources: full page, its "add/edit" form + type-fields fragments, and
	// its create/update/delete write endpoints. See forms.go for every
	// handler constructor below and the behavior contract in its package
	// doc comment.
	mux.HandleFunc("GET /ui/sources", handleSourcesPage(svc, statuses, assetVersion, log))
	mux.HandleFunc("GET /ui/sources/new", handleSourceNewPage(svc, statuses, assetVersion, log))
	mux.HandleFunc("GET /ui/sources/{id}/edit", handleSourceEditPage(svc, statuses, assetVersion, log))
	mux.HandleFunc("GET /ui/sources/{id}", redirectToTrailingSlash)
	mux.HandleFunc("GET /ui/sources/{id}/{$}", handleSourceOverviewPage(svc, reg, statuses, assetVersion, log))
	mux.HandleFunc("GET /ui/frag/sources/{id}/overview", handleSourceOverviewFrag(svc, reg, statuses, log))
	mux.HandleFunc("GET /ui/frag/source-type-fields", handleSourceTypeFieldsFrag(log))
	mux.HandleFunc("POST /ui/sources", handleSourceCreate(svc, log))
	mux.HandleFunc("POST /ui/sources/{id}", handleSourceUpdate(svc, log))
	mux.HandleFunc("POST /ui/sources/{id}/delete", handleSourceDelete(svc, log))

	// Sinks: exactly parallel to sources above.
	mux.HandleFunc("GET /ui/sinks", handleSinksPage(svc, statuses, assetVersion, log))
	mux.HandleFunc("GET /ui/sinks/new", handleSinkNewPage(svc, statuses, assetVersion, log))
	mux.HandleFunc("GET /ui/sinks/{id}/edit", handleSinkEditPage(svc, statuses, assetVersion, log))
	mux.HandleFunc("GET /ui/sinks/{id}", redirectToTrailingSlash)
	mux.HandleFunc("GET /ui/sinks/{id}/{$}", handleSinkOverviewPage(svc, reg, statuses, assetVersion, log))
	mux.HandleFunc("GET /ui/frag/sinks/{id}/overview", handleSinkOverviewFrag(svc, reg, statuses, log))
	mux.HandleFunc("GET /ui/frag/sink-type-fields", handleSinkTypeFieldsFrag(log))
	mux.HandleFunc("POST /ui/sinks", handleSinkCreate(svc, log))
	mux.HandleFunc("POST /ui/sinks/{id}", handleSinkUpdate(svc, log))
	mux.HandleFunc("POST /ui/sinks/{id}/delete", handleSinkDelete(svc, log))

	// Connectors: the list/add/edit/delete pages parallel sources/sinks
	// above, plus a per-connector overview page with live stats/streaming
	// data and live CEL validation diagnostics. See forms.go's
	// "--- Connectors ---" section for the behavior contract.
	mux.HandleFunc("GET /ui/connectors", handleConnectorsPage(svc, reg, assetVersion, log))
	mux.HandleFunc("GET /ui/connectors/new", handleConnectorNewPage(svc, reg, assetVersion, log))
	mux.HandleFunc("GET /ui/connectors/{id}/edit", handleConnectorEditPage(svc, reg, assetVersion, log))
	mux.HandleFunc("GET /ui/connectors/{id}", redirectToTrailingSlash)
	mux.HandleFunc("GET /ui/connectors/{id}/{$}", handleConnectorOverviewPage(svc, reg, statuses, assetVersion, log))
	mux.HandleFunc("GET /ui/cel-completions", handleCELCompletions)
	mux.HandleFunc("POST /ui/frag/validate-filters", handleValidateFiltersFrag(svc, log))
	mux.HandleFunc("GET /ui/frag/connectors/{id}/stats", handleConnectorStatsFrag(svc, reg, log))
	mux.HandleFunc("GET /ui/frag/connectors/{id}/overview", handleConnectorOverviewFrag(svc, reg, statuses, log))
	mux.HandleFunc("POST /ui/connectors", handleConnectorCreate(svc, reg, log))
	mux.HandleFunc("POST /ui/connectors/{id}", handleConnectorUpdate(svc, reg, log))
	mux.HandleFunc("POST /ui/connectors/{id}/delete", handleConnectorDelete(svc, reg, log))

	mux.HandleFunc("GET /ui/config", handleConfigPage(svc, assetVersion, log))
	mux.HandleFunc("POST /ui/config/import", handleConfigImport(svc, assetVersion, log))
	mux.HandleFunc("GET /ui/mcp", func(w http.ResponseWriter, r *http.Request) {
		data := mcpPageData{
			pageData: newPageData("MCP", assetVersion, "mcp"),
			Tools:    mcpserver.Catalog(),
		}
		renderPage(w, log, "mcp", data)
	})

	// Docs: the onboard operator manual — see docspages.go. /ui/docs 302s to
	// the first page; /ui/docs/{slug} serves one page or 404s for an unknown
	// slug. internal/app additionally 301-redirects the bare "/docs" and
	// "/docs/{slug}" paths (mounted on the admin mux, not this one) to these
	// two routes.
	mux.HandleFunc("GET /ui/docs", handleDocsIndex())
	mux.HandleFunc("GET /ui/docs/{slug}", handleDocPage(assetVersion, log))

	assets, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		// Unreachable: "assets" is a literal path component of the go:embed
		// directive below, so it always exists in assetsFS.
		panic(err)
	}
	fileServer := http.StripPrefix("/ui/assets/", http.FileServer(http.FS(assets)))
	mux.Handle("GET /ui/assets/", withImmutableCache(fileServer))

	return sameOriginGuard(mux)
}

func assetCacheVersion(version string) string {
	if version != "" && version != "dev" {
		return version
	}
	h := sha256.New()
	err := fs.WalkDir(assetsFS, "assets", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := assetsFS.ReadFile(path)
		if err != nil {
			return err
		}
		_, _ = h.Write([]byte(path))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(b)
		_, _ = h.Write([]byte{0})
		return nil
	})
	if err != nil {
		return "dev"
	}
	return "dev-" + hex.EncodeToString(h.Sum(nil))[:12]
}

// sameOriginGuard wraps next so every POST request is checked against a
// same-origin policy before reaching next — beacon's only defense against
// cross-site form/fetch submissions forging a write through the UI's
// state-changing endpoints (POST /ui/sources, /ui/sinks, /ui/connectors,
// and their .../delete and /ui/frag/validate-filters counterparts). GET
// requests (and every other method) pass through untouched: they're not
// state-changing, so there's nothing here for a forged cross-site request
// to gain by making one.
//
// Two signals are checked, in order, either sufficient on its own to allow
// the request through:
//
//   - Origin, sent by every fetch/XHR/form POST a modern browser makes,
//     same-origin or not. If present, its host must equal the request's own
//     Host or the request is rejected — this is the primary signal, and the
//     one every browser that matters sends.
//   - Sec-Fetch-Site, checked only when Origin is absent (some same-origin
//     requests omit Origin per the Fetch spec, and a handful of older
//     browsers never send it at all): if present, it must read
//     "same-origin" or "none" (a user-typed URL, bookmark, or browser
//     extension — not a cross-site page) or the request is rejected.
//
// A request carrying NEITHER header — curl, most non-browser HTTP clients,
// and any htmx request a stripping proxy scrubbed both headers from — is
// allowed. beacon has no cookie or bearer-token auth on /ui/* for such a
// client to have forged in the first place, so a headerless request is
// exactly as trusted as one that proves same-origin.
func sameOriginGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && !sameOrigin(r) {
			http.Error(w, "cross-origin POST forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sameOrigin implements sameOriginGuard's Origin/Sec-Fetch-Site check; see
// its doc comment for the full rationale.
//
// The Origin comparison is host:port only, deliberately ignoring scheme:
// beacon serves plain HTTP and has no view of any TLS-terminating proxy in
// front of it, so requiring a scheme match would break legitimate setups
// without blocking anything a host mismatch doesn't already block.
func sameOrigin(r *http.Request) bool {
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		return err == nil && u.Host == r.Host
	}
	if sfs := r.Header.Get("Sec-Fetch-Site"); sfs != "" {
		return sfs == "same-origin" || sfs == "none"
	}
	return true
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
