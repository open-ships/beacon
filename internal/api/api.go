// Package api implements beacon's REST configuration API: a huma-on-chi
// HTTP interface for CRUD over sources, sinks, and connectors. internal/app
// mounts the handler this package returns under the admin server's /api/
// prefix. Every write goes through internal/config.Service, so the same
// structural + CEL validation and hot-apply reconcile that already governs
// the CLI (Phase 4) and internal/config's own tests governs the HTTP
// surface too — this package is a thin, typed HTTP skin over Service, not
// a second place business rules live.
package api

import (
	"log/slog"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/identity"
	"github.com/open-ships/beacon/internal/inventory"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
)

type RuntimeInfo struct {
	Identity  identity.Appliance
	Devices   func() []bus.DeviceInfo
	Buses     func() []bus.EndpointStatus
	Inventory *inventory.Registry
	Statuses  func() []supervisor.Status
}

// New builds beacon's config REST API: a chi router with huma registered
// on it. Every operation is registered with its full "/api/v1/..." path
// (rather than relying on router-mount prefix stripping), so the returned
// handler can be mounted directly on a stdlib http.ServeMux via
// mux.Handle("/api/", handler) and still see the paths it registered.
//
// reg backs the live-metrics endpoints (get-connector-metrics,
// list-metrics); it is unused by the entity CRUD endpoints.
//
// version is embedded as the OpenAPI document's info.version and returned
// verbatim by GET /api/v1/system.
//
// log receives the underlying error whenever a handler is about to answer
// 500 (the client only ever sees a sanitized "internal error" body); nil
// defaults to slog.Default(), the same convention as config.NewService.
func New(svc *config.Service, reg *stats.Registry, version string, log *slog.Logger, runtimeInfo ...RuntimeInfo) (http.Handler, huma.API) {
	if log == nil {
		log = slog.Default()
	}

	router := chi.NewRouter()

	humaConfig := huma.DefaultConfig("beacon config API", version)
	// Served at /api/openapi.json (and .yaml); huma appends the extension.
	humaConfig.OpenAPIPath = "/api/openapi"
	// Disable huma's built-in docs UI: it pulls its renderer from a CDN,
	// which beacon (an offline gateway appliance) cannot rely on. A later
	// task serves a self-contained docs page instead.
	humaConfig.DocsPath = ""
	// Like OpenAPIPath, the schema route must carry the /api prefix itself:
	// app mounts this handler un-stripped, so huma's default "/schemas"
	// would 404 in the real deployment — and the SchemaLinkTransformer
	// stamps this path into every response's $schema field and Link header,
	// so those URLs must actually resolve. (DefaultConfig's create hook
	// reads SchemasPath at NewAPI time, so setting it here re-points both
	// the route and the stamped links.)
	humaConfig.SchemasPath = "/api/schemas"

	humaAPI := humachi.New(router, humaConfig)
	var runtime RuntimeInfo
	if len(runtimeInfo) > 0 {
		runtime = runtimeInfo[0]
	}

	registerSourceRoutes(humaAPI, svc, log)
	registerSinkRoutes(humaAPI, svc, log)
	registerConnectorRoutes(humaAPI, svc, log)
	registerFilterRoutes(humaAPI, svc, log)
	registerCatalogRoutes(humaAPI)
	registerSystemInfoRoutes(humaAPI, version, runtime)
	registerCommissioningRoutes(humaAPI, runtime, reg)
	registerMetricsRoutes(humaAPI, svc, reg, log)
	registerConfigIORoutes(humaAPI, svc, log)
	registerHealthRoutes(humaAPI, svc)
	registerDocsRoutes(router)

	return router, humaAPI
}
