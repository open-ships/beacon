package api

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
)

// --- Filter validation ---

type validateFiltersInput struct {
	Body struct {
		Filters []string `json:"filters" doc:"CEL filter expressions to validate (see spec §3.5 for the expression language)."`
	}
}

type validateFiltersOutput struct {
	Body struct {
		Valid bool `json:"valid"`
	}
}

// registerFilterRoutes registers POST /api/v1/filters/validate: a
// side-effect-free CEL-compile check so the UI/CLI can validate a
// connector's filters before submitting a PUT.
func registerFilterRoutes(api huma.API, svc *config.Service, log *slog.Logger) {
	huma.Register(api, huma.Operation{
		OperationID: "validate-filters",
		Method:      http.MethodPost,
		Path:        "/api/v1/filters/validate",
		Summary:     "Validate CEL filter expressions",
		Errors:      []int{http.StatusUnprocessableEntity},
	}, func(ctx context.Context, in *validateFiltersInput) (*validateFiltersOutput, error) {
		if err := svc.ValidateFilters(in.Body.Filters); err != nil {
			return nil, mapServiceErr(log, err)
		}
		out := &validateFiltersOutput{}
		out.Body.Valid = true
		return out, nil
	})
}

// --- System discovery ---

// canRoot, serialRoot, and hostGOOS are package vars (rather than hard-coded
// constants) so tests can point them at a fixture directory — and, for
// canRoot, force the "linux" branch — to exercise the discovery logic
// deterministically on any host, not just Linux CI.
var (
	canRoot    = "/sys/class/net"
	serialRoot = "/dev"
	hostGOOS   = runtime.GOOS
)

type systemOutput struct {
	Body struct {
		Version       string   `json:"version" doc:"beacon server version."`
		CANInterfaces []string `json:"can_interfaces" doc:"Detected SocketCAN network interfaces (Linux only; empty elsewhere)."`
		SerialPorts   []string `json:"serial_ports" doc:"Detected USB-serial device paths (typical USB-CAN/NMEA-0183 adapters)."`
	}
}

// registerSystemInfoRoutes registers GET /api/v1/system: the server version
// plus a best-effort hardware inventory, so the UI's "add source/sink" flow
// can offer detected CAN interfaces and serial ports instead of a blank text
// field.
func registerSystemInfoRoutes(api huma.API, version string) {
	huma.Register(api, huma.Operation{
		OperationID: "get-system",
		Method:      http.MethodGet,
		Path:        "/api/v1/system",
		Summary:     "Get server version and detected hardware interfaces",
	}, func(ctx context.Context, _ *struct{}) (*systemOutput, error) {
		out := &systemOutput{}
		out.Body.Version = version
		out.Body.CANInterfaces = discoverCAN()
		out.Body.SerialPorts = discoverSerial()
		return out, nil
	})
}

// DiscoverSystem exposes the same best-effort hardware inventory GET
// /api/v1/system reports (see registerSystemInfoRoutes above) to other
// in-process callers. internal/ui's source/sink add/edit forms call this
// directly to populate interface/port <datalist> suggestions, rather than
// beacon making an HTTP round trip to its own API from its own UI handler
// — this is a same-binary, cross-package function call, not a network
// call, so it stays synchronous and can't fail independently of the
// process itself.
func DiscoverSystem() (canIfaces, serialPorts []string) {
	return discoverCAN(), discoverSerial()
}

// discoverCAN best-effort lists SocketCAN network interface names present on
// this host: entries of canRoot (normally /sys/class/net) whose "type" file
// reads "280" (ARPHRD_CAN, the CAN bus link-layer type in Linux's if_arp.h).
// SocketCAN is Linux-only, so any other OS short-circuits to an empty list
// without touching the filesystem at all; both a missing canRoot and a
// per-interface read failure are treated as "no such interface" rather than
// an error, matching this endpoint's best-effort contract.
func discoverCAN() []string {
	if hostGOOS != "linux" {
		return []string{}
	}
	entries, err := os.ReadDir(canRoot)
	if err != nil {
		return []string{}
	}
	out := []string{}
	for _, e := range entries {
		typ, err := os.ReadFile(filepath.Join(canRoot, e.Name(), "type"))
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(typ)) == "280" {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// discoverSerial best-effort lists likely USB-serial device paths under
// serialRoot (normally /dev), matching the naming conventions Linux
// (ttyUSB*, ttyACM*) and macOS (tty.usbserial*, tty.usbmodem*) use for USB
// serial adapters — the typical connection for USB-CAN and NMEA 0183
// hardware. Unlike discoverCAN this is not OS-gated: on a host with none of
// these devices the glob patterns simply match nothing.
func discoverSerial() []string {
	patterns := []string{"ttyUSB*", "ttyACM*", "tty.usbserial*", "tty.usbmodem*"}
	out := []string{}
	for _, p := range patterns {
		matches, _ := filepath.Glob(filepath.Join(serialRoot, p))
		out = append(out, matches...)
	}
	sort.Strings(out)
	return out
}

// --- Live metrics ---

type connectorMetricsOutput struct {
	Body stats.Snapshot
}

type listMetricsOutput struct {
	Body struct {
		Connectors map[string]stats.Snapshot `json:"connectors" doc:"Live per-connector counters/rates, keyed by connector id."`
	}
}

// registerMetricsRoutes registers the two read endpoints over reg: a single
// connector's live counters (404 if the connector itself is unknown; a
// known-but-idle connector reports a zero Snapshot, not 404) and every
// connector's at once.
func registerMetricsRoutes(api huma.API, svc *config.Service, reg *stats.Registry, log *slog.Logger) {
	huma.Register(api, huma.Operation{
		OperationID: "get-connector-metrics",
		Method:      http.MethodGet,
		Path:        "/api/v1/connectors/{id}/metrics",
		Summary:     "Get live delivery metrics for a connector",
		Errors:      []int{http.StatusNotFound},
	}, func(ctx context.Context, in *idInput) (*connectorMetricsOutput, error) {
		if _, err := svc.GetConnector(ctx, in.ID); err != nil {
			return nil, mapServiceErr(log, err)
		}
		snap, _ := reg.Snapshot(in.ID) // ok=false (never recorded) -> zero Snapshot, which is correct here
		return &connectorMetricsOutput{Body: snap}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "list-metrics",
		Method:      http.MethodGet,
		Path:        "/api/v1/metrics",
		Summary:     "Get live delivery metrics for every connector",
	}, func(ctx context.Context, _ *struct{}) (*listMetricsOutput, error) {
		out := &listMetricsOutput{}
		out.Body.Connectors = reg.All()
		return out, nil
	})
}

// --- Config export / import ---

type exportOutput struct {
	Body model.Config
}

type importInput struct {
	Mode string `query:"mode" enum:"replace,merge" default:"replace" doc:"replace (default): the body becomes the whole configuration. merge: the body's entities are upserted by id onto the existing configuration; entities it does not mention are left untouched."`
	Body model.Config
}

type importOutput struct {
	Body StatusBody
}

// registerConfigIORoutes registers the whole-config export/import endpoints
// used by the UI's backup/restore flow (and mirrored offline by the `beacon
// export`/`beacon import` CLI verbs, which talk to the store directly
// instead).
func registerConfigIORoutes(api huma.API, svc *config.Service, log *slog.Logger) {
	huma.Register(api, huma.Operation{
		OperationID: "export-config",
		Method:      http.MethodGet,
		Path:        "/api/v1/config/export",
		Summary:     "Export the full configuration",
	}, func(ctx context.Context, _ *struct{}) (*exportOutput, error) {
		cfg, err := svc.Export(ctx)
		if err != nil {
			return nil, mapServiceErr(log, err)
		}
		return &exportOutput{Body: cfg}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "import-config",
		Method:      http.MethodPost,
		Path:        "/api/v1/config/import",
		Summary:     "Import a configuration (replace or merge)",
		Errors:      []int{http.StatusUnprocessableEntity},
	}, func(ctx context.Context, in *importInput) (*importOutput, error) {
		replace := in.Mode != "merge"
		if err := svc.Import(ctx, in.Body, replace); err != nil {
			return nil, mapServiceErr(log, err)
		}
		out := &importOutput{}
		out.Body.Status = svc.Statuses()
		return out, nil
	})
}

// --- Health ---

type healthOutput struct {
	Body struct {
		Status     string              `json:"status" doc:"\"ok\" when every component is up, \"degraded\" otherwise."`
		Components []supervisor.Status `json:"components"`
	}
}

// registerHealthRoutes registers GET /api/v1/health: the same body shape as
// the admin server's top-level /health (spec §5 lists health under the API
// surface too), built from the same Statuses() the admin handler reads —
// just reached through config.Service rather than the supervisor directly.
func registerHealthRoutes(api huma.API, svc *config.Service) {
	huma.Register(api, huma.Operation{
		OperationID: "get-health",
		Method:      http.MethodGet,
		Path:        "/api/v1/health",
		Summary:     "Get overall health",
	}, func(ctx context.Context, _ *struct{}) (*healthOutput, error) {
		statuses := svc.Statuses()
		out := &healthOutput{}
		out.Body.Status = supervisor.RollupHealth(statuses)
		out.Body.Components = statuses
		return out, nil
	})
}
