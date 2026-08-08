// dashboard.go implements beacon's live home view: the source -> connector
// -> sink DAG templates/frag_dashboard.html renders, and the GET
// /frag/dashboard handler templates/dashboard.html polls every 5s
// (hx-trigger="load, every 5s") — see dashboard.html's comment for why it
// ships an empty container rather than rendering this fragment inline, the
// same "ships empty, fetches client-side" shape the overview pages use.
package ui

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/inventory"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
	"github.com/open-ships/beacon/internal/sysinfo"
)

// relativeAge renders a timestamp as a compact "Ns/m/h ago" for the live
// dashboard; the zero time (never seen) becomes an em dash.
func relativeAge(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	switch d := time.Since(t); {
	case d < time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
}

// dashboardEndpointNode is one source/sink node rendered in the home DAG
// and in the metadata tables below it.
type dashboardEndpointNode struct {
	Kind           string // "source" or "sink"
	ID             string
	Name           string
	Type           string
	Detail         string
	Enabled        bool
	State          string // "up" | "degraded" | "error" | "restarting" | "disabled"
	ConnectorCount int
	Unused         bool
}

// componentState resolves one configured component's displayed state:
//
//   - its live supervisor status, if statuses (the reconciler's current
//     snapshot) currently reports one for (kind, id) — "up"/"degraded"/
//     "error", the supervisor.Status.State values components report.
//   - "disabled", if the operator has it turned off. A disabled component
//     is never started, so the supervisor can never report a status for it
//     — this is the expected, steady-state reason for absence.
//   - "restarting", if it's enabled but transiently missing from statuses
//     anyway — e.g. between a hot-apply's stop of the old instance and the
//     new one's start landing in the supervisor's state map. This is the
//     UI's documented tolerance for that window: it renders an explicit
//     restarting state, never a crash and never a silently dropped component.
func componentState(statuses []supervisor.Status, kind, id string, enabled bool) string {
	for _, s := range statuses {
		if s.Kind == kind && s.ID == id {
			return s.State
		}
	}
	if !enabled {
		return "disabled"
	}
	return "restarting"
}

func displayName(name, id string) string {
	if name != "" {
		return name
	}
	return id
}

func sourceEndpointNodes(sources []model.Source, statuses []supervisor.Status, connectorCounts map[string]int) []dashboardEndpointNode {
	nodes := make([]dashboardEndpointNode, len(sources))
	for i, s := range sources {
		nodes[i] = dashboardEndpointNode{
			Kind:           "source",
			ID:             s.ID,
			Name:           displayName(s.Name, s.ID),
			Type:           string(s.Type),
			Detail:         sourceDetail(s),
			Enabled:        s.Enabled,
			State:          componentState(statuses, "source", s.ID, s.Enabled),
			ConnectorCount: connectorCounts[s.ID],
		}
	}
	return nodes
}

func sinkEndpointNodes(sinks []model.Sink, statuses []supervisor.Status, connectorCounts map[string]int) []dashboardEndpointNode {
	nodes := make([]dashboardEndpointNode, len(sinks))
	for i, s := range sinks {
		nodes[i] = dashboardEndpointNode{
			Kind:           "sink",
			ID:             s.ID,
			Name:           displayName(s.Name, s.ID),
			Type:           string(s.Type),
			Detail:         sinkDetail(s),
			Enabled:        s.Enabled,
			State:          componentState(statuses, "sink", s.ID, s.Enabled),
			ConnectorCount: connectorCounts[s.ID],
		}
	}
	return nodes
}

func endpointConnectorCounts(connectors []model.Connector) (map[string]int, map[string]int) {
	sourceCounts := map[string]int{}
	sinkCounts := map[string]int{}
	for _, c := range connectors {
		sourceCounts[c.SourceID]++
		sinkCounts[c.SinkID]++
	}
	return sourceCounts, sinkCounts
}

func endpointNodeMap(nodes []dashboardEndpointNode) map[string]dashboardEndpointNode {
	m := make(map[string]dashboardEndpointNode, len(nodes))
	for _, n := range nodes {
		m[n.ID] = n
	}
	return m
}

func fallbackEndpointNode(kind, id string) dashboardEndpointNode {
	return dashboardEndpointNode{
		Kind:  kind,
		ID:    id,
		Name:  id,
		Type:  "missing",
		State: "unknown",
	}
}

// dashboardFlow is one source -> connector -> sink path in the DAG:
// model.Connector plus its live stats.Snapshot, its live supervisor state,
// and the resolved source/sink nodes used by the connector. The endpoint
// nodes are also collected separately for rendering: several flows may point
// at the same source or sink, but the graph renders that endpoint only once.
type dashboardFlow struct {
	model.Connector
	Snapshot        stats.Snapshot
	State           string
	BytesPerSecText string
	Source          dashboardEndpointNode
	Sink            dashboardEndpointNode
}

// dashboardFlows builds the DAG rows in svc.ListConnectors order. It also
// resolves the source/sink nodes each connector references.
func dashboardFlows(connectors []model.Connector, reg *stats.Registry, statuses []supervisor.Status, sources, sinks map[string]dashboardEndpointNode) []dashboardFlow {
	flows := make([]dashboardFlow, len(connectors))
	for i, c := range connectors {
		snap, _ := reg.Snapshot(c.ID)
		src, ok := sources[c.SourceID]
		if !ok {
			src = fallbackEndpointNode("source", c.SourceID)
		}
		sink, ok := sinks[c.SinkID]
		if !ok {
			sink = fallbackEndpointNode("sink", c.SinkID)
		}
		flows[i] = dashboardFlow{
			Connector:       c,
			Snapshot:        snap,
			State:           componentState(statuses, "connector", c.ID, c.Enabled),
			BytesPerSecText: humanizeBytes(snap.BytesPerSec, "/s"),
			Source:          src,
			Sink:            sink,
		}
	}
	return flows
}

// partitionGraphEndpointNodes splits configured endpoints into the main graph
// and the secondary unused row. An endpoint is unused only when it is enabled
// and no configured connector references it. Disabled endpoints stay in the
// main graph, and disabled connectors still count as configured connections.
// Fallback endpoints referenced by stale connectors also remain in the main
// graph so the dashboard keeps its existing missing-endpoint tolerance.
func partitionGraphEndpointNodes(configured []dashboardEndpointNode, flows []dashboardFlow, kind string) (graph, unused []dashboardEndpointNode) {
	seen := make(map[string]bool, len(configured)+len(flows))
	for _, node := range configured {
		seen[node.ID] = true
		if node.Enabled && node.ConnectorCount == 0 {
			node.Unused = true
			unused = append(unused, node)
			continue
		}
		graph = append(graph, node)
	}
	for _, flow := range flows {
		node := flow.Source
		if kind == "sink" {
			node = flow.Sink
		}
		if seen[node.ID] {
			continue
		}
		seen[node.ID] = true
		graph = append(graph, node)
	}
	return graph, unused
}

// dashboardData is frag_dashboard.html's "dashboard-content" data. The
// Empty* fields are populated only when no graph node of any kind exists.
// Configured endpoints render in the DAG even when Flows is empty.
type dashboardData struct {
	Sources           []dashboardEndpointNode
	Sinks             []dashboardEndpointNode
	GraphSources      []dashboardEndpointNode
	GraphSinks        []dashboardEndpointNode
	UnusedSources     []dashboardEndpointNode
	UnusedSinks       []dashboardEndpointNode
	Flows             []dashboardFlow
	Devices           []busDeviceRow
	CANInterfaces     []sysinfo.CANInterface
	CanCommitBaseline bool
	EmptyTitle        string
	EmptyMessage      string
	EmptyCTA          string
	EmptyHref         string
}

// busDeviceRow is one NMEA-2000 device observed on a CAN bus, rendered in the
// dashboard's "Bus devices" table. Name is formatted as hex so a device is
// recognizable across address changes.
type busDeviceRow struct {
	Endpoint     string
	Address      uint8
	Name         string
	NameKey      string
	Model        string
	Serial       string
	LastSeen     string
	Status       string
	Label        string
	Manufacturer string
	Role         string
	Instances    string
	Software     string
	Installation string
	Supported    string
}

// busDeviceRows adapts the supervisor's device snapshot for the template,
// formatting the packed NAME as hex and last-seen as a relative age.
func busDeviceRows(devices []bus.DeviceInfo) []busDeviceRow {
	rows := make([]busDeviceRow, 0, len(devices))
	for _, d := range devices {
		rows = append(rows, busDeviceRow{
			Endpoint:     d.Endpoint,
			Address:      d.Address,
			Name:         fmt.Sprintf("0x%016X", d.Name),
			NameKey:      fmt.Sprintf("%016X", d.Name),
			Model:        d.Model,
			Serial:       d.Serial,
			LastSeen:     relativeAge(d.LastSeen),
			Manufacturer: d.Manufacturer,
			Role:         strings.TrimSpace(d.DeviceClassName + " / " + d.DeviceFunctionName),
			Instances:    fmt.Sprintf("device %d / system %d", d.DeviceInstance, d.SystemInstance),
			Software:     strings.TrimSpace(d.SoftwareVersion + " " + d.ModelVersion),
			Installation: strings.TrimSpace(d.InstallationDescription1 + " " + d.InstallationDescription2),
			Supported:    fmt.Sprintf("%d TX / %d RX", len(d.TransmitPGNs), len(d.ReceivePGNs)),
		})
	}
	return rows
}

func inventoryDeviceRows(records []inventory.Record) []busDeviceRow {
	devices := make([]bus.DeviceInfo, len(records))
	for i := range records {
		devices[i] = records[i].DeviceInfo
	}
	rows := busDeviceRows(devices)
	for i := range records {
		rows[i].Status, rows[i].Label = records[i].Status, records[i].Label
	}
	return rows
}

func dashboardEmptyState() (title, message, cta, href string) {
	return "Add your first source", "Connect a source to start receiving data.", "Add a source", "/sources/new"
}

// handleDashboardFrag serves GET /frag/dashboard: the dashboard page's
// hx-trigger="load, every 5s" polling target (see templates/dashboard.html).
// Lists every configured source/sink/connector fresh on every poll — a
// source/sink/connector created or deleted elsewhere in the UI must show up
// on the dashboard within one poll interval, the same freshness every other
// live fragment in this package gives — and combines them with one
// statuses() snapshot (used for endpoint node state and connector error
// badges, so both reflect the exact same instant) and reg's live
// per-connector counters.
func handleDashboardFrag(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, devices func() []bus.DeviceInfo, runtime RuntimeInfo, log *slog.Logger) http.HandlerFunc {
	var canDetailsMu sync.Mutex
	var canDetails []sysinfo.CANInterface
	var canDetailsExpires time.Time
	loadCANDetails := func() []sysinfo.CANInterface {
		if runtime.CANDetails == nil {
			return nil
		}
		canDetailsMu.Lock()
		defer canDetailsMu.Unlock()
		if time.Now().Before(canDetailsExpires) {
			return append([]sysinfo.CANInterface(nil), canDetails...)
		}
		canDetails = runtime.CANDetails()
		canDetailsExpires = time.Now().Add(10 * time.Second)
		return append([]sysinfo.CANInterface(nil), canDetails...)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, err := svc.Export(r.Context())
		if err != nil {
			log.Error("ui: load dashboard config failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		sources, sinks, connectors := cfg.Sources, cfg.Sinks, cfg.Connectors

		live := statuses()
		sourceConnectorCounts, sinkConnectorCounts := endpointConnectorCounts(connectors)
		sourceNodes := sourceEndpointNodes(sources, live, sourceConnectorCounts)
		sinkNodes := sinkEndpointNodes(sinks, live, sinkConnectorCounts)
		flows := dashboardFlows(
			connectors,
			reg,
			live,
			endpointNodeMap(sourceNodes),
			endpointNodeMap(sinkNodes),
		)
		graphSources, unusedSources := partitionGraphEndpointNodes(sourceNodes, flows, "source")
		graphSinks, unusedSinks := partitionGraphEndpointNodes(sinkNodes, flows, "sink")
		data := dashboardData{
			Sources:       sourceNodes,
			Sinks:         sinkNodes,
			GraphSources:  graphSources,
			GraphSinks:    graphSinks,
			UnusedSources: unusedSources,
			UnusedSinks:   unusedSinks,
			Flows:         flows,
		}
		if len(data.Sources) == 0 && len(data.Sinks) == 0 && len(data.Flows) == 0 {
			data.EmptyTitle, data.EmptyMessage, data.EmptyCTA, data.EmptyHref = dashboardEmptyState()
		}
		if devices != nil {
			data.Devices = busDeviceRows(devices())
		}
		if runtime.Inventory != nil {
			data.Devices = inventoryDeviceRows(runtime.Inventory.Records())
			data.CanCommitBaseline = true
		}
		data.CANInterfaces = loadCANDetails()
		renderFragment(w, log, "dashboard-content", data)
	}
}
