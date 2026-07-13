// dashboard.go implements beacon's live dashboard: the connector-card grid
// plus components health strip templates/frag_dashboard.html renders, and
// the GET /ui/frag/dashboard handler templates/dashboard.html polls every
// 2s (hx-trigger="load, every 2s") — see dashboard.html's comment for why
// it ships an empty container rather than rendering this fragment inline,
// the same "ships empty, fetches client-side" shape
// templates/connector_detail.html's live stats block uses (see pages.go's
// "connector-detail" doc comment).
package ui

import (
	"log/slog"
	"net/http"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
)

// healthChip is one components-health-strip entry — one per configured
// source/sink (enabled or not; see healthChipState) — rendered by
// frag_dashboard.html's "dashboard-content" template.
type healthChip struct {
	Kind  string // "source" or "sink"
	ID    string
	Name  string
	State string // "up" | "degraded" | "error" | "restarting" | "disabled"
}

// healthChipState resolves one configured component's chip state:
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
//     dashboard's documented tolerance for that window: it renders a
//     neutral chip, never a crash and never a silently dropped component.
func healthChipState(statuses []supervisor.Status, kind, id string, enabled bool) string {
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

// healthChips builds the components-health-strip data: one chip per
// configured source, then one per configured sink, in list order.
func healthChips(sources []model.Source, sinks []model.Sink, statuses []supervisor.Status) []healthChip {
	chips := make([]healthChip, 0, len(sources)+len(sinks))
	for _, s := range sources {
		chips = append(chips, healthChip{
			Kind: "source", ID: s.ID, Name: s.Name,
			State: healthChipState(statuses, "source", s.ID, s.Enabled),
		})
	}
	for _, s := range sinks {
		chips = append(chips, healthChip{
			Kind: "sink", ID: s.ID, Name: s.Name,
			State: healthChipState(statuses, "sink", s.ID, s.Enabled),
		})
	}
	return chips
}

// dashboardConnectorCard is one connector-grid card's data: model.Connector
// plus its live stats.Snapshot (reg.Snapshot — same zero-value-when-absent
// contract connectorRow in forms.go relies on, so a just-created or idle
// connector's card renders zero rate/queue tiles rather than needing a
// presence check here), its live supervisor state (stateFor — "unknown"
// when statuses doesn't report one, e.g. disabled; the card only acts on
// "error", rendering an extra error badge alongside the plain
// enabled/disabled badge every other state leaves untouched), and its
// source/sink NAMES (nameOrID — forms.go's connectorRow uses the exact
// same helper for the connectors table's route column).
type dashboardConnectorCard struct {
	model.Connector
	Snapshot        stats.Snapshot
	State           string
	BytesPerSecText string
	SourceName      string
	SinkName        string
}

// dashboardConnectorCards builds the connector-grid data in svc.ListConnectors
// order. srcNames/sinkNamesByID are id->Name lookup maps (forms.go's
// sourceNames/sinkNames) built by the caller from the same source/sink
// lists it already fetched for the components health strip (healthChips).
func dashboardConnectorCards(connectors []model.Connector, reg *stats.Registry, statuses []supervisor.Status, srcNames, sinkNamesByID map[string]string) []dashboardConnectorCard {
	cards := make([]dashboardConnectorCard, len(connectors))
	for i, c := range connectors {
		snap, _ := reg.Snapshot(c.ID)
		cards[i] = dashboardConnectorCard{
			Connector:       c,
			Snapshot:        snap,
			State:           stateFor(statuses, "connector", c.ID),
			BytesPerSecText: humanizeBytes(snap.BytesPerSec, "/s"),
			SourceName:      nameOrID(srcNames, c.SourceID),
			SinkName:        nameOrID(sinkNamesByID, c.SinkID),
		}
	}
	return cards
}

// dashboardData is frag_dashboard.html's "dashboard-content" data. The
// Empty* fields are populated only when there are zero connectors (see
// dashboardEmptyState) — frag_dashboard.html renders the connector grid
// when Connectors is non-empty and this hero otherwise.
type dashboardData struct {
	Health       []healthChip
	Connectors   []dashboardConnectorCard
	EmptyTitle   string
	EmptyMessage string
	EmptyCTA     string
	EmptyHref    string
}

// dashboardEmptyState picks the empty-state hero's copy and CTA target for
// a dashboard with zero connectors configured: point the operator at
// /ui/sources first if there's nothing to wire up yet, or at /ui/connectors
// if sources (and presumably sinks) already exist but nothing routes
// between them yet — the "handle both" cases the behavior contract calls
// out explicitly, rather than always sending an operator who already has
// sources/sinks configured back to the sources page.
func dashboardEmptyState(hasSources bool) (title, message, cta, href string) {
	if !hasSources {
		return "Add your first source", "Connect a source to start receiving data.", "Add a source", "/ui/sources"
	}
	return "Add your first connector", "Wire a source to a sink with a connector to start routing data.", "Add a connector", "/ui/connectors"
}

// handleDashboardFrag serves GET /ui/frag/dashboard: the dashboard page's
// hx-trigger="load, every 2s" polling target (see templates/dashboard.html).
// Lists every configured source/sink/connector fresh on every poll — a
// source/sink/connector created or deleted elsewhere in the UI must show up
// on the dashboard within one poll interval, the same freshness every other
// live fragment in this package gives — and combines them with one
// statuses() snapshot (used for both the health strip and each connector
// card's error badge, so both reflect the exact same instant) and reg's
// live per-connector counters.
func handleDashboardFrag(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sources, err := svc.ListSources(r.Context())
		if err != nil {
			log.Error("ui: list sources failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		sinks, err := svc.ListSinks(r.Context())
		if err != nil {
			log.Error("ui: list sinks failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		connectors, err := svc.ListConnectors(r.Context())
		if err != nil {
			log.Error("ui: list connectors failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		live := statuses()
		data := dashboardData{
			Health:     healthChips(sources, sinks, live),
			Connectors: dashboardConnectorCards(connectors, reg, live, sourceNames(sources), sinkNames(sinks)),
		}
		if len(data.Connectors) == 0 {
			data.EmptyTitle, data.EmptyMessage, data.EmptyCTA, data.EmptyHref = dashboardEmptyState(len(sources) > 0)
		}
		renderFragment(w, log, "dashboard-content", data)
	}
}
