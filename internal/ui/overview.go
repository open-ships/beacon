package ui

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
)

type configRow struct {
	Label string
	Value string
	Code  bool
}

type overviewPageData struct {
	pageData
	Kind        string
	Heading     string
	EditHref    string
	ConfigRows  []configRow
	LiveDOMID   string
	LiveHref    string
	StreamHref  string
	StreamID    string
	Description string
}

type overviewLogRow struct {
	Level   string
	Message string
}

type overviewLiveData struct {
	DOMID                       string
	RefreshHref                 string
	Kind                        string
	ID                          string
	State                       string
	Err                         string
	Logs                        []overviewLogRow
	Snapshot                    stats.Snapshot
	BytesPerSecText             string
	QueueBytesText              string
	RetainedBytesText           string
	SourceDevices               []sourceDeviceRow
	SourceDeviceSort            sourceDeviceSortView
	SourceDeviceDetail          *sourceDeviceDetailView
	SourceDeviceRowsRefreshHref string
}

func sourceOverviewPageData(version string, s model.Source) overviewPageData {
	title := displayName(s.Name, s.ID)
	return overviewPageData{
		pageData: newPageData(title, version, "sources").withBreadcrumbs(
			breadcrumbItem{Label: "Sources", Href: "/sources"},
			breadcrumbItem{Label: title},
		),
		Kind:        "source",
		Heading:     title,
		EditHref:    "/sources/" + s.ID + "/edit",
		ConfigRows:  sourceConfigRows(s),
		LiveDOMID:   "source-overview-live",
		LiveHref:    "/frag/sources/" + s.ID + "/overview",
		StreamHref:  "/ui/streams/sources/" + s.ID,
		StreamID:    s.ID,
		Description: "Received message activity and runtime health for this source.",
	}
}

func sinkOverviewPageData(version string, s model.Sink) overviewPageData {
	title := displayName(s.Name, s.ID)
	return overviewPageData{
		pageData: newPageData(title, version, "sinks").withBreadcrumbs(
			breadcrumbItem{Label: "Sinks", Href: "/sinks"},
			breadcrumbItem{Label: title},
		),
		Kind:        "sink",
		Heading:     title,
		EditHref:    "/sinks/" + s.ID + "/edit",
		ConfigRows:  sinkConfigRows(s),
		LiveDOMID:   "sink-overview-live",
		LiveHref:    "/frag/sinks/" + s.ID + "/overview",
		StreamHref:  "/ui/streams/sinks/" + s.ID,
		StreamID:    s.ID,
		Description: "Sent message activity and runtime health for this sink.",
	}
}

func connectorOverviewPageData(version string, c model.Connector) overviewPageData {
	title := displayName(c.Name, c.ID)
	return overviewPageData{
		pageData: newPageData(title, version, "connectors").withBreadcrumbs(
			breadcrumbItem{Label: "Connectors", Href: "/connectors"},
			breadcrumbItem{Label: title},
		),
		Kind:        "connector",
		Heading:     title,
		EditHref:    "/connectors/" + c.ID + "/edit",
		ConfigRows:  connectorConfigRows(c),
		LiveDOMID:   "connector-overview-live",
		LiveHref:    "/frag/connectors/" + c.ID + "/overview",
		Description: "Matched and delivered message activity for this connector.",
	}
}

func sourceConfigRows(s model.Source) []configRow {
	rows := []configRow{
		{"ID", s.ID, true},
		{"Name", displayName(s.Name, s.ID), false},
		{"Type", string(s.Type), true},
		{"Enabled", boolText(s.Enabled), false},
	}
	add := func(label, value string, code bool) {
		if strings.TrimSpace(value) != "" {
			rows = append(rows, configRow{label, value, code})
		}
	}
	add("Interface", s.Interface, true)
	add("Port", s.Port, true)
	add("URL", s.URL, true)
	add("Topic", s.Topic, true)
	add("File path", s.FilePath, true)
	add("Address", s.Address, true)
	add("Format", s.Format, true)
	if len(s.Headers) > 0 {
		rows = append(rows, configRow{"Headers", strconv.Itoa(len(s.Headers)), false})
	}
	return rows
}

func sinkConfigRows(s model.Sink) []configRow {
	rows := []configRow{
		{"ID", s.ID, true},
		{"Name", displayName(s.Name, s.ID), false},
		{"Type", string(s.Type), true},
		{"Enabled", boolText(s.Enabled), false},
	}
	add := func(label, value string, code bool) {
		if strings.TrimSpace(value) != "" {
			rows = append(rows, configRow{label, value, code})
		}
	}
	add("Interface", s.Interface, true)
	add("Port", s.Port, true)
	add("Path", s.Path, true)
	add("Address", s.Address, true)
	add("URL", s.URL, true)
	add("Topic", s.Topic, true)
	add("File path", s.FilePath, true)
	add("Format", s.Format, true)
	if s.MaxFileBytes != 0 {
		rows = append(rows, configRow{"Max file bytes", strconv.FormatInt(s.MaxFileBytes, 10), false})
	}
	if s.MaxFiles != 0 {
		rows = append(rows, configRow{"Max files", strconv.Itoa(s.MaxFiles), false})
	}
	return rows
}

func connectorConfigRows(c model.Connector) []configRow {
	rows := []configRow{
		{"ID", c.ID, true},
		{"Name", displayName(c.Name, c.ID), false},
		{"Enabled", boolText(c.Enabled), false},
		{"Source", c.SourceID, true},
		{"Sink", c.SinkID, true},
		{"Bridge mode", string(c.EffectiveMode()), true},
		{"Forward management", boolText(c.ForwardManagement), false},
		{"Filters", strconv.Itoa(len(c.Filters)), false},
	}
	if len(c.Filters) > 0 {
		rows = append(rows, configRow{"Filter expression", strings.Join(c.Filters, " && "), true})
	}
	if c.Buffer.MaxMessages != 0 {
		rows = append(rows, configRow{"Max messages", strconv.FormatInt(c.Buffer.MaxMessages, 10), false})
	}
	if c.Buffer.MaxAge != 0 {
		rows = append(rows, configRow{"Max age", time.Duration(c.Buffer.MaxAge).String(), false})
	}
	if c.Buffer.MaxBytes != 0 {
		rows = append(rows, configRow{"Max bytes", strconv.FormatInt(c.Buffer.MaxBytes, 10), false})
	}
	return rows
}

func boolText(v bool) string {
	if v {
		return "enabled"
	}
	return "disabled"
}

func overviewStatus(statuses []supervisor.Status, kind, id string, enabled bool) (state, errText string) {
	for _, s := range statuses {
		if s.Kind == kind && s.ID == id {
			return s.State, s.Err
		}
	}
	if !enabled {
		return "disabled", ""
	}
	return "restarting", ""
}

func overviewLogs(kind, state, errText string) []overviewLogRow {
	if errText != "" {
		return []overviewLogRow{{Level: "error", Message: errText}}
	}
	switch state {
	case "up":
		return nil
	case "disabled":
		return []overviewLogRow{{Level: "info", Message: kind + " is disabled"}}
	case "restarting":
		return []overviewLogRow{{Level: "warn", Message: kind + " is enabled but not currently running"}}
	default:
		return []overviewLogRow{{Level: "warn", Message: kind + " state is " + state}}
	}
}

func sourceOverviewLiveData(
	s model.Source,
	reg *stats.Registry,
	statuses []supervisor.Status,
	deviceSorting sourceDeviceSort,
	selectedAddress *uint8,
	pgnSorting sourceDevicePGNSort,
) overviewLiveData {
	snap, _ := reg.SourceSnapshot(s.ID)
	sourceMetrics := reg.SourcePGNMetrics(s.ID)
	state, errText := overviewStatus(statuses, "source", s.ID, s.Enabled)
	refreshBase := "/frag/sources/" + s.ID + "/overview"
	sourceDevices := sourceDeviceRows(sourceMetrics, deviceSorting)
	decorateSourceDeviceRows(sourceDevices, refreshBase, deviceSorting, selectedAddress, pgnSorting)
	return overviewLiveData{
		DOMID:              "source-overview-live",
		RefreshHref:        sourceDeviceSortURL(refreshBase, deviceSorting, selectedAddress, pgnSorting),
		Kind:               "source",
		ID:                 s.ID,
		State:              state,
		Err:                errText,
		Logs:               overviewLogs("source", state, errText),
		Snapshot:           snap,
		BytesPerSecText:    humanizeBytes(snap.BytesPerSec, "/s"),
		QueueBytesText:     humanizeBytes(float64(snap.QueueBytes), ""),
		RetainedBytesText:  humanizeBytes(float64(snap.RetainedBytes), ""),
		SourceDevices:      sourceDevices,
		SourceDeviceSort:   sourceDeviceSortControls(refreshBase, deviceSorting, selectedAddress, pgnSorting),
		SourceDeviceDetail: sourceDeviceDetail(sourceMetrics, sourceDevices, selectedAddress, refreshBase, deviceSorting, pgnSorting),
		SourceDeviceRowsRefreshHref: sourceDeviceSortURL(
			"/frag/sources/"+s.ID+"/device-rows",
			deviceSorting,
			selectedAddress,
			pgnSorting,
		),
	}
}

func formatDuration(seconds float64) string {
	if seconds <= 0 {
		return "learning"
	}
	d := time.Duration(seconds * float64(time.Second))
	if d < time.Millisecond {
		return d.Round(time.Microsecond).String()
	}
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(100 * time.Millisecond).String()
}

func formatAge(seconds float64) string {
	if seconds < 0 {
		seconds = 0
	}
	d := time.Duration(seconds * float64(time.Second))
	if d < time.Second {
		return "now"
	}
	if d < time.Minute {
		return d.Round(time.Second).String() + " ago"
	}
	if d < time.Hour {
		return d.Round(time.Minute).String() + " ago"
	}
	return d.Round(time.Hour).String() + " ago"
}

func sinkOverviewLiveData(s model.Sink, reg *stats.Registry, statuses []supervisor.Status) overviewLiveData {
	snap, _ := reg.SinkSnapshot(s.ID)
	state, errText := overviewStatus(statuses, "sink", s.ID, s.Enabled)
	return overviewLiveData{
		DOMID:             "sink-overview-live",
		RefreshHref:       "/frag/sinks/" + s.ID + "/overview",
		Kind:              "sink",
		ID:                s.ID,
		State:             state,
		Err:               errText,
		Logs:              overviewLogs("sink", state, errText),
		Snapshot:          snap,
		BytesPerSecText:   humanizeBytes(snap.BytesPerSec, "/s"),
		QueueBytesText:    humanizeBytes(float64(snap.QueueBytes), ""),
		RetainedBytesText: humanizeBytes(float64(snap.RetainedBytes), ""),
	}
}

func connectorOverviewLiveData(c model.Connector, reg *stats.Registry, statuses []supervisor.Status) overviewLiveData {
	snap, _ := reg.Snapshot(c.ID)
	state, errText := overviewStatus(statuses, "connector", c.ID, c.Enabled)
	return overviewLiveData{
		DOMID:             "connector-overview-live",
		RefreshHref:       "/frag/connectors/" + c.ID + "/overview",
		Kind:              "connector",
		ID:                c.ID,
		State:             state,
		Err:               errText,
		Logs:              overviewLogs("connector", state, errText),
		Snapshot:          snap,
		BytesPerSecText:   humanizeBytes(snap.BytesPerSec, "/s"),
		QueueBytesText:    humanizeBytes(float64(snap.QueueBytes), ""),
		RetainedBytesText: humanizeBytes(float64(snap.RetainedBytes), ""),
	}
}

func handleSourceOverviewPage(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		source, err := svc.GetSource(r.Context(), r.PathValue("id"))
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get source failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		_ = reg
		_ = statuses
		renderPage(w, log, "overview", sourceOverviewPageData(version, source))
	}
}

func handleSinkOverviewPage(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sink, err := svc.GetSink(r.Context(), r.PathValue("id"))
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get sink failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		_ = reg
		_ = statuses
		renderPage(w, log, "overview", sinkOverviewPageData(version, sink))
	}
}

func handleConnectorOverviewPage(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		connector, err := svc.GetConnector(r.Context(), r.PathValue("id"))
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get connector failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		_ = reg
		_ = statuses
		renderPage(w, log, "overview", connectorOverviewPageData(version, connector))
	}
}

func handleSourceOverviewFrag(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		source, err := svc.GetSource(r.Context(), r.PathValue("id"))
		if err != nil {
			renderOverviewFragErr(w, log, err)
			return
		}
		sorting := sourceDeviceSortFromQuery(r.URL.Query().Get("sort"), r.URL.Query().Get("dir"))
		selectedAddress := sourceDeviceAddressFromQuery(r.URL.Query().Get("device"))
		pgnSorting := sourceDevicePGNSortFromQuery(r.URL.Query().Get("pgn_sort"), r.URL.Query().Get("pgn_dir"))
		renderFragment(w, log, "overview-live", sourceOverviewLiveData(source, reg, statuses(), sorting, selectedAddress, pgnSorting))
	}
}

func handleSourceDeviceRowsFrag(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		source, err := svc.GetSource(r.Context(), r.PathValue("id"))
		if err != nil {
			renderOverviewFragErr(w, log, err)
			return
		}
		sorting := sourceDeviceSortFromQuery(r.URL.Query().Get("sort"), r.URL.Query().Get("dir"))
		selectedAddress := sourceDeviceAddressFromQuery(r.URL.Query().Get("device"))
		pgnSorting := sourceDevicePGNSortFromQuery(r.URL.Query().Get("pgn_sort"), r.URL.Query().Get("pgn_dir"))
		data := sourceOverviewLiveData(source, reg, statuses(), sorting, selectedAddress, pgnSorting)
		renderFragment(w, log, "source-device-row-snapshot", data)
	}
}

func handleSinkOverviewFrag(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sink, err := svc.GetSink(r.Context(), r.PathValue("id"))
		if err != nil {
			renderOverviewFragErr(w, log, err)
			return
		}
		renderFragment(w, log, "overview-live", sinkOverviewLiveData(sink, reg, statuses()))
	}
}

func handleConnectorOverviewFrag(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		connector, err := svc.GetConnector(r.Context(), r.PathValue("id"))
		if err != nil {
			renderOverviewFragErr(w, log, err)
			return
		}
		renderFragment(w, log, "overview-live", connectorOverviewLiveData(connector, reg, statuses()))
	}
}

func renderOverviewFragErr(w http.ResponseWriter, log *slog.Logger, err error) {
	if errors.Is(err, config.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	log.Error("ui: overview fragment failed", "err", err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

type configPageData struct {
	pageData
	ConfigJSON string
	ConfigRows int
	Mode       string
	Alert      *alertData
}

func handleConfigPage(svc *config.Service, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		renderConfigPage(w, r, svc, version, log, nil)
	}
}

func handleConfigImport(svc *config.Service, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			renderConfigPage(w, r, svc, version, log, &alertData{Kind: "error", Message: "invalid form submission: " + err.Error()})
			return
		}
		var cfg model.Config
		raw := r.PostFormValue("config_json")
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			renderConfigPageWithJSON(w, r, svc, version, log, raw, r.PostFormValue("mode"), &alertData{Kind: "error", Message: "invalid JSON: " + err.Error()})
			return
		}
		replace := r.PostFormValue("mode") != "merge"
		if err := svc.Import(r.Context(), cfg, replace); err != nil {
			renderConfigPageWithJSON(w, r, svc, version, log, raw, r.PostFormValue("mode"), &alertData{Kind: "error", Message: entityWriteErrorMessage(err, "configuration", "json")})
			return
		}
		renderConfigPage(w, r, svc, version, log, &alertData{Kind: "success", Message: "configuration loaded"})
	}
}

func renderConfigPage(w http.ResponseWriter, r *http.Request, svc *config.Service, version string, log *slog.Logger, alert *alertData) {
	cfg, err := svc.Export(r.Context())
	if err != nil {
		log.Error("ui: export config failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Error("ui: marshal config failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	renderConfigPageWithJSON(w, r, svc, version, log, string(b), "replace", alert)
}

// configTextareaRows sizes the config editor to its content so the whole
// document renders on load without an inner scrollbar. A floor keeps small
// configs looking like a proper editor; there is deliberately no ceiling —
// the entire configuration should be visible, however large.
func configTextareaRows(raw string) int {
	const minRows = 20
	rows := strings.Count(raw, "\n") + 1
	if rows < minRows {
		return minRows
	}
	return rows
}

func renderConfigPageWithJSON(w http.ResponseWriter, _ *http.Request, _ *config.Service, version string, log *slog.Logger, raw, mode string, alert *alertData) {
	if mode == "" {
		mode = "replace"
	}
	data := configPageData{
		pageData: newPageData("Configuration JSON", version, "config").withBreadcrumbs(
			breadcrumbItem{Label: "Configuration JSON"},
		),
		ConfigJSON: raw,
		ConfigRows: configTextareaRows(raw),
		Mode:       mode,
		Alert:      alert,
	}
	renderPage(w, log, "config", data)
}

func redirectToTrailingSlash(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently) // #nosec G710 -- the target is the same-origin request path with a slash appended.
}
