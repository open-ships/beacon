// forms.go implements the Sources, Sinks, and Connectors CRUD pages: the
// page/fragment data types templates/sources.html, sinks.html,
// connectors.html, connector_detail.html, and every frag_*.html render
// against, the model<->form conversions, and the HTTP handlers Handler
// (ui.go) wires up at /sources, /sinks, /connectors, /frag/*.
//
// Sources and sinks are handled by deliberately parallel, non-generic code
// (sourceX / sinkX pairs) rather than a shared generic implementation: the
// two entities' type-specific fields differ enough (source http_sse/
// http_ws carries URL+Headers; sink http_sse/http_ws carries Path, and
// sink alone has tcp (Address) and file (FilePath/Format/MaxFileBytes/
// MaxFiles) types with no source equivalent) that a generic abstraction
// would need almost as much per-kind branching as just writing both out,
// while being harder to follow — the same tradeoff internal/api/entities.go
// already made for its source/sink/connector route registration. Connectors
// have no type-specific fields at all (source_id/sink_id are plain selects,
// not a type switch), so their section below has no typeFields analogue —
// otherwise it follows the same form-view/toModel/handler shape.
package ui

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	sinkruntime "github.com/open-ships/beacon/internal/sink"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
	"github.com/open-ships/beacon/internal/sysinfo"
)

// --- Shared helpers ---

// alertData renders a dismissible UI alert; Kind is "success" or
// "error" (anything else renders with the error styling, in
// frag_source_table.html/frag_sink_table.html's "*-panel-inner" template).
type alertData struct {
	Kind    string
	Message string
}

// generatedEntityID returns the short opaque id used by the source, sink,
// and connector creation forms. Four random bytes render as eight lowercase
// hexadecimal characters: compact enough to scan in the UI while avoiding
// user-authored naming and keeping collisions negligible at Beacon's scale.
func generatedEntityID() (string, error) {
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate entity id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func isHTMXRequest(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("HX-Request"), "true")
}

// writeOverviewDeleteResult handles delete actions originating on an entity
// overview. Successful deletes leave the now-invalid detail URL; failed
// deletes keep the operator in context and render the recovery message beside
// the action. List-page deletes retain their existing table-fragment response.
func writeOverviewDeleteResult(w http.ResponseWriter, r *http.Request, log *slog.Logger, listHref string, alert *alertData) bool {
	if r.URL.Query().Get("context") != "overview" {
		return false
	}
	if alert.Kind == "success" {
		w.Header().Set("HX-Redirect", listHref)
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	renderFragment(w, log, "overview-delete-alert", alert)
	return true
}

// referencingConnectorNames returns the ids of every connector referencing
// id as its source (kind="source") or sink (kind="sink"), for composing the
// delete-in-use error message: config.ErrInUse alone doesn't carry enough
// detail to name the referencing connector(s).
func referencingConnectorNames(r *http.Request, svc *config.Service, kind, id string) ([]string, error) {
	connectors, err := svc.ListConnectors(r.Context())
	if err != nil {
		return nil, err
	}
	var names []string
	for _, c := range connectors {
		if (kind == "source" && c.SourceID == id) || (kind == "sink" && c.SinkID == id) {
			names = append(names, c.ID)
		}
	}
	return names, nil
}

// parseHeaders parses a source add/edit form's headers textarea — one
// "Header-Name: value" per (non-blank) line, per
// frag_source_type_fields.html's documented format — into a map. Returns
// an error (never panics) on a line with no ":" separator or an empty
// header name, so malformed input becomes a validation alert (see
// writeSource) rather than a 500.
func parseHeaders(text string) (map[string]string, error) {
	headers := map[string]string{}
	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf(`headers line %d: %q is missing a ":" separator (expected "Header-Name: value")`, i+1, line)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("headers line %d: empty header name", i+1)
		}
		headers[key] = strings.TrimSpace(value)
	}
	if len(headers) == 0 {
		return nil, nil
	}
	return headers, nil
}

// formatHeaders is parseHeaders' inverse, rendering a headers map back to
// the same "Header-Name: value" per line textarea format so editing an
// existing source round-trips it. Sorted by key: map iteration order is
// random, and an edit form re-rendering its fields in a different order on
// every load would be a distracting, purposeless diff for the operator.
func formatHeaders(h map[string]string) string {
	if len(h) == 0 {
		return ""
	}
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	lines := make([]string, len(keys))
	for i, k := range keys {
		lines[i] = k + ": " + h[k]
	}
	return strings.Join(lines, "\n")
}

// sourceNames and sinkNames build id->Name lookup maps from a full source/
// sink list, for rendering source/sink NAMES rather than raw ids in the
// connectors table (connectorRows below) and the dashboard's connector
// cards (dashboard.go's dashboardConnectorCards) — nameOrID does the actual
// per-connector fallback-to-id lookup against the map these build.
func sourceNames(sources []model.Source) map[string]string {
	m := make(map[string]string, len(sources))
	for _, s := range sources {
		m[s.ID] = s.Name
	}
	return m
}

func sinkNames(sinks []model.Sink) map[string]string {
	m := make(map[string]string, len(sinks))
	for _, s := range sinks {
		m[s.ID] = s.Name
	}
	return m
}

// nameOrID looks id up in names (built by sourceNames/sinkNames) and
// returns its Name, falling back to id itself when the entity has no name
// set (model.Source/Sink don't require one — see model/validate.go) or, in
// principle, no longer exists at all (a connector referencing an id that
// was since deleted out from under it shouldn't render a blank cell).
func nameOrID(names map[string]string, id string) string {
	if n, ok := names[id]; ok && n != "" {
		return n
	}
	return id
}

// discoverHardware returns the same best-effort hardware inventory GET
// /api/v1/system reports (internal/sysinfo) for populating the
// interface/port <datalist> suggestions on the source/sink add/edit forms.
// Calling internal/sysinfo directly (rather than going through
// internal/api) keeps this package from depending on internal/api at all —
// a same-binary, cross-package function call either way, not a network
// call, so it stays synchronous and can't fail independently of the
// process itself.
func discoverHardware() (canIfaces, serialPorts []string) {
	return sysinfo.DiscoverCAN(), sysinfo.DiscoverSerial()
}

// writeFragmentSuccess renders panelOOBName (a "*-panel-oob" fragment,
// e.g. "source-panel-oob") from fragTemplates and appends an empty
// <div id="formContainerID">. The form the request came from has
// hx-target="#<formContainerID>" hx-swap="innerHTML", so the empty trailing
// div is what actually satisfies that swap — clearing the just-submitted
// form now that the write succeeded. The OOB-marked panel ahead of it
// (hx-swap-oob="true" is baked into every "*-panel-oob" template, see
// frag_source_table.html/frag_sink_table.html) updates the table wherever
// it currently lives in the DOM, independent of the form's own hx-target.
// This is htmx's standard "one response, two DOM updates" pattern.
func writeFragmentSuccess(w http.ResponseWriter, log *slog.Logger, panelOOBName, formContainerID string, data any) {
	var buf bytes.Buffer
	if err := fragTemplates.ExecuteTemplate(&buf, panelOOBName, data); err != nil {
		log.Error("ui: fragment render failed", "fragment", panelOOBName, "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(&buf, `<div id="%s"></div>`, formContainerID)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// --- Sources ---

// sourceRow is one row of the sources table (frag_source_table.html):
// model.Source plus its live supervisor state and type-specific endpoint
// Detail value.
type sourceRow struct {
	model.Source
	State  string
	Detail string
}

func sourceRows(sources []model.Source, statuses []supervisor.Status) []sourceRow {
	rows := make([]sourceRow, len(sources))
	for i, s := range sources {
		rows[i] = sourceRow{Source: s, State: componentState(statuses, "source", s.ID, s.Enabled), Detail: sourceDetail(s)}
	}
	return rows
}

func sourceDetail(s model.Source) string {
	switch s.Type {
	case model.SourceSocketCAN:
		return s.Interface
	case model.SourceUSBCAN:
		return s.Port
	case model.SourceHTTPSSE, model.SourceHTTPWS:
		return s.URL
	case model.SourceMQTT:
		return mqttDetail(s.URL, s.Topic)
	case model.SourceFile:
		return s.FilePath
	case model.SourceTCP, model.SourceUDP:
		return s.Address
	default:
		return ""
	}
}

// sourceTableData is frag_source_table.html's "source-panel"/
// "source-panel-oob" data: the table rows plus an optional one-shot alert
// (a just-completed write/delete's result).
type sourceTableData struct {
	Sources []sourceRow
	Alert   *alertData
}

// sourcesPageData is templates/sources.html's data: pageData (for
// layout.html) plus sourceTableData (for the embedded "source-panel" the
// page's "content" template renders on initial load — see mustPage in
// pages.go, which parses frag_source_table.html into the same template
// set as sources.html for exactly this).
type sourcesPageData struct {
	pageData
	sourceTableData
	SourceForm *sourceFormViewData
}

// sourceTypeFieldsData is frag_source_type_fields.html's data: which type
// is selected (decides which fields render), the current value of every
// type-specific field, and the discovered hardware lists for the
// socketcan/usbcan <datalist> suggestions (see discoverHardware).
//
// Every type's field is a struct member here, but only the selected
// type's fields actually render as inputs — so only they survive the next
// hx-include="closest form" round trip. Switching the type select from A
// to B discards whatever was typed into A's fields (they leave the DOM,
// so nothing resends them; switching back to A renders them blank), and
// nothing persists server-side until Save. That's deliberate: carrying
// unselected types' values through hidden inputs isn't worth the
// complexity for a form this small.
type sourceTypeFieldsData struct {
	Type          string
	Interface     string
	Port          string
	URL           string
	Topic         string
	HeadersText   string
	FilePath      string // file
	Address       string // tcp / udp
	Format        string // tcp / udp
	CANInterfaces []string
	SerialPorts   []string
}

// sourceFormViewData is frag_source_form.html's data. IsEdit controls
// whether the immutable id is shown disabled (edit) or carried only as a
// generated hidden value (create). InDialog switches the shared modal form's
// cancel/close behavior without duplicating its fields. The type doubles as
// the intermediate representation built from either a model.Source or a raw
// *http.Request after validation fails.
type sourceFormViewData struct {
	IsEdit     bool
	InDialog   bool
	ID         string
	Name       string
	Enabled    bool
	TypeFields sourceTypeFieldsData
	Alert      *alertData
}

// sourceFormViewFromModel builds a source-form view for the canonical edit
// page: every field pre-filled from the stored entity, headers rendered
// back through formatHeaders.
func sourceFormViewFromModel(v model.Source, can, serial []string) sourceFormViewData {
	return sourceFormViewData{
		IsEdit:  true,
		ID:      v.ID,
		Name:    v.Name,
		Enabled: v.Enabled,
		TypeFields: sourceTypeFieldsData{
			Type:          string(v.Type),
			Interface:     v.Interface,
			Port:          v.Port,
			URL:           v.URL,
			Topic:         v.Topic,
			HeadersText:   formatHeaders(v.Headers),
			FilePath:      v.FilePath,
			Address:       v.Address,
			Format:        v.Format,
			CANInterfaces: can,
			SerialPorts:   serial,
		},
	}
}

// blankSourceFormView builds a source create view with Enabled on and Type
// defaulted to "socketcan", keeping the initial select and type-specific
// fields in sync before any hx-get-driven change event fires. The handler
// supplies its generated ID because random generation can fail.
func blankSourceFormView(can, serial []string) sourceFormViewData {
	return sourceFormViewData{
		Enabled: true,
		TypeFields: sourceTypeFieldsData{
			Type:          string(model.SourceSocketCAN),
			CANInterfaces: can,
			SerialPorts:   serial,
		},
	}
}

// sourceFormViewFromRequest rebuilds a source-form view from a POSTed
// form's raw values (r.ParseForm must already have been called) — used to
// redisplay a submission that failed validation, preserving exactly what
// the operator typed rather than falling back to blank/stored values.
func sourceFormViewFromRequest(r *http.Request, isEdit bool, can, serial []string) sourceFormViewData {
	return sourceFormViewData{
		IsEdit:   isEdit,
		InDialog: r.PostFormValue("dialog") != "",
		ID:       r.PostFormValue("id"),
		Name:     r.PostFormValue("name"),
		Enabled:  r.PostFormValue("enabled") != "",
		TypeFields: sourceTypeFieldsData{
			Type:          r.PostFormValue("type"),
			Interface:     r.PostFormValue("interface"),
			Port:          r.PostFormValue("port"),
			URL:           r.PostFormValue("url"),
			Topic:         r.PostFormValue("topic"),
			HeadersText:   r.PostFormValue("headers"),
			FilePath:      r.PostFormValue("file_path"),
			Address:       r.PostFormValue("address"),
			Format:        r.PostFormValue("format"),
			CANInterfaces: can,
			SerialPorts:   serial,
		},
	}
}

// toModel converts a submitted form view into a model.Source ready for
// svc.PutSource. The only failure mode here is a malformed headers
// textarea (parseHeaders) — everything else (missing required
// type-specific field, bad URL, unknown type, ...) is left for
// config.Service's own structural validation to catch and report as a
// *config.ValidationError, so this package doesn't duplicate model
// validation rules.
func (f sourceFormViewData) toModel() (model.Source, error) {
	headers, err := parseHeaders(f.TypeFields.HeadersText)
	if err != nil {
		return model.Source{}, err
	}
	return model.Source{
		ID:        f.ID,
		Name:      f.Name,
		Type:      model.SourceType(f.TypeFields.Type),
		Enabled:   f.Enabled,
		Interface: f.TypeFields.Interface,
		Port:      f.TypeFields.Port,
		URL:       f.TypeFields.URL,
		Topic:     f.TypeFields.Topic,
		Headers:   headers,
		FilePath:  f.TypeFields.FilePath,
		Address:   f.TypeFields.Address,
		Format:    f.TypeFields.Format,
	}, nil
}

func renderSourcesPage(w http.ResponseWriter, r *http.Request, svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger, form *sourceFormViewData) {
	sources, err := svc.ListSources(r.Context())
	if err != nil {
		log.Error("ui: list sources failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	page := newPageData("Sources", version, "sources")
	if form != nil {
		current := "Add source"
		if form.IsEdit {
			current = "Edit " + form.ID
		}
		page = page.withBreadcrumbs(
			breadcrumbItem{Label: "Sources", Href: "/sources"},
			breadcrumbItem{Label: current},
		)
	}
	data := sourcesPageData{
		pageData:        page,
		sourceTableData: sourceTableData{Sources: sourceRows(sources, statuses())},
		SourceForm:      form,
	}
	renderPage(w, log, "sources", data)
}

// handleSourcesPage serves GET /sources: the full list page.
func handleSourcesPage(svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		renderSourcesPage(w, r, svc, statuses, version, log, nil)
	}
}

// handleSourceNewPage serves GET /sources/new as a modal fragment for htmx
// callers and as a full-page progressive-enhancement fallback otherwise.
func handleSourceNewPage(svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := discoverHardware()
		form := blankSourceFormView(can, serial)
		id, err := generatedEntityID()
		if err != nil {
			log.Error("ui: generate source id failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		form.ID = id
		if isHTMXRequest(r) {
			form.InDialog = true
			renderFragment(w, log, "source-create-dialog", form)
			return
		}
		renderSourcesPage(w, r, svc, statuses, version, log, &form)
	}
}

// handleSourceEditPage serves GET /sources/{id}/edit as a modal fragment for
// htmx callers and as a full-page progressive-enhancement fallback otherwise.
func handleSourceEditPage(svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := discoverHardware()
		v, err := svc.GetSource(r.Context(), r.PathValue("id"))
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get source failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		form := sourceFormViewFromModel(v, can, serial)
		if isHTMXRequest(r) {
			form.InDialog = true
			renderFragment(w, log, "source-create-dialog", form)
			return
		}
		renderSourcesPage(w, r, svc, statuses, version, log, &form)
	}
}

// handleSourceTypeFieldsFrag serves GET /frag/source-type-fields: the
// type select's hx-get target. hx-include="closest form" resends every
// field currently in the form as a query parameter, so the newly selected
// type's own field keeps its value if it happens to still be in the DOM —
// but a previously selected type's fields are NOT preserved across an
// A→B→A switch: B's render removed A's inputs from the DOM, so nothing
// resends them and A comes back blank (see sourceTypeFieldsData's doc
// comment for why that's deliberate).
func handleSourceTypeFieldsFrag(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := discoverHardware()
		q := r.URL.Query()
		data := sourceTypeFieldsData{
			Type:          q.Get("type"),
			Interface:     q.Get("interface"),
			Port:          q.Get("port"),
			URL:           q.Get("url"),
			Topic:         q.Get("topic"),
			HeadersText:   q.Get("headers"),
			FilePath:      q.Get("file_path"),
			Address:       q.Get("address"),
			Format:        q.Get("format"),
			CANInterfaces: can,
			SerialPorts:   serial,
		}
		renderFragment(w, log, "source-type-fields", data)
	}
}

// handleSourceCreate serves POST /sources.
func handleSourceCreate(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeSource(w, r, svc, log, true, "")
	}
}

// handleSourceUpdate serves POST /sources/{id}.
func handleSourceUpdate(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeSource(w, r, svc, log, false, r.PathValue("id"))
	}
}

// writeSource backs both handleSourceCreate (isCreate=true, pathID="") and
// handleSourceUpdate (isCreate=false, pathID from the URL). pathID, not
// whatever the form's own "id" field carries, is authoritative for
// updates: frag_source_form.html renders the id input disabled (so
// browsers never submit it) alongside a hidden input that does carry it,
// but trusting the URL path directly is simpler and can't be spoofed by
// tampering with that hidden field.
func writeSource(w http.ResponseWriter, r *http.Request, svc *config.Service, log *slog.Logger, isCreate bool, pathID string) {
	can, serial := discoverHardware()
	if err := r.ParseForm(); err != nil {
		// Malformed request body itself (not just a malformed field's
		// value): never a bare 500 for a user input problem.
		view := blankSourceFormView(can, serial)
		view.IsEdit, view.ID = !isCreate, pathID
		view.Alert = &alertData{Kind: "error", Message: "invalid form submission: " + err.Error()}
		renderFragment(w, log, "source-form", view)
		return
	}
	view := sourceFormViewFromRequest(r, !isCreate, can, serial)
	if !isCreate {
		view.ID = pathID
	} else if view.ID == "" {
		id, err := generatedEntityID()
		if err != nil {
			log.Error("ui: generate source id failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		view.ID = id
	}
	v, err := view.toModel()
	if err != nil {
		view.Alert = &alertData{Kind: "error", Message: err.Error()}
		renderFragment(w, log, "source-form", view)
		return
	}
	if err := svc.PutSource(r.Context(), v, isCreate); err != nil {
		if !isUserFacingWriteErr(err) {
			log.Error("ui: put source failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		view.Alert = &alertData{Kind: "error", Message: entityWriteErrorMessage(err, "source", v.ID)}
		renderFragment(w, log, "source-form", view)
		return
	}
	if isCreate {
		flashRedirect(w, fmt.Sprintf("Source %q created", v.ID), "/dashboard")
		return
	}
	if view.InDialog {
		w.Header().Set("HX-Refresh", "true")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	sources, err := svc.ListSources(r.Context())
	if err != nil {
		log.Error("ui: list sources failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	data := sourceTableData{
		Sources: sourceRows(sources, svc.Statuses()),
		Alert:   &alertData{Kind: "success", Message: fmt.Sprintf("source %q saved", v.ID)},
	}
	writeFragmentSuccess(w, log, "source-panel-oob", "source-form-container", data)
}

// isUserFacingWriteErr reports whether err from a Service PutXxx/DeleteXxx
// call is one of the "expected" outcomes (bad input, id conflict, unknown
// id) that should be shown to the operator as a form/table alert, as
// opposed to a store/IO failure that should be logged and answered with a
// sanitized 500 — the same split internal/api/entities.go's mapServiceErr
// makes for the HTTP API.
func isUserFacingWriteErr(err error) bool {
	var ve *config.ValidationError
	return errors.As(err, &ve) || errors.Is(err, config.ErrExists) || errors.Is(err, config.ErrNotFound)
}

// entityWriteErrorMessage renders a PutSource/PutSink/PutConnector error
// into user-facing text. Only errors isUserFacingWriteErr accepts reach
// here. kind is "source", "sink", or "connector", used solely to word
// ErrExists/ErrNotFound.
func entityWriteErrorMessage(err error, kind, id string) string {
	var ve *config.ValidationError
	switch {
	case errors.As(err, &ve):
		return ve.Msg
	case errors.Is(err, config.ErrExists):
		return fmt.Sprintf("%s %q already exists", kind, id)
	case errors.Is(err, config.ErrNotFound):
		return fmt.Sprintf("%s %q not found", kind, id)
	default:
		return err.Error()
	}
}

// handleSourceDelete serves POST /sources/{id}/delete.
func handleSourceDelete(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var alert *alertData
		switch err := svc.DeleteSource(r.Context(), id); {
		case err == nil:
			alert = &alertData{Kind: "success", Message: fmt.Sprintf("source %q deleted", id)}
		case errors.Is(err, config.ErrInUse):
			names, lerr := referencingConnectorNames(r, svc, "source", id)
			if lerr != nil {
				log.Error("ui: list connectors failed", "err", lerr)
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}
			alert = &alertData{Kind: "error", Message: fmt.Sprintf(
				"source %q is still referenced by connector(s) %s; delete or repoint them first",
				id, strings.Join(names, ", "))}
		case errors.Is(err, config.ErrNotFound):
			alert = &alertData{Kind: "error", Message: fmt.Sprintf("source %q not found", id)}
		default:
			log.Error("ui: delete source failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if writeOverviewDeleteResult(w, r, log, "/sources", alert) {
			return
		}
		sources, err := svc.ListSources(r.Context())
		if err != nil {
			log.Error("ui: list sources failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		data := sourceTableData{Sources: sourceRows(sources, svc.Statuses()), Alert: alert}
		renderFragment(w, log, "source-panel", data)
	}
}

// --- Sinks ---
//
// Exactly parallel to the Sources section above; see its comments for
// rationale not repeated here. The only structural differences: sinks have
// tcp/tcp_gateway (Address), file (FilePath/Format/MaxFileBytes/MaxFiles),
// mqtt (URL/Topic), http_post (URL/Headers/BatchSize/RequestTimeout/Gzip), and
// null (no endpoint fields) sink types; their http_sse/http_ws type carries
// a Path rather than a URL+Headers pair.

// sinkRow is one row of the sinks table (frag_sink_table.html):
// model.Sink plus its live supervisor state and its type-specific Detail
// value (see sinkDetail).
type sinkRow struct {
	model.Sink
	State  string
	Detail string
}

func sinkRows(sinks []model.Sink, statuses []supervisor.Status) []sinkRow {
	rows := make([]sinkRow, len(sinks))
	for i, s := range sinks {
		rows[i] = sinkRow{Sink: s, State: componentState(statuses, "sink", s.ID, s.Enabled), Detail: sinkDetail(s)}
	}
	return rows
}

// sinkDetail renders the sinks table's Detail column: the one field that
// actually says WHERE a sink writes to (or reads a push from), matching
// whichever field frag_sink_type_fields.html shows as that sink's primary
// input for its type — Interface for socketcan, Port for usbcan, Path for
// http_sse/http_ws, Address for tcp, FilePath for file, broker/topic for
// mqtt, URL for http_post, or its intentional-discard behavior for null.
func sinkDetail(s model.Sink) string {
	switch s.Type {
	case model.SinkSocketCAN:
		return s.Interface
	case model.SinkUSBCAN:
		return s.Port
	case model.SinkHTTPSSE, model.SinkHTTPWS:
		return s.Path
	case model.SinkHTTPPost:
		return s.URL
	case model.SinkTCP:
		return s.Address
	case model.SinkFile:
		return s.FilePath
	case model.SinkMQTT:
		return mqttDetail(s.URL, s.Topic)
	case model.SinkPostgres:
		return s.EffectivePostgresTable()
	case model.SinkTCPGateway:
		return s.Address
	case model.SinkNull:
		return "Accepts and discards events"
	default:
		return ""
	}
}

func mqttDetail(broker, topic string) string {
	if broker == "" {
		return topic
	}
	if topic == "" {
		return broker
	}
	return broker + " / " + topic
}

// sinkTableData is frag_sink_table.html's "sink-panel"/"sink-panel-oob"
// data.
type sinkTableData struct {
	Sinks []sinkRow
	Alert *alertData
}

// sinksPageData is templates/sinks.html's data.
type sinksPageData struct {
	pageData
	sinkTableData
	SinkForm *sinkFormViewData
}

// sinkTypeFieldsData is frag_sink_type_fields.html's data. URL/Topic back
// mqtt sinks. FilePath/Format/
// MaxFileBytes/MaxFiles back the file type's fields (model.Sink's
// file_path/format/max_file_bytes/max_files); MaxFileBytes and MaxFiles are
// plain strings for the same empty-string-means-unset reason
// connectorFormViewData's MaxMessages/MaxBytes are (see formatOptionalInt64/
// parseOptionalInt64) — 0/blank means "use the file sink's built-in
// default". DefaultMaxFileBytes/DefaultMaxFiles carry those defaults
// (model.DefaultMaxFileBytes/DefaultMaxFiles, rendered as decimal strings by
// fileSinkDefaults) purely for the template to show as input placeholders,
// so the actual numbers live in the model package, not hardcoded here or in
// the template.
type sinkTypeFieldsData struct {
	Type                     string
	Interface                string
	Port                     string
	Path                     string
	Address                  string
	URL                      string
	Topic                    string
	HeadersText              string
	BatchSize                string
	RequestTimeout           string
	Gzip                     bool
	DefaultHTTPPostBatchSize string
	MaxHTTPPostBatchSize     string
	DefaultHTTPPostTimeout   string
	FilePath                 string
	Format                   string
	MaxFileBytes             string
	MaxFiles                 string
	DefaultMaxFileBytes      string
	DefaultMaxFiles          string
	Table                    string
	AutoCreateTable          bool
	TimescaleDB              bool
	WriteTimeout             string
	DefaultPostgresTable     string
	DefaultPostgresBatchSize string
	MaxPostgresBatchSize     string
	DefaultPostgresTimeout   string
	PostgresDDL              string
	CANInterfaces            []string
	SerialPorts              []string
}

// fileSinkDefaults returns model.DefaultMaxFileBytes/DefaultMaxFiles as
// plain decimal strings, for sinkTypeFieldsData.DefaultMaxFileBytes/
// DefaultMaxFiles — every construction site below sets these two fields
// from here rather than a literal, so the placeholders shown on the file
// sink's max_file_bytes/max_files inputs can't drift from the model
// package's actual defaults.
func fileSinkDefaults() (maxFileBytes, maxFiles string) {
	return strconv.FormatInt(model.DefaultMaxFileBytes, 10), strconv.Itoa(model.DefaultMaxFiles)
}

func httpPostSinkDefaults() (batchSize, maxBatchSize, requestTimeout string) {
	return strconv.Itoa(model.DefaultHTTPPostBatchSize), strconv.Itoa(model.MaxHTTPPostBatchSize),
		time.Duration(model.DefaultHTTPPostRequestTimeout).String()
}

func postgresSinkDefaults() (table, batchSize, maxBatchSize, writeTimeout string) {
	return model.DefaultPostgresTable, strconv.Itoa(model.DefaultPostgresBatchSize),
		strconv.Itoa(model.MaxPostgresBatchSize), time.Duration(model.DefaultPostgresWriteTimeout).String()
}

func postgresDDLForForm(table string, timescaleDB bool) string {
	if strings.TrimSpace(table) == "" {
		table = model.DefaultPostgresTable
	}
	probe := model.Sink{ID: "ddl", Type: model.SinkPostgres, URL: "postgres://localhost/beacon", Table: table}
	if err := probe.Validate(); err != nil {
		return "-- Enter a valid table name to generate the DDL."
	}
	return sinkruntime.PostgresDDL(table, timescaleDB)
}

// sinkFormViewData is frag_sink_form.html's data.
type sinkFormViewData struct {
	IsEdit     bool
	InDialog   bool
	ID         string
	Name       string
	Enabled    bool
	TypeFields sinkTypeFieldsData
	Alert      *alertData
}

func sinkFormViewFromModel(v model.Sink, can, serial []string) sinkFormViewData {
	defMaxFileBytes, defMaxFiles := fileSinkDefaults()
	defBatchSize, maxBatchSize, defRequestTimeout := httpPostSinkDefaults()
	defPostgresTable, defPostgresBatch, maxPostgresBatch, defPostgresTimeout := postgresSinkDefaults()
	return sinkFormViewData{
		IsEdit:  true,
		ID:      v.ID,
		Name:    v.Name,
		Enabled: v.Enabled,
		TypeFields: sinkTypeFieldsData{
			Type:                     string(v.Type),
			Interface:                v.Interface,
			Port:                     v.Port,
			Path:                     v.Path,
			Address:                  v.Address,
			URL:                      v.URL,
			Topic:                    v.Topic,
			HeadersText:              formatHeaders(v.Headers),
			BatchSize:                formatOptionalInt(v.BatchSize),
			RequestTimeout:           formatMaxAge(v.RequestTimeout),
			Gzip:                     v.Gzip,
			DefaultHTTPPostBatchSize: defBatchSize,
			MaxHTTPPostBatchSize:     maxBatchSize,
			DefaultHTTPPostTimeout:   defRequestTimeout,
			FilePath:                 v.FilePath,
			Format:                   v.Format,
			MaxFileBytes:             formatOptionalInt64(v.MaxFileBytes),
			MaxFiles:                 formatOptionalInt(v.MaxFiles),
			DefaultMaxFileBytes:      defMaxFileBytes,
			DefaultMaxFiles:          defMaxFiles,
			Table:                    v.Table,
			AutoCreateTable:          v.AutoCreateTable,
			TimescaleDB:              v.TimescaleDB,
			WriteTimeout:             formatMaxAge(v.WriteTimeout),
			DefaultPostgresTable:     defPostgresTable,
			DefaultPostgresBatchSize: defPostgresBatch,
			MaxPostgresBatchSize:     maxPostgresBatch,
			DefaultPostgresTimeout:   defPostgresTimeout,
			PostgresDDL:              postgresDDLForForm(v.Table, v.TimescaleDB),
			CANInterfaces:            can,
			SerialPorts:              serial,
		},
	}
}

func blankSinkFormView(can, serial []string) sinkFormViewData {
	defMaxFileBytes, defMaxFiles := fileSinkDefaults()
	defBatchSize, maxBatchSize, defRequestTimeout := httpPostSinkDefaults()
	defPostgresTable, defPostgresBatch, maxPostgresBatch, defPostgresTimeout := postgresSinkDefaults()
	return sinkFormViewData{
		Enabled: true,
		TypeFields: sinkTypeFieldsData{
			Type:                     string(model.SinkSocketCAN),
			DefaultHTTPPostBatchSize: defBatchSize,
			MaxHTTPPostBatchSize:     maxBatchSize,
			DefaultHTTPPostTimeout:   defRequestTimeout,
			DefaultMaxFileBytes:      defMaxFileBytes,
			DefaultMaxFiles:          defMaxFiles,
			AutoCreateTable:          true,
			DefaultPostgresTable:     defPostgresTable,
			DefaultPostgresBatchSize: defPostgresBatch,
			MaxPostgresBatchSize:     maxPostgresBatch,
			DefaultPostgresTimeout:   defPostgresTimeout,
			PostgresDDL:              postgresDDLForForm("", false),
			CANInterfaces:            can,
			SerialPorts:              serial,
		},
	}
}

func sinkFormViewFromRequest(r *http.Request, isEdit bool, can, serial []string) sinkFormViewData {
	defMaxFileBytes, defMaxFiles := fileSinkDefaults()
	defBatchSize, maxBatchSize, defRequestTimeout := httpPostSinkDefaults()
	defPostgresTable, defPostgresBatch, maxPostgresBatch, defPostgresTimeout := postgresSinkDefaults()
	return sinkFormViewData{
		IsEdit:   isEdit,
		InDialog: r.PostFormValue("dialog") != "",
		ID:       r.PostFormValue("id"),
		Name:     r.PostFormValue("name"),
		Enabled:  r.PostFormValue("enabled") != "",
		TypeFields: sinkTypeFieldsData{
			Type:                     r.PostFormValue("type"),
			Interface:                r.PostFormValue("interface"),
			Port:                     r.PostFormValue("port"),
			Path:                     r.PostFormValue("path"),
			Address:                  r.PostFormValue("address"),
			URL:                      r.PostFormValue("url"),
			Topic:                    r.PostFormValue("topic"),
			HeadersText:              r.PostFormValue("headers"),
			BatchSize:                r.PostFormValue("batch_size"),
			RequestTimeout:           r.PostFormValue("request_timeout"),
			Gzip:                     r.PostFormValue("gzip") != "",
			DefaultHTTPPostBatchSize: defBatchSize,
			MaxHTTPPostBatchSize:     maxBatchSize,
			DefaultHTTPPostTimeout:   defRequestTimeout,
			FilePath:                 r.PostFormValue("file_path"),
			Format:                   r.PostFormValue("format"),
			MaxFileBytes:             r.PostFormValue("max_file_bytes"),
			MaxFiles:                 r.PostFormValue("max_files"),
			DefaultMaxFileBytes:      defMaxFileBytes,
			DefaultMaxFiles:          defMaxFiles,
			Table:                    r.PostFormValue("table"),
			AutoCreateTable:          r.PostFormValue("auto_create_table") != "",
			TimescaleDB:              r.PostFormValue("timescaledb") != "",
			WriteTimeout:             r.PostFormValue("write_timeout"),
			DefaultPostgresTable:     defPostgresTable,
			DefaultPostgresBatchSize: defPostgresBatch,
			MaxPostgresBatchSize:     maxPostgresBatch,
			DefaultPostgresTimeout:   defPostgresTimeout,
			PostgresDDL: postgresDDLForForm(
				r.PostFormValue("table"), r.PostFormValue("timescaledb") != ""),
			CANInterfaces: can,
			SerialPorts:   serial,
		},
	}
}

// toModel converts a submitted form view into a model.Sink. HTTP POST
// headers, batch size, request timeout, and gzip setting plus the file sink's two numeric
// limits are parsed here so malformed form text can render as an inline
// validation alert. Structural rules remain centralized in model.Sink.
func (f sinkFormViewData) toModel() (model.Sink, error) {
	headers, err := parseHeaders(f.TypeFields.HeadersText)
	if err != nil {
		return model.Sink{}, err
	}
	batchSize, err := parseOptionalInt(f.TypeFields.BatchSize)
	if err != nil {
		return model.Sink{}, fmt.Errorf("batch_size: %w", err)
	}
	requestTimeout, err := parseRequestTimeout(f.TypeFields.RequestTimeout)
	if err != nil {
		return model.Sink{}, err
	}
	writeTimeout, err := parseWriteTimeout(f.TypeFields.WriteTimeout)
	if err != nil {
		return model.Sink{}, err
	}
	maxFileBytes, err := parseOptionalInt64(f.TypeFields.MaxFileBytes)
	if err != nil {
		return model.Sink{}, fmt.Errorf("max_file_bytes: %w", err)
	}
	maxFiles, err := parseOptionalInt(f.TypeFields.MaxFiles)
	if err != nil {
		return model.Sink{}, fmt.Errorf("max_files: %w", err)
	}
	return model.Sink{
		ID:              f.ID,
		Name:            f.Name,
		Type:            model.SinkType(f.TypeFields.Type),
		Enabled:         f.Enabled,
		Interface:       f.TypeFields.Interface,
		Port:            f.TypeFields.Port,
		Path:            f.TypeFields.Path,
		Address:         f.TypeFields.Address,
		URL:             f.TypeFields.URL,
		Topic:           f.TypeFields.Topic,
		Headers:         headers,
		BatchSize:       batchSize,
		RequestTimeout:  requestTimeout,
		Gzip:            f.TypeFields.Gzip,
		FilePath:        f.TypeFields.FilePath,
		Format:          f.TypeFields.Format,
		MaxFileBytes:    maxFileBytes,
		MaxFiles:        maxFiles,
		Table:           f.TypeFields.Table,
		AutoCreateTable: f.TypeFields.AutoCreateTable,
		TimescaleDB:     f.TypeFields.TimescaleDB,
		WriteTimeout:    writeTimeout,
	}, nil
}

func renderSinksPage(w http.ResponseWriter, r *http.Request, svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger, form *sinkFormViewData) {
	sinks, err := svc.ListSinks(r.Context())
	if err != nil {
		log.Error("ui: list sinks failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	page := newPageData("Sinks", version, "sinks")
	if form != nil {
		current := "Add sink"
		if form.IsEdit {
			current = "Edit " + form.ID
		}
		page = page.withBreadcrumbs(
			breadcrumbItem{Label: "Sinks", Href: "/sinks"},
			breadcrumbItem{Label: current},
		)
	}
	data := sinksPageData{
		pageData:      page,
		sinkTableData: sinkTableData{Sinks: sinkRows(sinks, statuses())},
		SinkForm:      form,
	}
	renderPage(w, log, "sinks", data)
}

// handleSinksPage serves GET /sinks: the full list page.
func handleSinksPage(svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		renderSinksPage(w, r, svc, statuses, version, log, nil)
	}
}

// handleSinkNewPage serves GET /sinks/new as a modal fragment for htmx
// callers and as a full-page progressive-enhancement fallback otherwise.
func handleSinkNewPage(svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := discoverHardware()
		form := blankSinkFormView(can, serial)
		id, err := generatedEntityID()
		if err != nil {
			log.Error("ui: generate sink id failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		form.ID = id
		if isHTMXRequest(r) {
			form.InDialog = true
			renderFragment(w, log, "sink-create-dialog", form)
			return
		}
		renderSinksPage(w, r, svc, statuses, version, log, &form)
	}
}

// handleSinkEditPage serves GET /sinks/{id}/edit as a modal fragment for
// htmx callers and as a full-page progressive-enhancement fallback otherwise.
func handleSinkEditPage(svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := discoverHardware()
		v, err := svc.GetSink(r.Context(), r.PathValue("id"))
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get sink failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		form := sinkFormViewFromModel(v, can, serial)
		if isHTMXRequest(r) {
			form.InDialog = true
			renderFragment(w, log, "sink-create-dialog", form)
			return
		}
		renderSinksPage(w, r, svc, statuses, version, log, &form)
	}
}

// handleSinkTypeFieldsFrag serves GET /frag/sink-type-fields. The same
// type-switch caveat as handleSourceTypeFieldsFrag applies: a previously
// selected type's field values are discarded on switch, not carried over.
func handleSinkTypeFieldsFrag(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := discoverHardware()
		defMaxFileBytes, defMaxFiles := fileSinkDefaults()
		defBatchSize, maxBatchSize, defRequestTimeout := httpPostSinkDefaults()
		defPostgresTable, defPostgresBatch, maxPostgresBatch, defPostgresTimeout := postgresSinkDefaults()
		q := r.URL.Query()
		autoCreateTable := q.Get("auto_create_table") != ""
		if q.Get("type") == string(model.SinkPostgres) && !q.Has("auto_create_table") {
			autoCreateTable = true
		}
		data := sinkTypeFieldsData{
			Type:                     q.Get("type"),
			Interface:                q.Get("interface"),
			Port:                     q.Get("port"),
			Path:                     q.Get("path"),
			Address:                  q.Get("address"),
			URL:                      q.Get("url"),
			Topic:                    q.Get("topic"),
			HeadersText:              q.Get("headers"),
			BatchSize:                q.Get("batch_size"),
			RequestTimeout:           q.Get("request_timeout"),
			Gzip:                     q.Get("gzip") != "",
			DefaultHTTPPostBatchSize: defBatchSize,
			MaxHTTPPostBatchSize:     maxBatchSize,
			DefaultHTTPPostTimeout:   defRequestTimeout,
			FilePath:                 q.Get("file_path"),
			Format:                   q.Get("format"),
			MaxFileBytes:             q.Get("max_file_bytes"),
			MaxFiles:                 q.Get("max_files"),
			DefaultMaxFileBytes:      defMaxFileBytes,
			DefaultMaxFiles:          defMaxFiles,
			Table:                    q.Get("table"),
			AutoCreateTable:          autoCreateTable,
			TimescaleDB:              q.Get("timescaledb") != "",
			WriteTimeout:             q.Get("write_timeout"),
			DefaultPostgresTable:     defPostgresTable,
			DefaultPostgresBatchSize: defPostgresBatch,
			MaxPostgresBatchSize:     maxPostgresBatch,
			DefaultPostgresTimeout:   defPostgresTimeout,
			PostgresDDL:              postgresDDLForForm(q.Get("table"), q.Get("timescaledb") != ""),
			CANInterfaces:            can,
			SerialPorts:              serial,
		}
		renderFragment(w, log, "sink-type-fields", data)
	}
}

func handleSinkPostgresDDLFrag(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		data := sinkTypeFieldsData{
			AutoCreateTable: q.Get("auto_create_table") != "",
			TimescaleDB:     q.Get("timescaledb") != "",
			PostgresDDL:     postgresDDLForForm(q.Get("table"), q.Get("timescaledb") != ""),
		}
		renderFragment(w, log, "sink-postgres-ddl", data)
	}
}

// handleSinkCreate serves POST /sinks.
func handleSinkCreate(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeSink(w, r, svc, log, true, "")
	}
}

// handleSinkUpdate serves POST /sinks/{id}.
func handleSinkUpdate(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeSink(w, r, svc, log, false, r.PathValue("id"))
	}
}

// writeSink mirrors writeSource; see its comments for rationale.
func writeSink(w http.ResponseWriter, r *http.Request, svc *config.Service, log *slog.Logger, isCreate bool, pathID string) {
	can, serial := discoverHardware()
	if err := r.ParseForm(); err != nil {
		view := blankSinkFormView(can, serial)
		view.IsEdit, view.ID = !isCreate, pathID
		view.Alert = &alertData{Kind: "error", Message: "invalid form submission: " + err.Error()}
		renderFragment(w, log, "sink-form", view)
		return
	}
	view := sinkFormViewFromRequest(r, !isCreate, can, serial)
	if !isCreate {
		view.ID = pathID
	} else if view.ID == "" {
		id, err := generatedEntityID()
		if err != nil {
			log.Error("ui: generate sink id failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		view.ID = id
	}
	v, err := view.toModel()
	if err != nil {
		view.Alert = &alertData{Kind: "error", Message: err.Error()}
		renderFragment(w, log, "sink-form", view)
		return
	}
	if err := svc.PutSink(r.Context(), v, isCreate); err != nil {
		if !isUserFacingWriteErr(err) {
			log.Error("ui: put sink failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		view.Alert = &alertData{Kind: "error", Message: entityWriteErrorMessage(err, "sink", v.ID)}
		renderFragment(w, log, "sink-form", view)
		return
	}
	if isCreate {
		flashRedirect(w, fmt.Sprintf("Sink %q created", v.ID), "/dashboard")
		return
	}
	if view.InDialog {
		w.Header().Set("HX-Refresh", "true")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	sinks, err := svc.ListSinks(r.Context())
	if err != nil {
		log.Error("ui: list sinks failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	data := sinkTableData{
		Sinks: sinkRows(sinks, svc.Statuses()),
		Alert: &alertData{Kind: "success", Message: fmt.Sprintf("sink %q saved", v.ID)},
	}
	writeFragmentSuccess(w, log, "sink-panel-oob", "sink-form-container", data)
}

// handleSinkDelete serves POST /sinks/{id}/delete.
func handleSinkDelete(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var alert *alertData
		switch err := svc.DeleteSink(r.Context(), id); {
		case err == nil:
			alert = &alertData{Kind: "success", Message: fmt.Sprintf("sink %q deleted", id)}
		case errors.Is(err, config.ErrInUse):
			names, lerr := referencingConnectorNames(r, svc, "sink", id)
			if lerr != nil {
				log.Error("ui: list connectors failed", "err", lerr)
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}
			alert = &alertData{Kind: "error", Message: fmt.Sprintf(
				"sink %q is still referenced by connector(s) %s; delete or repoint them first",
				id, strings.Join(names, ", "))}
		case errors.Is(err, config.ErrNotFound):
			alert = &alertData{Kind: "error", Message: fmt.Sprintf("sink %q not found", id)}
		default:
			log.Error("ui: delete sink failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if writeOverviewDeleteResult(w, r, log, "/sinks", alert) {
			return
		}
		sinks, err := svc.ListSinks(r.Context())
		if err != nil {
			log.Error("ui: list sinks failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		data := sinkTableData{Sinks: sinkRows(sinks, svc.Statuses()), Alert: alert}
		renderFragment(w, log, "sink-panel", data)
	}
}

// --- Connectors ---
//
// Connectors follow the same form-view/toModel/handler shape as sources and
// sinks above, with three differences driven by model.Connector's shape
// (see forms.go's package doc comment): no type switch (source_id/sink_id
// are plain <select>s populated from svc.ListSources/ListSinks rather than
// a type-driven fieldset), a filters textarea (one CEL expression per line)
// with its own advisory live-validation endpoint, and a detail page with
// live stats read from reg (a *stats.Registry) instead of statuses.
// DeleteConnector has no ErrInUse case (config.Service: "nothing else
// references a connector"), so handleConnectorDelete's error switch is one
// case shorter than handleSourceDelete/handleSinkDelete's.

// connectorRow is one row of the connectors table
// (frag_connector_table.html): model.Connector plus its live stats
// snapshot, supervisor state, and source/sink NAMES (nameOrID — falls back
// to the raw id when the source/sink has no name set). reg.Snapshot returns a zero
// Snapshot for a connector it has never recorded (not yet started, or
// started but idle since boot) — so a row for a connector with no traffic
// yet renders zero queue depth/msg-per-second rather than needing a
// presence check here, matching the behavior contract's "zero row values
// when snapshot missing".
type connectorRow struct {
	model.Connector
	Snapshot   stats.Snapshot
	SourceName string
	SinkName   string
	State      string
}

// connectorRows builds connectorTableData's rows. sourceNames/sinkNames are
// id->Name lookup maps (see sourceNames/sinkNames above) built once by the
// caller from the same source/sink lists every connector-table-rendering
// handler already has to fetch for other reasons (the connector form's
// <select> options, or — for handlers that don't otherwise need them —
// fetched solely for this).
func connectorRows(connectors []model.Connector, reg *stats.Registry, statuses []supervisor.Status, srcNames, sinkNamesByID map[string]string) []connectorRow {
	rows := make([]connectorRow, len(connectors))
	for i, c := range connectors {
		snap, _ := reg.Snapshot(c.ID)
		rows[i] = connectorRow{
			Connector:  c,
			Snapshot:   snap,
			SourceName: nameOrID(srcNames, c.SourceID),
			SinkName:   nameOrID(sinkNamesByID, c.SinkID),
			State:      componentState(statuses, "connector", c.ID, c.Enabled),
		}
	}
	return rows
}

// connectorTableData is frag_connector_table.html's "connector-panel"/
// "connector-panel-oob" data.
type connectorTableData struct {
	Connectors []connectorRow
	Alert      *alertData
}

// connectorsPageData is templates/connectors.html's data.
type connectorsPageData struct {
	pageData
	connectorTableData
	ConnectorForm *connectorFormViewData
}

// connectorFormViewData is frag_connector_form.html's data — the
// connectors' analogue of sourceFormViewData/sinkFormViewData. Sources and
// Sinks carry the full list of configured sources/sinks so the form's
// source_id/sink_id <select>s can populate their <option>s and preserve the
// selected value across an edit or a validation-error re-render, the same
// way sourceTypeFieldsData carries CANInterfaces/SerialPorts for its
// <datalist>s. FiltersText/MaxMessages/MaxAge/MaxBytes are plain strings
// for the same reason sourceFormViewData's fields are: this struct doubles
// as the intermediate representation for both a stored model.Connector
// (opening the edit form) and a raw *http.Request (redisplaying a failed
// submission — see writeConnector).
type connectorFormViewData struct {
	IsEdit            bool
	InDialog          bool
	ID                string
	Name              string
	Enabled           bool
	SourceID          string
	SinkID            string
	Mode              string
	ForwardManagement bool
	FiltersText       string
	MaxMessages       string
	MaxAge            string
	MaxBytes          string
	Sources           []model.Source
	Sinks             []model.Sink
	Alert             *alertData
}

// parseFilters splits a filters textarea into one CEL expression per
// (non-blank, trimmed) line — the inverse of formatFilters. Unlike
// parseHeaders this never errors: an individual expression's syntax is
// checked later, either advisorily by handleValidateFiltersFrag or
// authoritatively by svc.PutConnector (config.Service.ValidateConfig CEL-
// compiles every connector's filters before persisting), not here.
func parseFilters(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

// formatFilters is parseFilters' inverse, rendering a filters slice back to
// one expression per textarea line so editing an existing connector
// round-trips it.
func formatFilters(filters []string) string {
	return strings.Join(filters, "\n")
}

// formatOptionalInt64 renders an optional authored value as its decimal
// string, or "" for 0. Connector count/byte limits instead use the effective
// helpers below so both independent safety defaults are visible in the editor.
func formatOptionalInt64(v int64) string {
	if v == 0 {
		return ""
	}
	return strconv.FormatInt(v, 10)
}

func formatMaxMessages(limits model.BufferLimits) string {
	return strconv.FormatInt(limits.ApplyDefaults().MaxMessages, 10)
}

func formatMaxBytes(limits model.BufferLimits) string {
	return strconv.FormatInt(limits.ApplyDefaults().MaxBytes, 10)
}

// parseOptionalInt64 is formatOptionalInt64's inverse: "" parses to 0
// (unset), matching the number input's empty→0 contract. A non-numeric
// value returns an error (surfaced as a validation alert, never a 500);
// config.Service separately rejects a negative value once toModel returns
// it (model.Connector.Validate: "buffer limits must not be negative").
func parseOptionalInt64(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a whole number", s)
	}
	return v, nil
}

// formatOptionalInt/parseOptionalInt are formatOptionalInt64/
// parseOptionalInt64's int-valued counterparts, for model.Sink.MaxFiles
// (an int, unlike the int64 buffer limits and MaxFileBytes) — same "" means
// 0/unset contract, delegated to the int64 versions rather than
// reimplemented.
func formatOptionalInt(v int) string {
	return formatOptionalInt64(int64(v))
}

func parseOptionalInt(s string) (int, error) {
	v, err := parseOptionalInt64(s)
	if err != nil {
		return 0, err
	}
	return int(v), nil
}

// formatMaxAge renders a buffer max-age as a Go duration string, or "" for
// 0 (unset) — the max_age text input's round-trip counterpart to
// parseMaxAge.
func formatMaxAge(d model.Duration) string {
	if d == 0 {
		return ""
	}
	return time.Duration(d).String()
}

// parseMaxAge parses the max_age text input's Go duration string ("24h",
// "90s", ...) into a model.Duration. "" parses to 0 (unset), matching the
// field's empty→0 contract; any other unparseable value returns an error
// that writeConnector surfaces as an inline validation alert with the
// operator's original input preserved, never a 500.
func parseMaxAge(s string) (model.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("max_age: invalid duration %q (expected a Go duration string like \"24h\")", s)
	}
	return model.Duration(d), nil
}

func parseRequestTimeout(s string) (model.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("request_timeout: invalid duration %q (expected a Go duration string like \"10s\")", s)
	}
	return model.Duration(d), nil
}

func parseWriteTimeout(s string) (model.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("write_timeout: invalid duration %q (expected a Go duration string like \"10s\")", s)
	}
	return model.Duration(d), nil
}

// connectorFormViewFromModel builds a connector-form view for the canonical
// edit page: every field pre-filled from the stored entity.
func connectorFormViewFromModel(v model.Connector, sources []model.Source, sinks []model.Sink) connectorFormViewData {
	return connectorFormViewData{
		IsEdit:            true,
		ID:                v.ID,
		Name:              v.Name,
		Enabled:           v.Enabled,
		SourceID:          v.SourceID,
		SinkID:            v.SinkID,
		Mode:              string(v.EffectiveMode()),
		ForwardManagement: v.ForwardManagement,
		FiltersText:       formatFilters(v.Filters),
		MaxMessages:       formatMaxMessages(v.Buffer),
		MaxAge:            formatMaxAge(v.Buffer.MaxAge),
		MaxBytes:          formatMaxBytes(v.Buffer),
		Sources:           sources,
		Sinks:             sinks,
	}
}

// blankConnectorFormView builds an enabled connector create view with the
// source/sink lists its selects need and semantic bridge mode selected.
func blankConnectorFormView(sources []model.Source, sinks []model.Sink) connectorFormViewData {
	return connectorFormViewData{
		Enabled:     true,
		Sources:     sources,
		Sinks:       sinks,
		Mode:        string(model.BridgeSemantic),
		MaxMessages: strconv.FormatInt(model.DefaultMaxMessages, 10),
		MaxBytes:    strconv.FormatInt(model.DefaultBufferMaxBytes, 10),
	}
}

// connectorFormViewFromRequest rebuilds a connector-form view from a
// POSTed form's raw values (r.ParseForm must already have been called) —
// used to redisplay a submission that failed validation, preserving
// exactly what the operator typed/selected rather than falling back to
// blank/stored values.
func connectorFormViewFromRequest(r *http.Request, isEdit bool, sources []model.Source, sinks []model.Sink) connectorFormViewData {
	return connectorFormViewData{
		IsEdit:            isEdit,
		InDialog:          r.PostFormValue("dialog") != "",
		ID:                r.PostFormValue("id"),
		Name:              r.PostFormValue("name"),
		Enabled:           r.PostFormValue("enabled") != "",
		SourceID:          r.PostFormValue("source_id"),
		SinkID:            r.PostFormValue("sink_id"),
		Mode:              r.PostFormValue("mode"),
		ForwardManagement: r.PostFormValue("forward_management") != "",
		FiltersText:       r.PostFormValue("filters"),
		MaxMessages:       r.PostFormValue("max_messages"),
		MaxAge:            r.PostFormValue("max_age"),
		MaxBytes:          r.PostFormValue("max_bytes"),
		Sources:           sources,
		Sinks:             sinks,
	}
}

// toModel converts a submitted form view into a model.Connector ready for
// svc.PutConnector. The only failure modes here are the three buffer
// fields' parses (parseOptionalInt64 x2, parseMaxAge) — everything else
// (missing/unknown source_id or sink_id, a CEL syntax error in Filters,
// ...) is left for config.Service's own structural + CEL validation to
// catch and report as a *config.ValidationError, so this package doesn't
// duplicate those rules.
func (f connectorFormViewData) toModel() (model.Connector, error) {
	maxMessages, err := parseOptionalInt64(f.MaxMessages)
	if err != nil {
		return model.Connector{}, fmt.Errorf("max_messages: %w", err)
	}
	maxAge, err := parseMaxAge(f.MaxAge)
	if err != nil {
		return model.Connector{}, err
	}
	maxBytes, err := parseOptionalInt64(f.MaxBytes)
	if err != nil {
		return model.Connector{}, fmt.Errorf("max_bytes: %w", err)
	}
	return model.Connector{
		ID:                f.ID,
		Name:              f.Name,
		Enabled:           f.Enabled,
		SourceID:          f.SourceID,
		SinkID:            f.SinkID,
		Mode:              model.BridgeMode(f.Mode),
		ForwardManagement: f.ForwardManagement,
		Filters:           parseFilters(f.FiltersText),
		Buffer: model.BufferLimits{
			MaxMessages: maxMessages,
			MaxAge:      maxAge,
			MaxBytes:    maxBytes,
		},
	}, nil
}

// listSourcesAndSinks fetches the two lists every connector-form render
// needs to populate its source_id/sink_id <select>s.
func listSourcesAndSinks(ctx context.Context, svc *config.Service) ([]model.Source, []model.Sink, error) {
	sources, err := svc.ListSources(ctx)
	if err != nil {
		return nil, nil, err
	}
	sinks, err := svc.ListSinks(ctx)
	if err != nil {
		return nil, nil, err
	}
	return sources, sinks, nil
}

func renderConnectorsPage(w http.ResponseWriter, r *http.Request, svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger, form *connectorFormViewData) {
	connectors, err := svc.ListConnectors(r.Context())
	if err != nil {
		log.Error("ui: list connectors failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	sources, sinks, err := listSourcesAndSinks(r.Context(), svc)
	if err != nil {
		log.Error("ui: list sources/sinks failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	page := newPageData("Connectors", version, "connectors")
	if form != nil {
		current := "Add connector"
		if form.IsEdit {
			current = "Edit " + form.ID
		}
		page = page.withBreadcrumbs(
			breadcrumbItem{Label: "Connectors", Href: "/connectors"},
			breadcrumbItem{Label: current},
		)
	}
	data := connectorsPageData{
		pageData:           page,
		connectorTableData: connectorTableData{Connectors: connectorRows(connectors, reg, statuses(), sourceNames(sources), sinkNames(sinks))},
		ConnectorForm:      form,
	}
	renderPage(w, log, "connectors", data)
}

// handleConnectorsPage serves GET /connectors: the full list page.
func handleConnectorsPage(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		renderConnectorsPage(w, r, svc, reg, statuses, version, log, nil)
	}
}

// handleConnectorNewPage serves GET /connectors/new as a modal fragment for
// htmx callers and as a full-page progressive-enhancement fallback otherwise.
func handleConnectorNewPage(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sources, sinks, err := listSourcesAndSinks(r.Context(), svc)
		if err != nil {
			log.Error("ui: list sources/sinks failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		form := blankConnectorFormView(sources, sinks)
		id, err := generatedEntityID()
		if err != nil {
			log.Error("ui: generate connector id failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		form.ID = id
		if isHTMXRequest(r) {
			form.InDialog = true
			renderFragment(w, log, "connector-create-dialog", form)
			return
		}
		renderConnectorsPage(w, r, svc, reg, statuses, version, log, &form)
	}
}

// handleConnectorEditPage serves GET /connectors/{id}/edit as a modal
// fragment for htmx callers and as a full-page progressive-enhancement
// fallback otherwise.
func handleConnectorEditPage(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sources, sinks, err := listSourcesAndSinks(r.Context(), svc)
		if err != nil {
			log.Error("ui: list sources/sinks failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		v, err := svc.GetConnector(r.Context(), r.PathValue("id"))
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get connector failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		form := connectorFormViewFromModel(v, sources, sinks)
		if isHTMXRequest(r) {
			form.InDialog = true
			renderFragment(w, log, "connector-create-dialog", form)
			return
		}
		renderConnectorsPage(w, r, svc, reg, statuses, version, log, &form)
	}
}

// filterValidateData is frag_filter_validate.html's data for non-JSON callers
// of handleValidateFiltersFrag. The enhanced connector editor requests JSON
// diagnostics instead; either path is advisory, and svc.PutConnector always
// re-validates authoritatively on Save.
type filterValidateData struct {
	OK  bool
	Err string
}

// handleValidateFiltersFrag serves POST /frag/validate-filters. Requests
// accepting application/json receive source ranges for live editor
// underlines; other callers retain the original HTML status fragment.
func handleValidateFiltersFrag(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			if acceptsJSON(r) {
				http.Error(w, "invalid form submission", http.StatusBadRequest)
				return
			}
			renderFragment(w, log, "filter-validate", filterValidateData{Err: "invalid form submission: " + err.Error()})
			return
		}
		if acceptsJSON(r) {
			renderCELValidationJSON(w, log, r.PostFormValue("filters"))
			return
		}
		filters := parseFilters(r.PostFormValue("filters"))
		data := filterValidateData{}
		if err := svc.ValidateFilters(filters); err != nil {
			data.Err = err.Error()
		} else {
			data.OK = true
		}
		renderFragment(w, log, "filter-validate", data)
	}
}

// handleConnectorCreate serves POST /connectors.
func handleConnectorCreate(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeConnector(w, r, svc, reg, statuses, log, true, "")
	}
}

// handleConnectorUpdate serves POST /connectors/{id}.
func handleConnectorUpdate(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeConnector(w, r, svc, reg, statuses, log, false, r.PathValue("id"))
	}
}

// writeConnector backs both handleConnectorCreate and handleConnectorUpdate,
// mirroring writeSource/writeSink; see writeSource's comments for the
// pathID-is-authoritative rationale.
func writeConnector(w http.ResponseWriter, r *http.Request, svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger, isCreate bool, pathID string) {
	sources, sinks, err := listSourcesAndSinks(r.Context(), svc)
	if err != nil {
		log.Error("ui: list sources/sinks failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if err := r.ParseForm(); err != nil {
		view := blankConnectorFormView(sources, sinks)
		view.IsEdit, view.ID = !isCreate, pathID
		view.Alert = &alertData{Kind: "error", Message: "invalid form submission: " + err.Error()}
		renderFragment(w, log, "connector-form", view)
		return
	}
	view := connectorFormViewFromRequest(r, !isCreate, sources, sinks)
	if !isCreate {
		view.ID = pathID
	} else if view.ID == "" {
		id, err := generatedEntityID()
		if err != nil {
			log.Error("ui: generate connector id failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		view.ID = id
	}
	v, err := view.toModel()
	if err != nil {
		view.Alert = &alertData{Kind: "error", Message: err.Error()}
		renderFragment(w, log, "connector-form", view)
		return
	}
	if err := svc.PutConnector(r.Context(), v, isCreate); err != nil {
		if !isUserFacingWriteErr(err) {
			log.Error("ui: put connector failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		view.Alert = &alertData{Kind: "error", Message: entityWriteErrorMessage(err, "connector", v.ID)}
		renderFragment(w, log, "connector-form", view)
		return
	}
	if isCreate {
		flashRedirect(w, fmt.Sprintf("Connector %q created", v.ID), "/dashboard")
		return
	}
	if view.InDialog {
		w.Header().Set("HX-Refresh", "true")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	connectors, err := svc.ListConnectors(r.Context())
	if err != nil {
		log.Error("ui: list connectors failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	data := connectorTableData{
		Connectors: connectorRows(connectors, reg, statuses(), sourceNames(sources), sinkNames(sinks)),
		Alert:      &alertData{Kind: "success", Message: fmt.Sprintf("connector %q saved", v.ID)},
	}
	writeFragmentSuccess(w, log, "connector-panel-oob", "connector-form-container", data)
}

// handleConnectorDelete serves POST /connectors/{id}/delete. Unlike
// handleSourceDelete/handleSinkDelete there is no ErrInUse case:
// config.Service.DeleteConnector never returns it ("nothing else
// references a connector").
func handleConnectorDelete(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var alert *alertData
		switch err := svc.DeleteConnector(r.Context(), id); {
		case err == nil:
			alert = &alertData{Kind: "success", Message: fmt.Sprintf("connector %q deleted", id)}
		case errors.Is(err, config.ErrNotFound):
			alert = &alertData{Kind: "error", Message: fmt.Sprintf("connector %q not found", id)}
		default:
			log.Error("ui: delete connector failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if writeOverviewDeleteResult(w, r, log, "/connectors", alert) {
			return
		}
		connectors, err := svc.ListConnectors(r.Context())
		if err != nil {
			log.Error("ui: list connectors failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		sources, sinks, err := listSourcesAndSinks(r.Context(), svc)
		if err != nil {
			log.Error("ui: list sources/sinks failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		data := connectorTableData{Connectors: connectorRows(connectors, reg, statuses(), sourceNames(sources), sinkNames(sinks)), Alert: alert}
		renderFragment(w, log, "connector-panel", data)
	}
}

// connectorStatsData is frag_connector_stats.html's "connector-stats" data.
// Byte counts are pre-humanized in Go, avoiding a template FuncMap.
// ConnectorID supplies the stable polling target URL.
type connectorStatsData struct {
	ConnectorID       string
	Snapshot          stats.Snapshot
	BytesPerSecText   string
	QueueBytesText    string
	RetainedBytesText string
	SparkPoints       string
}

// handleConnectorStatsFrag serves GET /frag/connectors/{id}/stats, an
// hx-trigger="load, every 2s" polling target using hx-swap="outerHTML".
// A known connector that has never recorded (not yet started, or
// idle since boot) renders zero-valued tiles rather than a notice —
// reg.Snapshot's "ok" bool is deliberately ignored here for the same reason
// connectorRows ignores it.
//
// An unknown connector id (deleted from elsewhere while this detail page
// is still open and polling) does NOT 404: since the polling attributes
// live on the element this response replaces (outerHTML), a 404 htmx
// simply wouldn't swap, and the poll would keep firing against a connector
// that no longer exists. Instead this renders "connector-stats-deleted", a
// 200 notice fragment with no hx-get/hx-trigger of its own — once swapped
// in, the polling element is gone and the loop halts.
func handleConnectorStatsFrag(svc *config.Service, reg *stats.Registry, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if _, err := svc.GetConnector(r.Context(), id); err != nil {
			if errors.Is(err, config.ErrNotFound) {
				renderFragment(w, log, "connector-stats-deleted", nil)
				return
			}
			log.Error("ui: get connector failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		snap, _ := reg.Snapshot(id)
		data := connectorStatsData{
			ConnectorID:       id,
			Snapshot:          snap,
			BytesPerSecText:   humanizeBytes(snap.BytesPerSec, "/s"),
			QueueBytesText:    humanizeBytes(float64(snap.QueueBytes), ""),
			RetainedBytesText: humanizeBytes(float64(snap.RetainedBytes), ""),
			SparkPoints:       sparklinePoints(snap.DepthHistory),
		}
		renderFragment(w, log, "connector-stats", data)
	}
}

// humanizeBytes formats n (a byte count or byte rate) using B/KB/MB units
// (1024-based), two decimal places beyond whole bytes. suffix is appended
// after the unit — "/s" for a rate (frag_connector_stats.html's bytes/s
// tile), "" for a static count (its queue-bytes tile).
func humanizeBytes(n float64, suffix string) string {
	const step = 1024.0
	switch {
	case n < step:
		return fmt.Sprintf("%.0f B%s", n, suffix)
	case n < step*step:
		return fmt.Sprintf("%.2f KB%s", n/step, suffix)
	default:
		return fmt.Sprintf("%.2f MB%s", n/(step*step), suffix)
	}
}

// sparkWidth/sparkHeight are frag_connector_stats.html's sparkline
// <svg viewBox="0 0 120 32">, shared here so sparklinePoints' scaling
// always matches what the template declares.
const (
	sparkWidth  = 120.0
	sparkHeight = 32.0
)

// sparklinePoints turns history (stats.Snapshot.DepthHistory, oldest to
// newest) into the space-separated "x,y x,y ..." string
// frag_connector_stats.html's <polyline points="..."> renders directly —
// computed here rather than in the template so the template stays free of
// arithmetic. X is spread evenly across [0, sparkWidth]; Y is scaled to
// [0, sparkHeight] and flipped (SVG y grows downward, so a larger depth
// maps to a SMALLER y) so the line rises for a growing queue the way a
// normal chart would, not an inverted one.
//
// Two cases would otherwise divide by zero and are handled as a flat line
// instead of an error: a single-sample history (no second x to spread
// across) is rendered as a 2-point horizontal line at mid-height, and a
// multi-sample history whose values are all equal — including all
// zero — (max-min spread of 0) is rendered as a flat line across the full
// width rather than computing 0/0. An empty history (connector never
// SetQueue'd) returns "", which the template's <polyline> renders as an
// empty, harmless line.
func sparklinePoints(history []int64) string {
	if len(history) == 0 {
		return ""
	}
	if len(history) == 1 {
		return fmt.Sprintf("0.00,%.2f %.2f,%.2f", sparkHeight/2, sparkWidth, sparkHeight/2)
	}
	lo, hi := history[0], history[0]
	for _, v := range history[1:] {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}
	spread := hi - lo
	points := make([]string, len(history))
	for i, v := range history {
		x := sparkWidth * float64(i) / float64(len(history)-1)
		y := sparkHeight / 2
		if spread != 0 {
			frac := float64(v-lo) / float64(spread)
			y = sparkHeight - frac*sparkHeight
		}
		points[i] = fmt.Sprintf("%.2f,%.2f", x, y)
	}
	return strings.Join(points, " ")
}
