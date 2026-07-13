// forms.go implements the Sources, Sinks, and Connectors CRUD pages: the
// page/fragment data types templates/sources.html, sinks.html,
// connectors.html, connector_detail.html, and every frag_*.html render
// against, the model<->form conversions, and the HTTP handlers Handler
// (ui.go) wires up at /ui/sources, /ui/sinks, /ui/connectors, /ui/frag/*.
//
// Sources and sinks are handled by deliberately parallel, non-generic code
// (sourceX / sinkX pairs) rather than a shared generic implementation: the
// two entities' type-specific fields differ enough (source http_sse/
// http_ws carries URL+Headers; sink http_sse/http_ws carries Path, and
// sink alone has a tcp type carrying Address) that a generic abstraction
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
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/open-ships/beacon/internal/api"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
)

// --- Shared helpers ---

// alertData renders a dismissible daisyUI alert; Kind is "success" or
// "error" (anything else renders with the error styling, in
// frag_source_table.html/frag_sink_table.html's "*-panel-inner" template).
type alertData struct {
	Kind    string
	Message string
}

// stateFor returns the State of the status in statuses matching kind/id
// ("source"/"sink", per internal/supervisor.Status.Kind's literal values —
// see internal/supervisor/supervisor.go), or "unknown" if none is reported
// yet (e.g. a disabled entity the supervisor never started, or a request
// racing just after a config write's reconcile).
func stateFor(statuses []supervisor.Status, kind, id string) string {
	for _, s := range statuses {
		if s.Kind == kind && s.ID == id {
			return s.State
		}
	}
	return "unknown"
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
// model.Source plus its live supervisor state.
type sourceRow struct {
	model.Source
	State string
}

func sourceRows(sources []model.Source, statuses []supervisor.Status) []sourceRow {
	rows := make([]sourceRow, len(sources))
	for i, s := range sources {
		rows[i] = sourceRow{Source: s, State: stateFor(statuses, "source", s.ID)}
	}
	return rows
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
}

// sourceTypeFieldsData is frag_source_type_fields.html's data: which type
// is selected (decides which fields render), the current value of every
// type-specific field, and the discovered hardware lists for the
// socketcan/usbcan <datalist> suggestions (see api.DiscoverSystem).
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
	HeadersText   string
	CANInterfaces []string
	SerialPorts   []string
}

// sourceFormViewData is frag_source_form.html's data. IsEdit controls
// whether the id field renders disabled+hidden (edit) or editable
// (create), per the behavior contract. It doubles as the intermediate
// representation built from either a model.Source (opening the edit form)
// or a raw *http.Request (redisplaying a submission that failed
// validation — see writeSource), which is why fields are plain strings
// rather than typed model.SourceType etc.
type sourceFormViewData struct {
	IsEdit     bool
	ID         string
	Name       string
	Enabled    bool
	TypeFields sourceTypeFieldsData
	Alert      *alertData
}

// sourceFormViewFromModel builds a source-form view for the edit path (GET
// /ui/frag/source-form?id=...): every field pre-filled from the stored
// entity, headers rendered back through formatHeaders.
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
			HeadersText:   formatHeaders(v.Headers),
			CANInterfaces: can,
			SerialPorts:   serial,
		},
	}
}

// blankSourceFormView builds a source-form view for the create path (GET
// /ui/frag/source-form with no id): every field empty, Type defaulted to
// "socketcan" so the initial render already shows its matching field
// (interface) instead of no type-specific field at all — the type select's
// first <option> is socketcan too (see frag_source_form.html), so this
// keeps the dropdown and the fields it controls in sync before any
// hx-get-driven change event ever fires.
func blankSourceFormView(can, serial []string) sourceFormViewData {
	return sourceFormViewData{
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
		IsEdit:  isEdit,
		ID:      r.PostFormValue("id"),
		Name:    r.PostFormValue("name"),
		Enabled: r.PostFormValue("enabled") != "",
		TypeFields: sourceTypeFieldsData{
			Type:          r.PostFormValue("type"),
			Interface:     r.PostFormValue("interface"),
			Port:          r.PostFormValue("port"),
			URL:           r.PostFormValue("url"),
			HeadersText:   r.PostFormValue("headers"),
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
		Headers:   headers,
	}, nil
}

// handleSourcesPage serves GET /ui/sources: the full page, table populated
// from svc.ListSources + the reconciler's live statuses.
func handleSourcesPage(svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sources, err := svc.ListSources(r.Context())
		if err != nil {
			log.Error("ui: list sources failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		data := sourcesPageData{
			pageData:        newPageData("Sources", version, "sources"),
			sourceTableData: sourceTableData{Sources: sourceRows(sources, statuses())},
		}
		renderPage(w, log, "sources", data)
	}
}

// handleSourceFormFrag serves GET /ui/frag/source-form (blank, create
// mode) and GET /ui/frag/source-form?id=<id> (edit mode).
func handleSourceFormFrag(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := api.DiscoverSystem()
		id := r.URL.Query().Get("id")
		if id == "" {
			renderFragment(w, log, "source-form", blankSourceFormView(can, serial))
			return
		}
		v, err := svc.GetSource(r.Context(), id)
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.Error(w, "source not found", http.StatusNotFound)
				return
			}
			log.Error("ui: get source failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		renderFragment(w, log, "source-form", sourceFormViewFromModel(v, can, serial))
	}
}

// handleSourceTypeFieldsFrag serves GET /ui/frag/source-type-fields: the
// type select's hx-get target. hx-include="closest form" resends every
// field currently in the form as a query parameter, so the newly selected
// type's own field keeps its value if it happens to still be in the DOM —
// but a previously selected type's fields are NOT preserved across an
// A→B→A switch: B's render removed A's inputs from the DOM, so nothing
// resends them and A comes back blank (see sourceTypeFieldsData's doc
// comment for why that's deliberate).
func handleSourceTypeFieldsFrag(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := api.DiscoverSystem()
		q := r.URL.Query()
		data := sourceTypeFieldsData{
			Type:          q.Get("type"),
			Interface:     q.Get("interface"),
			Port:          q.Get("port"),
			URL:           q.Get("url"),
			HeadersText:   q.Get("headers"),
			CANInterfaces: can,
			SerialPorts:   serial,
		}
		renderFragment(w, log, "source-type-fields", data)
	}
}

// handleSourceCreate serves POST /ui/sources.
func handleSourceCreate(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeSource(w, r, svc, log, true, "")
	}
}

// handleSourceUpdate serves POST /ui/sources/{id}.
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
	can, serial := api.DiscoverSystem()
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

// handleSourceDelete serves POST /ui/sources/{id}/delete.
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
// a fifth type (tcp, carrying Address) and their http_sse/http_ws type
// carries a Path rather than a URL+Headers pair.

// sinkRow is one row of the sinks table (frag_sink_table.html):
// model.Sink plus its live supervisor state.
type sinkRow struct {
	model.Sink
	State string
}

func sinkRows(sinks []model.Sink, statuses []supervisor.Status) []sinkRow {
	rows := make([]sinkRow, len(sinks))
	for i, s := range sinks {
		rows[i] = sinkRow{Sink: s, State: stateFor(statuses, "sink", s.ID)}
	}
	return rows
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
}

// sinkTypeFieldsData is frag_sink_type_fields.html's data.
type sinkTypeFieldsData struct {
	Type          string
	Interface     string
	Port          string
	Path          string
	Address       string
	CANInterfaces []string
	SerialPorts   []string
}

// sinkFormViewData is frag_sink_form.html's data.
type sinkFormViewData struct {
	IsEdit     bool
	ID         string
	Name       string
	Enabled    bool
	TypeFields sinkTypeFieldsData
	Alert      *alertData
}

func sinkFormViewFromModel(v model.Sink, can, serial []string) sinkFormViewData {
	return sinkFormViewData{
		IsEdit:  true,
		ID:      v.ID,
		Name:    v.Name,
		Enabled: v.Enabled,
		TypeFields: sinkTypeFieldsData{
			Type:          string(v.Type),
			Interface:     v.Interface,
			Port:          v.Port,
			Path:          v.Path,
			Address:       v.Address,
			CANInterfaces: can,
			SerialPorts:   serial,
		},
	}
}

func blankSinkFormView(can, serial []string) sinkFormViewData {
	return sinkFormViewData{
		TypeFields: sinkTypeFieldsData{
			Type:          string(model.SinkSocketCAN),
			CANInterfaces: can,
			SerialPorts:   serial,
		},
	}
}

func sinkFormViewFromRequest(r *http.Request, isEdit bool, can, serial []string) sinkFormViewData {
	return sinkFormViewData{
		IsEdit:  isEdit,
		ID:      r.PostFormValue("id"),
		Name:    r.PostFormValue("name"),
		Enabled: r.PostFormValue("enabled") != "",
		TypeFields: sinkTypeFieldsData{
			Type:          r.PostFormValue("type"),
			Interface:     r.PostFormValue("interface"),
			Port:          r.PostFormValue("port"),
			Path:          r.PostFormValue("path"),
			Address:       r.PostFormValue("address"),
			CANInterfaces: can,
			SerialPorts:   serial,
		},
	}
}

// toModel converts a submitted form view into a model.Sink. Unlike
// sourceFormViewData.toModel, there is no parse step that can fail here
// (sinks carry no headers-style free-text field) — kept as a method
// returning an error anyway for symmetry with sources and so writeSink can
// treat both uniformly.
func (f sinkFormViewData) toModel() (model.Sink, error) {
	return model.Sink{
		ID:        f.ID,
		Name:      f.Name,
		Type:      model.SinkType(f.TypeFields.Type),
		Enabled:   f.Enabled,
		Interface: f.TypeFields.Interface,
		Port:      f.TypeFields.Port,
		Path:      f.TypeFields.Path,
		Address:   f.TypeFields.Address,
	}, nil
}

// handleSinksPage serves GET /ui/sinks.
func handleSinksPage(svc *config.Service, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sinks, err := svc.ListSinks(r.Context())
		if err != nil {
			log.Error("ui: list sinks failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		data := sinksPageData{
			pageData:      newPageData("Sinks", version, "sinks"),
			sinkTableData: sinkTableData{Sinks: sinkRows(sinks, statuses())},
		}
		renderPage(w, log, "sinks", data)
	}
}

// handleSinkFormFrag serves GET /ui/frag/sink-form (blank, create mode)
// and GET /ui/frag/sink-form?id=<id> (edit mode).
func handleSinkFormFrag(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := api.DiscoverSystem()
		id := r.URL.Query().Get("id")
		if id == "" {
			renderFragment(w, log, "sink-form", blankSinkFormView(can, serial))
			return
		}
		v, err := svc.GetSink(r.Context(), id)
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.Error(w, "sink not found", http.StatusNotFound)
				return
			}
			log.Error("ui: get sink failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		renderFragment(w, log, "sink-form", sinkFormViewFromModel(v, can, serial))
	}
}

// handleSinkTypeFieldsFrag serves GET /ui/frag/sink-type-fields. The same
// type-switch caveat as handleSourceTypeFieldsFrag applies: a previously
// selected type's field values are discarded on switch, not carried over.
func handleSinkTypeFieldsFrag(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		can, serial := api.DiscoverSystem()
		q := r.URL.Query()
		data := sinkTypeFieldsData{
			Type:          q.Get("type"),
			Interface:     q.Get("interface"),
			Port:          q.Get("port"),
			Path:          q.Get("path"),
			Address:       q.Get("address"),
			CANInterfaces: can,
			SerialPorts:   serial,
		}
		renderFragment(w, log, "sink-type-fields", data)
	}
}

// handleSinkCreate serves POST /ui/sinks.
func handleSinkCreate(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeSink(w, r, svc, log, true, "")
	}
}

// handleSinkUpdate serves POST /ui/sinks/{id}.
func handleSinkUpdate(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeSink(w, r, svc, log, false, r.PathValue("id"))
	}
}

// writeSink mirrors writeSource; see its comments for rationale.
func writeSink(w http.ResponseWriter, r *http.Request, svc *config.Service, log *slog.Logger, isCreate bool, pathID string) {
	can, serial := api.DiscoverSystem()
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

// handleSinkDelete serves POST /ui/sinks/{id}/delete.
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
// with its own advisory validate-on-blur endpoint, and a detail page with
// live stats read from reg (a *stats.Registry) instead of statuses.
// DeleteConnector has no ErrInUse case (config.Service: "nothing else
// references a connector"), so handleConnectorDelete's error switch is one
// case shorter than handleSourceDelete/handleSinkDelete's.

// connectorRow is one row of the connectors table
// (frag_connector_table.html): model.Connector plus its live stats
// snapshot. reg.Snapshot returns a zero Snapshot for a connector it has
// never recorded (not yet started, or started but idle since boot) — so a
// row for a connector with no traffic yet renders zero queue depth/msg-per-
// second rather than needing a presence check here, matching the behavior
// contract's "zero row values when snapshot missing".
type connectorRow struct {
	model.Connector
	Snapshot stats.Snapshot
}

func connectorRows(connectors []model.Connector, reg *stats.Registry) []connectorRow {
	rows := make([]connectorRow, len(connectors))
	for i, c := range connectors {
		snap, _ := reg.Snapshot(c.ID)
		rows[i] = connectorRow{Connector: c, Snapshot: snap}
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
	IsEdit      bool
	ID          string
	Name        string
	Enabled     bool
	SourceID    string
	SinkID      string
	FiltersText string
	MaxMessages string
	MaxAge      string
	MaxBytes    string
	Sources     []model.Source
	Sinks       []model.Sink
	Alert       *alertData
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

// formatOptionalInt64 renders a buffer limit as its decimal string, or ""
// for 0 — 0 means "unset" (model.BufferLimits.ApplyDefaults treats an
// all-zero Buffer as no limit configured), so the number input starts empty
// rather than showing a literal "0" for a connector that has never had a
// limit set.
func formatOptionalInt64(v int64) string {
	if v == 0 {
		return ""
	}
	return strconv.FormatInt(v, 10)
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

// connectorFormViewFromModel builds a connector-form view for the edit path
// (GET /ui/frag/connector-form?id=...): every field pre-filled from the
// stored entity.
func connectorFormViewFromModel(v model.Connector, sources []model.Source, sinks []model.Sink) connectorFormViewData {
	return connectorFormViewData{
		IsEdit:      true,
		ID:          v.ID,
		Name:        v.Name,
		Enabled:     v.Enabled,
		SourceID:    v.SourceID,
		SinkID:      v.SinkID,
		FiltersText: formatFilters(v.Filters),
		MaxMessages: formatOptionalInt64(v.Buffer.MaxMessages),
		MaxAge:      formatMaxAge(v.Buffer.MaxAge),
		MaxBytes:    formatOptionalInt64(v.Buffer.MaxBytes),
		Sources:     sources,
		Sinks:       sinks,
	}
}

// blankConnectorFormView builds a connector-form view for the create path
// (GET /ui/frag/connector-form with no id): every field empty except the
// source/sink lists the <select>s need to populate their <option>s.
func blankConnectorFormView(sources []model.Source, sinks []model.Sink) connectorFormViewData {
	return connectorFormViewData{Sources: sources, Sinks: sinks}
}

// connectorFormViewFromRequest rebuilds a connector-form view from a
// POSTed form's raw values (r.ParseForm must already have been called) —
// used to redisplay a submission that failed validation, preserving
// exactly what the operator typed/selected rather than falling back to
// blank/stored values.
func connectorFormViewFromRequest(r *http.Request, isEdit bool, sources []model.Source, sinks []model.Sink) connectorFormViewData {
	return connectorFormViewData{
		IsEdit:      isEdit,
		ID:          r.PostFormValue("id"),
		Name:        r.PostFormValue("name"),
		Enabled:     r.PostFormValue("enabled") != "",
		SourceID:    r.PostFormValue("source_id"),
		SinkID:      r.PostFormValue("sink_id"),
		FiltersText: r.PostFormValue("filters"),
		MaxMessages: r.PostFormValue("max_messages"),
		MaxAge:      r.PostFormValue("max_age"),
		MaxBytes:    r.PostFormValue("max_bytes"),
		Sources:     sources,
		Sinks:       sinks,
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
		ID:       f.ID,
		Name:     f.Name,
		Enabled:  f.Enabled,
		SourceID: f.SourceID,
		SinkID:   f.SinkID,
		Filters:  parseFilters(f.FiltersText),
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

// handleConnectorsPage serves GET /ui/connectors: the full page, table
// populated from svc.ListConnectors + reg's live per-connector snapshots.
func handleConnectorsPage(svc *config.Service, reg *stats.Registry, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		connectors, err := svc.ListConnectors(r.Context())
		if err != nil {
			log.Error("ui: list connectors failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		data := connectorsPageData{
			pageData:           newPageData("Connectors", version, "connectors"),
			connectorTableData: connectorTableData{Connectors: connectorRows(connectors, reg)},
		}
		renderPage(w, log, "connectors", data)
	}
}

// handleConnectorFormFrag serves GET /ui/frag/connector-form (blank,
// create mode) and GET /ui/frag/connector-form?id=<id> (edit mode).
func handleConnectorFormFrag(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sources, sinks, err := listSourcesAndSinks(r.Context(), svc)
		if err != nil {
			log.Error("ui: list sources/sinks failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		id := r.URL.Query().Get("id")
		if id == "" {
			renderFragment(w, log, "connector-form", blankConnectorFormView(sources, sinks))
			return
		}
		v, err := svc.GetConnector(r.Context(), id)
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.Error(w, "connector not found", http.StatusNotFound)
				return
			}
			log.Error("ui: get connector failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		renderFragment(w, log, "connector-form", connectorFormViewFromModel(v, sources, sinks))
	}
}

// filterValidateData is frag_filter_validate.html's data: the advisory
// CEL-compile result of a filters textarea's blur event (see
// handleValidateFiltersFrag). Never blocks a submit — svc.PutConnector
// re-validates authoritatively regardless of what this fragment last said.
type filterValidateData struct {
	OK  bool
	Err string
}

// handleValidateFiltersFrag serves POST /ui/frag/validate-filters: the
// filters textarea's hx-post="blur changed" target. Advisory only — it
// never persists anything, just reports whether svc.ValidateFilters
// CEL-compiles the textarea's current lines.
func handleValidateFiltersFrag(svc *config.Service, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			renderFragment(w, log, "filter-validate", filterValidateData{Err: "invalid form submission: " + err.Error()})
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

// handleConnectorCreate serves POST /ui/connectors.
func handleConnectorCreate(svc *config.Service, reg *stats.Registry, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeConnector(w, r, svc, reg, log, true, "")
	}
}

// handleConnectorUpdate serves POST /ui/connectors/{id}.
func handleConnectorUpdate(svc *config.Service, reg *stats.Registry, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeConnector(w, r, svc, reg, log, false, r.PathValue("id"))
	}
}

// writeConnector backs both handleConnectorCreate and handleConnectorUpdate,
// mirroring writeSource/writeSink; see writeSource's comments for the
// pathID-is-authoritative rationale.
func writeConnector(w http.ResponseWriter, r *http.Request, svc *config.Service, reg *stats.Registry, log *slog.Logger, isCreate bool, pathID string) {
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
	connectors, err := svc.ListConnectors(r.Context())
	if err != nil {
		log.Error("ui: list connectors failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	data := connectorTableData{
		Connectors: connectorRows(connectors, reg),
		Alert:      &alertData{Kind: "success", Message: fmt.Sprintf("connector %q saved", v.ID)},
	}
	writeFragmentSuccess(w, log, "connector-panel-oob", "connector-form-container", data)
}

// handleConnectorDelete serves POST /ui/connectors/{id}/delete. Unlike
// handleSourceDelete/handleSinkDelete there is no ErrInUse case:
// config.Service.DeleteConnector never returns it ("nothing else
// references a connector").
func handleConnectorDelete(svc *config.Service, reg *stats.Registry, log *slog.Logger) http.HandlerFunc {
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
		connectors, err := svc.ListConnectors(r.Context())
		if err != nil {
			log.Error("ui: list connectors failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		data := connectorTableData{Connectors: connectorRows(connectors, reg), Alert: alert}
		renderFragment(w, log, "connector-panel", data)
	}
}

// connectorDetailData is templates/connector_detail.html's data: the
// config summary card's fields. MaxAgeText is pre-formatted in Go
// (formatMaxAge) rather than in the template — model.Duration is a plain
// `type Duration time.Duration` with no String method of its own (Go
// doesn't inherit time.Duration's method set through a type definition),
// so printing .Connector.Buffer.MaxAge directly in the template would
// render raw nanoseconds instead of "24h0m0s". The live stats block below
// the config summary is NOT part of this data — see the "connectors" pages
// map entry's doc comment in pages.go for why it's fetched client-side
// instead.
type connectorDetailData struct {
	pageData
	Connector  model.Connector
	MaxAgeText string
}

// handleConnectorDetailPage serves GET /ui/connectors/{id}: the config
// summary + live stats detail page. http.NotFound for an unknown id, per
// the behavior contract.
func handleConnectorDetailPage(svc *config.Service, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		v, err := svc.GetConnector(r.Context(), id)
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get connector failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		data := connectorDetailData{
			pageData:   newPageData(v.Name, version, "connectors"),
			Connector:  v,
			MaxAgeText: formatMaxAge(v.Buffer.MaxAge),
		}
		renderPage(w, log, "connector-detail", data)
	}
}

// connectorStatsData is frag_connector_stats.html's data: a stats.Snapshot
// plus its two byte-count fields pre-humanized in Go (see humanizeBytes) —
// the same "format in Go, not in the template" choice connectorDetailData
// makes for MaxAgeText, avoiding a template FuncMap entirely.
type connectorStatsData struct {
	Snapshot        stats.Snapshot
	BytesPerSecText string
	QueueBytesText  string
}

// handleConnectorStatsFrag serves GET /ui/frag/connectors/{id}/stats: the
// detail page's hx-trigger="load, every 2s" polling target. 404s for an
// unknown connector id, same as the page itself; a known connector reg has
// never recorded (not yet started, or idle since boot) renders zero-valued
// tiles rather than 404ing — reg.Snapshot's "ok" bool is deliberately
// ignored here for the same reason connectorRows ignores it.
func handleConnectorStatsFrag(svc *config.Service, reg *stats.Registry, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if _, err := svc.GetConnector(r.Context(), id); err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get connector failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		snap, _ := reg.Snapshot(id)
		data := connectorStatsData{
			Snapshot:        snap,
			BytesPerSecText: humanizeBytes(snap.BytesPerSec, "/s"),
			QueueBytesText:  humanizeBytes(float64(snap.QueueBytes), ""),
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
