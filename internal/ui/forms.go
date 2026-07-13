// forms.go implements the Sources and Sinks CRUD pages: the page/fragment
// data types templates/sources.html, sinks.html, and every frag_*.html
// render against, the model<->form conversions, and the HTTP handlers
// Handler (ui.go) wires up at /ui/sources, /ui/sinks, /ui/frag/*.
//
// Sources and sinks are handled by deliberately parallel, non-generic code
// (sourceX / sinkX pairs) rather than a shared generic implementation: the
// two entities' type-specific fields differ enough (source http_sse/
// http_ws carries URL+Headers; sink http_sse/http_ws carries Path, and
// sink alone has a tcp type carrying Address) that a generic abstraction
// would need almost as much per-kind branching as just writing both out,
// while being harder to follow — the same tradeoff internal/api/entities.go
// already made for its source/sink/connector route registration.
package ui

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/open-ships/beacon/internal/api"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
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
// type-specific field (all are carried through regardless of Type so
// switching the type select and back doesn't lose what was typed into the
// other type's fields), and the discovered hardware lists for the
// socketcan/usbcan <datalist> suggestions (see api.DiscoverSystem).
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
// type select's hx-get target (hx-include="closest form" resends every
// current form field as a query parameter, so switching the type and back
// preserves whatever was typed into the other type's fields).
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
		view.Alert = &alertData{Kind: "error", Message: sourceSinkWriteErrorMessage(err, "source", v.ID)}
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

// sourceSinkWriteErrorMessage renders a PutSource/PutSink error into
// user-facing text. Only errors isUserFacingWriteErr accepts reach here.
// kind is "source" or "sink", used solely to word ErrExists/ErrNotFound.
func sourceSinkWriteErrorMessage(err error, kind, id string) string {
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

// handleSinkTypeFieldsFrag serves GET /ui/frag/sink-type-fields.
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
		view.Alert = &alertData{Kind: "error", Message: sourceSinkWriteErrorMessage(err, "sink", v.ID)}
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
