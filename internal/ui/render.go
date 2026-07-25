package ui

import (
	"bytes"
	"log/slog"
	"net/http"
)

// renderPage executes the page registered under name against data (which
// must supply every field templates/layout.html and that page's own
// content template reference — see pageData and e.g. sourcesPageData),
// and writes it to w.
//
// The page is executed into a buffer first, and only a fully successful
// render is written to w: streaming the template straight into w would mean
// a mid-render failure had already sent a 200 status and a truncated body,
// with no way to take either back. With the buffer, any failure — a name
// missing from pages, or ExecuteTemplate erroring — is logged and answered
// with a clean 500 instead of a silently broken page. (Both failures
// indicate bugs — pages/routes drift, or a template referencing data the
// page doesn't provide — that ui_test.go/forms_test.go should catch first,
// but sources/sinks put live, request-dependent data through these
// templates, so surfacing them loudly beats rendering garbage.)
func renderPage(w http.ResponseWriter, log *slog.Logger, name string, data any) {
	t, ok := pages[name]
	if !ok {
		// Unreachable from HTTP: callers only pass names they've registered
		// a "GET /<name>" route for, which by construction exist in
		// pages. Guarded anyway so a future routing/pages drift fails loud
		// instead of executing a nil template.
		log.Error("ui: render failed", "page", name, "err", "no template registered for page")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "layout.html", data); err != nil {
		log.Error("ui: render failed", "page", name, "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// renderFragment executes the named template out of fragTemplates (an
// htmx response fragment — no layout.html wrapper) and writes it to w.
// Used by every /frag/* handler and by every source/sink write/delete
// handler in forms.go to re-render the table or form fragment. Buffered
// for the same reason renderPage is: a partial write on a mid-render
// failure would leave htmx swapping in truncated HTML with no clean way to
// signal the failure instead.
func renderFragment(w http.ResponseWriter, log *slog.Logger, name string, data any) {
	var buf bytes.Buffer
	if err := fragTemplates.ExecuteTemplate(&buf, name, data); err != nil {
		log.Error("ui: fragment render failed", "fragment", name, "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}
