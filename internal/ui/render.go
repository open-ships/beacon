package ui

import (
	"embed"
	"html/template"
	"net/http"
)

//go:embed templates/*.html
var templatesFS embed.FS

// baseLayout is templates/layout.html parsed alone. Each page clones it and
// parses its own content file into the clone (see mustPage) rather than
// parsing every template file into one shared *template.Template: html/
// template silently lets a later {{define "content"}} overwrite an earlier
// one within a single template set, so without cloning, two pages sharing
// this file (e.g. dashboard.html and a future sources.html, both defining
// "content") would collide and the last one parsed would win for every
// page.
var baseLayout = template.Must(template.ParseFS(templatesFS, "templates/layout.html"))

// mustPage clones baseLayout and parses templates/<file> (which must define
// a "content" template) into the clone, producing a self-contained
// *template.Template for one page. Panics on error: template files are
// embedded at build time, so a parse failure here is a programming error,
// not a runtime condition.
func mustPage(file string) *template.Template {
	t := template.Must(baseLayout.Clone())
	return template.Must(t.ParseFS(templatesFS, "templates/"+file))
}

// pages maps a page's nav key to its pre-parsed, self-contained template.
// Add an entry here for each new page as it's built (Tasks 3-5 add
// sources.html, sinks.html, connectors.html).
var pages = map[string]*template.Template{
	"dashboard": mustPage("dashboard.html"),
}

// navItem is one entry in the sidebar nav rendered by layout.html.
type navItem struct {
	Key   string // matches pageData.Active to highlight the current page
	Label string
	Href  string
}

// navItems is the fixed sidebar nav. Sources/Sinks/Connectors 404 until
// Tasks 3-4 build their handlers; the nav still links to them so the shell
// reflects beacon's final navigation shape.
var navItems = []navItem{
	{Key: "dashboard", Label: "Dashboard", Href: "/ui/dashboard"},
	{Key: "sources", Label: "Sources", Href: "/ui/sources"},
	{Key: "sinks", Label: "Sinks", Href: "/ui/sinks"},
	{Key: "connectors", Label: "Connectors", Href: "/ui/connectors"},
}

// pageData is layout.html's template data; every page's own data (once
// pages carry real content, from Task 5 onward) should embed this.
type pageData struct {
	Title        string
	AssetVersion string
	Active       string
	Nav          []navItem
}

// render executes the page registered under name (a pages key), wrapping it
// in layout.html, and writes it to w. assetVersion is the cache-busting
// query parameter layout.html appends to every vendored asset URL — see
// Handler's doc comment for why it's tied to beacon's own binary version
// rather than a hand-maintained constant.
//
// Errors from ExecuteTemplate are deliberately dropped: with static,
// compile-time-known template data they only happen after some output may
// already have been written (so a clean error response is no longer
// possible), and every page's data comes from this package's own fixed
// struct literals, not user input, so a template execution error here would
// indicate a bug caught by ui_test.go rather than a runtime condition
// callers need to react to.
func render(w http.ResponseWriter, name, title, assetVersion string) {
	t, ok := pages[name]
	if !ok {
		// Unreachable from HTTP: callers only pass names they've registered
		// a "GET /ui/<name>" route for, which by construction exist in
		// pages. Guarded anyway so a future routing/pages drift fails loud
		// instead of executing a nil template.
		http.Error(w, "page not found", http.StatusInternalServerError)
		return
	}
	data := pageData{
		Title:        title,
		AssetVersion: assetVersion,
		Active:       name,
		Nav:          navItems,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = t.ExecuteTemplate(w, "layout.html", data)
}
