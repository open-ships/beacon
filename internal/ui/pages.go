package ui

import (
	"embed"
	"html/template"
)

//go:embed templates/*.html
var templatesFS embed.FS

// baseLayout is templates/layout.html parsed alone. Each page clones it and
// parses its own content file(s) into the clone (see mustPage) rather than
// parsing every template file into one shared *template.Template: html/
// template silently lets a later {{define "content"}} overwrite an earlier
// one within a single template set, so without cloning, two pages sharing
// this file (e.g. dashboard.html and sources.html, both defining
// "content") would collide and the last one parsed would win for every
// page.
var baseLayout = template.Must(template.ParseFS(templatesFS, "templates/layout.html"))

// mustPage clones baseLayout and parses templates/<file> for each file in
// files (which together must define a "content" template, and may define
// other named templates "content" invokes — e.g. sources.html's "content"
// invokes frag_source_table.html's "source-panel") into the clone,
// producing a self-contained *template.Template for one page. Panics on
// error: template files are embedded at build time, so a parse failure
// here is a programming error, not a runtime condition.
func mustPage(files ...string) *template.Template {
	t := template.Must(baseLayout.Clone())
	for _, f := range files {
		t = template.Must(t.ParseFS(templatesFS, "templates/"+f))
	}
	return t
}

// pages maps a page's nav key to its pre-parsed, self-contained template.
// Sources/sinks/connectors pages additionally parse in their table fragment
// template so "content" can render the same "source-panel"/"sink-panel"/
// "connector-panel" markup that POST handlers also re-render standalone via
// fragTemplates below — keeping the initial page load and every htmx-driven
// update using identical markup. connector-detail and dashboard need no such
// fragment: their live blocks are never part of the initial server render —
// each page ships an empty container with hx-trigger="load, every 2s" that
// fetches "connector-stats" (frag_connector_stats.html) or "dashboard-content"
// (frag_dashboard.html), both part of fragTemplates, client-side immediately
// after load.
var pages = map[string]*template.Template{
	"dashboard":        mustPage("dashboard.html"),
	"sources":          mustPage("sources.html", "frag_source_table.html"),
	"sinks":            mustPage("sinks.html", "frag_sink_table.html"),
	"connectors":       mustPage("connectors.html", "frag_connector_table.html"),
	"connector-detail": mustPage("connector_detail.html"),
}

// fragTemplates holds every frag_*.html file parsed into ONE shared
// template set (unlike pages above, these are never cloned from
// baseLayout — a fragment is never wrapped in layout.html). This is safe
// only because every {{define}} name across every frag_*.html file is
// unique within the set (source-panel/source-panel-oob/source-form/
// source-type-fields and their sink-*/connector-* counterparts, plus
// filter-validate, connector-stats, and dashboard-content) — html/template
// panics at parse time on a duplicate define name in one set, so a future
// frag_*.html file must pick names that don't collide with these.
// renderFragment (render.go) executes a template from this set by name.
var fragTemplates = template.Must(template.ParseFS(templatesFS, "templates/frag_*.html"))

// navItem is one entry in the sidebar nav rendered by layout.html.
type navItem struct {
	Key   string // matches pageData.Active to highlight the current page
	Label string
	Href  string
}

// navItems is the fixed sidebar nav.
var navItems = []navItem{
	{Key: "dashboard", Label: "Dashboard", Href: "/ui/dashboard"},
	{Key: "sources", Label: "Sources", Href: "/ui/sources"},
	{Key: "sinks", Label: "Sinks", Href: "/ui/sinks"},
	{Key: "connectors", Label: "Connectors", Href: "/ui/connectors"},
}

// pageData is layout.html's template data; every page's own data embeds
// this (see sourcesPageData/sinksPageData in forms.go) so ExecuteTemplate
// against "layout.html" always finds Title/AssetVersion/Active/Nav
// regardless of what other fields that page's data type adds.
type pageData struct {
	Title        string
	AssetVersion string
	Active       string
	Nav          []navItem
}

// newPageData builds the pageData common to every full-page render. active
// is a pages map key and also highlights the matching navItem.
func newPageData(title, assetVersion, active string) pageData {
	return pageData{
		Title:        title,
		AssetVersion: assetVersion,
		Active:       active,
		Nav:          navItems,
	}
}
