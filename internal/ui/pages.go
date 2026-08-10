package ui

import (
	"embed"
	"html/template"

	"github.com/open-ships/beacon/internal/mcpserver"
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
// Sources/sinks/connectors pages additionally parse their table and form
// fragment templates so direct /new fallbacks, edit pages, modal fragments,
// and validation responses all use the same source-/sink-/connector-form
// markup. The dashboard also parses its fragment so the first response and
// later polling refreshes render the same dashboard-content template.
var pages = map[string]*template.Template{
	"dashboard": mustPage("dashboard.html", "frag_dashboard.html"),
	"sources": mustPage(
		"sources.html",
		"frag_source_table.html",
		"frag_source_form.html",
		"frag_source_type_fields.html",
	),
	"sinks": mustPage(
		"sinks.html",
		"frag_sink_table.html",
		"frag_sink_form.html",
		"frag_sink_type_fields.html",
	),
	"connectors": mustPage(
		"connectors.html",
		"frag_connector_table.html",
		"frag_connector_form.html",
	),
	"overview": mustPage("overview.html"),
	"config":   mustPage("config.html"),
	"docs":     mustPage("docs.html"),
	"mcp":      mustPage("mcp.html"),
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

// pageData is layout.html's template data; every page's own data embeds
// this (see sourcesPageData/sinksPageData in forms.go) so ExecuteTemplate
// against "layout.html" always finds Title/AssetVersion/Active regardless
// of what other fields that page's data type adds.
type pageData struct {
	Title        string
	AssetVersion string
	Active       string
	// Flash is a one-shot success banner shown once on a full page load,
	// carried across a create handler's HX-Redirect by the flash cookie (see
	// flash.go). Empty on every page that isn't the redirect destination.
	Flash       string
	Breadcrumbs []breadcrumbItem
}

type mcpPageData struct {
	pageData
	Tools []mcpserver.ToolInfo
}

type breadcrumbItem struct {
	Label string
	Href  string
}

// newPageData builds the pageData common to every full-page render. active
// is a pages map key and also highlights the matching navItem.
func newPageData(title, assetVersion, active string) pageData {
	return pageData{
		Title:        title,
		AssetVersion: assetVersion,
		Active:       active,
	}
}

func (d pageData) withBreadcrumbs(items ...breadcrumbItem) pageData {
	d.Breadcrumbs = items
	return d
}
