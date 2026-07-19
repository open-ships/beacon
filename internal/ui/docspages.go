// docspages.go implements beacon's onboard operator manual at GET /ui/docs
// and GET /ui/docs/{slug}: the markdown files embedded from docs/*.md,
// rendered to HTML once per file and cached (the embedded content never
// changes at runtime, so there is nothing to invalidate), and served inside
// templates/docs.html's sidebar + <article> layout — see pages.go's "docs"
// entry and ui.go's route registrations. internal/app additionally
// 301-redirects the bare "/docs" and "/docs/{slug}" paths (on the admin
// mux, spec §5) to these /ui/docs equivalents; "/docs" is one of
// model.ReservedPathPrefixes, so no HTTP sink can ever be configured to
// collide with either mount.
package ui

import (
	"bytes"
	"embed"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	goldmarkhtml "github.com/yuin/goldmark/renderer/html"
)

// docsFS embeds the manual's markdown source. Files are named
// "<NN>-<slug>.md" (e.g. "01-getting-started.md"): the numeric prefix fixes
// reading/sidebar order (see mustLoadDocPages) and is stripped to produce
// the URL slug ("getting-started").
//
//go:embed docs/*.md
var docsFS embed.FS

// docsMarkdown is the one goldmark converter every manual page renders
// through (see renderDoc). Configuration is deliberately narrow:
//
//   - extension.Table turns on GFM pipe tables (used by the envelope field
//     table, the CEL variable table, etc.) — nothing else from the GFM
//     bundle. In particular, no linkify: beacon's offline test walks every
//     UI page's src=/href= attributes and fails on any absolute http(s)
//     URL, so auto-linking a bare URL a .md file happens to mention would
//     break that guarantee. Manual content therefore cites URLs as inline
//     code, never as bare text or markdown link syntax — see the docs/
//     files themselves.
//   - parser.WithAutoHeadingID assigns each heading a stable "#id" anchor
//     derived from its text, so the sidebar (and any future deep link)
//     can target a specific section.
//   - Raw HTML passthrough is left OFF: goldmark's default (no
//     html.WithUnsafe option set) never emits a literal "<...>" tag a .md
//     file contains — the tag is replaced with an inert "<!-- raw HTML
//     omitted -->" comment (text content around it still renders). Every
//     manual page is embedded, trusted content — this isn't a defense
//     against a malicious author — but it's the same offline/XSS hygiene
//     the rest of the UI holds itself to, at zero cost, so it stays on.
//     See docspages_test.go for the assertion against this exact
//     converter.
var docsMarkdown = goldmark.New(
	goldmark.WithExtensions(extension.Table),
	goldmark.WithParserOptions(parser.WithAutoHeadingID()),
	goldmark.WithRendererOptions(goldmarkhtml.WithXHTML()),
)

// docPage is one manual page's metadata plus its raw markdown source,
// computed once by mustLoadDocPages from an embedded file — the embedded
// file set is fixed at build time, so there is nothing to recompute later.
type docPage struct {
	Slug     string // URL slug: filename minus its "NN-" order prefix and ".md" suffix
	Title    string // the file's first "# " (H1) heading, verbatim
	filename string // embed path under docs/, e.g. "01-getting-started.md"; also the render-cache key
	source   []byte // raw markdown, fed to docsMarkdown by renderDoc
}

// orderPrefixRE strips a doc filename's leading "NN-" ordering prefix to
// produce its URL slug.
var orderPrefixRE = regexp.MustCompile(`^\d+-`)

// docPages is every manual page, in sidebar/reading order (ascending
// filename — the "NN-" prefix sorts numerically as long as N stays
// zero-padded to a fixed width, true of every file this package ships).
var docPages = mustLoadDocPages()

// mustLoadDocPages reads every docs/*.md file out of docsFS and builds its
// docPage. Panics on error: the embedded file set is fixed at compile time,
// so a failure here (an unreadable embed, in practice impossible) is a
// build bug, not a runtime condition — the same contract pages.go's
// mustPage documents for template parsing.
func mustLoadDocPages() []docPage {
	entries, err := fs.ReadDir(docsFS, "docs")
	if err != nil {
		panic(err)
	}
	pages := make([]docPage, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		src, err := docsFS.ReadFile("docs/" + e.Name())
		if err != nil {
			panic(err)
		}
		slug := strings.TrimSuffix(orderPrefixRE.ReplaceAllString(e.Name(), ""), ".md")
		pages = append(pages, docPage{
			Slug:     slug,
			Title:    firstHeading(src),
			filename: e.Name(),
			source:   src,
		})
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].filename < pages[j].filename })
	return pages
}

// firstHeading returns the text of src's first level-1 ("# ", not "## " or
// deeper) markdown heading, or "" if it has none. Used as every manual
// page's sidebar/title text so the title lives in exactly one place — the
// page's own content — rather than a second, driftable metadata field.
func firstHeading(src []byte) string {
	for _, line := range strings.Split(string(src), "\n") {
		line = strings.TrimRight(line, "\r")
		if rest, ok := strings.CutPrefix(line, "# "); ok {
			return strings.TrimSpace(rest)
		}
	}
	return ""
}

// docBySlug returns the page whose Slug matches, or false if slug names no
// shipped page (handleDocPage answers that case with 404).
func docBySlug(slug string) (docPage, bool) {
	for _, p := range docPages {
		if p.Slug == slug {
			return p, true
		}
	}
	return docPage{}, false
}

// renderedDocs caches each page's rendered HTML the first time it's
// requested, keyed by docPage.filename. A sync.Map (rather than a plain map
// behind a mutex) is enough here: entries are written at most once per key
// in steady state and read far more often than written — exactly sync.Map's
// documented sweet spot — and even a race between two concurrent
// first-requests for the same page is harmless (see renderDoc): both would
// compute the identical rendering from the same immutable source and
// LoadOrStore converges on one winner.
var renderedDocs sync.Map // filename (string) -> template.HTML

// renderDoc returns p's markdown rendered to HTML, computing and caching it
// on first call and reusing the cached value on every call after — safe
// because p.source is embedded, immutable content, so the same input always
// renders to the same output.
func renderDoc(p docPage) template.HTML {
	if v, ok := renderedDocs.Load(p.filename); ok {
		return v.(template.HTML)
	}
	var buf bytes.Buffer
	// Convert only errors on a write failure from its output io.Writer;
	// bytes.Buffer.Write never fails, so err is unreachable here in
	// practice. Checked anyway rather than silently discarded.
	if err := docsMarkdown.Convert(p.source, &buf); err != nil {
		return template.HTML("<p>failed to render this page.</p>") // #nosec G203 -- static fallback with no user input.
	}
	rendered := template.HTML(buf.String()) // #nosec G203 -- embedded Markdown rendered with raw HTML passthrough disabled.
	actual, _ := renderedDocs.LoadOrStore(p.filename, rendered)
	return actual.(template.HTML)
}

// docNavItem is one templates/docs.html sidebar entry.
type docNavItem struct {
	Slug  string
	Title string
}

// docsPageData is templates/docs.html's data: pageData (for layout.html's
// chrome and main nav — Active is always "docs", set by handleDocPage)
// plus the manual's own sub-nav and the current page's rendered body.
type docsPageData struct {
	pageData
	Pages      []docNavItem // every manual page, in sidebar order
	ActiveSlug string       // highlights the current page within Pages
	Body       template.HTML
}

// docNavItems builds docsPageData.Pages from docPages.
func docNavItems() []docNavItem {
	items := make([]docNavItem, len(docPages))
	for i, p := range docPages {
		items[i] = docNavItem{Slug: p.Slug, Title: p.Title}
	}
	return items
}

// handleDocsIndex serves GET /ui/docs: a 302 to the first manual page (the
// same convention as a directory index), so operators can bookmark or link
// to plain "/ui/docs" without needing to know page order or which file is
// first.
func handleDocsIndex() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		first := ""
		if len(docPages) > 0 {
			first = docPages[0].Slug
		}
		http.Redirect(w, r, "/ui/docs/"+first, http.StatusFound)
	}
}

// handleDocPage serves GET /ui/docs/{slug}: the manual page whose slug
// matches, or a 404 for an unknown one.
func handleDocPage(version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		p, ok := docBySlug(slug)
		if !ok {
			http.NotFound(w, r)
			return
		}
		data := docsPageData{
			pageData:   newPageData(p.Title, version, "docs"),
			Pages:      docNavItems(),
			ActiveSlug: p.Slug,
			Body:       renderDoc(p),
		}
		renderPage(w, log, "docs", data)
	}
}
