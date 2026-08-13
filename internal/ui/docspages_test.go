// package ui (not ui_test): these tests exercise docPages/docsMarkdown
// directly — the manual's actual content and the exact goldmark converter
// production code renders it through — rather than only through HTTP, which
// ui_test.go's TestDocs* cases (route/sidebar/offline/no-inline-CSS
// behavior) already cover.
package ui

import (
	"strings"
	"testing"
)

// testRenderDoc calls renderDoc with a synthetic page and cleans its entry
// back out of the package-level renderedDocs cache afterward, so no
// synthetic test key can ever leak into another test's (or a real page's)
// view of the cache — the real docs/*.md keys can't collide with
// "test-*.md" names under the NN-slug.md convention today, but this keeps
// the isolation structural rather than coincidental.
func testRenderDoc(t *testing.T, p docPage) string {
	t.Helper()
	t.Cleanup(func() { renderedDocs.Delete(p.filename) })
	return string(renderDoc(p))
}

// TestDocsMarkdownEscapesRawHTML asserts the actual production converter
// (docsMarkdown — same var handleDocPage renders every manual page
// through) never passes a raw HTML tag through as markup: with no
// html.WithUnsafe option set (see docsMarkdown's doc comment), goldmark
// replaces the tag with an inert "<!-- raw HTML omitted -->" comment.
// That's goldmark's default-safe behavior, but a future accidental option
// change is exactly the kind of regression this test exists to catch.
func TestDocsMarkdownEscapesRawHTML(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"script tag", "before <script>alert(1)</script> after"},
		{"img onerror", `<img src=x onerror="alert(1)">`},
		{"inline style block", "<style>body{display:none}</style>"},
		{"raw anchor with href", `<a href="https://evil.example">click</a>`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			html := testRenderDoc(t, docPage{filename: "test-" + c.name + ".md", source: []byte(c.input)})
			for _, tag := range []string{"<script>", "<img", "<style>", "<a href="} {
				if strings.Contains(html, tag) {
					t.Fatalf("rendered output contains raw %q, want it stripped:\ninput: %s\noutput: %s", tag, c.input, html)
				}
			}
		})
	}
}

// TestDocsMarkdownRendersGFMTables confirms the GFM table extension is
// actually wired up (docsMarkdown's one deliberate extension beyond
// CommonMark defaults — see its doc comment for why nothing else from the
// GFM bundle is enabled) since every content page's field/CEL-variable
// tables depend on it.
func TestDocsMarkdownRendersGFMTables(t *testing.T) {
	html := testRenderDoc(t, docPage{filename: "test-table.md", source: []byte("| a | b |\n|---|---|\n| 1 | 2 |\n")})
	if !strings.Contains(html, "<table>") {
		t.Fatalf("GFM table did not render as <table>: %s", html)
	}
}

// TestDocsMarkdownDoesNotAutolink guards the offline constraint every
// manual content file writes to: docsMarkdown enables extension.Table only,
// not the full GFM bundle (which includes linkify, GitHub's bare-URL
// autolinking) — a future switch to extension.GFM would silently turn any
// bare "https://..." mention in a .md file into a clickable off-origin
// href=, breaking ui_test.go's offline-net walk. Angle-bracket autolinks
// (CommonMark core, not a GFM extension, so unaffected by that extension
// choice) are a separate hazard content authors must avoid directly, not
// something this converter can be configured to suppress.
func TestDocsMarkdownDoesNotAutolink(t *testing.T) {
	html := testRenderDoc(t, docPage{filename: "test-bare-url.md", source: []byte("See https://example.com for more.")})
	if strings.Contains(html, "<a ") || strings.Contains(html, "href=") {
		t.Fatalf("bare URL was auto-linked; want plain text: %s", html)
	}
}

// TestRenderDocCachesRendering exercises renderedDocs' sync.Map cache: two
// calls for the same filename must agree (trivially true for a pure
// converter over immutable source, but this is the behavior the cache
// exists to preserve — see renderDoc's doc comment) and a change to source
// alone (impossible in production, since docPages is fixed at build time,
// but exactly what would happen if the cache instead kept the FIRST source
// it ever saw for a given filename) proves the lookup really is
// filename-keyed rather than content-keyed.
func TestRenderDocCachesRendering(t *testing.T) {
	p := docPage{filename: "test-cache.md", source: []byte("# One\n")}
	first := testRenderDoc(t, p)
	second := testRenderDoc(t, p)
	if first != second {
		t.Fatalf("renderDoc gave different output for the same page across calls: %q vs %q", first, second)
	}

	stale := docPage{filename: "test-cache.md", source: []byte("# Two\n")}
	third := testRenderDoc(t, stale)
	if third != first {
		t.Fatalf("renderDoc did not return the cached (filename-keyed) rendering for a repeated filename")
	}
}

// TestFirstHeading covers firstHeading's contract directly: only a level-1
// "# " heading counts (not "## ", which handleDocPage relies on to avoid
// picking up a page's first subheading as its title), the first one wins,
// and no heading at all yields "".
func TestFirstHeading(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{"simple", "# Getting Started\n\nbody text\n", "Getting Started"},
		{"skips h2 before h1", "## Not this\n\n# This one\n", "This one"},
		{"first h1 wins", "# First\n\n# Second\n", "First"},
		{"no heading", "just a paragraph, no heading at all\n", ""},
		{"trims trailing space and CRLF", "# Padded  \r\n", "Padded"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := firstHeading([]byte(c.src)); got != c.want {
				t.Fatalf("firstHeading(%q) = %q, want %q", c.src, got, c.want)
			}
		})
	}
}

// TestDocPagesContentSanity is the "content sanity" gate over the manual's
// actual shipped files (internal/ui/docs/*.md, loaded into docPages at
// package init by mustLoadDocPages): every page has a non-empty title (an
// "h1-less" page would silently render a blank sidebar entry and browser
// tab title), every slug is unique (a collision would make one page
// permanently unreachable — the later one in docPages order would shadow
// the earlier in any map keyed by slug, and docBySlug's linear scan would
// always return the first, silently stranding the second), and the count
// matches the fourteen-file manual this task ships (a page accidentally
// dropped, or an extra stray file picked up, both fail loud here rather
// than only being noticed by a human skimming the sidebar).
func TestDocPagesContentSanity(t *testing.T) {
	const wantPages = 14
	if len(docPages) != wantPages {
		t.Fatalf("len(docPages) = %d, want %d", len(docPages), wantPages)
	}

	seenSlugs := map[string]string{} // slug -> filename that claimed it
	for _, p := range docPages {
		if strings.TrimSpace(p.Title) == "" {
			t.Errorf("%s: no top-level (\"# \") heading found; every manual page needs a title", p.filename)
		}
		if other, dup := seenSlugs[p.Slug]; dup {
			t.Errorf("slug %q is shared by %s and %s", p.Slug, other, p.filename)
		}
		seenSlugs[p.Slug] = p.filename
		if len(p.source) == 0 {
			t.Errorf("%s: empty file", p.filename)
		}
	}
}

// TestDocBySlugUnknown covers docBySlug's not-found path directly;
// ui_test.go's TestDocPageUnknownSlugIs404 covers the same behavior
// end-to-end through handleDocPage over HTTP.
func TestDocBySlugUnknown(t *testing.T) {
	if _, ok := docBySlug("does-not-exist"); ok {
		t.Fatal("docBySlug matched a slug that was never loaded")
	}
}
