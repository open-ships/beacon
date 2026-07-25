package ui_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/mcpserver"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/supervisor"
	"github.com/open-ships/beacon/internal/ui"
)

// fakeReconciler is a test double for config.Reconciler; beacon's web UI
// this task doesn't reconcile through it (the dashboard is a shell), but
// config.NewService requires one. Mirrors internal/api/entities_test.go's
// fakeReconciler.
type fakeReconciler struct{}

func (fakeReconciler) Reconcile(ctx context.Context) error { return nil }
func (fakeReconciler) Statuses() []supervisor.Status       { return nil }

// newAppMountedServer serves ui.Handler as internal/app's root fallback.
// The handler itself owns the bare-root redirect and every root-level UI
// route; app registers the more-specific API, MCP, health, and metrics paths.
func newAppMountedServer(t *testing.T) *httptest.Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := config.NewService(st, fakeReconciler{}, nil)
	handler := ui.Handler(svc, stats.NewRegistry(), fakeReconciler{}.Statuses, nil, "test", nil)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func mustStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		defer func() { _ = resp.Body.Close() }()
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(resp.Body)
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, want, buf.String())
	}
}

// externalRefPattern matches src="..." / href="..." attribute values (in
// either quote style) so a test can assert none of them point off-origin.
// Mirrors internal/api/docsui_test.go's externalRefPattern.
var externalRefPattern = regexp.MustCompile(`(?i)(?:src|href)\s*=\s*['"]([^'"]*)['"]`)

var absoluteURLPattern = regexp.MustCompile(`(?i)^https?://`)

// externalURLs returns every src=/href= attribute value in html that looks
// like an absolute http(s) URL — i.e. every reference that would require
// network access beyond the server beacon itself is running.
func externalURLs(html string) []string {
	var out []string
	for _, m := range externalRefPattern.FindAllStringSubmatch(html, -1) {
		if absoluteURLPattern.MatchString(m[1]) {
			out = append(out, m[1])
		}
	}
	return out
}

func TestRootRedirectsToDashboard(t *testing.T) {
	srv := newAppMountedServer(t)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusFound)

	loc := resp.Header.Get("Location")
	if loc != "/dashboard" {
		t.Fatalf("Location = %q, want /dashboard", loc)
	}
}

func TestLegacyUIPathsAreRemoved(t *testing.T) {
	srv := newAppMountedServer(t)
	for _, path := range []string{"/ui", "/ui/dashboard", "/ui/sources"} {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			mustStatus(t, resp, http.StatusNotFound)
		})
	}
}

// --- Same-origin guard on POST /* ---

// samePOSTOriginTarget is a POST endpoint that never needs any prior setup
// and, when the request reaches the handler (i.e. the guard let it
// through), always answers 200 — deleting an unknown connector id renders a
// "not found" alert rather than erroring. That makes it a good same-origin
// guard test target: any status other than 200 or 403 would mean the test
// itself is broken, not the guard.
const samePOSTOriginTarget = "/connectors/nope/delete"

func TestSameOriginGuardBlocksCrossOriginPOST(t *testing.T) {
	srv := newAppMountedServer(t)
	req, err := http.NewRequest(http.MethodPost, srv.URL+samePOSTOriginTarget, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "http://evil.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusForbidden)
}

func TestSameOriginGuardBlocksCrossSiteSecFetchSite(t *testing.T) {
	srv := newAppMountedServer(t)
	req, err := http.NewRequest(http.MethodPost, srv.URL+samePOSTOriginTarget, nil)
	if err != nil {
		t.Fatal(err)
	}
	// No Origin header (some same-origin requests omit it per the Fetch
	// spec) but a Sec-Fetch-Site that names a cross-site request.
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusForbidden)
}

func TestSameOriginGuardAllowsSameOriginPOST(t *testing.T) {
	srv := newAppMountedServer(t)
	req, err := http.NewRequest(http.MethodPost, srv.URL+samePOSTOriginTarget, nil)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", u.Scheme+"://"+u.Host)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
}

func TestSameOriginGuardAllowsSameOriginSecFetchSite(t *testing.T) {
	srv := newAppMountedServer(t)
	req, err := http.NewRequest(http.MethodPost, srv.URL+samePOSTOriginTarget, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
}

func TestSameOriginGuardAllowsHeaderlessPOST(t *testing.T) {
	srv := newAppMountedServer(t)
	// A plain http.Post, like curl or any non-browser client, sends neither
	// Origin nor Sec-Fetch-Site.
	resp, err := http.Post(srv.URL+samePOSTOriginTarget, "application/x-www-form-urlencoded", nil)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
}

func TestSameOriginGuardDoesNotApplyToGET(t *testing.T) {
	srv := newAppMountedServer(t)
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/dashboard", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "http://evil.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
}

func TestDashboardPageIsSelfContained(t *testing.T) {
	srv := newAppMountedServer(t)

	resp, err := http.Get(srv.URL + "/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusOK)

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	html := string(body)

	for _, want := range []string{"openships.ai", "beacon", "docs", "Home"} {
		if !strings.Contains(html, want) {
			t.Fatalf("dashboard page does not contain %q:\n%s", want, html)
		}
	}
	for _, forbidden := range []string{"obc-", "openbridge.bundle.js", "palettes.css", "NotoSans.ttf"} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("dashboard page still references %q:\n%s", forbidden, html)
		}
	}
	if strings.Contains(html, "app-sidebar") {
		t.Fatalf("dashboard page should not render a sidebar:\n%s", html)
	}
	if !strings.Contains(html, `href="/assets/favicon.svg`) {
		t.Fatalf("dashboard page does not reference the site favicon:\n%s", html)
	}
	for _, nav := range []string{`href="https://openships.ai"`, `href="/dashboard"`, `href="/docs"`, `href="/mcp/info"`} {
		if !strings.Contains(html, nav) {
			t.Fatalf("dashboard page header does not link to %q:\n%s", nav, html)
		}
	}
	brandStart := strings.Index(html, `<div class="brand-strip">`)
	navStart := strings.Index(html, `<nav class="app-nav"`)
	if brandStart == -1 || navStart == -1 {
		t.Fatalf("dashboard page header lost brand/nav structure:\n%s", html)
	}
	if site := strings.Index(html, `href="https://openships.ai"`); site < navStart {
		t.Fatalf("openships.ai should render in the right nav, not the left brand strip:\n%s", html)
	}
	if strings.Contains(html, `class="nav-link brand-site" href="https://openships.ai"`) {
		t.Fatalf("openships.ai should use the same nav-link text styling as docs:\n%s", html)
	}
	if product := strings.Index(html, `href="/dashboard"`); product < brandStart || product > navStart {
		t.Fatalf("beacon should be the only left-side brand link:\n%s", html)
	}
	if ext := externalURLs(html); len(ext) != 1 || ext[0] != "https://openships.ai" {
		t.Fatalf("dashboard page external links = %v, want only https://openships.ai", ext)
	}
}

func TestMCPReferencePageIsCompleteAndSelfContained(t *testing.T) {
	srv := newAppMountedServer(t)
	resp, err := http.Get(srv.URL + "/mcp/info")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusOK)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	html := string(body)
	for _, want := range []string{
		"Model Context Protocol", "offline ready", "Streamable HTTP", "2025-11-25",
		`id="installation">Install Beacon MCP</h3>`,
		"codex mcp add beacon --url http://127.0.0.1:2112/mcp",
		"claude mcp add --transport http beacon http://127.0.0.1:2112/mcp",
		"gemini mcp add beacon http://127.0.0.1:2112/mcp --transport http",
		`.vscode/mcp.json`, "get_health",
		`"url": "http://127.0.0.1:2112/mcp"`, `href="/mcp/info" class="nav-link menu-active"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("MCP page does not contain %q:\n%s", want, html)
		}
	}
	for _, tool := range mcpserver.Catalog() {
		if !strings.Contains(html, "<code>"+tool.Name+"</code>") {
			t.Fatalf("MCP page does not document tool %q", tool.Name)
		}
	}
	if ext := externalURLs(html); len(ext) != 1 || ext[0] != "https://openships.ai" {
		t.Fatalf("MCP page external links = %v, want only https://openships.ai", ext)
	}
}

func TestAssetsServed(t *testing.T) {
	srv := newAppMountedServer(t)

	cases := []struct {
		path       string
		ctContains string
		minSize    int // 0 means "just check >0 bytes"
	}{
		{"htmx.min.js", "javascript", 0},
		{"app.js", "javascript", 1024},
		{"app.css", "css", 1024},
		{"favicon.svg", "image/svg", 1024},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/assets/" + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			mustStatus(t, resp, http.StatusOK)

			ct := strings.ToLower(resp.Header.Get("Content-Type"))
			if !strings.Contains(ct, tc.ctContains) {
				t.Fatalf("Content-Type = %q, want it to contain %q", ct, tc.ctContains)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if len(body) == 0 {
				t.Fatal("asset body is empty")
			}
			if tc.minSize > 0 && len(body) < tc.minSize {
				t.Fatalf("%s is %d bytes, want > %d (looks truncated/fake)", tc.path, len(body), tc.minSize)
			}

			cc := resp.Header.Get("Cache-Control")
			if !strings.Contains(cc, "max-age") {
				t.Fatalf("Cache-Control = %q, want a long max-age directive", cc)
			}
		})
	}
}

func TestOpenBridgeAssetsAreNotServed(t *testing.T) {
	srv := newAppMountedServer(t)

	for _, path := range []string{"openbridge.bundle.js", "palettes.css", "NotoSans.ttf"} {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/assets/" + path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			mustStatus(t, resp, http.StatusNotFound)
		})
	}
}

// TestNoInlineStylingInTemplates enforces beacon UI's no-inline-CSS rule:
// templates must contain no style= attributes and no <style> blocks (every
// visual rule lives in app.css, not scattered across templates). Inline
// event handlers used by htmx fragments are outside this CSS-only check.
func TestNoInlineStylingInTemplates(t *testing.T) {
	entries, err := os.ReadDir("templates")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("templates directory is empty")
	}

	styleAttr := regexp.MustCompile(`(?i)\sstyle\s*=`)
	styleTag := regexp.MustCompile(`(?i)<style[\s>]`)

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".html") {
			continue
		}
		path := filepath.Join("templates", e.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if styleAttr.Match(body) {
			t.Errorf("%s contains a style= attribute; the no-inline-CSS rule forbids it", path)
		}
		if styleTag.Match(body) {
			t.Errorf("%s contains a <style> block; the no-inline-CSS rule forbids it", path)
		}
	}
}

// --- Docs manual (internal/ui/docspages.go) ---

// wantDocPages is every page internal/ui/docs/*.md ships — slug plus title
// (the file's first "# " heading) — in sidebar order. Hand-maintained
// rather than derived from the filesystem so a content-file typo and a
// test typo can't quietly agree with each other;
// TestDocsSidebarListsAllPages fails loud on drift in either direction.
var wantDocPages = []struct{ slug, title string }{
	{"getting-started", "Getting started"},
	{"can-setup", "CAN setup"},
	{"concepts", "Concepts"},
	{"filters", "Filters"},
	{"api", "API (for agents and scripts)"},
	{"troubleshooting", "Troubleshooting"},
}

func TestDocsIndexRedirectsToFirstPage(t *testing.T) {
	srv := newAppMountedServer(t)
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(srv.URL + "/docs")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusFound)
	want := "/docs/" + wantDocPages[0].slug
	if loc := resp.Header.Get("Location"); loc != want {
		t.Fatalf("Location = %q, want %q", loc, want)
	}
}

func TestDocPageServesKnownSlug(t *testing.T) {
	srv := newAppMountedServer(t)
	resp, err := http.Get(srv.URL + "/docs/getting-started")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusOK)

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	html := string(body)
	if !strings.Contains(html, "Getting started") {
		t.Fatalf("getting-started page does not contain its own heading:\n%s", html)
	}
	// The rendered <article> body, not just the sidebar link text.
	if !strings.Contains(html, "<article") {
		t.Fatalf("docs page has no <article> body:\n%s", html)
	}
}

func TestDocPageUnknownSlugIs404(t *testing.T) {
	srv := newAppMountedServer(t)
	resp, err := http.Get(srv.URL + "/docs/does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusNotFound)
}

// TestDocsSidebarListsAllPages checks every shipped page's sidebar entry —
// href AND title, as one `<a href="/docs/{slug}"...>{title}</a>` anchor,
// so a page whose title went blank or got swapped fails here even though
// its slug link would still be present — appears on a single rendered docs
// page (the sidebar is identical across every page; see docs.html), and
// that wantDocPages above names exactly the six pages actually shipped
// (neither list a stray extra nor missing one).
func TestDocsSidebarListsAllPages(t *testing.T) {
	srv := newAppMountedServer(t)
	resp, err := http.Get(srv.URL + "/docs/getting-started")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusOK)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	html := string(body)

	if got := strings.Count(html, `href="/docs/`); got != len(wantDocPages) {
		t.Fatalf("sidebar has %d /docs/ links, want %d (%v)", got, len(wantDocPages), wantDocPages)
	}
	// One regexp per page: its anchor must carry the right href AND close
	// with the right link text (docs.html renders the active page's anchor
	// with an extra class attribute between href and >, hence the [^>]*).
	for _, p := range wantDocPages {
		anchor := regexp.MustCompile(`<a href="/docs/` + regexp.QuoteMeta(p.slug) + `"[^>]*>` + regexp.QuoteMeta(p.title) + `</a>`)
		if !anchor.MatchString(html) {
			t.Fatalf("sidebar missing anchor for slug %q with title %q:\n%s", p.slug, p.title, html)
		}
	}
}

// TestDocsPagesAreSelfContained extends TestDashboardPageIsSelfContained's
// external-link check to every manual page: the only absolute http(s) href
// allowed is the requested openships.ai header link. Docs content itself
// cites URLs as inline code, never as markdown/HTML links (see
// docsMarkdown's doc comment).
func TestDocsPagesAreSelfContained(t *testing.T) {
	srv := newAppMountedServer(t)
	for _, p := range wantDocPages {
		t.Run(p.slug, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/docs/" + p.slug)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			mustStatus(t, resp, http.StatusOK)
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			html := string(body)
			if ext := externalURLs(html); len(ext) != 1 || ext[0] != "https://openships.ai" {
				t.Fatalf("docs page %q external links = %v, want only https://openships.ai", p.slug, ext)
			}
		})
	}
}

// TestDocsPagesContainNoInlineStyling extends TestNoInlineStylingInTemplates
// (which only scans the static templates/*.html files on disk, so it
// trivially passes for docs.html's own markup) to each manual page's actual
// rendered HTTP response: goldmark's raw-HTML-off configuration (see
// docsMarkdown's doc comment) should make it impossible for markdown
// content to ever inject a style= attribute or <style> block, but this
// exercises that guarantee against the real rendered output rather than
// just trusting the configuration.
func TestDocsPagesContainNoInlineStyling(t *testing.T) {
	srv := newAppMountedServer(t)
	styleAttr := regexp.MustCompile(`(?i)\sstyle\s*=`)
	styleTag := regexp.MustCompile(`(?i)<style[\s>]`)
	for _, p := range wantDocPages {
		t.Run(p.slug, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/docs/" + p.slug)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			html := string(body)
			if styleAttr.MatchString(html) {
				t.Errorf("docs page %q contains a style= attribute; the no-inline-CSS rule forbids it", p.slug)
			}
			if styleTag.MatchString(html) {
				t.Errorf("docs page %q contains a <style> block; the no-inline-CSS rule forbids it", p.slug)
			}
		})
	}
}

// TestDocsNavItemPresent checks layout.html's main nav (shared by every
// full page, not just docs) has the docs entry pointing at /docs.
func TestDocsNavItemPresent(t *testing.T) {
	srv := newAppMountedServer(t)
	resp, err := http.Get(srv.URL + "/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	html := string(body)
	if !strings.Contains(html, `href="/docs"`) {
		t.Fatalf("main nav does not link to /docs:\n%s", html)
	}
	if !strings.Contains(html, ">docs<") {
		t.Fatalf("main nav does not label the docs link \"docs\":\n%s", html)
	}
}
