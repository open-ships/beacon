package ui_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/config"
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

// newAppMountedServer builds ui.Handler and serves it exactly as
// internal/app mounts it in the real deployment: mux.Handle("/ui/",
// handler) plus the "GET /{$}" -> /ui/dashboard redirect (see app.go's
// Run). ui.go's routes are registered with full "/ui/..." paths on the
// assumption they're reached this way, so tests exercise that exact shape
// rather than serving the handler bare at "/" (same reasoning as
// internal/api/docsui_test.go's newAppMountedServer).
func newAppMountedServer(t *testing.T) *httptest.Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := config.NewService(st, fakeReconciler{}, nil)
	handler := ui.Handler(svc, stats.NewRegistry(), fakeReconciler{}.Statuses, "test", nil)

	mux := http.NewServeMux()
	mux.Handle("/ui/", handler)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/dashboard", http.StatusFound)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func mustStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		defer resp.Body.Close()
		var buf bytes.Buffer
		buf.ReadFrom(resp.Body)
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
	defer resp.Body.Close()
	mustStatus(t, resp, http.StatusFound)

	loc := resp.Header.Get("Location")
	if loc != "/ui/dashboard" {
		t.Fatalf("Location = %q, want /ui/dashboard", loc)
	}
}

func TestDashboardPageIsSelfContained(t *testing.T) {
	srv := newAppMountedServer(t)

	resp, err := http.Get(srv.URL + "/ui/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
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

	for _, want := range []string{"<obc-top-bar", "<obc-brilliance-menu", "Dashboard"} {
		if !strings.Contains(html, want) {
			t.Fatalf("dashboard page does not contain %q:\n%s", want, html)
		}
	}
	for _, nav := range []string{"/ui/dashboard", "/ui/sources", "/ui/sinks", "/ui/connectors"} {
		if !strings.Contains(html, nav) {
			t.Fatalf("dashboard page nav does not link to %q:\n%s", nav, html)
		}
	}
	if ext := externalURLs(html); len(ext) > 0 {
		t.Fatalf("dashboard page references external URL(s), violating the offline constraint: %v", ext)
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
		{"openbridge.bundle.js", "javascript", 1024},
		{"palettes.css", "css", 1024},
		{"NotoSans.ttf", "font", 1024},
		{"app.css", "css", 1024},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/ui/assets/" + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
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

// TestAppCSSFontURLHasCacheBuster guards the one asset URL that can't carry
// layout.html's runtime "?v=" version: the @font-face src baked into the
// compiled app.css. Assets are served immutable/max-age=1y, so a bare
// /ui/assets/NotoSans.ttf reference would pin browsers to a stale font
// forever across re-vendors; uisrc/input.css hand-pins the vendored
// OpenBridge package version as the cache-buster instead (see its comment).
// This test fails if a rebuild of app.css drops that query parameter.
func TestAppCSSFontURLHasCacheBuster(t *testing.T) {
	srv := newAppMountedServer(t)

	resp, err := http.Get(srv.URL + "/ui/assets/app.css")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	mustStatus(t, resp, http.StatusOK)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	css := string(body)
	if !strings.Contains(css, "NotoSans.ttf") {
		t.Fatal("app.css has no NotoSans.ttf @font-face reference")
	}
	if !strings.Contains(css, "NotoSans.ttf?v=") {
		t.Fatal("app.css references NotoSans.ttf without a ?v= cache-buster; " +
			"immutable-cached assets need one (see uisrc/input.css)")
	}
}

// TestNoInlineStylingInTemplates enforces beacon UI's no-inline-CSS rule:
// templates must contain no style= attributes and no <style> blocks (every
// visual rule lives in the compiled Tailwind/daisyUI stylesheet or
// OpenBridge's own component CSS, not scattered across templates). A small
// inline <script> for brilliance persistence is fine — this only checks for
// CSS.
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
