package api_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/api"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
)

// newAppMountedServer builds api.New's handler and serves it exactly as
// internal/app mounts it in the real deployment: mux.Handle("/api/",
// handler), with no StripPrefix. docsui.go's routes are registered with
// full "/api/..." paths on the assumption that they're reached this way
// (see TestSchemaLinksUnderAppMount in entities_test.go for the same
// reasoning applied to huma's own routes), so these tests exercise that
// exact shape rather than serving the handler bare at "/".
func newAppMountedServer(t *testing.T) *httptest.Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := config.NewService(st, &fakeReconciler{}, nil)
	handler, _ := api.New(svc, stats.NewRegistry(), "test", nil)

	mux := http.NewServeMux()
	mux.Handle("/api/", handler) // exactly how app.go mounts it
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// externalRefPattern matches src="..." / href="..." attribute values (in
// either quote style) so a test can assert none of them point off-origin.
var externalRefPattern = regexp.MustCompile(`(?i)(?:src|href)\s*=\s*['"]([^'"]*)['"]`)

// externalURLs returns every src=/href= attribute value in html that looks
// like an absolute http(s) URL — i.e. every reference that would require
// network access beyond the server beacon itself is running.
func externalURLs(html string) []string {
	var out []string
	for _, m := range externalRefPattern.FindAllStringSubmatch(html, -1) {
		if regexp.MustCompile(`(?i)^https?://`).MatchString(m[1]) {
			out = append(out, m[1])
		}
	}
	return out
}

func TestDocsPageIsSelfContained(t *testing.T) {
	srv := newAppMountedServer(t)

	resp, err := http.Get(srv.URL + "/api/docs")
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

	if !strings.Contains(html, "/api/assets/scalar.min.js") {
		t.Fatalf("docs page does not reference /api/assets/scalar.min.js:\n%s", html)
	}
	if !strings.Contains(html, "/api/openapi.json") {
		t.Fatalf("docs page does not point Scalar at /api/openapi.json:\n%s", html)
	}
	if ext := externalURLs(html); len(ext) > 0 {
		t.Fatalf("docs page references external URL(s), violating the offline constraint: %v", ext)
	}
}

func TestScalarBundleIsServed(t *testing.T) {
	srv := newAppMountedServer(t)

	resp, err := http.Get(srv.URL + "/api/assets/scalar.min.js")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusOK)

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(strings.ToLower(ct), "javascript") {
		t.Fatalf("Content-Type = %q, want it to contain \"javascript\"", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	const minSize = 100 * 1024 // 100KB: catches an accidentally-truncated or placeholder bundle
	if len(body) < minSize {
		t.Fatalf("scalar.min.js is %d bytes, want > %d (looks truncated/fake)", len(body), minSize)
	}
}

func TestScalarBundleHasLongCacheHeader(t *testing.T) {
	srv := newAppMountedServer(t)

	resp, err := http.Get(srv.URL + "/api/assets/scalar.min.js")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusOK)

	cc := resp.Header.Get("Cache-Control")
	if !strings.Contains(cc, "max-age") {
		t.Fatalf("Cache-Control = %q, want a long max-age directive", cc)
	}
}

func TestOpenAPIJSONServedAtDocsURL(t *testing.T) {
	srv := newAppMountedServer(t)

	// This must be the exact same path docsui.go points Scalar at.
	resp, err := http.Get(srv.URL + "/api/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	mustStatus(t, resp, http.StatusOK)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"openapi":"3.1`) {
		t.Fatalf("openapi.json does not look like an OpenAPI 3.1 document:\n%s", body)
	}
}
