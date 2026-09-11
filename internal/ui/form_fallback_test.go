package ui_test

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/model"
)

func TestNativeFormsRenderValidationAndRedirectAfterSaving(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, tc := range []struct {
		kind   string
		values url.Values
	}{
		{"sources", url.Values{"type": {"socketcan"}, "interface": {"can0"}}},
		{"sinks", url.Values{"type": {"null"}}},
		{"connectors", url.Values{"source_id": {"src1"}, "sink_id": {"sink1"}}},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			values := tc.values
			values.Set("id", "native-"+tc.kind)
			values.Set("name", strings.Repeat("x", model.MaxEntityNameBytes+1))
			post := func(path string) *http.Response {
				t.Helper()
				resp, err := client.PostForm(srv.URL+path, values)
				if err != nil {
					t.Fatal(err)
				}
				return resp
			}
			resp := post("/" + tc.kind)
			body := mustBody(t, resp)
			if resp.StatusCode != http.StatusOK || !strings.Contains(body, "<!doctype html>") || !strings.Contains(body, "maximum is 256") {
				t.Fatalf("native validation = %d, body %s", resp.StatusCode, body)
			}
			values.Set("name", "Native entity")
			resp = post("/" + tc.kind)
			_ = mustBody(t, resp)
			if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/dashboard" {
				t.Fatalf("native create = %d, location %q", resp.StatusCode, resp.Header.Get("Location"))
			}
			values.Set("name", "Updated native entity")
			resp = post("/" + tc.kind + "/" + values.Get("id"))
			_ = mustBody(t, resp)
			if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/"+tc.kind+"/"+values.Get("id")+"/" {
				t.Fatalf("native edit = %d, location %q", resp.StatusCode, resp.Header.Get("Location"))
			}
		})
	}
}

func TestEndpointCredentialsAreRedactedAcrossReadOnlyPages(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "remote", Type: model.SourceHTTPSSE, URL: "https://operator:password-canary@example.com/events?token=query-canary#fragment-canary"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "remote", Type: model.SinkHTTPPost, URL: "https://example.com/events?token=query-canary"}, true))
	for _, path := range []string{"/dashboard", "/sources", "/sinks", "/sources/remote/", "/sinks/remote/"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body := mustBody(t, resp)
		for _, secret := range []string{"password-canary", "query-canary", "fragment-canary"} {
			if strings.Contains(body, secret) {
				t.Errorf("%s exposes %s", path, secret)
			}
		}
	}
	// Redaction is presentation only: a read must not rewrite stored credentials.
	source, err := svc.GetSource(ctx, "remote")
	must(t, err)
	if !strings.Contains(source.URL, "password-canary") || !strings.Contains(source.URL, "query-canary") {
		t.Fatal("credentials were changed in storage")
	}
}

func TestNativeTypeChangePreservesPostgresTableChoiceWithoutSaving(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	values := url.Values{"id": {"preview"}, "name": {"Preview"}, "type": {"postgres"}, "rendered_type": {"http_post"}, "form_action": {"change_type"}}
	for _, current := range []string{"http_post", "postgres"} {
		values.Set("rendered_type", current)
		resp := postForm(t, srv, "/sinks", values)
		body := mustBody(t, resp)
		checked := strings.Contains(strings.Join(strings.Fields(body), " "), `name="auto_create_table" value="1" class="checkbox" checked`)
		if checked != (current != "postgres") {
			t.Fatalf("type update from %s: auto-create checked = %v", current, checked)
		}
		if _, err := svc.GetSink(context.Background(), "preview"); err == nil {
			t.Fatal("type preview persisted a sink")
		}
	}
}
