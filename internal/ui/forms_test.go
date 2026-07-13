package ui_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/store"
	"github.com/open-ships/beacon/internal/ui"
)

// newUIServerWithService mirrors newAppMountedServer (ui_test.go) but also
// returns the config.Service backing it, so tests here can seed
// entities/connectors directly through the service layer — e.g. to set up
// a delete-in-use scenario — without depending on the connectors UI, which
// doesn't exist yet (a later task). Duplicated rather than changing
// newAppMountedServer's signature, which every existing ui_test.go test
// already calls.
func newUIServerWithService(t *testing.T) (*httptest.Server, *config.Service) {
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
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, svc
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func postForm(t *testing.T, srv *httptest.Server, path string, values url.Values) *http.Response {
	t.Helper()
	resp, err := http.Post(srv.URL+path, "application/x-www-form-urlencoded", strings.NewReader(values.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// --- Table rendering ---

func TestSourcesPageRendersConfiguredEntities(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "Engine CAN", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}, true))

	resp, err := http.Get(srv.URL + "/ui/sources")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{"Engine CAN", "<code>can0</code>", "socketcan", "badge-success"} {
		if !strings.Contains(body, want) {
			t.Fatalf("sources page missing %q:\n%s", want, body)
		}
	}
}

func TestSinksPageRendersConfiguredEntities(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSink(context.Background(), model.Sink{
		ID: "out1", Name: "NMEA Out", Type: model.SinkTCP, Enabled: true, Address: "0.0.0.0:2000",
	}, true))

	resp, err := http.Get(srv.URL + "/ui/sinks")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{"NMEA Out", "<code>out1</code>", "tcp", "badge-success"} {
		if !strings.Contains(body, want) {
			t.Fatalf("sinks page missing %q:\n%s", want, body)
		}
	}
}

// --- Type-fields fragment per type ---

func TestSourceTypeFieldsFragmentPerType(t *testing.T) {
	srv, _ := newUIServerWithService(t)
	cases := []struct {
		typ  string
		want []string
	}{
		{"socketcan", []string{`name="interface"`, "<datalist"}},
		{"usbcan", []string{`name="port"`, "<datalist"}},
		{"http_sse", []string{`name="url"`, `name="headers"`}},
		{"http_ws", []string{`name="url"`, `name="headers"`}},
	}
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/ui/frag/source-type-fields?type=" + tc.typ)
			if err != nil {
				t.Fatal(err)
			}
			mustStatus(t, resp, http.StatusOK)
			body := mustBody(t, resp)
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Fatalf("type=%s fragment missing %q:\n%s", tc.typ, want, body)
				}
			}
		})
	}

	// Values from the query string (hx-include="closest form" resends the
	// whole form) are preserved into the rendered field.
	resp, err := http.Get(srv.URL + "/ui/frag/source-type-fields?type=socketcan&interface=can5")
	if err != nil {
		t.Fatal(err)
	}
	body := mustBody(t, resp)
	if !strings.Contains(body, `value="can5"`) {
		t.Fatalf("type-fields fragment did not preserve interface value:\n%s", body)
	}
}

func TestSinkTypeFieldsFragmentPerType(t *testing.T) {
	srv, _ := newUIServerWithService(t)
	cases := []struct {
		typ  string
		want []string
	}{
		{"socketcan", []string{`name="interface"`, "<datalist"}},
		{"usbcan", []string{`name="port"`, "<datalist"}},
		{"http_sse", []string{`name="path"`}},
		{"http_ws", []string{`name="path"`}},
		{"tcp", []string{`name="address"`}},
	}
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/ui/frag/sink-type-fields?type=" + tc.typ)
			if err != nil {
				t.Fatal(err)
			}
			mustStatus(t, resp, http.StatusOK)
			body := mustBody(t, resp)
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Fatalf("type=%s fragment missing %q:\n%s", tc.typ, want, body)
				}
			}
		})
	}
}

// --- Add/edit form fragment ---

func TestSourceFormFragEditModePreFillsAndLocksID(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "Engine", Type: model.SourceSocketCAN, Interface: "can0", Enabled: true,
	}, true))

	resp, err := http.Get(srv.URL + "/ui/frag/source-form?id=can0")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{"disabled", `type="hidden" name="id" value="can0"`, `value="Engine"`, "checked"} {
		if !strings.Contains(body, want) {
			t.Fatalf("edit form missing %q:\n%s", want, body)
		}
	}

	resp2, err := http.Get(srv.URL + "/ui/frag/source-form?id=doesnotexist")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp2, http.StatusNotFound)
}

// --- Create/update round trip ---

func TestSourceCreateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sources", url.Values{
		"id": {"can0"}, "name": {"Engine CAN"}, "type": {"socketcan"},
		"enabled": {"1"}, "interface": {"can0"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "hx-swap-oob") {
		t.Fatalf("create response missing out-of-band table swap:\n%s", body)
	}
	if !strings.Contains(body, "Engine CAN") || !strings.Contains(body, "alert-success") {
		t.Fatalf("create response missing success alert/table row:\n%s", body)
	}

	got, err := svc.GetSource(context.Background(), "can0")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "Engine CAN" || got.Interface != "can0" || !got.Enabled {
		t.Fatalf("persisted source = %+v", got)
	}

	resp2, err := http.Get(srv.URL + "/ui/sources")
	if err != nil {
		t.Fatal(err)
	}
	if body2 := mustBody(t, resp2); !strings.Contains(body2, "Engine CAN") {
		t.Fatalf("GET /ui/sources does not reflect created source:\n%s", body2)
	}
}

func TestSourceUpdateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "Old Name", Type: model.SourceSocketCAN, Interface: "can0",
	}, true))

	resp := postForm(t, srv, "/ui/sources/can0", url.Values{
		"id": {"can0"}, "name": {"New Name"}, "type": {"socketcan"}, "interface": {"can1"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "New Name") {
		t.Fatalf("update response missing new name:\n%s", body)
	}

	got, err := svc.GetSource(context.Background(), "can0")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "New Name" || got.Interface != "can1" {
		t.Fatalf("persisted source = %+v", got)
	}
}

func TestSourceHeadersRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sources", url.Values{
		"id": {"sse1"}, "name": {"SSE"}, "type": {"http_sse"},
		"url":     {"https://example.com/stream"},
		"headers": {"Authorization: Bearer tok\nX-Custom: value"},
	})
	mustStatus(t, resp, http.StatusOK)

	got, err := svc.GetSource(context.Background(), "sse1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Headers["Authorization"] != "Bearer tok" || got.Headers["X-Custom"] != "value" {
		t.Fatalf("parsed headers = %+v", got.Headers)
	}

	resp2, err := http.Get(srv.URL + "/ui/frag/source-form?id=sse1")
	if err != nil {
		t.Fatal(err)
	}
	body := mustBody(t, resp2)
	if !strings.Contains(body, "Authorization: Bearer tok") || !strings.Contains(body, "X-Custom: value") {
		t.Fatalf("edit form did not round-trip headers text:\n%s", body)
	}
}

func TestSinkCreateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"out1"}, "name": {"NMEA Out"}, "type": {"tcp"},
		"enabled": {"1"}, "address": {"0.0.0.0:2000"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "hx-swap-oob") || !strings.Contains(body, "alert-success") {
		t.Fatalf("create response missing oob swap/success alert:\n%s", body)
	}

	got, err := svc.GetSink(context.Background(), "out1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Address != "0.0.0.0:2000" || !got.Enabled {
		t.Fatalf("persisted sink = %+v", got)
	}
}

// --- Validation errors: 200 + alert + preserved values, never a 500 ---

func TestSourceCreateValidationErrorRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	// socketcan requires an interface; omit it.
	resp := postForm(t, srv, "/ui/sources", url.Values{
		"id": {"bad"}, "name": {"Bad Source"}, "type": {"socketcan"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") {
		t.Fatalf("expected a validation alert:\n%s", body)
	}
	if !strings.Contains(body, `value="Bad Source"`) {
		t.Fatalf("expected the submitted Name to be preserved:\n%s", body)
	}
	if _, err := svc.GetSource(context.Background(), "bad"); err == nil {
		t.Fatal("invalid source should not have been persisted")
	}
}

func TestSourceCreateMalformedHeadersRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sources", url.Values{
		"id": {"sse1"}, "name": {"SSE Source"}, "type": {"http_sse"},
		"url":     {"https://example.com/stream"},
		"headers": {"not-a-valid-header-line"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") {
		t.Fatalf("expected a validation alert for malformed headers:\n%s", body)
	}
	if !strings.Contains(body, "not-a-valid-header-line") {
		t.Fatalf("expected the submitted headers text to be preserved:\n%s", body)
	}
	if _, err := svc.GetSource(context.Background(), "sse1"); err == nil {
		t.Fatal("source with malformed headers should not have been persisted")
	}
}

func TestSourceCreateExistingIDRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "Engine", Type: model.SourceSocketCAN, Interface: "can0",
	}, true))

	resp := postForm(t, srv, "/ui/sources", url.Values{
		"id": {"can0"}, "name": {"Dup"}, "type": {"socketcan"}, "interface": {"can1"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") || !strings.Contains(body, "already exists") {
		t.Fatalf("expected an already-exists alert:\n%s", body)
	}
}

func TestSinkCreateValidationErrorRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"bad"}, "name": {"Bad Sink"}, "type": {"tcp"}, "address": {"not-a-valid-address"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") {
		t.Fatalf("expected a validation alert:\n%s", body)
	}
	if !strings.Contains(body, `value="Bad Sink"`) {
		t.Fatalf("expected the submitted Name to be preserved:\n%s", body)
	}
	if _, err := svc.GetSink(context.Background(), "bad"); err == nil {
		t.Fatal("invalid sink should not have been persisted")
	}
}

// --- Delete + delete-in-use ---

func TestSourceDeleteAndDeleteInUse(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "src1", Name: "Source One", Type: model.SourceSocketCAN, Interface: "can0"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "sink1", Name: "Sink One", Type: model.SinkTCP, Address: "127.0.0.1:9000"}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "conn1", Name: "Conn One", SourceID: "src1", SinkID: "sink1"}, true))

	resp := postForm(t, srv, "/ui/sources/src1/delete", nil)
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "conn1") || !strings.Contains(body, "alert-error") {
		t.Fatalf("expected an in-use alert naming conn1:\n%s", body)
	}
	if _, err := svc.GetSource(ctx, "src1"); err != nil {
		t.Fatalf("source should still exist after a failed (in-use) delete: %v", err)
	}

	must(t, svc.DeleteConnector(ctx, "conn1"))
	resp2 := postForm(t, srv, "/ui/sources/src1/delete", nil)
	mustStatus(t, resp2, http.StatusOK)
	body2 := mustBody(t, resp2)
	if !strings.Contains(body2, "alert-success") {
		t.Fatalf("expected a success alert:\n%s", body2)
	}
	if strings.Contains(body2, "<code>src1</code>") {
		t.Fatalf("source row should be gone after delete:\n%s", body2)
	}
	if _, err := svc.GetSource(ctx, "src1"); !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("GetSource after delete: err = %v, want ErrNotFound", err)
	}
}

func TestSinkDeleteAndDeleteInUse(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "src1", Name: "Source One", Type: model.SourceSocketCAN, Interface: "can0"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "sink1", Name: "Sink One", Type: model.SinkTCP, Address: "127.0.0.1:9000"}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "conn1", Name: "Conn One", SourceID: "src1", SinkID: "sink1"}, true))

	resp := postForm(t, srv, "/ui/sinks/sink1/delete", nil)
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "conn1") || !strings.Contains(body, "alert-error") {
		t.Fatalf("expected an in-use alert naming conn1:\n%s", body)
	}

	must(t, svc.DeleteConnector(ctx, "conn1"))
	resp2 := postForm(t, srv, "/ui/sinks/sink1/delete", nil)
	mustStatus(t, resp2, http.StatusOK)
	body2 := mustBody(t, resp2)
	if !strings.Contains(body2, "alert-success") {
		t.Fatalf("expected a success alert:\n%s", body2)
	}
	if _, err := svc.GetSink(ctx, "sink1"); !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("GetSink after delete: err = %v, want ErrNotFound", err)
	}
}

func TestSourceDeleteUnknownID(t *testing.T) {
	srv, _ := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sources/nope/delete", nil)
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") || !strings.Contains(body, "not found") {
		t.Fatalf("expected a not-found alert:\n%s", body)
	}
}
