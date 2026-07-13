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
	"time"

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

// newUIServerWithServiceAndRegistry mirrors newUIServerWithService but also
// returns the *stats.Registry backing it, for tests that need to seed
// counters/queue depth directly (the connector stats fragment/detail page
// tests) rather than depending on a running pipeline recording them.
func newUIServerWithServiceAndRegistry(t *testing.T) (*httptest.Server, *config.Service, *stats.Registry) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := config.NewService(st, fakeReconciler{}, nil)
	reg := stats.NewRegistry()
	handler := ui.Handler(svc, reg, fakeReconciler{}.Statuses, "test", nil)

	mux := http.NewServeMux()
	mux.Handle("/ui/", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, svc, reg
}

// seedSourceSink puts a minimal valid source and sink into svc, for
// connector tests that just need a valid source_id/sink_id pair to
// reference and don't care about the source/sink's own fields.
func seedSourceSink(t *testing.T, svc *config.Service) {
	t.Helper()
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "src1", Name: "Source One", Type: model.SourceSocketCAN, Interface: "can0"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "sink1", Name: "Sink One", Type: model.SinkTCP, Address: "127.0.0.1:9000"}, true))
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

	// A query-string value for the SELECTED type's own field is rendered
	// back into that field (hx-include="closest form" resends whatever
	// inputs are currently in the DOM). This holds only for the selected
	// type: another type's fields aren't in the DOM to be resent, so their
	// values do NOT survive an A→B→A type switch — deliberate, see
	// sourceTypeFieldsData's doc comment in forms.go.
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
	// The success alert must auto-dismiss (plan: "success toast ...
	// auto-dismiss"): the alert carries the data-autodismiss marker and the
	// swapped-in panel carries the hx-on::load timer that removes it.
	if !strings.Contains(body, `role="alert" data-autodismiss`) || !strings.Contains(body, "hx-on::load") {
		t.Fatalf("create response missing auto-dismiss mechanism (data-autodismiss + hx-on::load):\n%s", body)
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
	if !strings.Contains(body, `role="alert" data-autodismiss`) || !strings.Contains(body, "hx-on::load") {
		t.Fatalf("create response missing auto-dismiss mechanism (data-autodismiss + hx-on::load):\n%s", body)
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
	// Error alerts must NOT auto-dismiss — only success alerts carry the
	// data-autodismiss marker the panel's hx-on::load timer looks for.
	if strings.Contains(body, `role="alert" data-autodismiss`) {
		t.Fatalf("error alert must not carry the auto-dismiss marker:\n%s", body)
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
	if !strings.Contains(body2, `role="alert" data-autodismiss`) {
		t.Fatalf("delete success alert missing the auto-dismiss marker:\n%s", body2)
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

// --- Connectors ---

func TestConnectorsPageRendersConfiguredEntities(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "conn1", Name: "NMEA Bridge", SourceID: "src1", SinkID: "sink1",
		Filters: []string{"msg.pgn == 127250"}, Enabled: true,
	}, true))

	resp, err := http.Get(srv.URL + "/ui/connectors")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		`href="/ui/connectors/conn1"`, "NMEA Bridge",
		"<code>src1</code>", "<code>sink1</code>",
		"badge-success",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("connectors page missing %q:\n%s", want, body)
		}
	}
	// No traffic recorded yet for conn1: reg.Snapshot returns its zero
	// value for an id it has never seen (see connectorRows), so queue
	// depth/msg-per-second render as zero rather than being omitted.
	if !strings.Contains(body, "<td>0</td>") || !strings.Contains(body, "<td>0.00</td>") {
		t.Fatalf("connectors page did not render zero stats for a connector with no recorded traffic:\n%s", body)
	}
}

func TestConnectorFormFragEditModePreFillsAndLocksID(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "conn1", Name: "NMEA Bridge", SourceID: "src1", SinkID: "sink1",
		Filters: []string{"msg.pgn == 127250", "msg.priority == 4"},
		Buffer:  model.BufferLimits{MaxMessages: 500, MaxAge: model.Duration(24 * time.Hour), MaxBytes: 1048576},
		Enabled: true,
	}, true))

	resp, err := http.Get(srv.URL + "/ui/frag/connector-form?id=conn1")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"disabled", `type="hidden" name="id" value="conn1"`, `value="NMEA Bridge"`, "checked",
		"msg.pgn == 127250", "msg.priority == 4",
		`value="500"`, `value="24h0m0s"`, `value="1048576"`,
		`value="src1" selected`, `value="sink1" selected`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("edit form missing %q:\n%s", want, body)
		}
	}

	resp2, err := http.Get(srv.URL + "/ui/frag/connector-form?id=doesnotexist")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp2, http.StatusNotFound)
}

func TestConnectorCreateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)

	resp := postForm(t, srv, "/ui/connectors", url.Values{
		"id": {"conn1"}, "name": {"NMEA Bridge"}, "source_id": {"src1"}, "sink_id": {"sink1"},
		"enabled":      {"1"},
		"filters":      {"msg.pgn == 127250\n\n  msg.priority < 4  \n"},
		"max_messages": {"500"}, "max_age": {"24h"}, "max_bytes": {"1048576"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "hx-swap-oob") || !strings.Contains(body, "alert-success") {
		t.Fatalf("create response missing oob swap/success alert:\n%s", body)
	}
	if !strings.Contains(body, `role="alert" data-autodismiss`) || !strings.Contains(body, "hx-on::load") {
		t.Fatalf("create response missing auto-dismiss mechanism (data-autodismiss + hx-on::load):\n%s", body)
	}

	got, err := svc.GetConnector(context.Background(), "conn1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Filters) != 2 || got.Filters[0] != "msg.pgn == 127250" || got.Filters[1] != "msg.priority < 4" {
		t.Fatalf("persisted filters = %v, want [msg.pgn == 127250 msg.priority < 4] (blank lines dropped, surrounding whitespace trimmed)", got.Filters)
	}
	if got.Buffer.MaxMessages != 500 || time.Duration(got.Buffer.MaxAge) != 24*time.Hour || got.Buffer.MaxBytes != 1048576 {
		t.Fatalf("persisted buffer = %+v", got.Buffer)
	}
	if !got.Enabled || got.SourceID != "src1" || got.SinkID != "sink1" {
		t.Fatalf("persisted connector = %+v", got)
	}

	resp2, err := http.Get(srv.URL + "/ui/connectors")
	if err != nil {
		t.Fatal(err)
	}
	if body2 := mustBody(t, resp2); !strings.Contains(body2, "NMEA Bridge") {
		t.Fatalf("GET /ui/connectors does not reflect created connector:\n%s", body2)
	}
}

func TestConnectorUpdateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "conn1", Name: "Old Name", SourceID: "src1", SinkID: "sink1",
	}, true))

	resp := postForm(t, srv, "/ui/connectors/conn1", url.Values{
		"id": {"conn1"}, "name": {"New Name"}, "source_id": {"src1"}, "sink_id": {"sink1"},
		"max_age": {"90s"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "New Name") {
		t.Fatalf("update response missing new name:\n%s", body)
	}

	got, err := svc.GetConnector(context.Background(), "conn1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "New Name" || time.Duration(got.Buffer.MaxAge) != 90*time.Second {
		t.Fatalf("persisted connector = %+v", got)
	}
}

func TestConnectorMaxAgeParseErrorRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)

	resp := postForm(t, srv, "/ui/connectors", url.Values{
		"id": {"conn1"}, "name": {"Bad Connector"}, "source_id": {"src1"}, "sink_id": {"sink1"},
		"max_age": {"notaduration"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") {
		t.Fatalf("expected a validation alert:\n%s", body)
	}
	if !strings.Contains(body, `value="Bad Connector"`) || !strings.Contains(body, `value="notaduration"`) {
		t.Fatalf("expected the submitted Name/max_age to be preserved:\n%s", body)
	}
	if _, err := svc.GetConnector(context.Background(), "conn1"); err == nil {
		t.Fatal("connector with an unparseable max_age should not have been persisted")
	}
}

func TestConnectorCreateValidationErrorRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	// source_id is required (model.Connector.Validate); omit it.
	resp := postForm(t, srv, "/ui/connectors", url.Values{
		"id": {"conn1"}, "name": {"No Source"}, "sink_id": {"sink1"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") {
		t.Fatalf("expected a validation alert:\n%s", body)
	}
	if !strings.Contains(body, `value="No Source"`) {
		t.Fatalf("expected the submitted Name to be preserved:\n%s", body)
	}
	if _, err := svc.GetConnector(context.Background(), "conn1"); err == nil {
		t.Fatal("invalid connector should not have been persisted")
	}
}

func TestConnectorCreateExistingIDRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "conn1", Name: "Existing", SourceID: "src1", SinkID: "sink1",
	}, true))

	resp := postForm(t, srv, "/ui/connectors", url.Values{
		"id": {"conn1"}, "name": {"Dup"}, "source_id": {"src1"}, "sink_id": {"sink1"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") || !strings.Contains(body, "already exists") {
		t.Fatalf("expected an already-exists alert:\n%s", body)
	}
}

func TestConnectorDeleteRoundTripAndUnknownID(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	ctx := context.Background()
	must(t, svc.PutConnector(ctx, model.Connector{ID: "conn1", Name: "Conn One", SourceID: "src1", SinkID: "sink1"}, true))

	resp := postForm(t, srv, "/ui/connectors/conn1/delete", nil)
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-success") {
		t.Fatalf("expected a success alert:\n%s", body)
	}
	if !strings.Contains(body, `role="alert" data-autodismiss`) {
		t.Fatalf("delete success alert missing the auto-dismiss marker:\n%s", body)
	}
	if strings.Contains(body, "Conn One") || strings.Contains(body, "/ui/connectors/conn1") {
		t.Fatalf("connector row should be gone after delete:\n%s", body)
	}
	if _, err := svc.GetConnector(ctx, "conn1"); !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("GetConnector after delete: err = %v, want ErrNotFound", err)
	}

	// No ErrInUse case for connector delete (config.Service:
	// "nothing else references a connector") — the only failure mode left
	// to exercise is unknown id.
	resp2 := postForm(t, srv, "/ui/connectors/nope/delete", nil)
	mustStatus(t, resp2, http.StatusOK)
	body2 := mustBody(t, resp2)
	if !strings.Contains(body2, "alert-error") || !strings.Contains(body2, "not found") {
		t.Fatalf("expected a not-found alert:\n%s", body2)
	}
}

// --- CEL validate-on-blur fragment ---

func TestValidateFiltersFragmentHappyAndError(t *testing.T) {
	srv, _ := newUIServerWithService(t)

	resp := postForm(t, srv, "/ui/frag/validate-filters", url.Values{"filters": {"msg.pgn == 127250"}})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "filters OK") {
		t.Fatalf("expected a filters-OK line for a valid expression:\n%s", body)
	}

	resp2 := postForm(t, srv, "/ui/frag/validate-filters", url.Values{"filters": {"not a valid ((( expr"}})
	mustStatus(t, resp2, http.StatusOK)
	body2 := mustBody(t, resp2)
	if !strings.Contains(body2, "alert-error") {
		t.Fatalf("expected an error alert for an invalid CEL expression:\n%s", body2)
	}
}

// --- Detail page + live stats ---

func TestConnectorStatsFragmentRendersSnapshotNumbers(t *testing.T) {
	srv, svc, reg := newUIServerWithServiceAndRegistry(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{ID: "conn1", Name: "Conn One", SourceID: "src1", SinkID: "sink1"}, true))

	reg.Record("conn1", 42, 20480)
	reg.SetQueue("conn1", 7, 4096)

	resp, err := http.Get(srv.URL + "/ui/frag/connectors/conn1/stats")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	// Rates are computed over the trailing 10s window (internal/stats):
	// 42 total messages -> 4.20 msg/s; 20480 bytes -> 2048 B/s -> 2.00
	// KB/s. Queue depth 7; queue bytes 4096 -> 4.00 KB (not a rate, so no
	// window division). See humanizeBytes for the B/KB/MB unit selection.
	for _, want := range []string{"42", "4.20", "2.00 KB/s", "7", "4.00 KB"} {
		if !strings.Contains(body, want) {
			t.Fatalf("stats fragment missing %q:\n%s", want, body)
		}
	}

	resp2, err := http.Get(srv.URL + "/ui/frag/connectors/doesnotexist/stats")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp2, http.StatusNotFound)
}

func TestConnectorDetailPageRendersConfigSummary(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "conn1", Name: "NMEA Bridge", SourceID: "src1", SinkID: "sink1",
		Filters: []string{"msg.pgn == 127250"},
		Buffer:  model.BufferLimits{MaxMessages: 500, MaxAge: model.Duration(24 * time.Hour), MaxBytes: 1048576},
		Enabled: true,
	}, true))

	resp, err := http.Get(srv.URL + "/ui/connectors/conn1")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"NMEA Bridge", "<code>src1</code>", "<code>sink1</code>", "badge-success",
		"msg.pgn == 127250", "500", "24h0m0s", "1048576",
		`hx-get="/ui/frag/connectors/conn1/stats"`, `hx-trigger="load, every 2s"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("connector detail page missing %q:\n%s", want, body)
		}
	}
}

func TestConnectorDetailPage404ForUnknownID(t *testing.T) {
	srv, _ := newUIServerWithService(t)
	resp, err := http.Get(srv.URL + "/ui/connectors/doesnotexist")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusNotFound)
}
