package ui_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
	"github.com/open-ships/beacon/internal/msg"
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
	handler := ui.Handler(svc, stats.NewRegistry(), fakeReconciler{}.Statuses, nil, "test", nil)

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
	handler := ui.Handler(svc, reg, fakeReconciler{}.Statuses, nil, "test", nil)

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
	defer func() { _ = resp.Body.Close() }()
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
	for _, want := range []string{"Engine CAN", "<code>can0</code>", "socketcan", "Detail", "badge-success", `href="/ui/sources/new"`, `href="/ui/sources/can0/"`, `href="/ui/sources/can0/edit"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("sources page missing %q:\n%s", want, body)
		}
	}
}

func TestSourceNewPageOpensCreateForm(t *testing.T) {
	srv, _ := newUIServerWithService(t)

	resp, err := http.Get(srv.URL + "/ui/sources/new")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"Add source", `hx-post="/ui/sources"`, `name="id"`, `name="interface"`,
		`aria-label="Breadcrumb"`, `href="/ui/sources"`, `aria-current="page">Add source</span>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("source new page missing %q:\n%s", want, body)
		}
	}
	for _, notWant := range []string{`href="/ui/sources/new"`, `id="source-panel"`} {
		if strings.Contains(body, notWant) {
			t.Fatalf("source new page should not include %q:\n%s", notWant, body)
		}
	}
}

func TestSourceEditPageOpensEditForm(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "Engine CAN", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0",
	}, true))

	resp, err := http.Get(srv.URL + "/ui/sources/can0/edit")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"Edit source can0",
		`type="hidden" name="id" value="can0"`,
		`value="Engine CAN"`,
		`name="interface"`,
		`value="can0"`,
		`aria-label="Breadcrumb"`,
		`href="/ui/sources"`,
		`aria-current="page">Edit can0</span>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("sources page edit form missing %q:\n%s", want, body)
		}
	}
	for _, notWant := range []string{`href="/ui/sources/new"`, `id="source-panel"`} {
		if strings.Contains(body, notWant) {
			t.Fatalf("source edit page should not include %q:\n%s", notWant, body)
		}
	}

	resp2, err := http.Get(srv.URL + "/ui/sources/missing/edit")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp2, http.StatusNotFound)
}

func TestSourceOverviewPageRendersConfigAndLiveFragment(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "mqtt-in", Name: "MQTT input", Type: model.SourceMQTT, Enabled: false,
		URL: "mqtt://broker.local:1883", Topic: "vessels/main/#",
	}, true))

	resp, err := http.Get(srv.URL + "/ui/sources/mqtt-in/")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"source overview", "MQTT input", "<code>mqtt</code>",
		"<code>mqtt://broker.local:1883</code>", "<code>vessels/main/#</code>",
		`href="/ui/sources/mqtt-in/edit"`,
		`aria-label="Breadcrumb"`, `href="/ui/sources"`, `aria-current="page">MQTT input</span>`,
		`hx-get="/ui/frag/sources/mqtt-in/overview"`, `hx-trigger="load, every 2s"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("source overview page missing %q:\n%s", want, body)
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
	for _, want := range []string{"NMEA Out", "<code>out1</code>", "tcp", "<code>0.0.0.0:2000</code>", "badge-success", `href="/ui/sinks/new"`, `href="/ui/sinks/out1/"`, `href="/ui/sinks/out1/edit"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("sinks page missing %q:\n%s", want, body)
		}
	}
}

func TestSinkNewPageOpensCreateForm(t *testing.T) {
	srv, _ := newUIServerWithService(t)

	resp, err := http.Get(srv.URL + "/ui/sinks/new")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"Add sink", `hx-post="/ui/sinks"`, `name="id"`, `name="interface"`,
		`aria-label="Breadcrumb"`, `href="/ui/sinks"`, `aria-current="page">Add sink</span>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("sink new page missing %q:\n%s", want, body)
		}
	}
	for _, notWant := range []string{`href="/ui/sinks/new"`, `id="sink-panel"`} {
		if strings.Contains(body, notWant) {
			t.Fatalf("sink new page should not include %q:\n%s", notWant, body)
		}
	}
}

func TestSinkEditPageOpensEditForm(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSink(context.Background(), model.Sink{
		ID: "out1", Name: "NMEA Out", Type: model.SinkTCP, Enabled: true, Address: "0.0.0.0:2000",
	}, true))

	resp, err := http.Get(srv.URL + "/ui/sinks/out1/edit")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"Edit sink out1",
		`type="hidden" name="id" value="out1"`,
		`value="NMEA Out"`,
		`name="address"`,
		`value="0.0.0.0:2000"`,
		`aria-label="Breadcrumb"`,
		`href="/ui/sinks"`,
		`aria-current="page">Edit out1</span>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("sinks page edit form missing %q:\n%s", want, body)
		}
	}
	for _, notWant := range []string{`href="/ui/sinks/new"`, `id="sink-panel"`} {
		if strings.Contains(body, notWant) {
			t.Fatalf("sink edit page should not include %q:\n%s", notWant, body)
		}
	}

	resp2, err := http.Get(srv.URL + "/ui/sinks/missing/edit")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp2, http.StatusNotFound)
}

func TestSinkOverviewPageRendersConfigAndLiveFragment(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSink(context.Background(), model.Sink{
		ID: "mqtt-out", Name: "MQTT output", Type: model.SinkMQTT, Enabled: false,
		URL: "mqtt://broker.local:1883", Topic: "vessels/main/json",
	}, true))

	resp, err := http.Get(srv.URL + "/ui/sinks/mqtt-out/")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"sink overview", "MQTT output", "<code>mqtt</code>",
		"<code>mqtt://broker.local:1883</code>", "<code>vessels/main/json</code>",
		`href="/ui/sinks/mqtt-out/edit"`,
		`aria-label="Breadcrumb"`, `href="/ui/sinks"`, `aria-current="page">MQTT output</span>`,
		`hx-get="/ui/frag/sinks/mqtt-out/overview"`, `hx-trigger="load, every 2s"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("sink overview page missing %q:\n%s", want, body)
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
		{"mqtt", []string{`name="url"`, `name="topic"`, `mqtt://broker.local:1883`}},
		{"tcp", []string{`name="address"`, `name="format"`, "ydraw", "actisense"}},
		{"udp", []string{`name="address"`, `name="format"`, "ydraw", "actisense"}},
		{"file", []string{`name="file_path"`}},
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
		{"file", []string{`name="file_path"`, `name="format"`, `name="max_file_bytes"`, `name="max_files"`, "ndjson", "candump"}},
		{"mqtt", []string{`name="url"`, `name="topic"`, `mqtt://broker.local:1883`}},
		{"tcp_gateway", []string{`name="address"`, `name="format"`, "ydraw", "actisense"}},
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

func TestSourceEditPagePreFillsAndLocksID(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSource(context.Background(), model.Source{
		ID: "can0", Name: "Engine", Type: model.SourceSocketCAN, Interface: "can0", Enabled: true,
	}, true))

	resp, err := http.Get(srv.URL + "/ui/sources/can0/edit")
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

	resp2, err := http.Get(srv.URL + "/ui/sources/doesnotexist/edit")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp2, http.StatusNotFound)
}

// --- Create/update round trip ---

// flashCookieName mirrors the unexported flashCookie const in flash.go; kept
// as a literal here because this is an external (ui_test) package.
const flashCookieName = "beacon_flash"

// assertCreateRedirect asserts resp is a create handler's flash-redirect: an
// HTTP 200 carrying an HX-Redirect to the dashboard plus a one-shot flash
// cookie whose decoded message equals wantMsg. It consumes resp.Body.
func assertCreateRedirect(t *testing.T, resp *http.Response, wantMsg string) {
	t.Helper()
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("HX-Redirect"); got != "/ui/dashboard" {
		t.Fatalf("HX-Redirect = %q, want /ui/dashboard", got)
	}
	for _, c := range resp.Cookies() {
		if c.Name != flashCookieName {
			continue
		}
		b, err := base64.URLEncoding.DecodeString(c.Value)
		if err != nil {
			t.Fatalf("flash cookie value not base64: %v", err)
		}
		if string(b) != wantMsg {
			t.Fatalf("flash message = %q, want %q", b, wantMsg)
		}
		return
	}
	t.Fatalf("create response set no %q cookie", flashCookieName)
}

func TestSourceCreateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sources", url.Values{
		"id": {"can0"}, "name": {"Engine CAN"}, "type": {"socketcan"},
		"enabled": {"1"}, "interface": {"can0"},
	})
	// Create redirects the operator to the dashboard with a one-shot flash,
	// rather than swapping the table in place (that is the update path).
	assertCreateRedirect(t, resp, `Source "can0" created`)

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

	resp2, err := http.Get(srv.URL + "/ui/sources/sse1/edit")
	if err != nil {
		t.Fatal(err)
	}
	body := mustBody(t, resp2)
	if !strings.Contains(body, "Authorization: Bearer tok") || !strings.Contains(body, "X-Custom: value") {
		t.Fatalf("edit form did not round-trip headers text:\n%s", body)
	}
}

func TestMQTTSourceCreateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sources", url.Values{
		"id": {"mqtt-in"}, "name": {"MQTT input"}, "type": {"mqtt"},
		"enabled": {"1"}, "url": {"mqtt://broker.local:1883"}, "topic": {"vessels/main/engine/#"},
	})
	assertCreateRedirect(t, resp, `Source "mqtt-in" created`)

	got, err := svc.GetSource(context.Background(), "mqtt-in")
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "mqtt://broker.local:1883" || got.Topic != "vessels/main/engine/#" || !got.Enabled {
		t.Fatalf("persisted source = %+v", got)
	}

	resp2, err := http.Get(srv.URL + "/ui/sources/mqtt-in/edit")
	if err != nil {
		t.Fatal(err)
	}
	body2 := mustBody(t, resp2)
	for _, want := range []string{`name="url" value="mqtt://broker.local:1883"`, `name="topic" value="vessels/main/engine/#"`} {
		if !strings.Contains(body2, want) {
			t.Fatalf("edit form missing %q:\n%s", want, body2)
		}
	}
}

func TestSinkCreateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"out1"}, "name": {"NMEA Out"}, "type": {"tcp"},
		"enabled": {"1"}, "address": {"0.0.0.0:2000"},
	})
	assertCreateRedirect(t, resp, `Sink "out1" created`)

	got, err := svc.GetSink(context.Background(), "out1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Address != "0.0.0.0:2000" || !got.Enabled {
		t.Fatalf("persisted sink = %+v", got)
	}
}

func TestMQTTSinkCreateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"mqtt-out"}, "name": {"MQTT output"}, "type": {"mqtt"},
		"enabled": {"1"}, "url": {"mqtt://broker.local:1883"}, "topic": {"vessels/main/engine/json"},
	})
	assertCreateRedirect(t, resp, `Sink "mqtt-out" created`)

	got, err := svc.GetSink(context.Background(), "mqtt-out")
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "mqtt://broker.local:1883" || got.Topic != "vessels/main/engine/json" || !got.Enabled {
		t.Fatalf("persisted sink = %+v", got)
	}

	resp2, err := http.Get(srv.URL + "/ui/sinks/mqtt-out/edit")
	if err != nil {
		t.Fatal(err)
	}
	body2 := mustBody(t, resp2)
	for _, want := range []string{`name="url" value="mqtt://broker.local:1883"`, `name="topic" value="vessels/main/engine/json"`} {
		if !strings.Contains(body2, want) {
			t.Fatalf("edit form missing %q:\n%s", want, body2)
		}
	}
}

func TestTCPGatewaySinkCreateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"gw-out"}, "name": {"YD gateway out"}, "type": {"tcp_gateway"},
		"enabled": {"1"}, "address": {"192.168.4.1:1457"}, "format": {"ydraw"},
	})
	assertCreateRedirect(t, resp, `Sink "gw-out" created`)

	got, err := svc.GetSink(context.Background(), "gw-out")
	if err != nil {
		t.Fatal(err)
	}
	if got.Address != "192.168.4.1:1457" || got.Format != "ydraw" || !got.Enabled {
		t.Fatalf("persisted sink = %+v", got)
	}

	resp2, err := http.Get(srv.URL + "/ui/sinks/gw-out/edit")
	if err != nil {
		t.Fatal(err)
	}
	body := mustBody(t, resp2)
	for _, want := range []string{`name="address" value="192.168.4.1:1457"`, `option value="ydraw" selected`} {
		if !strings.Contains(body, want) {
			t.Fatalf("edit form missing %q:\n%s", want, body)
		}
	}
}

// --- File sink ---

// TestFileSinkCreateRoundTrip covers the file sink's four type-specific
// fields end to end: submitted through the form, persisted via
// svc.PutSink, and visible again on the sinks table (Detail column) and the
// edit form. max_file_bytes/max_files are left blank on the submission —
// blank means "use the file sink's built-in default" (model.
// DefaultMaxFileBytes/DefaultMaxFiles), which toModel's parseOptionalInt64/
// parseOptionalInt turn into 0, not an error.
func TestFileSinkCreateRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"navlog"}, "name": {"Nav log"}, "type": {"file"},
		"enabled": {"1"}, "file_path": {"/data/nav.log"}, "format": {"ndjson"},
	})
	assertCreateRedirect(t, resp, `Sink "navlog" created`)

	got, err := svc.GetSink(context.Background(), "navlog")
	if err != nil {
		t.Fatal(err)
	}
	if got.FilePath != "/data/nav.log" || got.Format != "ndjson" || got.MaxFileBytes != 0 || got.MaxFiles != 0 || !got.Enabled {
		t.Fatalf("persisted sink = %+v", got)
	}

	// The edit form round-trips file_path/format and shows the defaults as
	// placeholders (blank stored value -> blank input, default shown only
	// as a placeholder, not baked into the value).
	resp2, err := http.Get(srv.URL + "/ui/sinks/navlog/edit")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp2, http.StatusOK)
	body2 := mustBody(t, resp2)
	for _, want := range []string{
		`name="file_path" value="/data/nav.log"`,
		`option value="ndjson" selected`,
		`placeholder="104857600"`,
		`placeholder="5"`,
	} {
		if !strings.Contains(body2, want) {
			t.Fatalf("edit form missing %q:\n%s", want, body2)
		}
	}
}

// TestFileSinkMaxFileBytesAndMaxFilesRoundTrip covers explicit (non-blank)
// max_file_bytes/max_files values persisting and round-tripping back into
// the edit form as their literal values, not the defaults.
func TestFileSinkMaxFileBytesAndMaxFilesRoundTrip(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"navlog"}, "name": {"Nav log"}, "type": {"file"},
		"file_path": {"/data/nav.candump"}, "format": {"candump"},
		"max_file_bytes": {"52428800"}, "max_files": {"3"},
	})
	mustStatus(t, resp, http.StatusOK)

	got, err := svc.GetSink(context.Background(), "navlog")
	if err != nil {
		t.Fatal(err)
	}
	if got.Format != "candump" || got.MaxFileBytes != 52428800 || got.MaxFiles != 3 {
		t.Fatalf("persisted sink = %+v", got)
	}

	resp2, err := http.Get(srv.URL + "/ui/sinks/navlog/edit")
	if err != nil {
		t.Fatal(err)
	}
	body := mustBody(t, resp2)
	for _, want := range []string{
		`name="max_file_bytes" value="52428800"`,
		`name="max_files" value="3"`,
		`option value="candump" selected`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("edit form missing %q:\n%s", want, body)
		}
	}
}

// TestFileSinkCreateRelativePathRendersFormNot500 covers model.Sink.
// Validate's file_path-must-be-absolute rule surfacing as a 200 alert with
// the submitted value preserved, the same contract every other sink
// validation failure follows (see TestSinkCreateValidationErrorRendersFormNot500).
func TestFileSinkCreateRelativePathRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"navlog"}, "name": {"Nav log"}, "type": {"file"},
		"file_path": {"relative/nav.log"}, "format": {"ndjson"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") {
		t.Fatalf("expected a validation alert:\n%s", body)
	}
	if !strings.Contains(body, `name="file_path" value="relative/nav.log"`) {
		t.Fatalf("expected the submitted file_path to be preserved:\n%s", body)
	}
	if _, err := svc.GetSink(context.Background(), "navlog"); err == nil {
		t.Fatal("invalid file sink should not have been persisted")
	}
}

// TestFileSinkNonNumericMaxFileBytesRendersFormNot500 covers toModel's
// parseOptionalInt64 error path: a non-numeric max_file_bytes must become an
// inline validation alert (with the bad value preserved), never a 500 —
// the same contract TestConnectorMaxAgeParseErrorRendersFormNot500 covers
// for the connector form's buffer fields.
func TestFileSinkNonNumericMaxFileBytesRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"navlog"}, "name": {"Nav log"}, "type": {"file"},
		"file_path": {"/data/nav.log"}, "format": {"ndjson"},
		"max_file_bytes": {"not-a-number"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") {
		t.Fatalf("expected a validation alert:\n%s", body)
	}
	if !strings.Contains(body, `name="max_file_bytes" value="not-a-number"`) {
		t.Fatalf("expected the submitted max_file_bytes to be preserved:\n%s", body)
	}
	if _, err := svc.GetSink(context.Background(), "navlog"); err == nil {
		t.Fatal("file sink with a non-numeric max_file_bytes should not have been persisted")
	}
}

// TestFileSinkNonNumericMaxFilesRendersFormNot500 is
// TestFileSinkNonNumericMaxFileBytesRendersFormNot500's counterpart for
// max_files (parseOptionalInt, the int-valued sibling of parseOptionalInt64
// — see forms.go).
func TestFileSinkNonNumericMaxFilesRendersFormNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"navlog"}, "name": {"Nav log"}, "type": {"file"},
		"file_path": {"/data/nav.log"}, "format": {"ndjson"},
		"max_files": {"not-a-number"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") {
		t.Fatalf("expected a validation alert:\n%s", body)
	}
	if !strings.Contains(body, `name="max_files" value="not-a-number"`) {
		t.Fatalf("expected the submitted max_files to be preserved:\n%s", body)
	}
	if _, err := svc.GetSink(context.Background(), "navlog"); err == nil {
		t.Fatal("file sink with a non-numeric max_files should not have been persisted")
	}
}

// TestSinksPageShowsFilePathDetailForFileSink covers the sinks table's
// Detail column (frag_sink_table.html) rendering the file path for a file
// sink on the plain page load, not just the create-response OOB swap
// TestFileSinkCreateRoundTrip already covers.
func TestSinksPageShowsFilePathDetailForFileSink(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	must(t, svc.PutSink(context.Background(), model.Sink{
		ID: "navlog", Name: "Nav log", Type: model.SinkFile, Enabled: true,
		FilePath: "/data/nav.log", Format: model.FileFormatNDJSON,
	}, true))

	resp, err := http.Get(srv.URL + "/ui/sinks")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{"Nav log", "<code>navlog</code>", "file", "<code>/data/nav.log</code>"} {
		if !strings.Contains(body, want) {
			t.Fatalf("sinks page missing %q:\n%s", want, body)
		}
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

// TestSourceCreateParseFormErrorRendersAlertNot500 exercises writeSource's
// r.ParseForm() error branch — previously untested. A malformed percent-
// escape in the request's raw query string ("%zz") makes url.ParseQuery
// fail, which r.ParseForm() surfaces as a non-nil error even though the
// POST body itself is well-formed; this is a reliable, portable way to
// trigger the branch without depending on multipart-parsing edge cases.
// writeSink/writeConnector have the identical branch (see writeSource's
// doc comment); this one test stands in for all three, same as the rest of
// this file only exercising the sources-side variant of parallel source/
// sink logic (see forms.go's package doc comment).
func TestSourceCreateParseFormErrorRendersAlertNot500(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/ui/sources?%zz",
		strings.NewReader(url.Values{"id": {"bad"}, "name": {"Bad"}, "type": {"socketcan"}}.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") || !strings.Contains(body, "invalid form submission") {
		t.Fatalf("expected an invalid-form-submission alert:\n%s", body)
	}
	if _, err := svc.GetSource(context.Background(), "bad"); err == nil {
		t.Fatal("a request with a ParseForm error should not have persisted anything")
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
	// The route column shows the source/sink NAMES ("Source One"/"Sink
	// One" — seedSourceSink's names), not their raw ids: see
	// TestConnectorsPageNameFallsBackToIDWhenEmpty for the fallback case.
	for _, want := range []string{
		`href="/ui/connectors/new"`, `href="/ui/connectors/conn1/"`, `href="/ui/connectors/conn1/edit"`, "NMEA Bridge",
		"Source One", "Sink One",
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

// TestConnectorsPageNameFallsBackToIDWhenEmpty covers the other half of
// "names not raw ids" (see TestConnectorsPageRendersConfiguredEntities): a
// source/sink with no Name set (model.Source/Sink don't require one — see
// model/validate.go) renders its raw id in the connectors table instead of
// an empty cell.
func TestConnectorsPageNameFallsBackToIDWhenEmpty(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "src1", Type: model.SourceSocketCAN, Interface: "can0"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "sink1", Type: model.SinkTCP, Address: "127.0.0.1:9000"}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "conn1", Name: "Bridge", SourceID: "src1", SinkID: "sink1"}, true))

	resp, err := http.Get(srv.URL + "/ui/connectors")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "src1") || !strings.Contains(body, "sink1") {
		t.Fatalf("connectors page should fall back to the raw id when name is empty:\n%s", body)
	}
}

func TestConnectorNewPageOpensCreateForm(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)

	resp, err := http.Get(srv.URL + "/ui/connectors/new")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"Add connector",
		`hx-post="/ui/connectors"`,
		`name="id"`,
		`value="src1"`,
		`value="sink1"`,
		`aria-label="Breadcrumb"`,
		`href="/ui/connectors"`,
		`aria-current="page">Add connector</span>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("connector new page missing %q:\n%s", want, body)
		}
	}
}

func TestConnectorEditPageOpensEditForm(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "conn1", Name: "NMEA Bridge", SourceID: "src1", SinkID: "sink1",
		Filters: []string{"msg.pgn == 127250"},
		Buffer:  model.BufferLimits{MaxMessages: 500, MaxAge: model.Duration(24 * time.Hour), MaxBytes: 1048576},
		Enabled: true,
	}, true))

	resp, err := http.Get(srv.URL + "/ui/connectors/conn1/edit")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	for _, want := range []string{
		"Edit connector conn1",
		`type="hidden" name="id" value="conn1"`,
		`value="NMEA Bridge"`,
		"checked",
		`value="src1" selected`,
		`value="sink1" selected`,
		"msg.pgn == 127250",
		`value="500"`,
		`value="24h0m0s"`,
		`value="1048576"`,
		`aria-label="Breadcrumb"`,
		`href="/ui/connectors"`,
		`aria-current="page">Edit conn1</span>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("connector edit page missing %q:\n%s", want, body)
		}
	}

	resp2, err := http.Get(srv.URL + "/ui/connectors/doesnotexist/edit")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp2, http.StatusNotFound)
}

func TestConnectorEditPagePreFillsAndLocksID(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{
		ID: "conn1", Name: "NMEA Bridge", SourceID: "src1", SinkID: "sink1",
		Filters: []string{"msg.pgn == 127250", "msg.priority == 4"},
		Buffer:  model.BufferLimits{MaxMessages: 500, MaxAge: model.Duration(24 * time.Hour), MaxBytes: 1048576},
		Enabled: true,
	}, true))

	resp, err := http.Get(srv.URL + "/ui/connectors/conn1/edit")
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

	resp2, err := http.Get(srv.URL + "/ui/connectors/doesnotexist/edit")
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
	assertCreateRedirect(t, resp, `Connector "conn1" created`)

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

// TestCreateFlashShownOnDashboardOnce follows a create through its
// HX-Redirect: the dashboard renders the flash banner once and clears the
// cookie, so a subsequent load never shows it again.
func TestCreateFlashShownOnDashboardOnce(t *testing.T) {
	srv, _ := newUIServerWithService(t)
	resp := postForm(t, srv, "/ui/sinks", url.Values{
		"id": {"out1"}, "name": {"NMEA Out"}, "type": {"tcp"},
		"enabled": {"1"}, "address": {"0.0.0.0:2000"},
	})
	_ = resp.Body.Close()
	var flash *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == flashCookieName {
			flash = c
		}
	}
	if flash == nil {
		t.Fatal("create set no flash cookie to carry to the dashboard")
	}

	// The dashboard (the redirect target) renders the flash once...
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ui/dashboard", nil)
	req.AddCookie(flash)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body := mustBody(t, resp2)
	// The message renders with html/template's quote escaping (" -> &#34;).
	if !strings.Contains(body, "alert-success") || !strings.Contains(body, `Sink &#34;out1&#34; created`) {
		t.Fatalf("dashboard did not render the flash message:\n%s", body)
	}
	// ...and clears the cookie so it can never show twice.
	cleared := false
	for _, c := range resp2.Cookies() {
		if c.Name == flashCookieName && c.Value == "" {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("dashboard did not clear the flash cookie after rendering it")
	}

	// A dashboard load without the cookie shows no flash at all.
	resp3, err := http.Get(srv.URL + "/ui/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	if body3 := mustBody(t, resp3); strings.Contains(body3, `Sink &#34;out1&#34; created`) {
		t.Fatalf("flash leaked onto a cookieless dashboard load:\n%s", body3)
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

// TestConnectorMaxAgeParseErrorPreservesSelectionsAndFilters extends
// TestConnectorMaxAgeParseErrorRendersFormNot500's coverage: previously
// nothing asserted that the re-rendered form actually preserves the
// operator's source/sink <select> choices and filters textarea content, as
// opposed to merely showing an error and not persisting. See
// connectorFormViewFromRequest (built from the raw POSTed values, not
// toModel's parsed/reformatted round trip) and frag_connector_form.html's
// "selected"/textarea rendering.
func TestConnectorMaxAgeParseErrorPreservesSelectionsAndFilters(t *testing.T) {
	srv, svc := newUIServerWithService(t)
	seedSourceSink(t, svc)

	resp := postForm(t, srv, "/ui/connectors", url.Values{
		"id": {"conn1"}, "name": {"Bad Connector"}, "source_id": {"src1"}, "sink_id": {"sink1"},
		"filters": {"msg.pgn == 127250\nmsg.priority == 4"},
		"max_age": {"notaduration"},
	})
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "alert-error") {
		t.Fatalf("expected a validation alert:\n%s", body)
	}
	for _, want := range []string{
		`value="src1" selected`, `value="sink1" selected`,
		"msg.pgn == 127250", "msg.priority == 4",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected the submitted source/sink selection and filters to survive re-render, missing %q:\n%s", want, body)
		}
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
	// The polling attributes live on the fragment's own wrapper element
	// (swapped outerHTML into connector_detail.html's #connector-stats),
	// not on a stable parent — see TestConnectorStatsFragmentUnknownID
	// DeletedNoticeHaltsPolling for why that matters.
	if !strings.Contains(body, `hx-get="/ui/frag/connectors/conn1/stats"`) || !strings.Contains(body, `hx-trigger="load, every 2s"`) {
		t.Fatalf("stats fragment missing its own polling attributes:\n%s", body)
	}
}

// TestConnectorStatsFragmentRendersSparkline covers the queue-depth
// sparkline (spec §6, closed out in Phase 4): after two SetQueue calls with
// different depths, DepthHistory has 2 samples, so the fragment's inline
// SVG <polyline> must carry at least 2 "x,y" point pairs.
func TestConnectorStatsFragmentRendersSparkline(t *testing.T) {
	srv, svc, reg := newUIServerWithServiceAndRegistry(t)
	seedSourceSink(t, svc)
	must(t, svc.PutConnector(context.Background(), model.Connector{ID: "conn1", Name: "Conn One", SourceID: "src1", SinkID: "sink1"}, true))

	reg.SetQueue("conn1", 2, 200)
	reg.SetQueue("conn1", 9, 900)

	resp, err := http.Get(srv.URL + "/ui/frag/connectors/conn1/stats")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(body, "<polyline") {
		t.Fatalf("stats fragment missing sparkline <polyline>:\n%s", body)
	}
	i := strings.Index(body, `points="`)
	if i == -1 {
		t.Fatalf("stats fragment <polyline> missing points attribute:\n%s", body)
	}
	rest := body[i+len(`points="`):]
	points := rest[:strings.Index(rest, `"`)]
	if n := len(strings.Fields(points)); n < 2 {
		t.Fatalf("sparkline points = %q, want >= 2 point pairs, got %d", points, n)
	}
}

// TestConnectorStatsFragmentUnknownIDDeletedNoticeHaltsPolling covers the
// "connector deleted while its detail page is still open" case: the
// polling container's hx-trigger lives on the element the fragment
// response itself replaces (hx-swap="outerHTML" — see connector_detail.html
// and frag_connector_stats.html's comments), so a 404 response wouldn't
// swap at all and the poll would fire forever against a connector that no
// longer exists. Instead, an unknown id gets a 200 "no longer exists"
// notice that omits every polling attribute: once htmx swaps it in, the
// element that used to carry hx-trigger is gone, and nothing re-polls.
func TestConnectorStatsFragmentUnknownIDDeletedNoticeHaltsPolling(t *testing.T) {
	srv, _ := newUIServerWithService(t)

	resp, err := http.Get(srv.URL + "/ui/frag/connectors/doesnotexist/stats")
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, http.StatusOK)
	body := mustBody(t, resp)
	if !strings.Contains(strings.ToLower(body), "no longer exists") {
		t.Fatalf("expected a deleted-connector notice:\n%s", body)
	}
	if strings.Contains(body, "hx-trigger") {
		t.Fatalf("deleted-connector notice must carry no hx-trigger (it would keep polling):\n%s", body)
	}
	if strings.Contains(body, "hx-get") {
		t.Fatalf("deleted-connector notice must carry no hx-get (it would keep polling):\n%s", body)
	}
}

func TestOverviewFragmentsRenderStatsAndMessageStreams(t *testing.T) {
	srv, svc, reg := newUIServerWithServiceAndRegistry(t)
	ctx := context.Background()
	must(t, svc.PutSource(ctx, model.Source{ID: "src1", Name: "Source One", Type: model.SourceSocketCAN, Enabled: true, Interface: "can0"}, true))
	must(t, svc.PutSink(ctx, model.Sink{ID: "sink1", Name: "Sink One", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:9000"}, true))
	must(t, svc.PutConnector(ctx, model.Connector{ID: "conn1", Name: "NMEA Bridge", SourceID: "src1", SinkID: "sink1", Enabled: true}, true))

	env := &msg.Envelope{
		PGN: 127250, Source: 12, Dest: 255, Priority: 2,
		Payload: json.RawMessage(`{"heading":15708}`),
	}
	reg.RecordSource("src1", env)
	reg.RecordSink("sink1", "conn1", env)
	reg.RecordConnectorEvent("conn1", "received", env)
	reg.Record("conn1", 1, int64(env.SizeBytes()))

	for _, tc := range []struct {
		path  string
		wants []string
	}{
		{"/ui/frag/sources/src1/overview", []string{"source-overview-live", "Status", "Statistics", "Message stream", "Total messages", "127250", "heading", "received"}},
		{"/ui/frag/sinks/sink1/overview", []string{"sink-overview-live", "Status", "Statistics", "Message stream", "Total messages", "127250", "heading", "sent", "conn1"}},
		{"/ui/frag/connectors/conn1/overview", []string{"connector-overview-live", "Status", "Statistics", "Message stream", "Total messages", "127250", "heading", "received", "conn1"}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			mustStatus(t, resp, http.StatusOK)
			body := mustBody(t, resp)
			for _, want := range tc.wants {
				if !strings.Contains(body, want) {
					t.Fatalf("overview fragment missing %q:\n%s", want, body)
				}
			}
		})
	}
}

func TestConnectorOverviewPageRendersConfigSummary(t *testing.T) {
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
		"connector overview", "NMEA Bridge", "<code>src1</code>", "<code>sink1</code>",
		"msg.pgn == 127250", "500", "24h0m0s", "1048576",
		`href="/ui/connectors/conn1/edit"`,
		`aria-label="Breadcrumb"`, `href="/ui/connectors"`, `aria-current="page">NMEA Bridge</span>`,
		`hx-get="/ui/frag/connectors/conn1/overview"`, `hx-trigger="load, every 2s"`,
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
