package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

func httpPostEntries(n int) []queue.Entry {
	out := make([]queue.Entry, n)
	for i := range out {
		seq := int64(i + 41)
		out[i] = queue.Entry{Seq: seq, Env: &msg.Envelope{
			Seq: seq, ConnectorID: "engine", PGN: uint32(127488 + i),
			Timestamp: time.Unix(seq, 0).UTC(), Payload: json.RawMessage(`{"rpm":1200}`),
		}}
	}
	return out
}

func TestHTTPPostSinkPostsConfirmedAuthenticatedJSONBatch(t *testing.T) {
	type receivedRequest struct {
		method, contentType, authorization, apiKey, idempotencyKey string
		body                                                       []msg.Envelope
	}
	received := make(chan receivedRequest, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body []msg.Envelope
		_ = json.NewDecoder(r.Body).Decode(&body)
		received <- receivedRequest{
			method: r.Method, contentType: r.Header.Get("Content-Type"),
			authorization: r.Header.Get("Authorization"), apiKey: r.Header.Get("X-API-Key"),
			idempotencyKey: r.Header.Get("Idempotency-Key"), body: body,
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	rt, err := New(context.Background(), model.Sink{
		ID: "webhook", Type: model.SinkHTTPPost, URL: srv.URL + "/v1/envelopes", BatchSize: 25,
		Headers: map[string]string{"Authorization": "Bearer token", "X-API-Key": "secret"},
	}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	p := rt.(BatchPusher)
	if p.BatchSize() != 25 || DeliveryClassOf(rt) != DeliveryConfirmed {
		t.Fatalf("batch size/class = %d/%s", p.BatchSize(), DeliveryClassOf(rt))
	}

	entries := httpPostEntries(2)
	if err := p.PushBatch(context.Background(), entries); err != nil {
		t.Fatal(err)
	}
	if err := p.PushBatch(context.Background(), entries); err != nil {
		t.Fatal(err)
	}
	first, second := <-received, <-received
	for _, got := range []receivedRequest{first, second} {
		if got.method != http.MethodPost || got.contentType != "application/json" ||
			got.authorization != "Bearer token" || got.apiKey != "secret" {
			t.Fatalf("request metadata = %+v", got)
		}
		if len(got.body) != 2 || got.body[0].Seq != entries[0].Seq || got.body[1].PGN != entries[1].Env.PGN {
			t.Fatalf("request body = %+v", got.body)
		}
		if got.idempotencyKey == "" {
			t.Fatal("request omitted Idempotency-Key")
		}
	}
	if first.idempotencyKey != second.idempotencyKey {
		t.Fatalf("retry key changed: %q != %q", first.idempotencyKey, second.idempotencyKey)
	}
	if state, err := rt.State(); state != "up" || err != nil {
		t.Fatalf("state after 2xx = %q, %v", state, err)
	}
}

func TestHTTPPostSinkSupportsHTTPS(t *testing.T) {
	var called atomic.Bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Store(true)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	rt, err := New(context.Background(), model.Sink{ID: "secure", Type: model.SinkHTTPPost, URL: srv.URL}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	post := rt.(*httpPostSink)
	post.client = srv.Client() // trust httptest's private CA; production uses the system trust store
	defer rt.Stop()
	if err := post.PushBatch(context.Background(), httpPostEntries(1)); err != nil {
		t.Fatal(err)
	}
	if !called.Load() {
		t.Fatal("HTTPS endpoint was not called")
	}
}

func TestHTTPPostSinkGzipsPayload(t *testing.T) {
	received := make(chan []msg.Envelope, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Encoding"); got != "gzip" {
			t.Errorf("Content-Encoding = %q, want gzip", got)
		}
		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("open gzip payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Errorf("read gzip payload: %v", err)
		}
		if err := reader.Close(); err != nil {
			t.Errorf("close gzip payload: %v", err)
		}
		var body []msg.Envelope
		if err := json.NewDecoder(bytes.NewReader(decompressed)).Decode(&body); err != nil {
			t.Errorf("decode gzip payload: %v", err)
		}
		received <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	rt, err := New(context.Background(), model.Sink{
		ID: "compressed", Type: model.SinkHTTPPost, URL: srv.URL, Gzip: true,
	}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	entries := httpPostEntries(2)
	if err := rt.(BatchPusher).PushBatch(context.Background(), entries); err != nil {
		t.Fatal(err)
	}
	body := <-received
	if len(body) != 2 || body[0].Seq != entries[0].Seq || body[1].PGN != entries[1].Env.PGN {
		t.Fatalf("decompressed body = %+v", body)
	}
}

func TestHTTPPostSinkReturnsRetryAfterAndRecordsRequestMetrics(t *testing.T) {
	met, prom, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "2")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	rt, err := New(context.Background(), model.Sink{
		ID: "limited", Type: model.SinkHTTPPost, URL: srv.URL, Gzip: true,
	}, nil, nil, nil, met)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	err = rt.(BatchPusher).PushBatch(context.Background(), httpPostEntries(3))
	if delay, ok := RetryAfter(err); !ok || delay != 2*time.Second {
		t.Fatalf("Retry-After = %v, %v; want 2s, true (err=%v)", delay, ok, err)
	}

	rec := httptest.NewRecorder()
	prom.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		"beacon_sink_http_requests_total{",
		"beacon_sink_http_payload_envelopes_total{",
		"beacon_sink_http_retry_after_seconds_count{",
		`encoding="gzip"`, `sink="limited"`, `status="429"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("HTTP sink metrics missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		value string
		want  time.Duration
		ok    bool
	}{
		{value: "15", want: 15 * time.Second, ok: true},
		{value: now.Add(45 * time.Second).Format(http.TimeFormat), want: 45 * time.Second, ok: true},
		{value: now.Add(-time.Minute).Format(http.TimeFormat), want: 0, ok: true},
		{value: "later", ok: false},
		{value: "-1", ok: false},
		{value: "+1", ok: false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			got, ok := parseRetryAfter(tc.value, now)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("parseRetryAfter(%q) = %v, %v; want %v, %v", tc.value, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestHTTPPostSinkSurfacesNon2xxAndRecovers(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "invalid API key", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	rt, err := New(context.Background(), model.Sink{ID: "webhook", Type: model.SinkHTTPPost, URL: srv.URL}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	p := rt.(BatchPusher)
	if err := p.PushBatch(context.Background(), httpPostEntries(1)); err == nil || !stringsContain(err.Error(), "401 Unauthorized", "invalid API key") {
		t.Fatalf("non-2xx error = %v", err)
	}
	if state, err := rt.State(); state != "degraded" || err == nil {
		t.Fatalf("state after non-2xx = %q, %v", state, err)
	}
	if err := p.PushBatch(context.Background(), httpPostEntries(1)); err != nil {
		t.Fatal(err)
	}
	if state, err := rt.State(); state != "up" || err != nil {
		t.Fatalf("state after recovery = %q, %v", state, err)
	}
}

func stringsContain(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}

func TestHTTPPostSinkTimesOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	rt, err := New(context.Background(), model.Sink{
		ID: "slow", Type: model.SinkHTTPPost, URL: srv.URL, RequestTimeout: model.Duration(20 * time.Millisecond),
	}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	err = rt.(BatchPusher).PushBatch(context.Background(), httpPostEntries(1))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error = %v, want deadline exceeded", err)
	}
}

func TestHTTPPostSinkDoesNotFollowRedirects(t *testing.T) {
	var destinationCalls atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		destinationCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	rt, err := New(context.Background(), model.Sink{ID: "redirect", Type: model.SinkHTTPPost, URL: redirect.URL}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	if err := rt.(BatchPusher).PushBatch(context.Background(), httpPostEntries(1)); err == nil || !strings.Contains(err.Error(), "307 Temporary Redirect") {
		t.Fatalf("redirect error = %v", err)
	}
	if destinationCalls.Load() != 0 {
		t.Fatal("HTTP POST sink followed a redirect")
	}
}
