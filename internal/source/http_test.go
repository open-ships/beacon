package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

// eventuallyState polls rt.State until it reports want, or the deadline passes.
func eventuallyState(rt Runtime, want string, d time.Duration) bool {
	deadline := time.After(d)
	for {
		if s, _ := rt.State(); s == want {
			return true
		}
		select {
		case <-deadline:
			s, _ := rt.State()
			return s == want
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func envelopeJSON(pgn uint32) string {
	e := msg.Envelope{Seq: 99, ConnectorID: "upstream", PGN: pgn, Source: 7, Dest: 255,
		Priority: 2, Timestamp: time.Now(), Payload: json.RawMessage(`{"heading":15708}`)}
	b, _ := json.Marshal(&e)
	return string(b)
}

func TestSSEDialerReceivesAndReconnects(t *testing.T) {
	conns := make(chan struct{}, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conns <- struct{}{}
		if got := r.Header.Get("Accept"); !strings.Contains(got, "text/event-stream") {
			t.Errorf("Accept = %q", got)
		}
		if r.Header.Get("X-Token") != "secret" {
			t.Errorf("custom header missing")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		_, _ = fmt.Fprintf(w, "id: upstream:99\ndata: %s\n\n", envelopeJSON(127250))
		fl.Flush()
		// close connection to force a reconnect
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Source{
		ID: "up", Name: "Upstream", Type: model.SourceHTTPSSE, Enabled: true,
		URL: srv.URL, Headers: map[string]string{"X-Token": "secret"},
	}, nil, slog.Default(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	ch, unsub := rt.Subscribe(16)
	defer unsub()

	e := <-ch
	if e.PGN != 127250 {
		t.Fatalf("pgn = %d", e.PGN)
	}
	if e.Seq != 0 || e.ConnectorID != "" {
		t.Fatalf("upstream seq/connector must be cleared: %+v", e)
	}

	// server closed the stream; the dialer must reconnect
	select {
	case <-conns:
	case <-time.After(time.Second):
		t.Fatal("no first connection?")
	}
	select {
	case <-conns:
	case <-time.After(5 * time.Second):
		t.Fatal("dialer did not reconnect after stream end")
	}
}

// A source whose endpoint never comes up (bad host, missing port) must report
// "degraded" for its entire life and never flash "up" — the dialer must not
// optimistically claim health before a connection is actually established.
func TestDialerNeverReportsUpWhenNeverConnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts int32
	neverConnects := func(context.Context, model.Source, func(*msg.Envelope), func()) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("dial tcp: address broker: missing port in address")
	}
	rt, err := newDialerSource(ctx, model.Source{ID: "bad", Type: model.SourceMQTT, Enabled: true},
		slog.Default(), nil, nil, neverConnects)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	// Poll aggressively across several reconnect cycles. Before the fix the
	// dialer set "up" at the top of every retry, so this reliably caught it.
	deadline := time.After(1500 * time.Millisecond)
	for {
		if state, _ := rt.State(); state == "up" {
			t.Fatalf("source that never connects reported %q", state)
		}
		select {
		case <-deadline:
			if n := atomic.LoadInt32(&attempts); n < 2 {
				t.Fatalf("dialer did not retry; attempts=%d", n)
			}
			return
		default:
		}
	}
}

// Once the endpoint connects the source reports "up", and it returns to
// "degraded" when that connection drops.
func TestDialerReportsUpOnlyAfterConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	var once sync.Once
	run := func(runCtx context.Context, _ model.Source, _ func(*msg.Envelope), connected func()) error {
		first := false
		once.Do(func() { first = true })
		if !first {
			<-runCtx.Done() // later reconnects just hang, keeping state at degraded
			return runCtx.Err()
		}
		connected()
		<-release
		return errors.New("connection lost")
	}
	rt, err := newDialerSource(ctx, model.Source{ID: "ok", Type: model.SourceMQTT, Enabled: true},
		slog.Default(), nil, nil, run)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	if !eventuallyState(rt, "up", time.Second) {
		t.Fatal("source never reported up after connect")
	}
	close(release) // drop the connection
	if !eventuallyState(rt, "degraded", time.Second) {
		t.Fatal("source did not return to degraded after disconnect")
	}
}

func TestWSDialerReceives(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		_ = c.Write(r.Context(), websocket.MessageText, []byte(envelopeJSON(128259)))
		time.Sleep(time.Second)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Source{
		ID: "upws", Name: "Upstream WS", Type: model.SourceHTTPWS, Enabled: true,
		URL: "ws" + strings.TrimPrefix(srv.URL, "http"),
	}, nil, slog.Default(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	ch, unsub := rt.Subscribe(16)
	defer unsub()
	select {
	case e := <-ch:
		if e.PGN != 128259 {
			t.Fatalf("pgn = %d", e.PGN)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no envelope from WS dialer")
	}
}
