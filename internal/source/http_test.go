package source

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

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
	}, nil, slog.Default(), nil)
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
	}, nil, slog.Default(), nil)
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
