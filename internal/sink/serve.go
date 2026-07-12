package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/coder/websocket"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

const clientBuf = 256

type client struct {
	ch   chan queue.Entry
	drop func()
}

// serveHandler streams entries to one connected client; implementations
// differ only in wire format (SSE frames vs WS text messages).
type serveHandler func(s *serveSink, w http.ResponseWriter, r *http.Request)

type serveSink struct {
	id   string
	path string
	ds   *DataServer
	log  *slog.Logger
	met  *metrics.Set

	mu      sync.Mutex
	clients map[*client]bool
	replays map[string]ReplayReader
	stopped bool
}

func newServeSink(cfg model.Sink, ds *DataServer, log *slog.Logger, met *metrics.Set, h serveHandler) (Runtime, error) {
	s := &serveSink{id: cfg.ID, path: cfg.Path, ds: ds, log: log, met: met,
		clients: map[*client]bool{}, replays: map[string]ReplayReader{}}
	ds.SetRoute(cfg.Path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h(s, w, r) }))
	return s, nil
}

func (s *serveSink) ID() string { return s.id }
func (s *serveSink) State() (string, error) {
	return "up", nil // route is registered; per-client failures are per-client
}

func (s *serveSink) Stop() {
	s.ds.RemoveRoute(s.path)
	s.mu.Lock()
	s.stopped = true
	for c := range s.clients {
		close(c.ch)
		delete(s.clients, c)
	}
	s.mu.Unlock()
}

func (s *serveSink) RegisterConnector(id string, r ReplayReader) {
	s.mu.Lock()
	s.replays[id] = r
	s.mu.Unlock()
}

func (s *serveSink) UnregisterConnector(id string) {
	s.mu.Lock()
	delete(s.replays, id)
	s.mu.Unlock()
}

// Broadcast delivers entries to every connected client; clients that
// cannot keep up are dropped (they can replay on reconnect).
//
// Deleting the map entry currently being visited by `range s.clients` is
// well-defined in Go (a key removed during iteration is simply not produced
// again), so dropping a slow client inside this loop is safe. Once dropped
// here, removeClient's `if s.clients[c]` guard (see below) sees the entry
// already gone and skips closing c.ch a second time when the client's own
// goroutine unwinds — so there is exactly one close per client regardless
// of which path (overflow here, or a normal disconnect) removes it first.
func (s *serveSink) Broadcast(entries []queue.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		for c := range s.clients {
			select {
			case c.ch <- e:
			default:
				delete(s.clients, c)
				close(c.ch)
				go c.drop()
			}
		}
	}
}

func (s *serveSink) addClient(drop func()) (*client, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, false
	}
	c := &client{ch: make(chan queue.Entry, clientBuf), drop: drop}
	s.clients[c] = true
	s.met.SinkClients(s.id, 1)
	return c, true
}

// removeClient retires a client exactly once: if Broadcast already dropped
// it (overflow) or Stop already closed it, s.clients[c] is false here and
// the close is skipped — see the Broadcast comment above. The metrics
// decrement always fires so addClient's +1 is matched by exactly one -1.
func (s *serveSink) removeClient(c *client) {
	s.mu.Lock()
	if s.clients[c] {
		delete(s.clients, c)
		close(c.ch)
	}
	s.mu.Unlock()
	s.met.SinkClients(s.id, -1)
}

// parseAfter parses "nav:12,engine:9" into cursors.
func parseAfter(vals ...string) map[string]int64 {
	out := map[string]int64{}
	for _, v := range vals {
		for _, pair := range strings.Split(v, ",") {
			pair = strings.TrimSpace(pair)
			i := strings.LastIndex(pair, ":")
			if i <= 0 {
				continue
			}
			seq, err := strconv.ParseInt(pair[i+1:], 10, 64)
			if err != nil {
				continue
			}
			out[pair[:i]] = seq
		}
	}
	return out
}

// replayEntries streams history for each requested connector cursor.
func (s *serveSink) replayEntries(ctx context.Context, cursors map[string]int64, emit func(queue.Entry) error) error {
	for connectorID, after := range cursors {
		s.mu.Lock()
		reader, ok := s.replays[connectorID]
		s.mu.Unlock()
		if !ok {
			continue
		}
		cur := after
		for {
			entries, err := reader.Read(ctx, cur, 256)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				break
			}
			for _, e := range entries {
				if err := emit(e); err != nil {
					return err
				}
				cur = e.Seq
			}
		}
	}
	return nil
}

func eventID(e queue.Entry) string {
	return fmt.Sprintf("%s:%d", e.Env.ConnectorID, e.Seq)
}

func serveSSE(s *serveSink, w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	fl.Flush()

	writeEvent := func(e queue.Entry) error {
		doc, err := json.Marshal(e.Env)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "id: %s\ndata: %s\n\n", eventID(e), doc); err != nil {
			return err
		}
		fl.Flush()
		return nil
	}

	// Subscribe live first, then replay: duplicates over gaps (at-least-once).
	ctx := r.Context()
	c, ok := s.addClient(func() {})
	if !ok {
		return
	}
	defer s.removeClient(c)

	cursors := parseAfter(r.Header.Get("Last-Event-ID"), r.URL.Query().Get("after"))
	if err := s.replayEntries(ctx, cursors, writeEvent); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case e, open := <-c.ch:
			if !open {
				return
			}
			if err := writeEvent(e); err != nil {
				return
			}
		}
	}
}

func serveWS(s *serveSink, w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := r.Context()

	writeEvent := func(e queue.Entry) error {
		doc, err := json.Marshal(e.Env)
		if err != nil {
			return err
		}
		return conn.Write(ctx, websocket.MessageText, doc)
	}

	c, ok := s.addClient(func() { conn.Close(websocket.StatusPolicyViolation, "client too slow") })
	if !ok {
		return
	}
	defer s.removeClient(c)

	if err := s.replayEntries(ctx, parseAfter(r.URL.Query().Get("after")), writeEvent); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case e, open := <-c.ch:
			if !open {
				return
			}
			if err := writeEvent(e); err != nil {
				return
			}
		}
	}
}
