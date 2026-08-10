package sink

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

const (
	clientBuf              = 64
	maxServeClients        = 32
	replayReadLimit        = 8
	streamClientWriteLimit = 2 * time.Second
)

type client struct {
	ch   chan queue.Entry
	drop func()
}

// serveHandler streams entries to one connected client; implementations
// differ only in wire format (SSE frames vs WS text messages).
type serveHandler func(s *serveSink, w http.ResponseWriter, r *http.Request)

type serveSink struct {
	id     string
	path   string
	ds     *DataServer
	log    *slog.Logger
	met    *metrics.Set
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	clients  map[*client]bool
	replays  map[string]ReplayReader
	reserved int
	stopped  bool
	handlers sync.WaitGroup
}

func newServeSink(cfg model.Sink, ds *DataServer, log *slog.Logger, met *metrics.Set, h serveHandler) (Runtime, error) {
	ctx, cancel := context.WithCancel(ds.runCtx)
	s := &serveSink{id: cfg.ID, path: cfg.Path, ds: ds, log: log, met: met,
		ctx: ctx, cancel: cancel, clients: map[*client]bool{}, replays: map[string]ReplayReader{}}
	ds.SetRoute(cfg.Path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.beginHandler() {
			http.NotFound(w, r)
			return
		}
		defer s.handlers.Done()

		// A stream belongs to both the client request and this configured sink
		// generation. Cancelling either one must end replay database reads and
		// live writes; otherwise a handler that already entered replay could
		// outlive route removal and overlap a replacement sink at the same path.
		ctx, cancel := context.WithCancel(s.ctx)
		stopRequestCancel := context.AfterFunc(r.Context(), cancel)
		defer func() {
			stopRequestCancel()
			cancel()
		}()
		h(s, w, r.WithContext(ctx))
	}))
	return s, nil
}

func (s *serveSink) ID() string                   { return s.id }
func (s *serveSink) DeliveryClass() DeliveryClass { return DeliveryResumable }
func (s *serveSink) State() (string, error) {
	return "up", nil // route is registered; per-client failures are per-client
}

func (s *serveSink) Stop() {
	s.ds.RemoveRoute(s.path)
	s.mu.Lock()
	s.stopped = true
	s.cancel()
	for c := range s.clients {
		close(c.ch)
		delete(s.clients, c)
	}
	s.mu.Unlock()
	// beginHandler adds to handlers while holding mu and refuses additions once
	// stopped is set, so Wait cannot race a new zero-to-one Add. Replay readers
	// receive the cancelled sink context and streaming writes are independently
	// bounded by streamClientWriteLimit.
	s.handlers.Wait()
}

func (s *serveSink) beginHandler() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false
	}
	s.handlers.Add(1)
	return true
}

// clientCount reports connected clients (test hook, like bus.Manager's).
func (s *serveSink) clientCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
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
func (s *serveSink) Broadcast(entries []queue.Entry) BroadcastReport {
	report := BroadcastReport{Accepted: make([]bool, len(entries))}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range entries {
		// The connector queue makes every entry replayable even when there
		// are no live clients, so availability is the resumable boundary.
		report.Accepted[i] = true
		for c := range s.clients {
			select {
			case c.ch <- e:
			default:
				delete(s.clients, c)
				close(c.ch)
				go c.drop()
				report.RecipientDrops++
			}
		}
	}
	return report
}

func (s *serveSink) addClient(drop func()) (*client, bool) {
	if !s.reserveClient() {
		return nil, false
	}
	return s.activateReservedClient(drop)
}

func (s *serveSink) reserveClient() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || len(s.clients)+s.reserved >= maxServeClients {
		return false
	}
	s.reserved++
	return true
}

func (s *serveSink) releaseClientReservation() {
	s.mu.Lock()
	if s.reserved > 0 {
		s.reserved--
	}
	s.mu.Unlock()
}

func (s *serveSink) activateReservedClient(drop func()) (*client, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reserved == 0 {
		return nil, false
	}
	s.reserved--
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
			// Keep replay memory bounded even when every retained Envelope is
			// near the remote wire limit and all client slots are occupied.
			entries, err := reader.Read(ctx, cur, replayReadLimit)
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

func writeSSEChunk(controller *http.ResponseController, write func() error) error {
	if err := controller.SetWriteDeadline(time.Now().Add(streamClientWriteLimit)); err != nil {
		return err
	}
	if err := write(); err != nil {
		return err
	}
	return controller.Flush()
}

func serveSSE(s *serveSink, w http.ResponseWriter, r *http.Request) {
	_, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	c, ok := s.addClient(func() {})
	if !ok {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "stream client limit reached", http.StatusServiceUnavailable)
		return
	}
	defer s.removeClient(c)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	controller := http.NewResponseController(w)
	if err := writeSSEChunk(controller, func() error {
		w.WriteHeader(http.StatusOK)
		return nil
	}); err != nil {
		return
	}

	writeEvent := func(e queue.Entry) error {
		doc, err := e.Env.WireBytes()
		if err != nil {
			return err
		}
		return writeSSEChunk(controller, func() error {
			_, err := fmt.Fprintf(w, "id: %s\ndata: %s\n\n", eventID(e), doc)
			return err
		})
	}

	// Subscribe live first, then replay: duplicates over gaps (at-least-once).
	ctx := r.Context()
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
	if !s.reserveClient() {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "stream client limit reached", http.StatusServiceUnavailable)
		return
	}
	reserved := true
	defer func() {
		if reserved {
			s.releaseClientReservation()
		}
	}()
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()
	c, ok := s.activateReservedClient(func() { _ = conn.Close(websocket.StatusPolicyViolation, "client too slow") })
	reserved = false
	if !ok {
		return
	}
	defer s.removeClient(c)
	// CloseRead spawns the read pump this write-only handler otherwise
	// lacks: it services control frames (ping/pong/close) and returns a
	// context cancelled when the connection dies or r.Context() is
	// cancelled (e.g. DataServer.Stop). Without it a dead client is never
	// noticed until a write happens to fail, leaking the handler goroutine,
	// its channel, and the SinkClients count.
	ctx := conn.CloseRead(r.Context())

	writeEvent := func(e queue.Entry) error {
		doc, err := e.Env.WireBytes()
		if err != nil {
			return err
		}
		writeCtx, cancel := context.WithTimeout(ctx, streamClientWriteLimit)
		defer cancel()
		return conn.Write(writeCtx, websocket.MessageText, doc)
	}

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
