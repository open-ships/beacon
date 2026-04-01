// Package sse implements a Server-Sent Events HTTP sink.
package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-ships/beacon/internal/admin"
	"github.com/open-ships/beacon/internal/buffer"
	"github.com/open-ships/beacon/internal/n2k"
)

const clientChannelBuffer = 64

// sseClient tracks a connected SSE client with its cursor.
type sseClient struct {
	ch         chan []byte
	lastSentID atomic.Int64
}

// broadcaster fan-outs messages to all connected SSE clients.
type broadcaster struct {
	mu      sync.Mutex
	clients map[*sseClient]struct{}
}

func newBroadcaster() *broadcaster {
	return &broadcaster{clients: make(map[*sseClient]struct{})}
}

func (b *broadcaster) add(c *sseClient) {
	b.mu.Lock()
	b.clients[c] = struct{}{}
	b.mu.Unlock()
}

func (b *broadcaster) remove(c *sseClient) {
	b.mu.Lock()
	delete(b.clients, c)
	b.mu.Unlock()
}

func (b *broadcaster) broadcast(data []byte, msgID int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for c := range b.clients {
		select {
		case c.ch <- data:
			c.lastSentID.Store(msgID)
		default:
			// drop on full — slow client
		}
	}
}

func (b *broadcaster) clientCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.clients)
}

func (b *broadcaster) minDeliveredID(fallback int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.clients) == 0 {
		return fallback
	}
	var minID int64 = -1
	for c := range b.clients {
		cid := c.lastSentID.Load()
		if minID < 0 || cid < minID {
			minID = cid
		}
	}
	if minID < 0 {
		return fallback
	}
	return minID
}

// SSESink implements the Sink interface using SSE over HTTP.
type SSESink struct {
	address  string
	path     string
	log      *slog.Logger
	metrics  *admin.Metrics
	buf      buffer.Buffer
	bc       *broadcaster
	server   *http.Server
	listener net.Listener
	ready    chan struct{}
}

// NewSSESink creates a new SSESink.
func NewSSESink(address, path string, log *slog.Logger, opts ...SSEOption) *SSESink {
	s := &SSESink{
		address: address,
		path:    path,
		log:     log,
		bc:      newBroadcaster(),
		ready:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// SSEOption is a functional option for SSESink.
type SSEOption func(*SSESink)

// WithMetrics sets the metrics on the SSE sink.
func WithMetrics(m *admin.Metrics) SSEOption {
	return func(s *SSESink) { s.metrics = m }
}

// WithBuffer sets the buffer for Last-Event-ID replay.
func WithBuffer(b buffer.Buffer) SSEOption {
	return func(s *SSESink) { s.buf = b }
}

// Name implements sink.Sink.
func (s *SSESink) Name() string { return "sink_sse" }

// Ready returns a channel that is closed once the listener is bound and serving.
func (s *SSESink) Ready() <-chan struct{} { return s.ready }

// Start begins the HTTP server. It blocks until ctx is cancelled.
func (s *SSESink) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc(s.path, s.handleSSE)

	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("SSE listen on %s: %w", s.address, err)
	}
	s.listener = ln

	s.server = &http.Server{Handler: mux}
	s.log.Info("SSE sink listening", "address", ln.Addr().String(), "path", s.path)

	// Signal readiness
	close(s.ready)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.server.Shutdown(shutdownCtx)
	}()

	if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *SSESink) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	flusher.Flush() // send headers immediately so the client's Do() can return

	c := &sseClient{ch: make(chan []byte, clientChannelBuffer)}
	s.bc.add(c)
	defer func() {
		s.bc.remove(c)
		if s.metrics != nil {
			s.metrics.SinkClients.Record(r.Context(), int64(s.bc.clientCount()), admin.AttrSink(s.Name()))
		}
	}()

	if s.metrics != nil {
		s.metrics.SinkClients.Record(r.Context(), int64(s.bc.clientCount()), admin.AttrSink(s.Name()))
	}

	s.log.Debug("SSE client connected", "remote", r.RemoteAddr)

	// Handle Last-Event-ID replay
	if lastEventID := r.Header.Get("Last-Event-ID"); lastEventID != "" && s.buf != nil {
		if id, err := strconv.ParseInt(lastEventID, 10, 64); err == nil && id > 0 {
			s.replayFrom(r.Context(), w, flusher, id)
		}
	}

	for {
		select {
		case data := <-c.ch:
			if _, err := w.Write(data); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// replayFrom reads messages from the buffer starting after the given ID and writes them as SSE events.
func (s *SSESink) replayFrom(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, afterID int64) {
	msgs, err := s.buf.Read(ctx, afterID, 1000)
	if err != nil {
		s.log.Warn("SSE replay read error", "err", err)
		return
	}
	for _, msg := range msgs {
		data, err := json.Marshal(msg)
		if err != nil {
			continue
		}
		line := fmt.Sprintf("id: %d\nevent: %d\ndata: %s\n\n", msg.ID, msg.PGN, data)
		if _, err := w.Write([]byte(line)); err != nil {
			return
		}
	}
	flusher.Flush()
}

// Send serializes msg as an SSE event and broadcasts it.
func (s *SSESink) Send(msg *n2k.ParsedMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// SSE format: id: <id>\nevent: <pgn>\ndata: <json>\n\n
	line := fmt.Sprintf("id: %d\nevent: %d\ndata: %s\n\n", msg.ID, msg.PGN, data)
	s.bc.broadcast([]byte(line), msg.ID)
	return nil
}

// MinDeliveredID returns the minimum lastSentID across all live SSE clients.
// If no clients are connected, returns fallback.
func (s *SSESink) MinDeliveredID(fallback int64) int64 {
	return s.bc.minDeliveredID(fallback)
}

// Close closes the HTTP server gracefully.
func (s *SSESink) Close() error {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}
	return nil
}

// HealthCheck returns nil if the listener is active.
func (s *SSESink) HealthCheck(_ context.Context) error {
	if s.listener == nil {
		return fmt.Errorf("SSE listener not started")
	}
	return nil
}

// ClientCount returns the number of currently connected SSE clients.
func (s *SSESink) ClientCount() int {
	return s.bc.clientCount()
}

// Addr returns the listener's address (useful in tests with port 0).
func (s *SSESink) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.address
}
