// Package tcp implements an NDJSON TCP sink.
package tcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-ships/beacon/internal/admin"
	"github.com/open-ships/beacon/internal/n2k"
)

const writeDeadline = 5 * time.Second

// client wraps a net.Conn with per-client cursor tracking.
type client struct {
	conn       net.Conn
	lastSentID atomic.Int64
}

// TCPSink accepts multiple TCP clients and writes NDJSON to each.
type TCPSink struct {
	address  string
	log      *slog.Logger
	metrics  *admin.Metrics
	listener net.Listener

	mu      sync.RWMutex
	clients map[*client]struct{}

	ready chan struct{}
}

// NewTCPSink creates a new TCPSink.
func NewTCPSink(address string, log *slog.Logger, metrics ...*admin.Metrics) *TCPSink {
	var m *admin.Metrics
	if len(metrics) > 0 {
		m = metrics[0]
	}
	return &TCPSink{
		address: address,
		log:     log,
		metrics: m,
		clients: make(map[*client]struct{}),
		ready:   make(chan struct{}),
	}
}

// Name implements sink.Sink.
func (t *TCPSink) Name() string { return "sink_tcp" }

// Ready returns a channel that is closed once the listener is bound and accepting.
func (t *TCPSink) Ready() <-chan struct{} { return t.ready }

// Start begins accepting TCP connections. It blocks until ctx is cancelled.
func (t *TCPSink) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", t.address)
	if err != nil {
		return fmt.Errorf("TCP listen on %s: %w", t.address, err)
	}
	t.listener = ln
	t.log.Info("TCP sink listening", "address", t.address)

	// Signal readiness
	close(t.ready)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				t.log.Error("accept error", "err", err)
				return err
			}
		}
		c := &client{conn: conn}
		t.addClient(c)
		go t.handleClient(ctx, c)
	}
}

func (t *TCPSink) addClient(c *client) {
	t.mu.Lock()
	t.clients[c] = struct{}{}
	count := len(t.clients)
	t.mu.Unlock()
	if t.metrics != nil {
		t.metrics.SinkClients.Record(context.Background(), int64(count), admin.AttrSink(t.Name()))
	}
}

func (t *TCPSink) removeClient(c *client) {
	t.mu.Lock()
	delete(t.clients, c)
	count := len(t.clients)
	t.mu.Unlock()
	c.conn.Close()
	if t.metrics != nil {
		t.metrics.SinkClients.Record(context.Background(), int64(count), admin.AttrSink(t.Name()))
	}
}

func (t *TCPSink) handleClient(ctx context.Context, c *client) {
	defer t.removeClient(c)
	t.log.Debug("TCP client connected", "remote", c.conn.RemoteAddr().String())

	// Block until context is done or the client disconnects.
	// Reading detects client disconnection (EOF / reset).
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		buf := make([]byte, 1)
		for {
			if _, err := c.conn.Read(buf); err != nil {
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
	case <-doneCh:
	}
}

// Send marshals msg as NDJSON and writes it to all connected clients.
func (t *TCPSink) Send(msg *n2k.ParsedMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	data = append(data, '\n')

	t.mu.RLock()
	clients := make([]*client, 0, len(t.clients))
	for c := range t.clients {
		clients = append(clients, c)
	}
	t.mu.RUnlock()

	for _, c := range clients {
		c.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
		if _, err := c.conn.Write(data); err != nil {
			t.log.Debug("TCP write error, removing client", "err", err)
			// Do NOT update lastSentID on failure
			go t.removeClient(c)
		} else {
			// Update per-client cursor on success
			c.lastSentID.Store(msg.ID)
		}
	}
	return nil
}

// MinDeliveredID returns the minimum lastSentID across all live clients.
// If no clients are connected, returns fallback.
func (t *TCPSink) MinDeliveredID(fallback int64) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.clients) == 0 {
		return fallback
	}

	var minID int64 = -1
	for c := range t.clients {
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

// Close closes the listener and all client connections.
func (t *TCPSink) Close() error {
	if t.listener != nil {
		t.listener.Close()
	}
	t.mu.Lock()
	for c := range t.clients {
		c.conn.Close()
		delete(t.clients, c)
	}
	t.mu.Unlock()
	return nil
}

// HealthCheck returns nil if the listener is active.
func (t *TCPSink) HealthCheck(_ context.Context) error {
	if t.listener == nil {
		return fmt.Errorf("TCP listener not started")
	}
	return nil
}

// ClientCount returns the number of currently connected clients.
func (t *TCPSink) ClientCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.clients)
}
