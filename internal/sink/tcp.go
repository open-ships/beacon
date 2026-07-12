package sink

import (
	"encoding/json"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

// tcpSink serves a live NDJSON tail; no replay (nc-grade simplicity).
type tcpSink struct {
	id  string
	log *slog.Logger
	met *metrics.Set
	ln  net.Listener

	mu      sync.Mutex
	conns   map[net.Conn]bool
	stopped bool
}

func newTCPSink(cfg model.Sink, log *slog.Logger, met *metrics.Set) (Runtime, error) {
	ln, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, err
	}
	s := &tcpSink{id: cfg.ID, log: log, met: met, ln: ln, conns: map[net.Conn]bool{}}
	go s.acceptLoop()
	return s, nil
}

func (s *tcpSink) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = true
		s.mu.Unlock()
		s.met.SinkClients(s.id, 1)
	}
}

func (s *tcpSink) ID() string             { return s.id }
func (s *tcpSink) Addr() string           { return s.ln.Addr().String() }
func (s *tcpSink) State() (string, error) { return "up", nil }

func (s *tcpSink) Broadcast(entries []queue.Entry) {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, e := range entries {
		doc, err := json.Marshal(e.Env)
		if err != nil {
			continue
		}
		doc = append(doc, '\n')
		for _, c := range conns {
			_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if _, err := c.Write(doc); err != nil {
				s.dropConn(c)
			}
		}
	}
}

func (s *tcpSink) dropConn(c net.Conn) {
	s.mu.Lock()
	known := s.conns[c]
	delete(s.conns, c)
	s.mu.Unlock()
	if known {
		_ = c.Close()
		s.met.SinkClients(s.id, -1)
	}
}

func (s *tcpSink) Stop() {
	s.mu.Lock()
	s.stopped = true
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.conns = map[net.Conn]bool{}
	s.mu.Unlock()
	_ = s.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}
