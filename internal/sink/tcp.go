package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
	"github.com/open-ships/beacon/internal/retry"
)

const (
	maxTCPClients         = 32
	tcpClientWriteTimeout = 2 * time.Second
	tcpStableListenerAge  = 30 * time.Second
)

// tcpSink serves a live NDJSON tail; no replay (nc-grade simplicity).
type tcpSink struct {
	id   string
	log  *slog.Logger
	met  *metrics.Set
	ln   net.Listener
	addr string
	ds   *DataServer

	cancel context.CancelFunc

	// Broadcasts can arrive concurrently when several connectors share this
	// sink. Serialize them so their per-connection deadlines cannot extend or
	// shorten one another and each batch remains contiguous on the wire.
	broadcastMu sync.Mutex
	mu          sync.Mutex
	conns       map[net.Conn]bool
	stopped     bool
	lastErr     error
	wg          sync.WaitGroup
}

func newTCPSink(cfg model.Sink, ds *DataServer, log *slog.Logger, met *metrics.Set) (Runtime, error) {
	if log == nil {
		log = slog.Default()
	}
	ln, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, err
	}
	ln = ds.WrapListener(ln)
	runCtx, cancel := context.WithCancel(context.Background())
	s := &tcpSink{
		id: cfg.ID, log: log, met: met, ln: ln, addr: ln.Addr().String(), ds: ds,
		cancel: cancel, conns: map[net.Conn]bool{},
	}
	s.wg.Add(1)
	go s.acceptLoop(runCtx)
	return s, nil
}

func (s *tcpSink) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	backoff := retry.NewBackoff(250*time.Millisecond, time.Minute)
	listeningSince := time.Now()
	for {
		s.mu.Lock()
		ln := s.ln
		s.mu.Unlock()
		conn, err := ln.Accept()
		if err != nil {
			// Accept can fail while the listening socket itself is still open
			// (for example after a transient descriptor-pressure error). Close
			// that socket before rebinding, otherwise every retry can wedge on
			// EADDRINUSE even though this loop has stopped accepting clients.
			_ = ln.Close()
			s.mu.Lock()
			if s.stopped {
				s.mu.Unlock()
				return
			}
			s.lastErr = fmt.Errorf("accept TCP clients: %w", err)
			s.mu.Unlock()
			s.log.Error("TCP sink listener failed; rebinding", "sink", s.id, "err", err)
			delay := tcpAcceptRetryDelay(backoff, listeningSince, time.Now())
			for {
				if !retry.Sleep(ctx, delay) {
					return
				}
				next, listenErr := net.Listen("tcp", s.addr)
				if listenErr != nil {
					s.mu.Lock()
					s.lastErr = fmt.Errorf("rebind TCP sink: %w", listenErr)
					s.mu.Unlock()
					delay = backoff.Next()
					continue
				}
				next = s.ds.WrapListener(next)
				s.mu.Lock()
				if s.stopped {
					s.mu.Unlock()
					_ = next.Close()
					return
				}
				s.ln = next
				s.lastErr = nil
				s.mu.Unlock()
				s.log.Info("TCP sink listener rebound", "sink", s.id, "address", next.Addr())
				// Do not reset outage history merely because bind succeeded. Under
				// descriptor pressure, bind can succeed after closing the previous
				// listener and the very next Accept can fail immediately; resetting
				// here turns that cycle into a permanent 250ms retry/log storm.
				listeningSince = time.Now()
				break
			}
			continue
		}
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		if len(s.conns) >= maxTCPClients {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.conns[conn] = true
		s.met.SinkClients(s.id, 1)
		s.wg.Add(1)
		s.mu.Unlock()
		go s.watchConn(conn)
	}
}

func tcpAcceptRetryDelay(backoff *retry.Backoff, listeningSince, now time.Time) time.Duration {
	if now.Sub(listeningSince) >= tcpStableListenerAge {
		backoff.Reset()
	}
	return backoff.Next()
}

// watchConn observes the otherwise write-only connection's read side so a
// peer that disconnects while no messages are flowing is retired promptly.
// TCP sink clients are receive-only; sending a byte is treated as a protocol
// violation and closes the connection instead of creating an inbound drain.
func (s *tcpSink) watchConn(conn net.Conn) {
	defer s.wg.Done()
	var probe [1]byte
	_, _ = conn.Read(probe[:])
	s.dropConn(conn)
}

func (s *tcpSink) ID() string { return s.id }
func (s *tcpSink) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}
func (s *tcpSink) State() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return "error", errors.New("TCP sink is stopped")
	}
	if s.lastErr != nil {
		return "error", s.lastErr
	}
	return "up", nil
}
func (s *tcpSink) DeliveryClass() DeliveryClass { return DeliveryBestEffort }

func (s *tcpSink) clientCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *tcpSink) Broadcast(entries []queue.Entry) BroadcastReport {
	s.broadcastMu.Lock()
	defer s.broadcastMu.Unlock()

	report := BroadcastReport{Accepted: make([]bool, len(entries))}
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	// One batch gets one wall-clock budget regardless of client count. Giving
	// every client a fresh timeout made 32 stalled peers hold a connector for
	// roughly a minute and could extend shutdown well beyond App's deadline.
	writeDeadline := time.Now().Add(tcpClientWriteTimeout)
	var failed map[net.Conn]struct{}

	for i, e := range entries {
		wire, err := e.Env.WireBytes()
		if err != nil {
			report.Err = err
			continue
		}
		doc := make([]byte, len(wire)+1)
		copy(doc, wire)
		doc[len(wire)] = '\n'
		accepted := false
		for _, c := range conns {
			if _, alreadyFailed := failed[c]; alreadyFailed {
				continue
			}
			if err := c.SetWriteDeadline(writeDeadline); err != nil {
				if failed == nil {
					failed = make(map[net.Conn]struct{})
				}
				failed[c] = struct{}{}
				s.dropConn(c)
				report.RecipientDrops++
				report.Err = err
				continue
			}
			if _, err := c.Write(doc); err != nil {
				if failed == nil {
					failed = make(map[net.Conn]struct{})
				}
				failed[c] = struct{}{}
				s.dropConn(c)
				report.RecipientDrops++
				report.Err = err
			} else {
				accepted = true
			}
		}
		report.Accepted[i] = accepted
	}
	return report
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
	if s.stopped {
		s.mu.Unlock()
		s.wg.Wait()
		return
	}
	s.stopped = true
	s.cancel()
	ln := s.ln
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.conns = map[net.Conn]bool{}
	s.mu.Unlock()
	_ = ln.Close()
	// Emptying s.conns above claimed these conns: a concurrent Broadcast
	// write failure now finds dropConn's `known` false and won't double-
	// close or double-decrement. Each accepted conn's +1 gets exactly one
	// -1 here, keeping the gauge drift-free across sink restarts.
	for _, c := range conns {
		_ = c.Close()
		s.met.SinkClients(s.id, -1)
	}
	s.wg.Wait()
}
