package source

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/retry"
	"github.com/open-ships/beacon/internal/stats"
)

// runFunc runs one connection attempt, publishing envelopes until error. It
// must call connected exactly once, after the endpoint is actually established
// (dialed and subscribed), so the dialer only then reports "up".
type runFunc func(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error

var (
	sourceReconnectMin        = 250 * time.Millisecond
	sourceReconnectMax        = time.Minute
	sourceStableConnectionAge = 30 * time.Second
	sourceHTTPAttemptTimeout  = 15 * time.Second
)

func newSourceHTTPClient() *http.Client {
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true}
	if defaults, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = defaults.Clone()
	}
	transport.DialContext = (&net.Dialer{
		Timeout: sourceHTTPAttemptTimeout, KeepAlive: 30 * time.Second,
	}).DialContext
	transport.ResponseHeaderTimeout = sourceHTTPAttemptTimeout
	transport.TLSHandshakeTimeout = sourceHTTPAttemptTimeout
	transport.MaxConnsPerHost = 1
	transport.MaxIdleConnsPerHost = 1
	return &http.Client{Transport: transport}
}

// dialerSource maintains a dial-reconnect loop around a runFunc.
type dialerSource struct {
	id     string
	hub    *hub
	cancel context.CancelFunc

	mu      sync.Mutex
	state   string
	lastErr error
	wg      sync.WaitGroup
	stop    sync.Once
}

func newDialerSource(ctx context.Context, cfg model.Source, log *slog.Logger, met *metrics.Set, reg *stats.Registry, run runFunc) (Runtime, error) {
	runCtx, cancel := context.WithCancel(ctx)
	s := &dialerSource{id: cfg.ID, hub: newHub(met, cfg.ID, reg), cancel: cancel, state: "degraded"}
	publish := func(e *msg.Envelope) {
		e.Seq, e.ConnectorID = 0, "" // upstream identifiers do not survive re-ingest
		if e.DeviceName != nil && e.DeviceNameHex == "" {
			e.DeviceNameHex = fmt.Sprintf("%016X", *e.DeviceName)
		}
		if e.OriginIngress == "" {
			e.OriginIngress = e.Ingress
			if e.OriginIngress == "" {
				e.OriginIngress = cfg.ID
			}
		}
		e.Ingress = cfg.ID
		e.ObservedAt = time.Now().UTC()
		met.SourceMessages(runCtx, cfg.ID, 1)
		reg.RecordSource(cfg.ID, e)
		s.hub.publish(e)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		backoff := retry.NewBackoff(sourceReconnectMin, sourceReconnectMax)
		consecutiveFailures := 0
		for runCtx.Err() == nil {
			var connectedAt time.Time
			// connected is invoked by run once the endpoint is actually
			// established. Report "up" immediately, but do not reset retry history
			// until the session has remained useful for a while: a peer that accepts
			// a handshake and drops it immediately must still reach the full backoff.
			connected := func() {
				if connectedAt.IsZero() {
					connectedAt = time.Now()
				}
				s.setState("up", nil)
			}
			err := run(runCtx, cfg, publish, connected)
			if runCtx.Err() != nil {
				return
			}
			if !connectedAt.IsZero() && time.Since(connectedAt) >= sourceStableConnectionAge {
				backoff.Reset()
				consecutiveFailures = 0
			}
			s.setState("degraded", err)
			consecutiveFailures++
			delay := backoff.Next()
			if consecutiveFailures == 1 || consecutiveFailures%60 == 0 {
				log.Warn("source disconnected; reconnecting", "source", cfg.ID, "err", err,
					"retry_in", delay, "consecutive_failures", consecutiveFailures)
			} else {
				log.Debug("source still disconnected", "source", cfg.ID, "err", err,
					"retry_in", delay, "consecutive_failures", consecutiveFailures)
			}
			if !retry.Sleep(runCtx, delay) {
				return
			}
		}
	}()
	return s, nil
}

func (s *dialerSource) setState(state string, err error) {
	s.mu.Lock()
	s.state, s.lastErr = state, err
	s.mu.Unlock()
}

func (s *dialerSource) ID() string { return s.id }
func (s *dialerSource) Subscribe(buf int) (<-chan *msg.Envelope, func()) {
	return s.hub.subscribe(buf)
}
func (s *dialerSource) State() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, s.lastErr
}
func (s *dialerSource) Stop() {
	s.stop.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.hub.closeAll()
	})
}

// runSSE consumes a Server-Sent Events stream of envelope JSON.
func runSSE(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}
	client := newSourceHTTPClient()
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sse endpoint returned %s", resp.Status)
	}
	connected()
	sc := bufio.NewScanner(resp.Body)
	// Allow the bounded document plus the SSE field prefix and Scanner's token
	// bookkeeping. A conventional "data: " prefix must not make an Envelope at
	// the exact ingress limit fail one byte early.
	sc.Buffer(make([]byte, 0, 64*1024), msg.MaxWireEnvelopeBytes+64)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue // id:, event:, retry:, comments, blank separators
		}
		var e msg.Envelope
		document := []byte(strings.TrimSpace(line[5:]))
		if len(document) > msg.MaxWireEnvelopeBytes || json.Unmarshal(document, &e) != nil ||
			msg.ValidateRemote(&e, len(document)) != nil {
			continue // tolerate junk lines
		}
		publish(&e)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return errors.New("sse stream ended")
}

// runWS consumes NDJSON text messages from a WebSocket.
func runWS(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error {
	client := newSourceHTTPClient()
	defer client.CloseIdleConnections()
	opts := &websocket.DialOptions{HTTPClient: client, HTTPHeader: http.Header{}}
	for k, v := range cfg.Headers {
		opts.HTTPHeader.Set(k, v)
	}
	c, _, err := websocket.Dial(ctx, cfg.URL, opts)
	if err != nil {
		return err
	}
	defer func() { _ = c.CloseNow() }()
	connected()
	c.SetReadLimit(msg.MaxWireEnvelopeBytes)
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return err
		}
		var e msg.Envelope
		if err := json.Unmarshal(data, &e); err != nil || msg.ValidateRemote(&e, len(data)) != nil {
			continue
		}
		publish(&e)
	}
}
