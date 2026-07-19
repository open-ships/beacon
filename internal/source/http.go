package source

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/stats"
)

// runFunc runs one connection attempt, publishing envelopes until error. It
// must call connected exactly once, after the endpoint is actually established
// (dialed and subscribed), so the dialer only then reports "up".
type runFunc func(ctx context.Context, cfg model.Source, publish func(*msg.Envelope), connected func()) error

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
	s := &dialerSource{id: cfg.ID, hub: newHub(met, cfg.ID), cancel: cancel, state: "degraded"}
	publish := func(e *msg.Envelope) {
		e.Seq, e.ConnectorID = 0, "" // upstream identifiers do not survive re-ingest
		met.SourceMessages(runCtx, cfg.ID, 1)
		reg.RecordSource(cfg.ID, e)
		s.hub.publish(e)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		const baseBackoff = 250 * time.Millisecond
		backoff := baseBackoff
		// connected is invoked by run once the endpoint is actually
		// established. The source starts (and stays) "degraded" until then, so
		// an endpoint that never connects — bad host, missing port — never
		// flashes "up". Only reached from run() on this goroutine, so the
		// backoff reset is race-free.
		connected := func() {
			s.setState("up", nil)
			backoff = baseBackoff // a real connection earns a fresh backoff budget
		}
		for runCtx.Err() == nil {
			err := run(runCtx, cfg, publish, connected)
			if runCtx.Err() != nil {
				return
			}
			s.setState("degraded", err)
			log.Warn("source disconnected; reconnecting", "source", cfg.ID, "err", err)
			select {
			case <-runCtx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sse endpoint returned %s", resp.Status)
	}
	connected()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue // id:, event:, retry:, comments, blank separators
		}
		var e msg.Envelope
		if err := json.Unmarshal([]byte(strings.TrimSpace(line[5:])), &e); err != nil {
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
	opts := &websocket.DialOptions{HTTPHeader: http.Header{}}
	for k, v := range cfg.Headers {
		opts.HTTPHeader.Set(k, v)
	}
	c, _, err := websocket.Dial(ctx, cfg.URL, opts)
	if err != nil {
		return err
	}
	defer func() { _ = c.CloseNow() }()
	connected()
	c.SetReadLimit(1024 * 1024)
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return err
		}
		var e msg.Envelope
		if err := json.Unmarshal(data, &e); err != nil {
			continue
		}
		publish(&e)
	}
}
