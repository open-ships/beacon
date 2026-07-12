// Package bus owns one n2k.Client per physical CAN endpoint. NMEA 2000
// address claiming allows one bus participant per interface, so sources
// and sinks on the same interface share a client via refcounted handles.
package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	n2k "github.com/open-ships/n2k"
	"github.com/open-ships/n2k/pgn"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/msg"
)

type Endpoint struct {
	Kind string // "socketcan" | "usbcan"
	Name string // interface name or serial port path
}

func (ep Endpoint) option() (n2k.Option, error) {
	switch ep.Kind {
	case "socketcan":
		return n2k.CAN(ep.Name), nil
	case "usbcan":
		return n2k.USB(ep.Name), nil
	default:
		return nil, fmt.Errorf("unknown CAN endpoint kind %q", ep.Kind)
	}
}

type Manager struct {
	log       *slog.Logger
	met       *metrics.Set
	extraOpts []n2k.Option

	mu      sync.Mutex
	clients map[Endpoint]*busClient
}

func NewManager(log *slog.Logger, met *metrics.Set, extraOpts ...n2k.Option) *Manager {
	return &Manager{log: log, met: met, extraOpts: extraOpts, clients: map[Endpoint]*busClient{}}
}

func (m *Manager) clientCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.clients)
}

// Acquire returns a refcounted handle on the endpoint's shared client,
// starting it if this is the first reference.
func (m *Manager) Acquire(ctx context.Context, ep Endpoint) (*Handle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bc, ok := m.clients[ep]
	if !ok {
		opt, err := ep.option()
		if err != nil {
			return nil, err
		}
		runCtx, cancel := context.WithCancel(context.Background())
		bc = &busClient{
			mgr: m, ep: ep, opt: opt, cancel: cancel,
			subs:  map[int64]chan *msg.Envelope{},
			state: "degraded",
		}
		m.clients[ep] = bc
		bc.wg.Add(1)
		go bc.run(runCtx)
	}
	bc.refs++
	return &Handle{bc: bc}, nil
}

type busClient struct {
	mgr    *Manager
	ep     Endpoint
	opt    n2k.Option
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	refs    int
	client  *n2k.Client
	subs    map[int64]chan *msg.Envelope
	nextSub int64
	state   string
	lastErr error
}

// run maintains the client: (re)connect, pump Receive into subscribers,
// back off on failure, until cancelled by the last Release.
func (bc *busClient) run(ctx context.Context) {
	defer bc.wg.Done()
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		opts := append([]n2k.Option{bc.opt, n2k.IncludeUnknown(), n2k.WithLogger(bc.mgr.log)},
			bc.mgr.extraOpts...)
		client, err := n2k.NewClient(ctx, opts...)
		if err != nil {
			bc.setState("error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
			continue
		}
		bc.mu.Lock()
		bc.client = client
		bc.mu.Unlock()
		bc.setState("up", nil)
		backoff = 250 * time.Millisecond

		for m, err := range client.Receive() {
			if err != nil {
				bc.mgr.log.Debug("n2k receive error", "endpoint", bc.ep.Name, "err", err)
				continue
			}
			e, err := msg.FromPGN(m)
			if err != nil {
				bc.mgr.log.Debug("envelope conversion error", "err", err)
				continue
			}
			bc.broadcast(e)
		}
		// Receive ended: client dead or ctx cancelled.
		_ = client.Close()
		bc.mu.Lock()
		bc.client = nil
		bc.mu.Unlock()
		if ctx.Err() == nil {
			bc.setState("degraded", errors.New("receive loop ended; reconnecting"))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
		}
	}
}

func (bc *busClient) broadcast(e *msg.Envelope) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	for _, ch := range bc.subs {
		select {
		case ch <- e:
		default: // slow subscriber: drop rather than block the bus
		}
	}
}

func (bc *busClient) setState(state string, err error) {
	bc.mu.Lock()
	bc.state, bc.lastErr = state, err
	bc.mu.Unlock()
	var v int64
	switch state {
	case "up":
		v = 2
	case "degraded":
		v = 1
	}
	bc.mgr.met.SetComponentState("bus", bc.ep.Kind+":"+bc.ep.Name, v)
}

type Handle struct {
	bc       *busClient
	released sync.Once
}

// Subscribe returns a channel of decoded envelopes received on the shared
// client, and an unsubscribe function that removes it. buf sets the
// channel's buffer size; a full subscriber has envelopes dropped rather than
// blocking the shared receive loop.
func (h *Handle) Subscribe(buf int) (<-chan *msg.Envelope, func()) {
	bc := h.bc
	bc.mu.Lock()
	id := bc.nextSub
	bc.nextSub++
	ch := make(chan *msg.Envelope, buf)
	bc.subs[id] = ch
	bc.mu.Unlock()
	return ch, func() {
		bc.mu.Lock()
		delete(bc.subs, id)
		bc.mu.Unlock()
	}
}

// Write re-encodes the envelope onto the bus. Requires Raw bytes.
func (h *Handle) Write(ctx context.Context, e *msg.Envelope) error {
	if len(e.Raw) == 0 {
		return fmt.Errorf("pgn %d: envelope has no raw bytes; cannot encode to CAN", e.PGN)
	}
	h.bc.mu.Lock()
	client := h.bc.client
	h.bc.mu.Unlock()
	if client == nil {
		return fmt.Errorf("bus %s not connected", h.bc.ep.Name)
	}
	m, err := pgn.DecodeMessage(e.Info(), e.Raw)
	if err != nil {
		return fmt.Errorf("decode envelope for CAN write: %w", err)
	}
	return client.Write(m).Wait()
}

// State reports the shared client's current connection state ("up",
// "degraded", or "error") and the last error observed, if any.
func (h *Handle) State() (state string, lastErr error) {
	h.bc.mu.Lock()
	defer h.bc.mu.Unlock()
	return h.bc.state, h.bc.lastErr
}

// Release decrements the handle's reference on the shared client. The last
// Release for an endpoint cancels the receive loop and closes the client.
func (h *Handle) Release() {
	h.released.Do(func() {
		bc := h.bc
		mgr := bc.mgr
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		bc.mu.Lock()
		bc.refs--
		last := bc.refs == 0
		bc.mu.Unlock()
		if last {
			bc.cancel()
			delete(mgr.clients, bc.ep)
		}
	})
}
