// Package source runs configured sources and fans their envelopes out to
// subscribing connectors.
package source

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

// Runtime is a running source: it broadcasts envelopes to subscribers.
type Runtime interface {
	ID() string
	Subscribe(buf int) (<-chan *msg.Envelope, func())
	Stop()
	State() (string, error) // "up" | "degraded" | "error"
}

// New starts the source runtime for cfg. CAN types acquire from mgr;
// HTTP types dial cfg.URL with reconnect backoff (250ms→5s).
func New(ctx context.Context, cfg model.Source, mgr *bus.Manager, log *slog.Logger, met *metrics.Set) (Runtime, error) {
	switch cfg.Type {
	case model.SourceSocketCAN, model.SourceUSBCAN:
		return newCANSource(ctx, cfg, mgr, log, met)
	case model.SourceHTTPSSE:
		return newDialerSource(ctx, cfg, log, met, runSSE)
	case model.SourceHTTPWS:
		return newDialerSource(ctx, cfg, log, met, runWS)
	default:
		return nil, fmt.Errorf("source %q: unknown type %q", cfg.ID, cfg.Type)
	}
}

// hub is a non-blocking broadcast: slow subscribers drop, never block.
type hub struct {
	mu   sync.Mutex
	subs map[int64]chan *msg.Envelope
	next int64
}

func newHub() *hub { return &hub{subs: map[int64]chan *msg.Envelope{}} }

func (h *hub) subscribe(buf int) (<-chan *msg.Envelope, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.next
	h.next++
	ch := make(chan *msg.Envelope, buf)
	h.subs[id] = ch
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if c, ok := h.subs[id]; ok {
			delete(h.subs, id)
			close(c)
		}
	}
}

func (h *hub) publish(e *msg.Envelope) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

func (h *hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, ch := range h.subs {
		delete(h.subs, id)
		close(ch)
	}
}

// canSource adapts a bus.Handle subscription into a source Runtime.
type canSource struct {
	id     string
	handle *bus.Handle
	hub    *hub
	cancel context.CancelFunc
}

func newCANSource(ctx context.Context, cfg model.Source, mgr *bus.Manager, log *slog.Logger, met *metrics.Set) (Runtime, error) {
	ep := bus.Endpoint{Kind: string(cfg.Type), Name: cfg.Interface}
	if cfg.Type == model.SourceUSBCAN {
		ep.Name = cfg.Port
	}
	handle, err := mgr.Acquire(ctx, ep)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &canSource{id: cfg.ID, handle: handle, hub: newHub(), cancel: cancel}
	ch, unsub := handle.Subscribe(256)
	go func() {
		defer unsub()
		for {
			select {
			case <-runCtx.Done():
				return
			case e, ok := <-ch:
				if !ok {
					return
				}
				met.SourceMessages(runCtx, cfg.ID, 1)
				s.hub.publish(e)
			}
		}
	}()
	return s, nil
}

func (s *canSource) ID() string { return s.id }
func (s *canSource) Subscribe(buf int) (<-chan *msg.Envelope, func()) {
	return s.hub.subscribe(buf)
}
func (s *canSource) State() (string, error) { return s.handle.State() }
func (s *canSource) Stop() {
	s.cancel()
	s.hub.closeAll()
	s.handle.Release()
}
