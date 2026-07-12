// Package sink runs configured sinks. CAN sinks push-confirm each message
// onto the bus; HTTP/TCP sinks broadcast to connected clients, with
// replay served straight from connector queues (SSE/WS only).
package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

var ErrSkip = errors.New("sink skipped message")

type Runtime interface {
	ID() string
	Stop()
	State() (string, error)
}

// Pusher sinks confirm each delivery (CAN). ErrSkip means "cannot carry
// this message, count it and move on" (e.g. envelope without raw bytes).
type Pusher interface {
	Push(ctx context.Context, e *msg.Envelope) error
}

// Broadcaster sinks fan out to connected clients without confirmation.
type Broadcaster interface {
	Broadcast(entries []queue.Entry)
}

// ReplayReader is what serve-mode sinks use to replay history for a client.
type ReplayReader interface {
	Read(ctx context.Context, after int64, limit int) ([]queue.Entry, error)
}

// ConnectorRegistrar lets connectors attach their queue for client replay.
type ConnectorRegistrar interface {
	RegisterConnector(id string, r ReplayReader)
	UnregisterConnector(id string)
}

func New(ctx context.Context, cfg model.Sink, mgr *bus.Manager, ds *DataServer, log *slog.Logger, met *metrics.Set) (Runtime, error) {
	switch cfg.Type {
	case model.SinkSocketCAN, model.SinkUSBCAN:
		return newCANSink(ctx, cfg, mgr)
	case model.SinkHTTPSSE:
		return newServeSink(cfg, ds, log, met, serveSSE)
	case model.SinkHTTPWS:
		return newServeSink(cfg, ds, log, met, serveWS)
	case model.SinkTCP:
		return newTCPSink(cfg, log, met)
	default:
		return nil, fmt.Errorf("sink %q: unknown type %q", cfg.ID, cfg.Type)
	}
}

type canSink struct {
	id     string
	handle *bus.Handle
}

func newCANSink(ctx context.Context, cfg model.Sink, mgr *bus.Manager) (Runtime, error) {
	ep := bus.Endpoint{Kind: string(cfg.Type), Name: cfg.Interface}
	if cfg.Type == model.SinkUSBCAN {
		ep.Name = cfg.Port
	}
	handle, err := mgr.Acquire(ctx, ep)
	if err != nil {
		return nil, err
	}
	return &canSink{id: cfg.ID, handle: handle}, nil
}

func (s *canSink) ID() string { return s.id }

// Push writes one envelope onto the bus; envelopes without raw bytes are
// skipped (ErrSkip) since they cannot be encoded.
func (s *canSink) Push(ctx context.Context, e *msg.Envelope) error {
	if len(e.Raw) == 0 {
		return ErrSkip
	}
	return s.handle.Write(ctx, e)
}

func (s *canSink) State() (string, error) { return s.handle.State() }
func (s *canSink) Stop()                  { s.handle.Release() }
