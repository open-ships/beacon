// Package sink runs configured sinks. CAN/file/null sinks push-confirm each
// message; HTTP/TCP sinks broadcast to connected clients, with replay served
// straight from connector queues (SSE/WS only).
package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/brutella/can"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/n2kwire"
	"github.com/open-ships/beacon/internal/queue"
)

var ErrSkip = errors.New("sink skipped message")

type DeliveryClass string

const (
	DeliveryConfirmed  DeliveryClass = "confirmed"
	DeliveryResumable  DeliveryClass = "resumable"
	DeliveryBestEffort DeliveryClass = "best_effort"
	DeliveryObserved   DeliveryClass = "observe_only"
)

type Runtime interface {
	ID() string
	Stop()
	State() (string, error)
}

// DeliveryClassifier makes the route boundary explicit. Implementations
// that do not provide it are classified from their Pusher/Broadcaster seam
// by DeliveryClassOf.
type DeliveryClassifier interface {
	DeliveryClass() DeliveryClass
}

func DeliveryClassOf(r Runtime) DeliveryClass {
	if c, ok := r.(DeliveryClassifier); ok {
		return c.DeliveryClass()
	}
	if _, ok := r.(Pusher); ok {
		return DeliveryConfirmed
	}
	return DeliveryBestEffort
}

// Pusher sinks confirm each delivery (CAN). ErrSkip means "cannot carry
// this message, count it and move on" (e.g. envelope without raw bytes).
type Pusher interface {
	Push(ctx context.Context, e *msg.Envelope) error
}

// WirePusher preserves the envelope's original N2K source identity and raw
// payload instead of re-originating it through Beacon's claimed client.
type WirePusher interface {
	PushWire(ctx context.Context, e *msg.Envelope) error
}

// Broadcaster sinks fan out to connected clients without confirmation.
type Broadcaster interface {
	Broadcast(entries []queue.Entry) BroadcastReport
}

type BroadcastReport struct {
	Accepted       []bool
	RecipientDrops int64
	Err            error
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
	case model.SinkFile:
		return newFileSink(cfg, log)
	case model.SinkMQTT:
		return newMQTTSink(ctx, cfg, log, met)
	case model.SinkTCPGateway:
		return newGatewaySink(ctx, cfg, mgr)
	case model.SinkNull:
		return newNullSink(cfg), nil
	default:
		return nil, fmt.Errorf("sink %q: unknown type %q", cfg.ID, cfg.Type)
	}
}

type canSink struct {
	id            string
	handle        *bus.Handle
	wireInterface string

	wireMu   sync.Mutex
	wireBus  *can.Bus
	wireErr  error
	fragment *n2kwire.Fragmenter
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
	wireInterface := ""
	if cfg.Type == model.SinkSocketCAN {
		wireInterface = cfg.Interface
	}
	return &canSink{id: cfg.ID, handle: handle, wireInterface: wireInterface,
		fragment: n2kwire.NewFragmenter()}, nil
}

func (s *canSink) ID() string                   { return s.id }
func (s *canSink) DeliveryClass() DeliveryClass { return DeliveryConfirmed }

// Push writes one envelope onto the bus; envelopes without raw bytes, or
// whose raw bytes cannot be re-decoded for CAN transmission (e.g. an
// UnknownPGN with no cataloged decoder — see bus.ErrNotEncodable), are
// skipped (ErrSkip) rather than retried forever.
func (s *canSink) Push(ctx context.Context, e *msg.Envelope) error {
	if len(e.Raw) == 0 {
		return ErrSkip
	}
	err := s.handle.Write(ctx, e)
	if errors.Is(err, bus.ErrNotEncodable) {
		return ErrSkip
	}
	return err
}

func (s *canSink) PushWire(ctx context.Context, e *msg.Envelope) error {
	if s.wireInterface == "" {
		return errors.New("transparent forwarding is only available for SocketCAN sinks")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	if s.wireBus == nil {
		wireBus, err := can.NewBusForInterfaceWithName(s.wireInterface)
		if err != nil {
			s.wireErr = err
			return err
		}
		s.wireBus = wireBus
	}
	frames, err := s.fragment.Frames(e)
	if err != nil {
		s.wireErr = err
		return err
	}
	for _, frame := range frames {
		if err := s.wireBus.Publish(frame); err != nil {
			s.wireErr = err
			return err
		}
	}
	s.wireErr = nil
	return nil
}

func (s *canSink) State() (string, error) {
	s.wireMu.Lock()
	err := s.wireErr
	s.wireMu.Unlock()
	if err != nil {
		return "degraded", err
	}
	return s.handle.State()
}
func (s *canSink) Stop() {
	s.wireMu.Lock()
	if s.wireBus != nil {
		_ = s.wireBus.Disconnect()
		s.wireBus = nil
	}
	s.wireMu.Unlock()
	s.handle.Release()
}

// newGatewaySink transmits onto a remote NMEA-2000 bus through a TCP WiFi
// gateway (Yacht Devices RAW or Actisense). It reuses canSink: the bus
// manager's "tcp" endpoint kind gives it the same shared, refcounted n2k
// client — with address claiming, transport auto-reconnect (n2k
// WithReconnect), and the manager's reconnect/state machine — that the
// socketcan/usbcan sinks get, and Handle.Write's re-encode path applies
// unchanged (UnknownPGN envelopes are skipped via ErrNotEncodable exactly as
// on CAN hardware).
func newGatewaySink(ctx context.Context, cfg model.Sink, mgr *bus.Manager) (Runtime, error) {
	handle, err := mgr.Acquire(ctx, bus.Endpoint{Kind: "tcp", Name: cfg.Address, Format: cfg.Format})
	if err != nil {
		return nil, err
	}
	return &canSink{id: cfg.ID, handle: handle, fragment: n2kwire.NewFragmenter()}, nil
}
