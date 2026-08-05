package sink

import (
	"context"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

// nullSink accepts every message and intentionally discards it. It is a
// confirmed Pusher so connectors checkpoint accepted entries and expose the
// same delivered-message, byte, event-stream, and sink statistics as other
// confirmed sinks.
type nullSink struct {
	id string
}

func newNullSink(cfg model.Sink) Runtime {
	return &nullSink{id: cfg.ID}
}

func (s *nullSink) ID() string                   { return s.id }
func (s *nullSink) Stop()                        {}
func (s *nullSink) State() (string, error)       { return "up", nil }
func (s *nullSink) DeliveryClass() DeliveryClass { return DeliveryConfirmed }

func (s *nullSink) Push(ctx context.Context, _ *msg.Envelope) error {
	return ctx.Err()
}
