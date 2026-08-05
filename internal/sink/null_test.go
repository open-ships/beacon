package sink

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

func TestNullSinkAcceptsWithoutDependencies(t *testing.T) {
	rt, err := New(context.Background(), model.Sink{
		ID: "discard", Name: "Discard", Type: model.SinkNull, Enabled: true,
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	if rt.ID() != "discard" {
		t.Fatalf("ID = %q, want discard", rt.ID())
	}
	if state, err := rt.State(); state != "up" || err != nil {
		t.Fatalf("State = %q, %v; want up, nil", state, err)
	}
	if got := DeliveryClassOf(rt); got != DeliveryConfirmed {
		t.Fatalf("DeliveryClass = %q, want %q", got, DeliveryConfirmed)
	}
	if err := rt.(Pusher).Push(context.Background(), &msg.Envelope{PGN: 127250}); err != nil {
		t.Fatalf("Push = %v, want nil", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rt.(Pusher).Push(canceled, &msg.Envelope{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Push with canceled context = %v, want context.Canceled", err)
	}
}
