package sink

import (
	"context"
	"log/slog"
	"testing"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

func TestMQTTSinkBroadcastWithoutBrokerReportsDegraded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Sink{
		ID: "mqtt", Name: "MQTT", Type: model.SinkMQTT, Enabled: true,
		URL: "mqtt://127.0.0.1:1", Topic: "beacon/test",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	rt.(Broadcaster).Broadcast([]queue.Entry{{Env: &msg.Envelope{PGN: 127250}}})
	state, stateErr := rt.State()
	if state != "degraded" || stateErr == nil {
		t.Fatalf("state = %q/%v, want degraded/non-nil error", state, stateErr)
	}
}
