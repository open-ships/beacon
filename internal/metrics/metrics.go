// Package metrics owns the OTel instrument set. A nil *Set no-ops every
// method so components never need nil checks around instrumentation.
package metrics

import (
	"context"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	api "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type gaugeKey struct{ kind, id string }

type Set struct {
	connectorMessages api.Int64Counter
	connectorBytes    api.Int64Counter
	sourceMessages    api.Int64Counter
	queueDepth        api.Int64ObservableGauge
	queueBytes        api.Int64ObservableGauge
	componentState    api.Int64ObservableGauge
	sinkClients       api.Int64UpDownCounter

	mu     sync.Mutex
	depths map[string][2]int64 // connector -> {depth, bytes}
	states map[gaugeKey]int64
}

func New() (*Set, http.Handler, error) {
	reg := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, nil, err
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("beacon")

	s := &Set{depths: map[string][2]int64{}, states: map[gaugeKey]int64{}}
	s.connectorMessages, _ = meter.Int64Counter("beacon.connector.messages")
	s.connectorBytes, _ = meter.Int64Counter("beacon.connector.bytes")
	s.sourceMessages, _ = meter.Int64Counter("beacon.source.messages")
	s.sinkClients, _ = meter.Int64UpDownCounter("beacon.sink.clients")
	s.queueDepth, _ = meter.Int64ObservableGauge("beacon.connector.queue.depth")
	s.queueBytes, _ = meter.Int64ObservableGauge("beacon.connector.queue.bytes")
	s.componentState, _ = meter.Int64ObservableGauge("beacon.component.state")
	_, err = meter.RegisterCallback(func(_ context.Context, o api.Observer) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for c, v := range s.depths {
			o.ObserveInt64(s.queueDepth, v[0], api.WithAttributes(attribute.String("connector", c)))
			o.ObserveInt64(s.queueBytes, v[1], api.WithAttributes(attribute.String("connector", c)))
		}
		for k, v := range s.states {
			o.ObserveInt64(s.componentState, v,
				api.WithAttributes(attribute.String("kind", k.kind), attribute.String("id", k.id)))
		}
		return nil
	}, s.queueDepth, s.queueBytes, s.componentState)
	if err != nil {
		return nil, nil, err
	}
	return s, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}

func (s *Set) ConnectorMessages(ctx context.Context, connector, stage string, n int64) {
	if s == nil {
		return
	}
	s.connectorMessages.Add(ctx, n, api.WithAttributes(
		attribute.String("connector", connector), attribute.String("stage", stage)))
}

func (s *Set) ConnectorBytes(ctx context.Context, connector string, n int64) {
	if s == nil {
		return
	}
	s.connectorBytes.Add(ctx, n, api.WithAttributes(attribute.String("connector", connector)))
}

func (s *Set) SourceMessages(ctx context.Context, source string, n int64) {
	if s == nil {
		return
	}
	s.sourceMessages.Add(ctx, n, api.WithAttributes(attribute.String("source", source)))
}

func (s *Set) SinkClients(sink string, delta int64) {
	if s == nil {
		return
	}
	s.sinkClients.Add(context.Background(), delta, api.WithAttributes(attribute.String("sink", sink)))
}

func (s *Set) SetQueueDepth(connector string, depth, bytes int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.depths[connector] = [2]int64{depth, bytes}
	s.mu.Unlock()
}

func (s *Set) SetComponentState(kind, id string, state int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.states[gaugeKey{kind, id}] = state
	s.mu.Unlock()
}

// RemoveConnector drops the queue depth/bytes gauge entry for a connector.
// Called when a connector is removed from config entirely so its last
// observed queue depth doesn't linger in the exposition forever.
func (s *Set) RemoveConnector(connector string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.depths, connector)
	s.mu.Unlock()
}

// RemoveComponent drops the component-state gauge entry for a source, sink,
// or connector. Called when a component is stopped and no longer desired
// (disabled or deleted) so its last state doesn't linger in the exposition.
func (s *Set) RemoveComponent(kind, id string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.states, gaugeKey{kind, id})
	s.mu.Unlock()
}
