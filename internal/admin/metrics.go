// Package admin implements the admin HTTP server, health checks, and OTel metrics.
package admin

import (
	"fmt"

	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	otelapi "go.opentelemetry.io/otel/attribute"
)

// Metrics holds all OTel metric handles.
type Metrics struct {
	Meter metric.Meter

	// CAN metrics
	CANFramesTotal  metric.Int64Counter
	CANErrorsTotal  metric.Int64Counter

	// PGN metrics
	PGNMessagesTotal metric.Int64Counter
	PGNUnknownTotal  metric.Int64Counter

	// Buffer metrics
	BufferInsertsTotal metric.Int64Counter
	BufferSizeRows     metric.Int64Gauge
	BufferPrunedTotal  metric.Int64Counter

	// Sink metrics
	SinkSentTotal    metric.Int64Counter
	SinkDroppedTotal metric.Int64Counter
	SinkClients      metric.Int64Gauge
	SinkLagMessages  metric.Int64Gauge

	// Filter metrics
	FilterEvaluatedTotal metric.Int64Counter
	FilterErrorsTotal    metric.Int64Counter
}

// Attribute helper functions for consistent label usage.

// AttrIface returns a metric.MeasurementOption with the "iface" attribute.
func AttrIface(iface string) metric.MeasurementOption {
	return metric.WithAttributes(otelapi.String("iface", iface))
}

// AttrType returns a metric.MeasurementOption with the "type" attribute.
func AttrType(t string) metric.MeasurementOption {
	return metric.WithAttributes(otelapi.String("type", t))
}

// AttrSink returns a metric.MeasurementOption with the "sink" attribute.
func AttrSink(sink string) metric.MeasurementOption {
	return metric.WithAttributes(otelapi.String("sink", sink))
}

// AttrPGN returns a metric.MeasurementOption with the "pgn" attribute.
func AttrPGN(pgn uint32) metric.MeasurementOption {
	return metric.WithAttributes(otelapi.Int64("pgn", int64(pgn)))
}

// AttrReason returns a metric.MeasurementOption with the "reason" attribute.
func AttrReason(reason string) metric.MeasurementOption {
	return metric.WithAttributes(otelapi.String("reason", reason))
}

// InitMetrics sets up the OTel prometheus exporter and registers all metrics.
func InitMetrics() (*Metrics, *sdkmetric.MeterProvider, error) {
	exporter, err := otelprom.New()
	if err != nil {
		return nil, nil, fmt.Errorf("prometheus exporter: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("beacon")

	m := &Metrics{Meter: meter}

	if m.CANFramesTotal, err = meter.Int64Counter(
		"beacon.can.frames",
		metric.WithDescription("Raw CAN frames received"),
	); err != nil {
		return nil, nil, err
	}

	if m.CANErrorsTotal, err = meter.Int64Counter(
		"beacon.can.errors",
		metric.WithDescription("CAN error frames"),
	); err != nil {
		return nil, nil, err
	}

	if m.PGNMessagesTotal, err = meter.Int64Counter(
		"beacon.pgn.messages",
		metric.WithDescription("Decoded N2K PGN messages"),
	); err != nil {
		return nil, nil, err
	}

	if m.PGNUnknownTotal, err = meter.Int64Counter(
		"beacon.pgn.unknown",
		metric.WithDescription("Frames with unrecognised PGN"),
	); err != nil {
		return nil, nil, err
	}

	if m.BufferInsertsTotal, err = meter.Int64Counter(
		"beacon.buffer.inserts",
		metric.WithDescription("Messages written to SQLite buffer"),
	); err != nil {
		return nil, nil, err
	}

	if m.BufferSizeRows, err = meter.Int64Gauge(
		"beacon.buffer.size_rows",
		metric.WithDescription("Current row count in the buffer"),
	); err != nil {
		return nil, nil, err
	}

	if m.BufferPrunedTotal, err = meter.Int64Counter(
		"beacon.buffer.pruned",
		metric.WithDescription("Rows deleted by ring-buffer pruning"),
	); err != nil {
		return nil, nil, err
	}

	if m.SinkSentTotal, err = meter.Int64Counter(
		"beacon.sink.sent",
		metric.WithDescription("Messages delivered to a sink"),
	); err != nil {
		return nil, nil, err
	}

	if m.SinkDroppedTotal, err = meter.Int64Counter(
		"beacon.sink.dropped",
		metric.WithDescription("Messages dropped"),
	); err != nil {
		return nil, nil, err
	}

	if m.SinkClients, err = meter.Int64Gauge(
		"beacon.sink.clients",
		metric.WithDescription("Currently connected clients"),
	); err != nil {
		return nil, nil, err
	}

	if m.SinkLagMessages, err = meter.Int64Gauge(
		"beacon.sink.lag_messages",
		metric.WithDescription("Buffer rows behind the sink checkpoint"),
	); err != nil {
		return nil, nil, err
	}

	if m.FilterEvaluatedTotal, err = meter.Int64Counter(
		"beacon.filter.evaluated",
		metric.WithDescription("CEL filter evaluations"),
	); err != nil {
		return nil, nil, err
	}

	if m.FilterErrorsTotal, err = meter.Int64Counter(
		"beacon.filter.errors",
		metric.WithDescription("CEL eval errors"),
	); err != nil {
		return nil, nil, err
	}

	return m, provider, nil
}
