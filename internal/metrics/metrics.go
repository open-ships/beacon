// Package metrics owns the OTel instrument set. A nil *Set no-ops every
// method so components never need nil checks around instrumentation.
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	api "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/open-ships/beacon/internal/stats"
)

type gaugeKey struct{ kind, id string }

type Set struct {
	connectorMessages      api.Int64Counter
	connectorBytes         api.Int64Counter
	sourceMessages         api.Int64Counter
	subscriberDrops        api.Int64Counter
	queueDepth             api.Int64ObservableGauge
	queueBytes             api.Int64ObservableGauge
	componentState         api.Int64ObservableGauge
	sinkClients            api.Int64UpDownCounter
	sourcePGNMessages      api.Int64ObservableCounter
	sourcePGNFrequency     api.Float64ObservableGauge
	sourcePGNPeriod        api.Float64ObservableGauge
	sourcePGNLastSeen      api.Float64ObservableGauge
	sourcePGNPayload       api.Float64ObservableGauge
	sourcePGNGap           api.Int64ObservableGauge
	sourcePGNGapRatio      api.Float64ObservableGauge
	sourcePGNGaps          api.Int64ObservableCounter
	sourcePGNTiming        api.Float64ObservableGauge
	sourcePGNTraffic       api.Float64ObservableGauge
	sourcePGNDecode        api.Int64ObservableCounter
	sourcePGNDecodeOutcome api.Int64ObservableCounter
	sourcePGNDecodeMissing api.Int64ObservableCounter
	sourcePGNBursts        api.Int64ObservableCounter
	sourcePGNPayloadLength api.Int64ObservableCounter
	sourcePGNDestination   api.Int64ObservableCounter
	sourcePGNPriority      api.Int64ObservableCounter
	sourcePGNIdentity      api.Int64ObservableCounter
	sourcePGNStatus        api.Int64ObservableGauge
	sourcePGNRaw           api.Float64ObservableGauge
	sourcePGNRawByte       api.Float64ObservableGauge
	sourcePGNField         api.Float64ObservableGauge
	sourcePGNFieldState    api.Float64ObservableGauge
	sourcePGNFieldQuality  api.Int64ObservableCounter
	sourcePGNCategory      api.Float64ObservableGauge

	mu     sync.Mutex
	depths map[string][2]int64 // connector -> {depth, bytes}
	states map[gaugeKey]int64
}

func New(registries ...*stats.Registry) (*Set, http.Handler, error) {
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
	s.subscriberDrops, _ = meter.Int64Counter("beacon.subscriber.dropped")
	s.sinkClients, _ = meter.Int64UpDownCounter("beacon.sink.clients")
	s.queueDepth, _ = meter.Int64ObservableGauge("beacon.connector.queue.depth")
	s.queueBytes, _ = meter.Int64ObservableGauge("beacon.connector.queue.bytes")
	s.componentState, _ = meter.Int64ObservableGauge("beacon.component.state")
	s.sourcePGNMessages, _ = meter.Int64ObservableCounter("beacon.source.pgn.messages")
	s.sourcePGNFrequency, _ = meter.Float64ObservableGauge("beacon.source.pgn.frequency_hz")
	s.sourcePGNPeriod, _ = meter.Float64ObservableGauge("beacon.source.pgn.expected_period_seconds")
	s.sourcePGNLastSeen, _ = meter.Float64ObservableGauge("beacon.source.pgn.last_seen_unixtime")
	s.sourcePGNPayload, _ = meter.Float64ObservableGauge("beacon.source.pgn.payload_bytes")
	s.sourcePGNGap, _ = meter.Int64ObservableGauge("beacon.source.pgn.gap_active")
	s.sourcePGNGapRatio, _ = meter.Float64ObservableGauge("beacon.source.pgn.gap_ratio")
	s.sourcePGNGaps, _ = meter.Int64ObservableCounter("beacon.source.pgn.gaps")
	s.sourcePGNTiming, _ = meter.Float64ObservableGauge("beacon.source.pgn.timing_seconds")
	s.sourcePGNTraffic, _ = meter.Float64ObservableGauge("beacon.source.pgn.traffic")
	s.sourcePGNDecode, _ = meter.Int64ObservableCounter("beacon.source.pgn.decode.messages")
	s.sourcePGNDecodeOutcome, _ = meter.Int64ObservableCounter("beacon.source.pgn.decode.outcomes")
	s.sourcePGNDecodeMissing, _ = meter.Int64ObservableCounter("beacon.source.pgn.decode.missing_fields")
	s.sourcePGNBursts, _ = meter.Int64ObservableCounter("beacon.source.pgn.bursts")
	s.sourcePGNPayloadLength, _ = meter.Int64ObservableCounter("beacon.source.pgn.payload_length.messages")
	s.sourcePGNDestination, _ = meter.Int64ObservableCounter("beacon.source.pgn.destination.messages")
	s.sourcePGNPriority, _ = meter.Int64ObservableCounter("beacon.source.pgn.priority.messages")
	s.sourcePGNIdentity, _ = meter.Int64ObservableCounter("beacon.source.pgn.identity_changes")
	s.sourcePGNStatus, _ = meter.Int64ObservableGauge("beacon.source.pgn.status")
	s.sourcePGNRaw, _ = meter.Float64ObservableGauge("beacon.source.pgn.raw_payload")
	s.sourcePGNRawByte, _ = meter.Float64ObservableGauge("beacon.source.pgn.raw_byte")
	s.sourcePGNField, _ = meter.Float64ObservableGauge("beacon.source.pgn.field.value")
	s.sourcePGNFieldState, _ = meter.Float64ObservableGauge("beacon.source.pgn.field.state")
	s.sourcePGNFieldQuality, _ = meter.Int64ObservableCounter("beacon.source.pgn.field.quality")
	s.sourcePGNCategory, _ = meter.Float64ObservableGauge("beacon.source.pgn.field.category_summary")
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
	if len(registries) > 0 && registries[0] != nil {
		reg := registries[0]
		_, err = meter.RegisterCallback(func(_ context.Context, o api.Observer) error {
			observeSourcePGNMetrics(o, s, reg.AllSourcePGNMetrics())
			return nil
		}, s.sourcePGNMessages, s.sourcePGNFrequency, s.sourcePGNPeriod,
			s.sourcePGNLastSeen, s.sourcePGNPayload, s.sourcePGNGap,
			s.sourcePGNGapRatio, s.sourcePGNGaps, s.sourcePGNTiming, s.sourcePGNTraffic,
			s.sourcePGNDecode, s.sourcePGNDecodeOutcome, s.sourcePGNDecodeMissing,
			s.sourcePGNBursts, s.sourcePGNPayloadLength, s.sourcePGNDestination,
			s.sourcePGNPriority, s.sourcePGNIdentity, s.sourcePGNStatus,
			s.sourcePGNRaw, s.sourcePGNRawByte,
			s.sourcePGNField, s.sourcePGNFieldState, s.sourcePGNFieldQuality, s.sourcePGNCategory)
		if err != nil {
			return nil, nil, err
		}
	}
	return s, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}

func observeSourcePGNMetrics(o api.Observer, set *Set, all map[string][]stats.SourcePGNMetric) {
	for _, streams := range all {
		for _, stream := range streams {
			manufacturerCode := ""
			if stream.ManufacturerCode != nil {
				manufacturerCode = strconv.Itoa(int(*stream.ManufacturerCode))
			}
			base := []attribute.KeyValue{
				attribute.String("source", stream.SourceID),
				attribute.Int64("pgn", int64(stream.PGN)),
				attribute.String("pgn_name", stream.PGNName),
				attribute.Int64("source_address", int64(stream.SourceAddress)),
				attribute.String("device_name", stream.DeviceNameHex),
				attribute.String("variant", stream.Variant),
				attribute.String("transport", stream.Transport),
				attribute.String("manufacturer_code", manufacturerCode),
			}
			statusAttrs := appendMetricAttribute(base, attribute.String("status", stream.Status))
			o.ObserveInt64(set.sourcePGNStatus, 1, api.WithAttributes(statusAttrs...))
			if !stream.Observed {
				continue
			}
			o.ObserveInt64(set.sourcePGNMessages, stream.Messages, api.WithAttributes(base...))
			o.ObserveFloat64(set.sourcePGNFrequency, stream.FrequencyHz, api.WithAttributes(base...))
			o.ObserveFloat64(set.sourcePGNPeriod, stream.ExpectedPeriodSeconds, api.WithAttributes(base...))
			o.ObserveFloat64(set.sourcePGNLastSeen, float64(stream.LastSeen.UnixNano())/1e9, api.WithAttributes(base...))
			for statistic, value := range map[string]float64{
				"shortest": stream.ShortestPeriodSeconds, "median": stream.ExpectedPeriodSeconds,
				"p90": stream.PeriodP90Seconds, "p95": stream.PeriodP95Seconds, "p99": stream.PeriodP99Seconds,
				"mad": stream.JitterMADSeconds,
			} {
				attrs := appendMetricAttribute(base, attribute.String("statistic", statistic))
				o.ObserveFloat64(set.sourcePGNTiming, value, api.WithAttributes(attrs...))
			}
			for statistic, value := range map[string]float64{
				"messages_per_second":        stream.RecentMessagesPerSec,
				"bytes_per_second":           stream.RecentBytesPerSec,
				"estimated_bus_load_percent": stream.EstimatedBusLoadPercent,
				"source_share_percent":       stream.TrafficSharePercent,
			} {
				attrs := appendMetricAttribute(base, attribute.String("statistic", statistic))
				o.ObserveFloat64(set.sourcePGNTraffic, value, api.WithAttributes(attrs...))
			}
			for status, count := range stream.DecodeStatuses {
				attrs := appendMetricAttribute(base, attribute.String("status", status))
				o.ObserveInt64(set.sourcePGNDecode, count, api.WithAttributes(attrs...))
			}
			for outcome, count := range map[string]int64{
				"complete": stream.DecodeComplete, "incomplete": stream.DecodeIncomplete,
				"fallback": stream.DecodeFallback, "unknown": stream.UnknownMessages,
			} {
				attrs := appendMetricAttribute(base, attribute.String("outcome", outcome))
				o.ObserveInt64(set.sourcePGNDecodeOutcome, count, api.WithAttributes(attrs...))
			}
			for field, count := range stream.MissingDecodedFields {
				attrs := appendMetricAttribute(base, attribute.String("field", field))
				o.ObserveInt64(set.sourcePGNDecodeMissing, count, api.WithAttributes(attrs...))
			}
			o.ObserveInt64(set.sourcePGNBursts, stream.BurstCount, api.WithAttributes(base...))
			o.ObserveInt64(set.sourcePGNIdentity, stream.IdentityChanges, api.WithAttributes(base...))
			for destination, count := range stream.DestinationCounts {
				attrs := appendMetricAttribute(base, attribute.String("destination", destination))
				o.ObserveInt64(set.sourcePGNDestination, count, api.WithAttributes(attrs...))
			}
			for priority, count := range stream.PriorityCounts {
				attrs := appendMetricAttribute(base, attribute.String("priority", priority))
				o.ObserveInt64(set.sourcePGNPriority, count, api.WithAttributes(attrs...))
			}
			for statistic, value := range map[string]float64{
				"last": float64(stream.PayloadBytesLast), "min": float64(stream.PayloadBytesMin),
				"max": float64(stream.PayloadBytesMax), "mean": stream.PayloadBytesMean,
			} {
				attrs := appendMetricAttribute(base, attribute.String("statistic", statistic))
				o.ObserveFloat64(set.sourcePGNPayload, value, api.WithAttributes(attrs...))
			}
			gap := int64(0)
			if stream.GapActive {
				gap = 1
			}
			o.ObserveInt64(set.sourcePGNGap, gap, api.WithAttributes(base...))
			o.ObserveFloat64(set.sourcePGNGapRatio, stream.GapRatio, api.WithAttributes(base...))
			o.ObserveInt64(set.sourcePGNGaps, stream.GapCount, api.WithAttributes(base...))
			if stream.Raw != nil {
				for length, count := range stream.Raw.LengthCounts {
					attrs := appendMetricAttribute(base, attribute.String("length_bytes", length))
					o.ObserveInt64(set.sourcePGNPayloadLength, count, api.WithAttributes(attrs...))
				}
				for statistic, value := range map[string]float64{
					"distinct_payloads":          float64(stream.Raw.DistinctPayloads),
					"fingerprint_overflow":       float64(stream.Raw.DistinctPayloadOverflow),
					"unchanged_seconds":          stream.Raw.UnchangedSeconds,
					"hamming_distance_mean_bits": stream.Raw.HammingDistanceMean,
					"hamming_distance_p95_bits":  stream.Raw.HammingDistanceP95,
				} {
					attrs := appendMetricAttribute(base, attribute.String("statistic", statistic))
					o.ObserveFloat64(set.sourcePGNRaw, value, api.WithAttributes(attrs...))
				}
				for _, rawByte := range stream.Raw.Bytes {
					byteBase := appendMetricAttribute(base, attribute.Int("offset", rawByte.Offset))
					for statistic, value := range map[string]float64{
						"minimum": float64(rawByte.Minimum), "maximum": float64(rawByte.Maximum),
						"mode": float64(rawByte.MostCommon), "mode_share": rawByte.MostCommonShare,
						"entropy_bits": rawByte.EntropyBits, "changed_share": rawByte.ChangedShare,
					} {
						attrs := appendMetricAttribute(byteBase, attribute.String("statistic", statistic))
						o.ObserveFloat64(set.sourcePGNRawByte, value, api.WithAttributes(attrs...))
					}
					if mask, err := strconv.ParseUint(rawByte.ChangedBitMaskHex, 16, 8); err == nil {
						attrs := appendMetricAttribute(byteBase, attribute.String("statistic", "changed_bit_mask"))
						o.ObserveFloat64(set.sourcePGNRawByte, float64(mask), api.WithAttributes(attrs...))
					}
				}
			}

			for _, field := range stream.Fields {
				fieldBase := appendMetricAttribute(base,
					attribute.String("field", field.Field), attribute.String("unit", field.Unit))
				if field.Kind == "number" {
					values := map[string]*float64{
						"last": field.LastNumeric, "min": field.Minimum, "max": field.Maximum,
						"mean": field.Mean, "stddev": field.StdDev, "last_change": field.LastChange,
						"p05": field.P05, "p50": field.P50, "p95": field.P95, "p99": field.P99,
						"rate_of_change_per_second": field.LastRateOfChange,
						"catalog_minimum":           field.CatalogMinimum, "catalog_maximum": field.CatalogMaximum,
					}
					for statistic, value := range values {
						if value == nil {
							continue
						}
						attrs := appendMetricAttribute(fieldBase, attribute.String("statistic", statistic))
						o.ObserveFloat64(set.sourcePGNField, *value, api.WithAttributes(attrs...))
					}
				}
				for statistic, value := range map[string]float64{
					"availability_percent": field.AvailabilityPercent,
					"stuck_seconds":        field.StuckSeconds,
				} {
					attrs := appendMetricAttribute(fieldBase, attribute.String("statistic", statistic))
					o.ObserveFloat64(set.sourcePGNFieldState, value, api.WithAttributes(attrs...))
				}
				for kind, count := range map[string]int64{
					"present": field.PresentMessages, "missing": field.MissingMessages,
					"invalid": field.InvalidCount, "novel_category": field.NovelValueCount,
				} {
					attrs := appendMetricAttribute(fieldBase, attribute.String("kind", kind))
					o.ObserveInt64(set.sourcePGNFieldQuality, count, api.WithAttributes(attrs...))
				}
				if field.Kind == "category" {
					distinct, total, top := categorySummary(field)
					for statistic, value := range map[string]float64{
						"distinct_values": float64(distinct), "samples": float64(total),
						"top_value_share": top, "overflow_samples": float64(field.Other),
					} {
						attrs := appendMetricAttribute(fieldBase, attribute.String("statistic", statistic))
						o.ObserveFloat64(set.sourcePGNCategory, value, api.WithAttributes(attrs...))
					}
				}
			}
		}
	}
}

func boolMetric(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func categorySummary(field stats.FieldDistribution) (distinct int, total int64, topShare float64) {
	var top int64
	for _, count := range field.Values {
		distinct++
		total += count
		if count > top {
			top = count
		}
	}
	total += field.Other
	if field.Other > 0 {
		distinct++
	}
	if total > 0 {
		topShare = float64(top) / float64(total)
	}
	return distinct, total, topShare
}

func appendMetricAttribute(base []attribute.KeyValue, values ...attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(base)+len(values))
	out = append(out, base...)
	out = append(out, values...)
	return out
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

// SourceDrops records envelopes dropped by a full subscriber channel on a
// non-blocking broadcast (hub.publish for HTTP/CAN sources, busClient's
// internal broadcast for bus subscribers). component is the source id for
// hub drops, or "bus:<kind>:<name>" for a shared CAN client's internal
// broadcast.
func (s *Set) SourceDrops(ctx context.Context, component string, n int64) {
	if s == nil {
		return
	}
	s.subscriberDrops.Add(ctx, n, api.WithAttributes(attribute.String("component", component)))
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
