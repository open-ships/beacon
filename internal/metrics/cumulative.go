package metrics

import "github.com/prometheus/client_golang/prometheus"

// Cumulative instruments need explicit deletion when a configured entity is
// removed. OTel cumulative aggregators keep old label sets for the provider's
// lifetime, eventually sending new active entities into cardinality overflow.
// Native vectors preserve the public names/labels/buckets while letting the
// supervisor retire counters and histograms with their owning entity.
func (s *Set) registerCumulativeMetrics(reg *prometheus.Registry) {
	// Preserve the instrumentation-scope labels exported by the OTel
	// instruments these replace, including their empty version/schema values.
	scope := prometheus.Labels{"otel_scope_name": "beacon", "otel_scope_version": "", "otel_scope_schema_url": ""}
	s.connectorMessages = prometheus.NewCounterVec(prometheus.CounterOpts{ConstLabels: scope, Name: "beacon_connector_messages_total", Help: "Connector messages by delivery stage."}, []string{"connector", "stage"})
	s.connectorBytes = prometheus.NewCounterVec(prometheus.CounterOpts{ConstLabels: scope, Name: "beacon_connector_bytes_total", Help: "Connector boundary-accepted bytes."}, []string{"connector"})
	s.sourceMessages = prometheus.NewCounterVec(prometheus.CounterOpts{ConstLabels: scope, Name: "beacon_source_messages_total", Help: "Source messages received."}, []string{"source"})
	s.subscriberDrops = prometheus.NewCounterVec(prometheus.CounterOpts{ConstLabels: scope, Name: "beacon_subscriber_dropped_total", Help: "Messages dropped by full subscriber buffers."}, []string{"component"})
	s.sinkClients = prometheus.NewGaugeVec(prometheus.GaugeOpts{ConstLabels: scope, Name: "beacon_sink_clients", Help: "Connected sink clients."}, []string{"sink"})
	s.sinkHTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{ConstLabels: scope, Name: "beacon_sink_http_requests_total", Help: "HTTP sink request attempts, including retries."}, []string{"sink", "status", "encoding"})
	s.sinkHTTPEnvelopes = prometheus.NewCounterVec(prometheus.CounterOpts{ConstLabels: scope, Name: "beacon_sink_http_payload_envelopes_total", Help: "Envelopes included in HTTP sink request attempts."}, []string{"sink", "status", "encoding"})
	payloadBuckets := []float64{512, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304}
	s.sinkHTTPPayloadBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{ConstLabels: scope, Name: "beacon_sink_http_payload_size_bytes", Help: "HTTP sink request payload size on the wire.", Buckets: payloadBuckets}, []string{"sink", "status", "encoding"})
	s.sinkHTTPOriginalBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{ConstLabels: scope, Name: "beacon_sink_http_payload_uncompressed_size_bytes", Help: "HTTP sink request payload size before compression.", Buckets: payloadBuckets}, []string{"sink", "status", "encoding"})
	s.sinkHTTPLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{ConstLabels: scope, Name: "beacon_sink_http_request_latency_seconds", Help: "HTTP sink request latency through response-body read.", Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}}, []string{"sink", "status", "encoding"})
	s.sinkHTTPRetryAfter = prometheus.NewHistogramVec(prometheus.HistogramOpts{ConstLabels: scope, Name: "beacon_sink_http_retry_after_seconds", Help: "Valid Retry-After delay returned by an HTTP sink endpoint.", Buckets: []float64{0, .25, .5, 1, 2, 5, 10, 30, 60, 300, 900, 3600}}, []string{"sink", "status"})
	reg.MustRegister(s.connectorMessages, s.connectorBytes, s.sourceMessages, s.subscriberDrops,
		s.sinkClients, s.sinkHTTPRequests, s.sinkHTTPEnvelopes, s.sinkHTTPPayloadBytes,
		s.sinkHTTPOriginalBytes, s.sinkHTTPLatency, s.sinkHTTPRetryAfter)
}

// RemoveSource retires cumulative state only on deletion, after source Stop.
// Disabled and restarted entities retain their counters.
func (s *Set) RemoveSource(id string) {
	if s == nil {
		return
	}
	s.sourceMessages.DeleteLabelValues(id)
	s.RemoveSourceDrops(id)
}

// RemoveSourceDrops also retires a bus endpoint's diagnostic counter after
// its last handle has been released and its receive loop has exited.
func (s *Set) RemoveSourceDrops(component string) {
	if s != nil {
		s.subscriberDrops.DeleteLabelValues(component)
	}
}

func (s *Set) RemoveSink(id string) {
	if s == nil {
		return
	}
	labels := prometheus.Labels{"sink": id}
	s.sinkClients.DeleteLabelValues(id)
	s.sinkHTTPRequests.DeletePartialMatch(labels)
	s.sinkHTTPEnvelopes.DeletePartialMatch(labels)
	s.sinkHTTPPayloadBytes.DeletePartialMatch(labels)
	s.sinkHTTPOriginalBytes.DeletePartialMatch(labels)
	s.sinkHTTPLatency.DeletePartialMatch(labels)
	s.sinkHTTPRetryAfter.DeletePartialMatch(labels)
}
