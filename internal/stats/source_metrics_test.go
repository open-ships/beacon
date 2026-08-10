package stats

import (
	"encoding/json"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/n2kcatalog"
	"github.com/open-ships/beacon/internal/store"
)

func TestSourcePGNMetricsTrackSenderFrequencyPayloadAndValues(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })
	deviceName := uint64(0x1122334455667788)

	for i := 0; i < 6; i++ {
		reg.RecordSource("can0", &msg.Envelope{
			PGN: 127250, PGNName: "Vessel Heading", Source: 12, DeviceName: &deviceName,
			Raw:     []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Payload: json.RawMessage(`{"heading":10,"mode":"magnetic"}`),
			Physical: map[string]n2kcatalog.PhysicalField{
				"heading": {Value: 10, Unit: "rad"},
			},
		})
		now = now.Add(time.Second)
	}
	reg.RecordSource("can0", &msg.Envelope{PGN: 127250, Source: 44, Raw: []byte{1}})

	metrics := reg.SourcePGNMetrics("can0")
	if len(metrics) != 2 {
		t.Fatalf("streams = %d, want two sender-specific rows: %+v", len(metrics), metrics)
	}
	var stream SourcePGNMetric
	for _, metric := range metrics {
		if metric.SourceAddress == 12 {
			stream = metric
		}
	}
	if stream.Messages != 6 || stream.PGN != 127250 || stream.PGNName != "Vessel Heading" {
		t.Fatalf("stream identity/totals = %+v", stream)
	}
	if stream.DeviceNameHex != "1122334455667788" {
		t.Fatalf("device name = %q", stream.DeviceNameHex)
	}
	if math.Abs(stream.FrequencyHz-1) > 0.001 || math.Abs(stream.ExpectedPeriodSeconds-1) > 0.001 {
		t.Fatalf("frequency = %v Hz period = %v s, want 1", stream.FrequencyHz, stream.ExpectedPeriodSeconds)
	}
	if math.Abs(stream.PeriodP90Seconds-1) > 0.001 || math.Abs(stream.PeriodP99Seconds-1) > 0.001 {
		t.Fatalf("period p90/p99 = %v / %v, want 1", stream.PeriodP90Seconds, stream.PeriodP99Seconds)
	}
	if stream.PayloadBytesLast != 8 || stream.PayloadBytesMin != 8 || stream.PayloadBytesMax != 8 || stream.PayloadBytesMean != 8 {
		t.Fatalf("payload distribution = %+v", stream)
	}
	heading := findField(t, stream.Fields, "heading")
	if heading.Kind != "number" || heading.Unit != "rad" || heading.Samples != 6 || heading.Mean == nil || *heading.Mean != 10 {
		t.Fatalf("heading distribution = %+v", heading)
	}
	mode := findField(t, stream.Fields, "mode")
	if mode.Kind != "category" || mode.Values["magnetic"] != 6 || mode.Last != "magnetic" {
		t.Fatalf("mode distribution = %+v", mode)
	}
}

func TestSourcePGNMetricsExposeWireDecodeAddressingAndFieldQuality(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })
	minimum, maximum := 0.0, 100.0
	record := func(raw []byte, payload string, physical map[string]n2kcatalog.PhysicalField) {
		reg.RecordSource("can0", &msg.Envelope{
			PGN: 130999, Source: 9, Dest: 255, Priority: 3, Raw: raw,
			Payload: json.RawMessage(payload), Physical: physical,
			Decode: msg.DecodeInfo{Status: "unknown"},
		})
		now = now.Add(time.Second)
	}
	record([]byte{0, 10}, `{"temperature":10}`, map[string]n2kcatalog.PhysicalField{
		"temperature": {Value: 10, Unit: "K", Minimum: &minimum, Maximum: &maximum},
	})
	record([]byte{0, 10}, `{}`, nil)
	record([]byte{1, 14}, `{"temperature":1000}`, map[string]n2kcatalog.PhysicalField{
		"temperature": {Value: 1000, Unit: "K", Minimum: &minimum, Maximum: &maximum},
	})

	stream := reg.SourcePGNMetrics("can0")[0]
	if stream.DecodeStatus != "unknown" || stream.UnknownMessages != 3 || stream.DecodeStatuses["unknown"] != 3 {
		t.Fatalf("decode diagnostics = %+v", stream)
	}
	if stream.DestinationCounts["255"] != 3 || stream.PriorityCounts["3"] != 3 {
		t.Fatalf("addressing diagnostics = dest %+v priority %+v", stream.DestinationCounts, stream.PriorityCounts)
	}
	if stream.Raw == nil || stream.Raw.DistinctPayloads != 2 || stream.Raw.LengthCounts["2"] != 3 || len(stream.Raw.Samples) != 2 {
		t.Fatalf("raw payload diagnostics = %+v", stream.Raw)
	}
	if len(stream.Raw.Bytes) != 2 || math.Abs(stream.Raw.Bytes[0].ChangedShare-0.5) > 0.001 || stream.Raw.HammingDistanceP95 != 2 {
		t.Fatalf("raw byte distributions = %+v", stream.Raw)
	}
	field := findField(t, stream.Fields, "temperature")
	if field.PresentMessages != 2 || field.MissingMessages != 1 || math.Abs(field.AvailabilityPercent-66.6667) > 0.01 {
		t.Fatalf("field availability = %+v", field)
	}
	if field.LastRateOfChange == nil || field.Maximum == nil || *field.Maximum != 1000 {
		t.Fatalf("field quality = %+v", field)
	}
}

func TestSourceMetricEventsSurviveRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "beacon.db")
	now := time.Unix(1_700_000_000, 0).UTC()
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ReplaceConfig(t.Context(), model.Config{Sources: []model.Source{{ID: "can0"}}}); err != nil {
		t.Fatal(err)
	}
	reg := newRegistryAt(func() time.Time { return now })
	if err := reg.AttachSourceMetricPersistence(t.Context(), st.DB()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		reg.RecordSource("can0", &msg.Envelope{
			PGN: 127250, PGNName: "Vessel Heading", Source: 12, Dest: 255, Priority: 2,
			Raw: []byte{1, 2, 3, 4}, Decode: msg.DecodeInfo{Status: "decoded", Complete: true},
		})
		now = now.Add(time.Second)
	}
	if err := reg.CloseSourceMetricPersistence(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	restarted := newRegistryAt(func() time.Time { return now })
	if err := restarted.AttachSourceMetricPersistence(t.Context(), st.DB()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.CloseSourceMetricPersistence(t.Context()) }()
	if events := restarted.SourceMetricEvents("can0", 20); len(events) == 0 {
		t.Fatalf("loaded events = %+v, want persisted source lifecycle events", events)
	}
	if metrics := restarted.SourcePGNMetrics("can0"); len(metrics) != 0 {
		t.Fatalf("live metrics should remain process-local after restart: %+v", metrics)
	}
}

func TestSourcePGNMetricsDetectCurrentAndRecoveredGaps(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })
	envelope := &msg.Envelope{PGN: 128259, Source: 7, Raw: []byte{1, 2}}
	for i := 0; i < 5; i++ {
		reg.RecordSource("can0", envelope)
		now = now.Add(time.Second)
	}

	now = now.Add(3100 * time.Millisecond)
	stream := reg.SourcePGNMetrics("can0")[0]
	if !stream.GapActive || stream.Status != "gap" || stream.GapRatio < 4 {
		t.Fatalf("active gap not detected: %+v", stream)
	}

	reg.RecordSource("can0", envelope)
	stream = reg.SourcePGNMetrics("can0")[0]
	if stream.GapActive || stream.GapCount != 1 || stream.LastGapAt == nil || stream.LongestGapSeconds < 4 {
		t.Fatalf("recovered gap not retained: %+v", stream)
	}
}

func TestRemoveSourceDropsPGNMetrics(t *testing.T) {
	reg := NewRegistry()
	reg.RecordSource("can0", &msg.Envelope{PGN: 127250, Source: 1})
	reg.RemoveSource("can0")
	if metrics := reg.SourcePGNMetrics("can0"); len(metrics) != 0 {
		t.Fatalf("removed source metrics = %+v", metrics)
	}
}

func TestSourceDiagnosticsAreSampledWhileTrafficCountersRemainExact(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })
	for i := 0; i < 100; i++ {
		reg.RecordSource("can0", &msg.Envelope{
			PGN: 130999, Source: 9, Dest: 255, Priority: 3,
			Raw: []byte{byte(i)}, Payload: json.RawMessage(`{"mode":"fast"}`),
			Decode: msg.DecodeInfo{Status: "decoded", Complete: true},
		})
	}
	stream := reg.SourcePGNMetrics("can0")[0]
	if stream.Messages != 100 || stream.DecodeComplete != 100 ||
		stream.DestinationCounts["255"] != 100 || stream.PriorityCounts["3"] != 100 {
		t.Fatalf("exact traffic counters = %+v", stream)
	}
	if stream.DiagnosticSamples != 1 || stream.Raw == nil || stream.Raw.LengthCounts["1"] != 1 {
		t.Fatalf("sampled diagnostics = %+v", stream)
	}
	mode := findField(t, stream.Fields, "mode")
	if mode.Samples != 1 || mode.PresentMessages != 1 || mode.AvailabilityPercent != 100 {
		t.Fatalf("sampled field distribution = %+v", mode)
	}

	now = now.Add(time.Second)
	reg.RecordSource("can0", &msg.Envelope{
		PGN: 130999, Source: 9, Dest: 255, Priority: 3,
		Raw: []byte{101}, Payload: json.RawMessage(`{"mode":"slow"}`),
		Decode: msg.DecodeInfo{Status: "decoded", Complete: true},
	})
	stream = reg.SourcePGNMetrics("can0")[0]
	if stream.Messages != 101 || stream.DiagnosticSamples != 2 || stream.Raw.LengthCounts["1"] != 2 {
		t.Fatalf("counters after next diagnostic interval = %+v", stream)
	}
}

func TestSourceSummariesSkipRichDiagnosticsAndRichAddressReadIsScoped(t *testing.T) {
	reg := NewRegistry()
	for _, address := range []uint8{9, 10} {
		reg.RecordSource("can0", &msg.Envelope{
			PGN: 130999, Source: address, Raw: []byte{1, 2},
			Payload: json.RawMessage(`{"mode":"fast"}`),
		})
	}
	summaries := reg.SourcePGNSummaries("can0")
	if len(summaries) != 2 {
		t.Fatalf("summaries = %d, want two streams", len(summaries))
	}
	for _, summary := range summaries {
		if summary.Messages != 1 || summary.PayloadBytesMean != 2 {
			t.Fatalf("compact counters = %+v", summary)
		}
		if summary.Raw != nil || len(summary.Fields) != 0 || len(summary.DecodeStatuses) != 0 {
			t.Fatalf("summary included rich diagnostics: %+v", summary)
		}
	}
	rich := reg.SourcePGNMetricsForAddress("can0", 9)
	if len(rich) != 1 || rich[0].SourceAddress != 9 {
		t.Fatalf("address-scoped metrics = %+v", rich)
	}
	if rich[0].Raw == nil || len(rich[0].Fields) == 0 {
		t.Fatalf("address-scoped metrics omitted diagnostics: %+v", rich[0])
	}
}

func TestSourceLastPayloadIsExactBoundedAndFilterablePerSensorPGN(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })
	deviceName := uint64(0x1122334455667788)
	for _, payload := range []string{`{"value":1}`, `{"value":2}`} {
		reg.RecordSource("can0", &msg.Envelope{
			PGN: 130999, Source: 9, DeviceName: &deviceName,
			Payload: json.RawMessage(payload),
		})
	}
	reg.RecordSource("can0", &msg.Envelope{
		PGN: 127250, Source: 9, DeviceName: &deviceName,
		Payload: json.RawMessage(`{"heading":1.5}`),
	})
	reg.RecordSource("can0", &msg.Envelope{
		PGN: 128259, Source: 44, Payload: json.RawMessage(`{"speed":3.2}`),
	})

	address := uint8(9)
	pgn := uint32(130999)
	payloads := reg.SourcePGNLastPayloadsFiltered("can0", SourcePGNMetricFilter{
		PGN: &pgn, SourceAddress: &address, DeviceNameHex: "0x1122334455667788",
	})
	if len(payloads) != 1 {
		t.Fatalf("filtered payloads = %+v", payloads)
	}
	if got := string(payloads[0].Payload); got != `{"value":2}` {
		t.Fatalf("latest payload = %s, want latest message only", got)
	}
	all := reg.SourcePGNLastPayloadsFiltered("can0", SourcePGNMetricFilter{})
	if len(all) != 3 {
		t.Fatalf("all sensor/PGN payloads = %+v", all)
	}
	if all[0].SourceAddress != 9 || all[0].PGN != 127250 || all[1].PGN != 130999 || all[2].SourceAddress != 44 {
		t.Fatalf("payload ordering/identity = %+v", all)
	}
}

func TestSourceStreamCardinalityIsBoundedPerSource(t *testing.T) {
	reg := NewRegistry()
	for i := 0; i < maxSourceStreamsPerSource+50; i++ {
		reg.getSourceStream("can0", &msg.Envelope{PGN: uint32(i + 1), Source: uint8(i % 255)})
	}
	reg.mu.Lock()
	count := 0
	for key := range reg.sourceStreams {
		if key.source == "can0" {
			count++
		}
	}
	reg.mu.Unlock()
	if count != maxSourceStreamsPerSource {
		t.Fatalf("source streams = %d, want bounded at %d", count, maxSourceStreamsPerSource)
	}
}

func findField(t *testing.T, fields []FieldDistribution, name string) FieldDistribution {
	t.Helper()
	for _, field := range fields {
		if field.Field == name {
			return field
		}
	}
	t.Fatalf("field %q not found in %+v", name, fields)
	return FieldDistribution{}
}
