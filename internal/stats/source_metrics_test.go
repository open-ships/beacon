package stats

import (
	"encoding/json"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if field.OutOfRangeCount != 1 || !field.Anomalous || field.LastRateOfChange == nil {
		t.Fatalf("field quality = %+v", field)
	}
}

func TestSourceTrafficBaselineAndEventsSurviveRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "beacon.db")
	now := time.Unix(1_700_000_000, 0).UTC()
	st, err := store.Open(dbPath)
	if err != nil {
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
	baselines, err := reg.CommitSourceTrafficBaseline(t.Context(), "can0")
	if err != nil {
		t.Fatal(err)
	}
	if len(baselines) != 1 || math.Abs(baselines[0].ExpectedFrequencyHz-1) > 0.001 {
		t.Fatalf("committed baselines = %+v", baselines)
	}
	reg.RecordSource("can0", &msg.Envelope{
		PGN: 127250, PGNName: "Vessel Heading", Source: 12, Dest: 255, Priority: 2,
		Raw: []byte{1, 2, 3, 255}, Decode: msg.DecodeInfo{Status: "decoded", Complete: true},
	})
	changed := reg.SourcePGNMetrics("can0")[0]
	if changed.BaselineStatus != "changed" || changed.Status != "changed" ||
		!strings.Contains(strings.Join(changed.BaselineIssues, " "), "raw byte") {
		t.Fatalf("raw baseline change = %+v", changed)
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
	if loaded := restarted.SourceTrafficBaselines("can0"); len(loaded) != 1 || loaded[0].PGN != 127250 {
		t.Fatalf("loaded baselines = %+v", loaded)
	}
	if events := restarted.SourceMetricEvents("can0", 20); len(events) < 2 {
		t.Fatalf("loaded events = %+v, want stream and baseline events", events)
	}
	now = now.Add(11 * time.Second)
	metrics := restarted.SourcePGNMetrics("can0")
	if len(metrics) != 1 || metrics[0].Observed || metrics[0].Status != "missing" || !metrics[0].GapActive {
		t.Fatalf("missing expected stream = %+v", metrics)
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

func TestSourcePGNMetricsFlagLargePhysicalValueChange(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })
	record := func(value float64) {
		reg.RecordSource("can0", &msg.Envelope{
			PGN: 130312, Source: 9, Payload: json.RawMessage(`{"temperature":10}`),
			Physical: map[string]n2kcatalog.PhysicalField{"temperature": {Value: value, Unit: "K"}},
		})
		now = now.Add(time.Second)
	}
	for i := 0; i < 6; i++ {
		record(10)
	}
	record(100)

	stream := reg.SourcePGNMetrics("can0")[0]
	if !stream.AnomalyActive || !stream.RecentAnomaly || stream.Status != "anomaly" || stream.AnomalyCount != 1 {
		t.Fatalf("stream anomaly = %+v", stream)
	}
	if stream.AnomalyField != "temperature" || stream.AnomalyReason == "" {
		t.Fatalf("anomaly explanation = %+v", stream)
	}
	field := findField(t, stream.Fields, "temperature")
	if !field.Anomalous || field.AnomalyCount != 1 || field.Maximum == nil || *field.Maximum != 100 {
		t.Fatalf("field anomaly = %+v", field)
	}

	record(10)
	stream = reg.SourcePGNMetrics("can0")[0]
	if stream.AnomalyActive || !stream.RecentAnomaly || stream.AnomalyField != "temperature" {
		t.Fatalf("recent anomaly context should survive a normal sample: %+v", stream)
	}
}

func TestSourcePGNMetricsFlagLargeGenericDecodedValueChange(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })
	for i := 0; i < 6; i++ {
		reg.RecordSource("gateway", &msg.Envelope{
			PGN: 99999, Source: 3, Payload: json.RawMessage(`{"sensor":10}`),
		})
		now = now.Add(time.Second)
	}
	reg.RecordSource("gateway", &msg.Envelope{
		PGN: 99999, Source: 3, Payload: json.RawMessage(`{"sensor":1000}`),
	})
	stream := reg.SourcePGNMetrics("gateway")[0]
	if !stream.AnomalyActive || stream.AnomalyField != "sensor" {
		t.Fatalf("generic decoded anomaly = %+v", stream)
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
