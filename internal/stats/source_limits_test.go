package stats

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/store"
)

func TestSourceMetricStreamCapacityOmitsOnlyRichDiagnostics(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })
	for i := 0; i < maxSourceStreamsPerSource; i++ {
		reg.RecordSource("can0", &msg.Envelope{PGN: uint32(i + 1), Source: uint8(i % 255)})
		now = now.Add(time.Millisecond)
	}

	// A novel stream at capacity is omitted from rich metrics, rather than
	// churning the stable working set. Exact source traffic still advances.
	reg.RecordSource("can0", &msg.Envelope{PGN: 900_001, Source: 1})
	if got := len(reg.SourcePGNMetrics("can0")); got != maxSourceStreamsPerSource {
		t.Fatalf("tracked streams = %d, want %d", got, maxSourceStreamsPerSource)
	}
	snapshot, ok := reg.SourceSnapshot("can0")
	if !ok {
		t.Fatal("source snapshot missing")
	}
	if snapshot.TotalMessages != maxSourceStreamsPerSource+1 ||
		snapshot.SourceMetricStreams != maxSourceStreamsPerSource ||
		snapshot.SourceMetricStreamLimit != maxSourceStreamsPerSource ||
		snapshot.SourceMetricMessagesOmitted != 1 {
		t.Fatalf("capacity accounting = %+v", snapshot)
	}

	// Stale streams are reclaimed lazily when a novel key arrives.
	now = now.Add(sourceStreamRetention + time.Second)
	reg.RecordSource("can0", &msg.Envelope{PGN: 900_002, Source: 2})
	snapshot, _ = reg.SourceSnapshot("can0")
	if snapshot.TotalMessages != maxSourceStreamsPerSource+2 || snapshot.SourceMetricStreams != 1 ||
		snapshot.SourceMetricStreamsExpired != maxSourceStreamsPerSource ||
		snapshot.SourceMetricMessagesOmitted != 1 {
		t.Fatalf("expiry accounting = %+v", snapshot)
	}
}

func TestSourceMetricGlobalCapacityAndRemovalAccounting(t *testing.T) {
	reg := NewRegistry()
	for sourceIndex, source := range []string{"can0", "can1"} {
		for i := 0; i < maxSourceStreamsPerSource; i++ {
			reg.getSourceStream(source, &msg.Envelope{
				PGN: uint32(sourceIndex*maxSourceStreamsPerSource + i + 1), Source: uint8(i),
			})
		}
	}
	capacity := reg.SourceMetricCapacity("can0")
	if capacity.GlobalTrackedStreams != maxSourceStreamsTotal || capacity.GlobalStreamLimit != maxSourceStreamsTotal {
		t.Fatalf("global capacity = %+v", capacity)
	}

	reg.RecordSource("can2", &msg.Envelope{PGN: 900_003, Source: 3})
	capacity = reg.SourceMetricCapacity("can2")
	if capacity.TrackedStreams != 0 || capacity.MessagesOmitted != 1 || capacity.GlobalTrackedStreams != maxSourceStreamsTotal {
		t.Fatalf("global overflow accounting = %+v", capacity)
	}
	snapshot, ok := reg.SourceSnapshot("can2")
	if !ok || snapshot.TotalMessages != 1 {
		t.Fatalf("canonical source accounting at global limit = %+v, %v", snapshot, ok)
	}

	reg.RemoveSource("can0")
	reg.RecordSource("can2", &msg.Envelope{PGN: 900_003, Source: 3})
	capacity = reg.SourceMetricCapacity("can2")
	if capacity.TrackedStreams != 1 || capacity.GlobalTrackedStreams != maxSourceStreamsPerSource+1 {
		t.Fatalf("capacity after source removal = %+v", capacity)
	}
}

func TestDecodedAndRawDiagnosticsHaveStrictRetentionBounds(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })

	fields := make(map[string]int, maxSourceFields+10)
	for i := 0; i < maxSourceFields+10; i++ {
		fields[fmt.Sprintf("field_%02d", i)] = i
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	raw := bytes.Repeat([]byte{0x5a}, maxRetainedRawBytes+100)
	wantRaw := bytes.Clone(raw)
	wantPayload := bytes.Clone(payload)
	missing := make([]string, maxMissingFieldObservations+7)
	for i := range missing {
		missing[i] = fmt.Sprintf("missing_%02d", i)
	}
	envelope := &msg.Envelope{
		PGN: 130999, Source: 7, Raw: raw, Payload: payload,
		Decode: msg.DecodeInfo{Status: "partial", Missing: missing},
	}
	reg.RecordSource("can0", envelope)

	// Instrumentation must never mutate the canonical Envelope.
	if !bytes.Equal(envelope.Raw, wantRaw) || !bytes.Equal(envelope.Payload, wantPayload) {
		t.Fatal("diagnostic collection changed canonical Envelope data")
	}
	metric := reg.SourcePGNMetrics("can0")[0]
	if len(metric.Fields) != maxSourceFields || metric.DiagnosticTruncations != 1 {
		t.Fatalf("decoded field bounds = fields %d truncations %d", len(metric.Fields), metric.DiagnosticTruncations)
	}
	wantMissingOverflow := int64(len(missing) - maxMissingDecodedFields)
	if len(metric.MissingDecodedFields) != maxMissingDecodedFields || metric.MissingDecodedOverflow != wantMissingOverflow {
		t.Fatalf("missing-field bounds = %d fields, %d overflow", len(metric.MissingDecodedFields), metric.MissingDecodedOverflow)
	}
	if metric.Raw == nil || !metric.Raw.LastHexTruncated || metric.Raw.LastLength != len(raw) ||
		metric.Raw.RetainedByteLimit != maxRetainedRawBytes || len(metric.Raw.LastHex) != maxRetainedRawBytes*2 ||
		len(metric.Raw.Bytes) != maxAnalyzedPayloadBytes || len(metric.Raw.Samples) != 1 ||
		!metric.Raw.Samples[0].HexTruncated {
		t.Fatalf("raw diagnostic bounds = %+v", metric.Raw)
	}
}

func TestCategoricalDiagnosticOverflowDoesNotCreateEventChurn(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	reg := newRegistryAt(func() time.Time { return now })
	for i := 0; i < maxCategoryValues+5; i++ {
		payload := json.RawMessage(fmt.Sprintf(`{"mode":"value_%02d"}`, i))
		reg.RecordSource("can0", &msg.Envelope{PGN: 130999, Source: 4, Payload: payload})
		now = now.Add(sourceDiagnosticInterval)
	}
	field := findField(t, reg.SourcePGNMetrics("can0")[0].Fields, "mode")
	if len(field.Values) != maxCategoryValues || field.Other != 5 {
		t.Fatalf("categorical bounds = values %d, other %d", len(field.Values), field.Other)
	}
	events := reg.SourceMetricEvents("can0", maxSourceMetricEvents)
	novelEvents := 0
	for _, event := range events {
		if event.Kind == "new_categorical_value" {
			novelEvents++
		}
	}
	if novelEvents != maxCategoryValues-1 {
		t.Fatalf("novel categorical events = %d, want %d before overflow", novelEvents, maxCategoryValues-1)
	}
}

func TestLatestDecodedPayloadIsOwnedBoundedAndRecovers(t *testing.T) {
	reg := NewRegistry()
	oversized := json.RawMessage(`{"value":"` + strings.Repeat("x", maxRetainedDecodedPayloadBytes) + `"}`)
	want := bytes.Clone(oversized)
	reg.RecordSource("can0", &msg.Envelope{PGN: 130999, Source: 9, Payload: oversized})
	if !bytes.Equal(oversized, want) {
		t.Fatal("latest-payload diagnostics mutated canonical JSON")
	}
	metric := reg.SourcePGNMetrics("can0")[0]
	if !metric.LatestPayloadTruncated || metric.LatestPayloadBytes != len(oversized) || metric.LatestPayloadTruncations != 1 {
		t.Fatalf("oversized latest-payload accounting = %+v", metric)
	}
	if payloads := reg.SourcePGNLastPayloadsFiltered("can0", SourcePGNMetricFilter{}); len(payloads) != 0 {
		t.Fatalf("invalid JSON prefix escaped latest-payload cache: %+v", payloads)
	}

	small := json.RawMessage(`{"value":2}`)
	reg.RecordSource("can0", &msg.Envelope{PGN: 130999, Source: 9, Payload: small})
	small[9] = '3' // prove the cache owns its bounded copy
	payloads := reg.SourcePGNLastPayloadsFiltered("can0", SourcePGNMetricFilter{})
	if len(payloads) != 1 || string(payloads[0].Payload) != `{"value":2}` || payloads[0].Truncated {
		t.Fatalf("recovered latest payloads = %+v", payloads)
	}
	metric = reg.SourcePGNMetrics("can0")[0]
	if metric.LatestPayloadTruncated || metric.LatestPayloadTruncations != 1 {
		t.Fatalf("latest-payload state did not recover: %+v", metric)
	}
}

func TestRawDiagnosticMapsAndRingsStayBounded(t *testing.T) {
	var wire sourceWireStats
	now := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < maxTrackedPayloadLengths+5; i++ {
		raw := bytes.Repeat([]byte{byte(i + 1)}, i+1)
		wire.record(now, raw)
		now = now.Add(time.Second)
	}
	for i := 0; i < maxTrackedFingerprints+5; i++ {
		raw := []byte{0xaa, byte(i), 0x55}
		wire.record(now, raw)
		now = now.Add(time.Second)
	}
	snapshot := wire.snapshot(now)
	if len(wire.lengths) != maxTrackedPayloadLengths || snapshot.LengthCountOverflow != 5 {
		t.Fatalf("length cache = %d, overflow %d", len(wire.lengths), snapshot.LengthCountOverflow)
	}
	if len(wire.fingerprints) != maxTrackedFingerprints || snapshot.DistinctPayloadOverflow == 0 {
		t.Fatalf("fingerprint cache = %d, overflow %d", len(wire.fingerprints), snapshot.DistinctPayloadOverflow)
	}
	if len(snapshot.Samples) > maxRawSamples || len(wire.previous) > maxRetainedRawBytes ||
		len(snapshot.Bytes) > maxAnalyzedPayloadBytes {
		t.Fatalf("raw rings exceeded limits: samples=%d previous=%d bytes=%d", len(snapshot.Samples), len(wire.previous), len(snapshot.Bytes))
	}
	var byteValues sourceWireStats
	for i := 0; i < maxTrackedByteValues+5; i++ {
		byteValues.record(now.Add(time.Duration(i)*time.Second), []byte{byte(i)})
	}
	byteSnapshot := byteValues.snapshot(now)
	if len(byteValues.bytes[0].counts) != maxTrackedByteValues || byteSnapshot.Bytes[0].OtherSamples != 5 {
		t.Fatalf("byte-value cache = %d, overflow %d", len(byteValues.bytes[0].counts), byteSnapshot.Bytes[0].OtherSamples)
	}
}

func TestDiagnosticTextAndStageMapsAreBounded(t *testing.T) {
	reg := NewRegistry()
	reg.SetRuntime("route", "confirmed", "failed", errors.New(strings.Repeat("x", maxRuntimeErrorBytes+100)))
	for i := 0; i < maxStageTotals+5; i++ {
		reg.RecordStage("route", fmt.Sprintf("stage_%02d", i), 1)
	}
	snapshot, _ := reg.Snapshot("route")
	if len(snapshot.LastError) > maxRuntimeErrorBytes || len(snapshot.StageTotals) != maxStageTotals || snapshot.StageTotalsOverflow != 5 {
		t.Fatalf("bounded connector diagnostics = %+v", snapshot)
	}

	envelope := &msg.Envelope{
		PGNName: strings.Repeat("n", maxEventLabelBytes+50),
		Payload: json.RawMessage(`"` + strings.Repeat("p", maxEventPayloadBytes+50) + `"`),
	}
	reg.RecordConnectorEvent("route", "received", envelope)
	event := reg.Recent("connector", "route", 1)[0]
	if len(event.PGNName) > maxEventLabelBytes || len(event.Payload) > maxEventPayloadBytes+3 {
		t.Fatalf("bounded event = %+v", event)
	}
}

func TestDeviceIdentityCacheBoundAndRemovedWithSource(t *testing.T) {
	reg := NewRegistry()
	now := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < maxTrackedSourceDeviceNames+1; i++ {
		name := uint64(i + 1)
		reg.sourceIdentityEvents(now, "can0", &msg.Envelope{Source: uint8(i), DeviceName: &name})
	}
	reg.mu.Lock()
	count := reg.sourceDeviceCount
	omitted := reg.sourceDiagnostics["can0"].deviceNamesOmitted
	reg.mu.Unlock()
	if count != maxTrackedSourceDeviceNames || omitted != 1 {
		t.Fatalf("identity cache = %d, omitted %d", count, omitted)
	}
	reg.RemoveSource("can0")
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.sourceDeviceCount != 0 || len(reg.sourceDeviceAddresses) != 0 || len(reg.sourceAddressNames) != 0 {
		t.Fatalf("identity cache survived source removal: count=%d devices=%d addresses=%d",
			reg.sourceDeviceCount, len(reg.sourceDeviceAddresses), len(reg.sourceAddressNames))
	}
	if _, ok := reg.sourceDiagnostics["can0"]; ok {
		t.Fatal("diagnostic capacity accounting survived source removal")
	}
}

func TestSourceMetricEventTextMapsAreBoundedAndSnapshotsOwned(t *testing.T) {
	reg := NewRegistry()
	details := make(map[string]string, maxSourceMetricEventDetails+5)
	for i := 0; i < maxSourceMetricEventDetails+5; i++ {
		details[fmt.Sprintf("detail_%02d_%s", i, strings.Repeat("k", maxSourceMetricEventKindBytes))] =
			strings.Repeat("v", maxSourceMetricEventDetailBytes+100)
	}
	source := strings.Repeat("s", maxDiagnosticLabelBytes+100)
	reg.recordSourceMetricEvent(SourceMetricEvent{
		SourceID: source, Kind: strings.Repeat("k", maxSourceMetricEventKindBytes+20),
		Severity: "warning", Summary: strings.Repeat("x", maxSourceMetricEventSummaryBytes+100),
		Details: details,
	})
	boundedSource, _ := boundedDiagnosticText(source, maxDiagnosticLabelBytes)
	events := reg.SourceMetricEvents(boundedSource, 1)
	if len(events) != 1 || len(events[0].SourceID) > maxDiagnosticLabelBytes ||
		len(events[0].Kind) > maxSourceMetricEventKindBytes || len(events[0].Summary) > maxSourceMetricEventSummaryBytes ||
		len(events[0].Details) != maxSourceMetricEventDetails {
		t.Fatalf("bounded lifecycle event = %+v", events)
	}
	for key, value := range events[0].Details {
		if len(key) > maxSourceMetricEventKindBytes || len(value) > maxSourceMetricEventDetailBytes {
			t.Fatalf("unbounded lifecycle detail %q=%q", key, value)
		}
		delete(events[0].Details, key)
		break
	}
	if got := reg.SourceMetricEvents(boundedSource, 1); len(got[0].Details) != maxSourceMetricEventDetails {
		t.Fatal("caller mutation changed retained lifecycle-event details")
	}
}

func TestOversizedPersistedSourceMetricEventIsNotLoaded(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "beacon.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.DB().Exec(`INSERT INTO sources (id, doc) VALUES (?, ?)`, "can0", `{}`); err != nil {
		t.Fatal(err)
	}
	doc := `{"source_id":"can0","summary":"` +
		strings.Repeat("x", maxSourceMetricEventDocumentBytes+1) + `"}`
	insert := func(source, doc string) {
		t.Helper()
		if _, err := st.DB().Exec(`INSERT INTO source_metric_events
		(ts, source_id, pgn, source_address, kind, severity, doc)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, 1, source, 127250, 1, "test", "info", doc); err != nil {
			t.Fatal(err)
		}
	}
	insert("can0", doc)
	insert("can0", `{"source_id":"can0","summary":"retained"}`)
	insert("removed", `{"source_id":"removed","summary":"orphan"}`)
	events, err := loadSourceMetricEvents(t.Context(), st.DB())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].SourceID != "can0" || events[0].Summary != "retained" {
		t.Fatalf("persisted-event filtering = %+v, want only bounded configured-source row", events)
	}
}
