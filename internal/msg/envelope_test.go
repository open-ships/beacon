package msg

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/n2k/pgn"
	"github.com/open-ships/n2k/raw"
)

func heading(v uint64) *pgn.VesselHeading {
	h := v
	return &pgn.VesselHeading{
		Info: pgn.MessageInfo{
			Timestamp: time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC),
			Priority:  pgn.Priority(2),
			PGN:       127250,
			SourceId:  12,
		},
		Heading: &h,
	}
}

func TestFromPGNKnown(t *testing.T) {
	e, err := FromPGN(heading(15708))
	if err != nil {
		t.Fatal(err)
	}
	if e.PGN != 127250 || e.Source != 12 || e.Priority != 2 || e.Dest != 255 {
		t.Fatalf("header mismatch: %+v", e)
	}
	if len(e.Raw) == 0 {
		t.Fatal("Raw must be populated for known PGNs (canonical re-encode)")
	}
	if e.PayloadMap()["heading"] == nil {
		t.Fatalf("payload heading missing: %s", e.Payload)
	}
	if e.PGNName != "Vessel Heading" || e.Variant != "vesselHeading" || e.Decode.Status != "decoded" || !e.Decode.Complete {
		t.Fatalf("semantic metadata missing: %+v", e)
	}
	physical, ok := e.Physical["heading"]
	if !ok || physical.Unit != "rad" || physical.Value < 1.5707 || physical.Value > 1.5709 {
		t.Fatalf("physical heading = %+v", physical)
	}
	// Raw must round-trip through the codec back to the same PGN
	back, err := pgn.DecodeMessage(e.Info(), e.Raw)
	if err != nil {
		t.Fatal(err)
	}
	if back.PGNNumber() != 127250 {
		t.Fatalf("round-trip PGN = %d", back.PGNNumber())
	}
}

func TestPayloadPreservesCompleteN2KStruct(t *testing.T) {
	original := heading(15708)
	e, err := FromPGN(original)
	if err != nil {
		t.Fatal(err)
	}
	var payload pgn.VesselHeading
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Info.PGN != original.Info.PGN || payload.Info.SourceId != original.Info.SourceId ||
		payload.Info.Priority == nil || *payload.Info.Priority != *original.Info.Priority ||
		payload.Heading == nil || *payload.Heading != *original.Heading {
		t.Fatalf("payload does not preserve the n2k message fields: %+v", payload)
	}
	if _, ok := e.PayloadMap()["info"]; !ok {
		t.Fatalf("payload is missing info: %s", e.Payload)
	}
	if e.PayloadMap()["heading"] == nil {
		t.Fatalf("payload lost data fields: %s", e.Payload)
	}
}

func TestPayloadPreservesCompleteMessageInfo(t *testing.T) {
	instance, source := uint64(0), uint64(0)
	pressure := int64(1020690)
	original := &pgn.ActualPressure{
		Info: pgn.MessageInfo{
			Timestamp:             time.Date(2026, 6, 29, 14, 10, 17, 530566931, time.UTC),
			ReceivedAt:            time.Date(2026, 7, 25, 12, 41, 35, 729702000, time.UTC),
			TransportTimestamp:    250 * time.Millisecond,
			HasTransportTimestamp: true,
			AdapterID:             "file:/captures/nav.log",
			NetworkID:             "can2",
			Direction:             raw.DirectionReceived,
			Priority:              pgn.Priority(5),
			PGN:                   130314,
			SourceId:              6,
		},
		Instance: &instance,
		Source:   &source,
		Pressure: &pressure,
	}

	e, err := FromPGN(original)
	if err != nil {
		t.Fatal(err)
	}
	nativeJSON, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.Payload, nativeJSON) {
		t.Fatalf("payload is not the verbatim n2k struct JSON\n got: %s\nwant: %s", e.Payload, nativeJSON)
	}
	var payloadDocument struct {
		Info map[string]json.RawMessage `json:"info"`
	}
	if err := json.Unmarshal(e.Payload, &payloadDocument); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"timestamp", "receivedAt", "transportTimestamp", "hasTransportTimestamp",
		"adapterId", "networkId", "direction", "priority", "pgn", "sourceId", "targetId",
	} {
		if _, ok := payloadDocument.Info[key]; !ok {
			t.Fatalf("native MessageInfo field %q missing from payload.info: %s", key, e.Payload)
		}
	}

	document, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		Payload  pgn.ActualPressure         `json:"payload"`
		Metadata map[string]json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(document, &event); err != nil {
		t.Fatal(err)
	}
	if !event.Payload.Info.ReceivedAt.Equal(original.Info.ReceivedAt) ||
		event.Payload.Info.TransportTimestamp != original.Info.TransportTimestamp ||
		!event.Payload.Info.HasTransportTimestamp ||
		event.Payload.Info.AdapterID != original.Info.AdapterID ||
		event.Payload.Info.NetworkID != original.Info.NetworkID ||
		event.Payload.Info.Direction != original.Info.Direction {
		t.Fatalf("native MessageInfo was not preserved verbatim: %+v", event.Payload.Info)
	}
	if event.Payload.Instance == nil || *event.Payload.Instance != instance ||
		event.Payload.Source == nil || *event.Payload.Source != source ||
		event.Payload.Pressure == nil || *event.Payload.Pressure != pressure {
		t.Fatalf("ActualPressure fields were not preserved: %+v", event.Payload)
	}
	for _, key := range []string{
		"received_at", "transport_timestamp", "has_transport_timestamp",
		"adapter_id", "network_id", "direction",
	} {
		if _, ok := event.Metadata[key]; ok {
			t.Fatalf("native MessageInfo field %q was duplicated into Beacon metadata: %s", key, document)
		}
	}

	var roundTrip Envelope
	if err := json.Unmarshal(document, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip.Payload, e.Payload) {
		t.Fatalf("native payload did not survive envelope ingestion\n got: %s\nwant: %s", roundTrip.Payload, e.Payload)
	}
}

func TestFromPGNUnknown(t *testing.T) {
	u := &pgn.UnknownPGN{
		Info: pgn.MessageInfo{PGN: 130999, SourceId: 9, Timestamp: time.Now()},
		Data: []byte{1, 2, 3, 4},
	}
	e, err := FromPGN(u)
	if err != nil {
		t.Fatal(err)
	}
	var payload pgn.UnknownPGN
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		t.Fatalf("unknown payload is not n2k UnknownPGN JSON: %v", err)
	}
	if payload.Info.PGN != 130999 || payload.Info.SourceId != 9 || !bytes.Equal(payload.Data, []byte{1, 2, 3, 4}) {
		t.Fatalf("unknown payload lost n2k struct fields: %+v", payload)
	}
	if len(e.Raw) != 4 {
		t.Fatalf("raw = %v", e.Raw)
	}
	if e.Dest != 255 || e.Priority != 6 {
		t.Fatalf("defaults not applied: %+v", e)
	}
	if e.Decode.Status != "unknown" {
		t.Fatalf("decode status = %q", e.Decode.Status)
	}
}

func TestEnvelopeJSONShape(t *testing.T) {
	e, _ := FromPGN(heading(15708))
	e.Seq = 42
	e.ConnectorID = "nav"
	b, _ := json.Marshal(e)
	var m map[string]json.RawMessage
	_ = json.Unmarshal(b, &m)
	if len(m) != 3 || len(m["payload"]) == 0 || len(m["metadata"]) == 0 || len(m["raw"]) == 0 {
		t.Fatalf("top-level envelope must contain only payload, metadata, and raw: %s", b)
	}

	var payload pgn.VesselHeading
	if err := json.Unmarshal(m["payload"], &payload); err != nil {
		t.Fatalf("payload cannot deserialize directly into n2k type: %v", err)
	}
	if payload.Info.PGN != 127250 || payload.Info.SourceId != 12 || payload.Heading == nil || *payload.Heading != 15708 {
		t.Fatalf("deserialized n2k payload lost fields: %+v", payload)
	}

	var metadata Metadata
	if err := json.Unmarshal(m["metadata"], &metadata); err != nil {
		t.Fatalf("metadata cannot deserialize: %v", err)
	}
	if metadata.Seq != 42 || metadata.ConnectorID != "nav" || metadata.PGNName != "Vessel Heading" ||
		metadata.Decode.Status != "decoded" {
		t.Fatalf("metadata lost Beacon fields: %+v", metadata)
	}
	var raw []byte
	if err := json.Unmarshal(m["raw"], &raw); err != nil || len(raw) == 0 {
		t.Fatalf("raw CAN bytes are not a separate top-level value: %s (%v)", m["raw"], err)
	}
	var metadataFields map[string]json.RawMessage
	if err := json.Unmarshal(m["metadata"], &metadataFields); err != nil {
		t.Fatal(err)
	}
	if _, ok := metadataFields["raw"]; ok {
		t.Fatalf("raw CAN bytes leaked into metadata: %s", m["metadata"])
	}
}

func TestWireBytesAreCachedAcrossConsumers(t *testing.T) {
	e, err := FromPGN(heading(15708))
	if err != nil {
		t.Fatal(err)
	}
	first, err := e.WireBytes()
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.WireBytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) == 0 || len(second) == 0 || &first[0] != &second[0] {
		t.Fatal("WireBytes did not reuse the cached canonical encoding")
	}
	if got := e.SizeBytes(); got != len(first) {
		t.Fatalf("SizeBytes = %d, want cached wire length %d", got, len(first))
	}
	marshaled, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(marshaled, first) {
		t.Fatalf("MarshalJSON diverged from cached wire bytes\n got: %s\nwant: %s", marshaled, first)
	}
}

func TestEnvelopeJSONRoundTripRestoresInternalFields(t *testing.T) {
	e, err := FromPGN(heading(15708))
	if err != nil {
		t.Fatal(err)
	}
	e.Seq = 42
	e.ConnectorID = "nav"
	e.Ingress = "can0"
	e.OriginIngress = "bridge"

	doc, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var got Envelope
	if err := json.Unmarshal(doc, &got); err != nil {
		t.Fatal(err)
	}
	if got.Seq != e.Seq || got.ConnectorID != e.ConnectorID || got.PGN != e.PGN ||
		got.Source != e.Source || got.Dest != e.Dest || got.Priority != e.Priority ||
		!got.Timestamp.Equal(e.Timestamp) || got.Ingress != e.Ingress ||
		got.OriginIngress != e.OriginIngress || !bytes.Equal(got.Payload, e.Payload) ||
		!bytes.Equal(got.Raw, e.Raw) {
		t.Fatalf("round trip mismatch\n got: %+v\nwant: %+v", &got, e)
	}
}

func TestEnvelopeJSONAddsInfoToHandBuiltPayload(t *testing.T) {
	e := &Envelope{
		PGN:       127250,
		Source:    12,
		Dest:      255,
		Priority:  2,
		Timestamp: time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC),
		Payload:   json.RawMessage(`{"heading":15708}`),
	}
	doc, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Payload pgn.VesselHeading `json:"payload"`
	}
	if err := json.Unmarshal(doc, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Payload.Info.PGN != 127250 || wire.Payload.Info.SourceId != 12 ||
		wire.Payload.Heading == nil || *wire.Payload.Heading != 15708 {
		t.Fatalf("augmented payload = %+v", wire.Payload)
	}
}

func TestEnvelopeJSONRequiresTopLevelRaw(t *testing.T) {
	e, err := FromPGN(heading(15708))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(doc, &wire); err != nil {
		t.Fatal(err)
	}
	delete(wire, "raw")
	withoutRaw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	var got Envelope
	if err := json.Unmarshal(withoutRaw, &got); err == nil {
		t.Fatal("envelope without top-level raw was accepted")
	}
}

func TestSizeBytes(t *testing.T) {
	e, _ := FromPGN(heading(15708))
	if e.SizeBytes() < len(e.Payload)+len(e.Raw) {
		t.Fatalf("SizeBytes too small: %d", e.SizeBytes())
	}
}

func TestPayloadMapConcurrent(t *testing.T) {
	e := &Envelope{Payload: json.RawMessage(`{"a":1,"b":"x"}`)}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e.PayloadMap()["a"] == nil {
				t.Error("missing key a")
			}
		}()
	}
	wg.Wait()
}
