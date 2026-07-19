package msg

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/n2k/pgn"
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

func TestPayloadOmitsInfo(t *testing.T) {
	e, err := FromPGN(heading(15708))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.PayloadMap()["info"]; ok {
		t.Fatalf("payload still contains info: %s", e.Payload)
	}
	if e.PayloadMap()["heading"] == nil {
		t.Fatalf("stripping info lost data fields: %s", e.Payload)
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
	if string(e.Payload) != "null" {
		t.Fatalf("unknown payload = %s, want null", e.Payload)
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
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"id", "connector", "pgn", "source", "dest", "priority", "timestamp", "observed_at", "pgn_name", "decode", "physical", "payload", "raw"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("marshaled envelope missing %q: %s", k, b)
		}
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
