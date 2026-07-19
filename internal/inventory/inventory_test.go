package inventory

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/store"
)

func TestCommissioningBaselineDetectsChangedAndMissingDevices(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "beacon.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	r := New(st)
	device := bus.DeviceInfo{Endpoint: "socketcan:can0", Name: 0xFEDCBA9876543210,
		LastSeen: time.Now(), Model: "Compass", Serial: "A1"}
	if err := r.Observe(context.Background(), []bus.DeviceInfo{device}); err != nil {
		t.Fatal(err)
	}
	if got := r.Records()[0].Status; got != "new" {
		t.Fatalf("status = %s", got)
	}
	if err := r.CommitBaseline(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.Records()[0].Status; got != "online" {
		t.Fatalf("status = %s", got)
	}
	device.Model = "Compass v2"
	if err := r.Observe(context.Background(), []bus.DeviceInfo{device}); err != nil {
		t.Fatal(err)
	}
	if got := r.Records()[0].Status; got != "changed" {
		t.Fatalf("status = %s", got)
	}
	if _, err := st.DB().Exec(`UPDATE device_inventory SET last_seen = ?`, time.Now().Add(-2*time.Minute).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.Records()[0].Status; got != "missing" {
		t.Fatalf("status = %s", got)
	}
}

func TestStaleObservationDoesNotRegressCurrentDevice(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "beacon.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	r := New(st)
	newest := bus.DeviceInfo{Endpoint: "socketcan:can0", Name: 42,
		LastSeen: time.Now(), Model: "Current"}
	if err := r.Observe(context.Background(), []bus.DeviceInfo{newest}); err != nil {
		t.Fatal(err)
	}
	stale := newest
	stale.LastSeen = newest.LastSeen.Add(-time.Minute)
	stale.Model = "Stale"
	if err := r.Observe(context.Background(), []bus.DeviceInfo{stale}); err != nil {
		t.Fatal(err)
	}
	got := r.Records()[0]
	if got.Model != "Current" || !got.LastSeen.Equal(newest.LastSeen.UTC()) {
		t.Fatalf("stale observation regressed record: %+v", got.DeviceInfo)
	}
}
