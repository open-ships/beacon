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
	// A running registry intentionally keeps fresher in-memory Bus endpoint
	// observations than its coarsened persistence heartbeat. Recreate it to
	// model a restart loading only the durable commissioning history.
	r = New(st)
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

func TestStableObservationUsesMemoryUntilPersistenceHeartbeat(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "beacon.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	r := New(st)
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	device := bus.DeviceInfo{Endpoint: "socketcan:can0", Name: 42, LastSeen: t0, Model: "Compass"}
	if err := r.Observe(context.Background(), []bus.DeviceInfo{device}); err != nil {
		t.Fatal(err)
	}

	device.LastSeen = t0.Add(time.Minute)
	if err := r.Observe(context.Background(), []bus.DeviceInfo{device}); err != nil {
		t.Fatal(err)
	}
	var persisted int64
	if err := st.DB().QueryRow(`SELECT last_seen FROM device_inventory WHERE endpoint = ? AND device_name = ?`,
		device.Endpoint, nameKey(device.Name)).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != t0.UnixNano() {
		t.Fatalf("stable observation rewrote SQLite last_seen = %v, want %v", time.Unix(0, persisted), t0)
	}
	if got := r.Records()[0].LastSeen; !got.Equal(device.LastSeen) {
		t.Fatalf("live inventory LastSeen = %v, want %v", got, device.LastSeen)
	}

	device.LastSeen = t0.Add(inventoryPersistInterval + time.Second)
	if err := r.Observe(context.Background(), []bus.DeviceInfo{device}); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(`SELECT last_seen FROM device_inventory WHERE endpoint = ? AND device_name = ?`,
		device.Endpoint, nameKey(device.Name)).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != device.LastSeen.UnixNano() {
		t.Fatalf("heartbeat last_seen = %v, want %v", time.Unix(0, persisted), device.LastSeen)
	}
}

func TestChangedObservationRetriesImmediatelyAfterPersistenceFailure(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "beacon.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	r := New(st)
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	device := bus.DeviceInfo{Endpoint: "socketcan:can0", Name: 42, LastSeen: t0, Model: "Compass"}
	if err := r.Observe(context.Background(), []bus.DeviceInfo{device}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`
		CREATE TRIGGER fail_inventory_update BEFORE UPDATE ON device_inventory
		BEGIN SELECT RAISE(FAIL, 'injected inventory write failure'); END`); err != nil {
		t.Fatal(err)
	}
	device.Model = "Compass v2"
	device.LastSeen = t0.Add(time.Second)
	if err := r.Observe(context.Background(), []bus.DeviceInfo{device}); err == nil {
		t.Fatal("changed observation unexpectedly persisted through failure trigger")
	}
	if _, err := st.DB().Exec(`DROP TRIGGER fail_inventory_update`); err != nil {
		t.Fatal(err)
	}

	// The retry is well inside the five-minute heartbeat. It still must write
	// because the new fingerprint has not reached durable state yet.
	device.LastSeen = t0.Add(2 * time.Second)
	if err := r.Observe(context.Background(), []bus.DeviceInfo{device}); err != nil {
		t.Fatal(err)
	}
	loaded := New(st)
	if err := loaded.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := loaded.Records()[0].Model; got != "Compass v2" {
		t.Fatalf("persisted model after retry = %q", got)
	}
}

func TestUncommissionedInventoryIsBoundedButExpectedDevicesRemain(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "beacon.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	r := New(st)
	expected := bus.DeviceInfo{Endpoint: "socketcan:can0", Name: 1, LastSeen: time.Now(), Model: "Expected"}
	if err := r.Observe(context.Background(), []bus.DeviceInfo{expected}); err != nil {
		t.Fatal(err)
	}
	if err := r.CommitBaseline(context.Background()); err != nil {
		t.Fatal(err)
	}

	devices := make([]bus.DeviceInfo, 0, maxUncommissionedDevices+10)
	for i := 0; i < maxUncommissionedDevices+10; i++ {
		devices = append(devices, bus.DeviceInfo{
			Endpoint: "socketcan:noise", Name: uint64(10_000 + i),
			LastSeen: time.Now().Add(time.Duration(i) * time.Millisecond), Model: "Noise",
		})
	}
	if err := r.Observe(context.Background(), devices); err != nil {
		t.Fatal(err)
	}
	var uncommissioned, expectedCount int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM device_inventory WHERE expected = 0`).Scan(&uncommissioned); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM device_inventory WHERE expected = 1`).Scan(&expectedCount); err != nil {
		t.Fatal(err)
	}
	if uncommissioned != maxUncommissionedDevices || expectedCount != 1 {
		t.Fatalf("inventory counts = uncommissioned %d expected %d", uncommissioned, expectedCount)
	}
	if len(r.latest) != maxUncommissionedDevices+1 || len(r.Records()) != maxUncommissionedDevices+1 {
		t.Fatalf("in-memory inventory = latest %d records %d", len(r.latest), len(r.Records()))
	}
}

func TestObservationCacheIsBoundedDuringPersistentWriteFailures(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "beacon.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	r := New(st)
	ctx := context.Background()
	initial := bus.DeviceInfo{Endpoint: "socketcan:can0", Name: 1, LastSeen: time.Now(), Model: "Expected"}
	if err := r.Observe(ctx, []bus.DeviceInfo{initial}); err != nil {
		t.Fatal(err)
	}
	if err := r.CommitBaseline(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`CREATE TRIGGER fail_inventory_insert BEFORE INSERT ON device_inventory
		BEGIN SELECT RAISE(FAIL, 'injected inventory write failure'); END`); err != nil {
		t.Fatal(err)
	}
	var newest bus.DeviceInfo
	for batch := 0; batch < 100; batch++ {
		devices := make([]bus.DeviceInfo, 32)
		for i := range devices {
			n := batch*len(devices) + i + 2
			devices[i] = bus.DeviceInfo{Endpoint: initial.Endpoint, Name: uint64(n), LastSeen: initial.LastSeen.Add(time.Duration(n) * time.Millisecond)}
		}
		newest = devices[len(devices)-1]
		if err := r.Observe(ctx, devices); err == nil {
			t.Fatal("write should fail")
		}
		if len(r.latest) > maxUncommissionedDevices+1 {
			t.Fatalf("cache grew to %d", len(r.latest))
		}
	}
	if _, ok := r.latest[deviceKey(initial.Endpoint, initial.Name)]; !ok {
		t.Fatal("commissioned device evicted")
	}
	if _, ok := r.latest[deviceKey(newest.Endpoint, newest.Name)]; !ok {
		t.Fatal("newest observation evicted")
	}
	if len(r.persistedSeen) != 1 || len(r.persistedFP) != 1 {
		t.Fatal("failed writes entered durable cache")
	}
	if _, err := st.DB().Exec(`DROP TRIGGER fail_inventory_insert`); err != nil {
		t.Fatal(err)
	}
	if err := r.Observe(ctx, []bus.DeviceInfo{newest}); err != nil {
		t.Fatal(err)
	}
	if len(r.Records()) != 2 {
		t.Fatalf("recovery records = %d", len(r.Records()))
	}
}
