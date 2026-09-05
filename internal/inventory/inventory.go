// Package inventory persists commissioning observations and operator-approved
// device baselines keyed by bus endpoint and stable ISO Device NAME.
package inventory

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/bus"
	"github.com/open-ships/beacon/internal/store"
)

const (
	onlineWindow             = 90 * time.Second
	inventoryPersistInterval = 5 * time.Minute
	// One NMEA 2000 network has only 254 source addresses. Keeping four full
	// networks of uncommissioned history is generous while preventing a noisy
	// or spoofed bus from growing RAM and SQLite without bound. Operator-
	// approved commissioning records are never evicted by this policy.
	maxUncommissionedDevices = 1024
)

type Record struct {
	bus.DeviceInfo
	FirstSeen time.Time `json:"first_seen"`
	Expected  bool      `json:"expected"`
	Label     string    `json:"label,omitempty"`
	Status    string    `json:"status"`
	Changed   bool      `json:"changed"`
}

type fingerprint struct {
	ManufacturerCode         uint16 `json:"manufacturer_code"`
	DeviceInstance           uint8  `json:"device_instance"`
	SystemInstance           uint8  `json:"system_instance"`
	DeviceClass              uint8  `json:"device_class"`
	DeviceFunction           uint8  `json:"device_function"`
	N2KVersion               uint64 `json:"n2k_version"`
	ProductCode              uint64 `json:"product_code"`
	Model                    string `json:"model"`
	SoftwareVersion          string `json:"software_version"`
	ModelVersion             string `json:"model_version"`
	Serial                   string `json:"serial"`
	CertificationLevel       uint64 `json:"certification_level"`
	InstallationDescription1 string `json:"installation_description_1"`
	InstallationDescription2 string `json:"installation_description_2"`
	ManufacturerInformation  string `json:"manufacturer_information"`
}

type Registry struct {
	st            *store.Store
	mu            sync.RWMutex
	records       []Record
	latest        map[string]bus.DeviceInfo
	persistedSeen map[string]time.Time
	persistedFP   map[string]fingerprint
}

func New(st *store.Store) *Registry {
	return &Registry{
		st: st, records: []Record{}, latest: map[string]bus.DeviceInfo{},
		persistedSeen: map[string]time.Time{}, persistedFP: map[string]fingerprint{},
	}
}

func nameKey(name uint64) string { return fmt.Sprintf("%016X", name) }

func deviceKey(endpoint string, name uint64) string { return endpoint + "\x00" + nameKey(name) }

func fp(device bus.DeviceInfo) fingerprint {
	return fingerprint{
		ManufacturerCode: device.ManufacturerCode, DeviceInstance: device.DeviceInstance,
		SystemInstance: device.SystemInstance, DeviceClass: device.DeviceClass,
		DeviceFunction: device.DeviceFunction, N2KVersion: device.N2KVersion,
		ProductCode: device.ProductCode, Model: device.Model,
		SoftwareVersion: device.SoftwareVersion, ModelVersion: device.ModelVersion,
		Serial: device.Serial, CertificationLevel: device.CertificationLevel,
		InstallationDescription1: device.InstallationDescription1,
		InstallationDescription2: device.InstallationDescription2,
		ManufacturerInformation:  device.ManufacturerInformation,
	}
}

func (r *Registry) Observe(ctx context.Context, devices []bus.DeviceInfo) error {
	now := time.Now().UTC()
	normalized := make([]bus.DeviceInfo, 0, len(devices))

	r.mu.Lock()
	known := make(map[string]bus.DeviceInfo, len(r.records)+len(r.latest))
	for _, record := range r.records {
		known[deviceKey(record.Endpoint, record.Name)] = record.DeviceInfo
	}
	for key, device := range r.latest {
		known[key] = device
	}
	persisted := make(map[string]time.Time, len(r.persistedSeen))
	for key, seen := range r.persistedSeen {
		persisted[key] = seen
	}
	persistedFingerprints := make(map[string]fingerprint, len(r.persistedFP))
	for key, fingerprint := range r.persistedFP {
		persistedFingerprints[key] = fingerprint
	}
	for _, device := range devices {
		if device.LastSeen.IsZero() {
			device.LastSeen = now
		} else {
			device.LastSeen = device.LastSeen.UTC()
		}
		key := deviceKey(device.Endpoint, device.Name)
		if previous, ok := known[key]; ok && device.LastSeen.Before(previous.LastSeen) {
			continue
		}
		r.latest[key] = device
		normalized = append(normalized, device)
	}
	r.boundLatestLocked()
	r.applyLatestLocked(now)
	r.mu.Unlock()

	type observation struct {
		device bus.DeviceInfo
		doc    string
	}
	writes := make([]observation, 0, len(normalized))
	for _, device := range normalized {
		key := deviceKey(device.Endpoint, device.Name)
		lastPersisted, persistedBefore := persisted[key]
		persistedFingerprint, fingerprintExists := persistedFingerprints[key]
		// Compare against durable state, not r.latest. If the previous write
		// failed, r.latest already contains this observation; comparing to it
		// would postpone the retry until the five-minute heartbeat.
		meaningfulChange := !fingerprintExists || persistedFingerprint != fp(device)
		heartbeatDue := !persistedBefore || device.LastSeen.Sub(lastPersisted) >= inventoryPersistInterval
		if !meaningfulChange && !heartbeatDue {
			continue
		}
		current, err := json.Marshal(device)
		if err != nil {
			return err
		}
		writes = append(writes, observation{device: device, doc: string(current)})
	}
	if len(writes) == 0 {
		return nil
	}

	tx, err := r.st.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO device_inventory(endpoint, device_name, first_seen, last_seen, current_doc)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(endpoint, device_name) DO UPDATE SET
			  last_seen = excluded.last_seen, current_doc = excluded.current_doc
			WHERE excluded.last_seen > device_inventory.last_seen
			   OR (excluded.last_seen = device_inventory.last_seen
			       AND excluded.current_doc <> device_inventory.current_doc)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, item := range writes {
		seen := item.device.LastSeen.UnixNano()
		if _, err := stmt.ExecContext(ctx, item.device.Endpoint, nameKey(item.device.Name), seen, seen, item.doc); err != nil {
			return fmt.Errorf("observe device %s/%s: %w", item.device.Endpoint, nameKey(item.device.Name), err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM device_inventory
		WHERE expected = 0 AND rowid IN (
		  SELECT rowid FROM device_inventory
		  WHERE expected = 0
		  ORDER BY last_seen DESC
		  LIMIT -1 OFFSET ?
		)`, maxUncommissionedDevices); err != nil {
		return fmt.Errorf("bound uncommissioned inventory: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return r.Refresh(ctx)
}

// boundLatestLocked is independent of persistence success. A full or failing
// database must not let changing Device NAMEs grow the observation cache.
// Commissioned records remain protected, just as in the durable inventory.
func (r *Registry) boundLatestLocked() {
	if len(r.latest) <= maxUncommissionedDevices {
		return
	}
	expected := make(map[string]bool)
	for _, record := range r.records {
		if record.Expected {
			expected[deviceKey(record.Endpoint, record.Name)] = true
		}
	}
	keys := make([]string, 0, len(r.latest))
	for key := range r.latest {
		if !expected[key] {
			keys = append(keys, key)
		}
	}
	if len(keys) <= maxUncommissionedDevices {
		return
	}
	sort.Slice(keys, func(i, j int) bool {
		left, right := r.latest[keys[i]].LastSeen, r.latest[keys[j]].LastSeen
		if left.Equal(right) {
			return keys[i] < keys[j]
		}
		return left.After(right)
	})
	for _, key := range keys[maxUncommissionedDevices:] {
		delete(r.latest, key)
	}
}

func (r *Registry) Refresh(ctx context.Context) error {
	rows, err := r.st.DB().QueryContext(ctx, `
		SELECT first_seen, last_seen, expected, label, baseline_doc, current_doc
		FROM device_inventory ORDER BY endpoint, device_name`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	now := time.Now().UTC()
	records := []Record{}
	persistedSeen := map[string]time.Time{}
	persistedFingerprints := map[string]fingerprint{}
	for rows.Next() {
		var first, last int64
		var expected int
		var label, baselineDoc, currentDoc string
		if err := rows.Scan(&first, &last, &expected, &label, &baselineDoc, &currentDoc); err != nil {
			return err
		}
		var device bus.DeviceInfo
		if err := json.Unmarshal([]byte(currentDoc), &device); err != nil {
			return err
		}
		device.LastSeen = time.Unix(0, last).UTC()
		key := deviceKey(device.Endpoint, device.Name)
		persistedSeen[key] = device.LastSeen
		persistedFingerprints[key] = fp(device)
		currentFP, _ := json.Marshal(fp(device))
		changed := expected != 0 && baselineDoc != "" && baselineDoc != string(currentFP)
		status := "historical"
		online := now.Sub(device.LastSeen) <= onlineWindow
		switch {
		case expected == 0 && online:
			status = "new"
		case expected != 0 && !online:
			status = "missing"
		case expected != 0 && changed:
			status = "changed"
		case expected != 0:
			status = "online"
		}
		records = append(records, Record{DeviceInfo: device, FirstSeen: time.Unix(0, first).UTC(),
			Expected: expected != 0, Label: label, Status: status, Changed: changed})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	r.records = records
	r.persistedSeen = persistedSeen
	r.persistedFP = persistedFingerprints
	for key := range r.latest {
		if _, retained := persistedSeen[key]; !retained {
			delete(r.latest, key)
		}
	}
	r.applyLatestLocked(now)
	r.mu.Unlock()
	return nil
}

func (r *Registry) applyLatestLocked(now time.Time) {
	for i := range r.records {
		record := &r.records[i]
		if latest, ok := r.latest[deviceKey(record.Endpoint, record.Name)]; ok &&
			!latest.LastSeen.Before(record.LastSeen) {
			record.DeviceInfo = latest
		}
		record.Status = recordStatus(*record, now)
	}
}

func recordStatus(record Record, now time.Time) string {
	online := now.Sub(record.LastSeen) <= onlineWindow
	switch {
	case !record.Expected && online:
		return "new"
	case record.Expected && !online:
		return "missing"
	case record.Expected && record.Changed:
		return "changed"
	case record.Expected:
		return "online"
	default:
		return "historical"
	}
}

func (r *Registry) Records() []Record {
	if r == nil {
		return []Record{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := append([]Record(nil), r.records...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Status != out[j].Status {
			return out[i].Status < out[j].Status
		}
		return out[i].LastSeen.After(out[j].LastSeen)
	})
	return out
}

func (r *Registry) CommitBaseline(ctx context.Context) error {
	rows, err := r.st.DB().QueryContext(ctx, `SELECT endpoint, device_name, last_seen, current_doc FROM device_inventory`)
	if err != nil {
		return err
	}
	type update struct{ endpoint, name, baseline string }
	var updates []update
	for rows.Next() {
		var endpoint, name, current string
		var last int64
		if err := rows.Scan(&endpoint, &name, &last, &current); err != nil {
			_ = rows.Close()
			return err
		}
		var device bus.DeviceInfo
		if err := json.Unmarshal([]byte(current), &device); err != nil {
			_ = rows.Close()
			return err
		}
		if time.Since(time.Unix(0, last)) > onlineWindow {
			continue
		}
		baseline, _ := json.Marshal(fp(device))
		updates = append(updates, update{endpoint, name, string(baseline)})
	}
	if err := rows.Close(); err != nil {
		return err
	}
	tx, err := r.st.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, item := range updates {
		if _, err := tx.ExecContext(ctx, `UPDATE device_inventory SET expected = 1, baseline_doc = ? WHERE endpoint = ? AND device_name = ?`,
			item.baseline, item.endpoint, item.name); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return r.Refresh(ctx)
}

func (r *Registry) SetLabel(ctx context.Context, endpoint string, name uint64, label string) error {
	result, err := r.st.DB().ExecContext(ctx, `UPDATE device_inventory SET label = ? WHERE endpoint = ? AND device_name = ?`,
		label, endpoint, nameKey(name))
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return sql.ErrNoRows
	}
	return r.Refresh(ctx)
}
