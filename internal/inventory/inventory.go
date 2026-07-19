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

const onlineWindow = 90 * time.Second

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
	st      *store.Store
	mu      sync.RWMutex
	records []Record
}

func New(st *store.Store) *Registry { return &Registry{st: st, records: []Record{}} }

func nameKey(name uint64) string { return fmt.Sprintf("%016X", name) }

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
	for _, device := range devices {
		if device.LastSeen.IsZero() {
			device.LastSeen = now
		} else {
			device.LastSeen = device.LastSeen.UTC()
		}
		current, err := json.Marshal(device)
		if err != nil {
			return err
		}
		seen := device.LastSeen.UnixNano()
		_, err = r.st.DB().ExecContext(ctx, `
			INSERT INTO device_inventory(endpoint, device_name, first_seen, last_seen, current_doc)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(endpoint, device_name) DO UPDATE SET
			  last_seen = excluded.last_seen, current_doc = excluded.current_doc
			WHERE excluded.last_seen >= device_inventory.last_seen`,
			device.Endpoint, nameKey(device.Name), seen, seen, string(current))
		if err != nil {
			return fmt.Errorf("observe device %s/%s: %w", device.Endpoint, nameKey(device.Name), err)
		}
	}
	return r.Refresh(ctx)
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
	r.mu.Unlock()
	return nil
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
