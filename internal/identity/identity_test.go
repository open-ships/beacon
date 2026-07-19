package identity

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/store"
)

func TestLoadOrCreatePersistsStableNAME(t *testing.T) {
	path := filepath.Join(t.TempDir(), "beacon.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := LoadOrCreate(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	second, err := LoadOrCreate(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	if first.DeviceNAME != second.DeviceNAME || first.SerialNumber != second.SerialNumber {
		t.Fatalf("identity changed across restart: first=%+v second=%+v", first, second)
	}
	if len(second.DeviceNameHex) != 16 {
		t.Fatalf("hex NAME = %q, want 16 digits", second.DeviceNameHex)
	}
	if second.ManufacturerCode != ExperimentalManufacturer || second.N2KVersion != N2KVersion {
		t.Fatalf("identity defaults = %+v", second)
	}
}

func TestLoadRejectsIdentityOutside21BitRange(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "beacon.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.DB().Exec(
		`INSERT INTO appliance_identity(id, identity_number, created_at) VALUES (1, ?, ?)`,
		1<<21, 1,
	); err != nil {
		t.Fatal(err)
	}

	_, err = LoadOrCreate(context.Background(), st)
	if err == nil || !strings.Contains(err.Error(), "outside the 21-bit range") {
		t.Fatalf("LoadOrCreate error = %v, want invalid identity range", err)
	}
}
