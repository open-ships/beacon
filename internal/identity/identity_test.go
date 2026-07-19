package identity

import (
	"context"
	"path/filepath"
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
