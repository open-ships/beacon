package n2kcatalog

import (
	"testing"

	"github.com/open-ships/n2k/pgn"
)

func TestDescribeVesselHeadingPhysicalValues(t *testing.T) {
	heading := uint64(1234)
	m := &pgn.VesselHeading{Heading: &heading}
	metadata, physical := Describe(m, nil)
	if metadata.Name != "Vessel Heading" || metadata.Variant != "vesselHeading" {
		t.Fatalf("metadata = %+v", metadata)
	}
	got, ok := physical["heading"]
	if !ok || got.Unit != "rad" || got.Value < 0.12339 || got.Value > 0.12341 {
		t.Fatalf("physical heading = %+v", got)
	}
}

func TestListIsSortedAndLookupUsesSameCatalog(t *testing.T) {
	all := List()
	if len(all) < 300 {
		t.Fatalf("catalog only has %d PGNs", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].Number >= all[i].Number {
			t.Fatal("catalog is not sorted")
		}
	}
	if item, ok := Lookup(127250); !ok || item.Variants[0].Description != "Vessel Heading" {
		t.Fatalf("lookup = %+v, %v", item, ok)
	}
}

func TestDeviceFunctionNameUsesDeviceClass(t *testing.T) {
	for _, tc := range []struct {
		class, function uint8
		want            string
	}{
		{25, 130, "PC Gateway"},
		{20, 130, "Emergency Position Indicating Radio Beacon (EPIRB)"},
		{25, 255, ""},
	} {
		if got := DeviceFunctionName(tc.class, tc.function); got != tc.want {
			t.Errorf("class %d function %d = %q, want %q", tc.class, tc.function, got, tc.want)
		}
	}
}
