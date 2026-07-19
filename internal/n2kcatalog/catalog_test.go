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
