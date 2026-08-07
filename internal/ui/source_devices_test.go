package ui

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	n2k "github.com/open-ships/n2k"
	"github.com/open-ships/n2k/pgn"

	"github.com/open-ships/beacon/internal/stats"
)

func TestSourceDeviceRowsOrderIdentityAndTraffic(t *testing.T) {
	now := time.Now().UTC()
	garminName := n2k.DeviceName{
		IdentityNumber: 301, ManufacturerCode: uint16(pgn.Garmin),
		DeviceClass: 25, DeviceFunction: 130,
	}.Pack(true)
	raymarineName := n2k.DeviceName{
		IdentityNumber: 1201, ManufacturerCode: uint16(pgn.Raymarine),
		DeviceClass: 25, DeviceFunction: 130, DeviceInstance: 2, SystemInstance: 1,
	}.Pack(true)
	claimPayload := make([]byte, 8)
	binary.LittleEndian.PutUint64(claimPayload, garminName)

	metrics := []stats.SourcePGNMetric{
		{
			Observed: true, SourceAddress: 12, PGN: 126996, DeviceName: &raymarineName,
			Messages: 3, PayloadBytesMean: 8, RecentMessagesPerSec: 1.25, RecentBytesPerSec: 6,
			EstimatedBusLoadPercent: 0.25, LastSeen: now,
			Fields: []stats.FieldDistribution{
				{Field: "productCode", Last: "2345"},
				{Field: "modelId", Last: "Axiom Pro"},
				{Field: "softwareVersionCode", Last: "4.8.1"},
				{Field: "modelVersion", Last: "Mk II"},
				{Field: "modelSerialCode", Last: "RAY-42"},
			},
		},
		{
			Observed: true, SourceAddress: 3, PGN: 60928,
			Messages: 1, PayloadBytesMean: 8, RecentMessagesPerSec: 0.5, RecentBytesPerSec: 2,
			EstimatedBusLoadPercent: 0.05, LastSeen: now.Add(-time.Second),
			Raw: &stats.RawPayloadDiagnostics{LastHex: hex.EncodeToString(claimPayload)},
		},
		{
			Observed: true, SourceAddress: 12, PGN: 126998, DeviceName: &raymarineName,
			RecentMessagesPerSec: 0.75, RecentBytesPerSec: 2, LastSeen: now,
			Fields: []stats.FieldDistribution{
				{Field: "manufacturerInformation", Last: "Raymarine UK"},
			},
		},
		{
			Observed: false, SourceAddress: 1, PGN: 127250,
			Messages: 100, PayloadBytesMean: 8, LastSeen: now,
		},
	}

	rows := sourceDeviceRows(metrics, sourceDeviceSortFromQuery("", ""))
	if len(rows) != 2 {
		t.Fatalf("device rows = %d, want 2: %+v", len(rows), rows)
	}
	if rows[0].Address != 3 || rows[1].Address != 12 {
		t.Fatalf("addresses = [%d, %d], want [3, 12]", rows[0].Address, rows[1].Address)
	}
	if rows[0].DeviceName != fmt.Sprintf("0x%016X", garminName) {
		t.Fatalf("claim Device NAME = %q, want packed Garmin NAME", rows[0].DeviceName)
	}
	if rows[0].Manufacturer != "Garmin" || rows[0].IdentityNumber != "301" {
		t.Fatalf("claim identity = manufacturer %q / identity %q", rows[0].Manufacturer, rows[0].IdentityNumber)
	}
	if rows[1].Manufacturer != "Raymarine" || rows[1].ManufacturerCode != "1851" {
		t.Fatalf("product manufacturer = %q (%s)", rows[1].Manufacturer, rows[1].ManufacturerCode)
	}
	if rows[1].ManufacturerInformation != "Raymarine UK" {
		t.Fatalf("manufacturer information = %q", rows[1].ManufacturerInformation)
	}
	if rows[1].Model != "Axiom Pro" || rows[1].ProductCode != "2345" {
		t.Fatalf("model/product = %q / %q", rows[1].Model, rows[1].ProductCode)
	}
	if rows[1].Instances != "device 2 / system 1" {
		t.Fatalf("instances = %q", rows[1].Instances)
	}
	if rows[1].Software != "4.8.1 · Mk II" || rows[1].Serial != "RAY-42" {
		t.Fatalf("software/serial = %q / %q", rows[1].Software, rows[1].Serial)
	}
	if rows[0].TrafficShareText != "25.0%" || rows[1].TrafficShareText != "75.0%" {
		t.Fatalf("traffic shares = %q / %q", rows[0].TrafficShareText, rows[1].TrafficShareText)
	}
	if rows[0].MessagesPerSecText != "0.50" || rows[1].MessagesPerSecText != "2.00" {
		t.Fatalf("message rates = %q / %q", rows[0].MessagesPerSecText, rows[1].MessagesPerSecText)
	}
	if rows[0].BytesPerSecText != "2 B/s" || rows[1].BytesPerSecText != "8 B/s" {
		t.Fatalf("byte rates = %q / %q", rows[0].BytesPerSecText, rows[1].BytesPerSecText)
	}
}

func TestSourceDeviceRowsSortsByEverySupportedColumnAndDirection(t *testing.T) {
	now := time.Now().UTC()
	metrics := []stats.SourcePGNMetric{
		{
			Observed: true, SourceAddress: 3, PGN: 127250,
			Messages: 10, PayloadBytesMean: 10,
			RecentMessagesPerSec: 2, RecentBytesPerSec: 30, LastSeen: now.Add(-2 * time.Second),
		},
		{
			Observed: true, SourceAddress: 7, PGN: 127251,
			Messages: 30, PayloadBytesMean: 10,
			RecentMessagesPerSec: 1, RecentBytesPerSec: 40, LastSeen: now,
		},
		{
			Observed: true, SourceAddress: 12, PGN: 127252,
			Messages: 20, PayloadBytesMean: 10,
			RecentMessagesPerSec: 3, RecentBytesPerSec: 20, LastSeen: now.Add(-time.Second),
		},
	}

	for _, tc := range []struct {
		key       string
		direction string
		want      []uint8
	}{
		{sourceDeviceSortAddress, sourceDeviceSortAsc, []uint8{3, 7, 12}},
		{sourceDeviceSortAddress, sourceDeviceSortDesc, []uint8{12, 7, 3}},
		{sourceDeviceSortMessages, sourceDeviceSortAsc, []uint8{7, 3, 12}},
		{sourceDeviceSortMessages, sourceDeviceSortDesc, []uint8{12, 3, 7}},
		{sourceDeviceSortBytes, sourceDeviceSortAsc, []uint8{12, 3, 7}},
		{sourceDeviceSortBytes, sourceDeviceSortDesc, []uint8{7, 3, 12}},
		{sourceDeviceSortShare, sourceDeviceSortAsc, []uint8{3, 12, 7}},
		{sourceDeviceSortShare, sourceDeviceSortDesc, []uint8{7, 12, 3}},
		{sourceDeviceSortLastSeen, sourceDeviceSortAsc, []uint8{3, 12, 7}},
		{sourceDeviceSortLastSeen, sourceDeviceSortDesc, []uint8{7, 12, 3}},
	} {
		t.Run(tc.key+"_"+tc.direction, func(t *testing.T) {
			rows := sourceDeviceRows(metrics, sourceDeviceSortFromQuery(tc.key, tc.direction))
			for i, want := range tc.want {
				if rows[i].Address != want {
					t.Fatalf("row %d address = %d, want %d; rows = %+v", i, rows[i].Address, want, rows)
				}
			}
		})
	}
}

func TestSourceDeviceSortControlsToggleAndDefault(t *testing.T) {
	const base = "/frag/sources/src1/overview"

	defaultSort := sourceDeviceSortFromQuery("", "")
	defaultPGNSort := sourceDevicePGNSortFromQuery("", "")
	if defaultSort.key != sourceDeviceSortAddress || defaultSort.direction != sourceDeviceSortAsc {
		t.Fatalf("default sort = %+v, want address ascending", defaultSort)
	}
	controls := sourceDeviceSortControls(base, defaultSort, nil, defaultPGNSort)
	if controls.Address.AriaSort != "ascending" || controls.Address.Indicator != "↑" {
		t.Fatalf("default address control = %+v", controls.Address)
	}
	if controls.Address.Href != base+"?dir=desc&sort=address" {
		t.Fatalf("address toggle href = %q", controls.Address.Href)
	}
	if controls.Messages.Href != base+"?dir=desc&sort=messages" {
		t.Fatalf("messages initial href = %q", controls.Messages.Href)
	}
	if controls.LastSeen.Href != base+"?dir=desc&sort=last_seen" {
		t.Fatalf("last seen initial href = %q", controls.LastSeen.Href)
	}

	messageSort := sourceDeviceSortFromQuery(sourceDeviceSortMessages, sourceDeviceSortDesc)
	controls = sourceDeviceSortControls(base, messageSort, nil, defaultPGNSort)
	if controls.Messages.AriaSort != "descending" || controls.Messages.Indicator != "↓" {
		t.Fatalf("message control = %+v", controls.Messages)
	}
	if controls.Messages.Href != base+"?dir=asc&sort=messages" {
		t.Fatalf("message toggle href = %q", controls.Messages.Href)
	}

	lastSeenSort := sourceDeviceSortFromQuery(sourceDeviceSortLastSeen, sourceDeviceSortDesc)
	controls = sourceDeviceSortControls(base, lastSeenSort, nil, defaultPGNSort)
	if controls.LastSeen.AriaSort != "descending" || controls.LastSeen.Indicator != "↓" {
		t.Fatalf("last seen control = %+v", controls.LastSeen)
	}
	if controls.LastSeen.Href != base+"?dir=asc&sort=last_seen" {
		t.Fatalf("last seen toggle href = %q", controls.LastSeen.Href)
	}
}

func TestSourceDeviceDetailBuildsMetadataAndPGNStatistics(t *testing.T) {
	now := time.Now().UTC()
	address := uint8(12)
	sorting := sourceDeviceSortFromQuery(sourceDeviceSortBytes, sourceDeviceSortDesc)
	devices := []sourceDeviceRow{{
		Address: 12, DeviceName: "0x0102030405060708", IdentityNumber: "301",
		Manufacturer: "Raymarine", ManufacturerCode: "1851",
		Model: "Axiom Pro", ProductCode: "2345", Instances: "device 2 / system 1",
		Software: "4.8.1", Serial: "RAY-42",
	}}
	metrics := []stats.SourcePGNMetric{
		{
			Observed: true, SourceAddress: 12, PGN: 129025, PGNName: "Position, Rapid Update",
			Status: "active", DecodeStatus: "decoded", Messages: 20, PayloadBytesMean: 8,
			PayloadBytesLast: 8, RecentMessagesPerSec: 3.5, RecentBytesPerSec: 28,
			EstimatedBusLoadPercent: 0.12, ExpectedPeriodSeconds: 0.1,
			ShortestPeriodSeconds: 0.09, LongestPeriodSeconds: 0.14,
			PeriodP90Seconds: 0.11, PeriodP99Seconds: 0.13,
			JitterMADSeconds: 0.005, JitterPercent: 5, LastSeen: now, AgeSeconds: 0.2,
		},
		{
			Observed: true, SourceAddress: 12, PGN: 127250, PGNName: "Vessel Heading",
			Status: "active", DecodeStatus: "decoded", Messages: 10, PayloadBytesMean: 8,
			PayloadBytesLast: 8, RecentMessagesPerSec: 1, RecentBytesPerSec: 8,
			ExpectedPeriodSeconds: 1, PeriodP90Seconds: 1.1, PeriodP99Seconds: 1.2,
			LastSeen: now.Add(-time.Second), AgeSeconds: 1,
		},
	}

	detail := sourceDeviceDetail(
		metrics,
		devices,
		&address,
		"/frag/sources/src1/overview",
		sorting,
		sourceDevicePGNSortFromQuery("", ""),
	)
	if detail == nil {
		t.Fatal("device detail is nil")
	}
	if detail.Device.Model != "Axiom Pro" || detail.Device.Serial != "RAY-42" {
		t.Fatalf("detail metadata = %+v", detail.Device)
	}
	if len(detail.PGNs) != 2 || detail.PGNs[0].PGN != 127250 || detail.PGNs[1].PGN != 129025 {
		t.Fatalf("PGN ordering = %+v", detail.PGNs)
	}
	if detail.PGNs[1].MessagesPerSec != "3.50" || detail.PGNs[1].BytesPerSec != "28 B/s" {
		t.Fatalf("PGN rates = %+v", detail.PGNs[1])
	}
	if detail.PGNs[1].PeriodP90 != "110ms" || detail.PGNs[1].PeriodP99 != "130ms" {
		t.Fatalf("PGN percentiles = p90 %q / p99 %q", detail.PGNs[1].PeriodP90, detail.PGNs[1].PeriodP99)
	}
	if detail.RefreshHref != "/frag/sources/src1/overview?detail=1&device=12&dir=desc&pgn_dir=asc&pgn_sort=pgn&sort=bytes" {
		t.Fatalf("detail refresh href = %q", detail.RefreshHref)
	}
	if detail.CloseHref != "/frag/sources/src1/overview?dir=desc&sort=bytes" {
		t.Fatalf("detail close href = %q", detail.CloseHref)
	}
}

func TestPGNActivityGrade(t *testing.T) {
	cases := []struct {
		name      string
		age       float64
		periodP90 float64
		want      string
	}{
		{"unlearned cadence", 5, 0, "warming"},
		{"inside p90", 0.9, 1, "fresh"},
		{"sub-second slack absorbs refresh jitter", 0.55, 0.1, "fresh"},
		{"overdue past p90", 2, 1, "late"},
		{"missed several periods", 10, 1, "stale"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			level, label := pgnActivityGrade(tc.age, tc.periodP90)
			if level != tc.want {
				t.Fatalf("level = %q, want %q", level, tc.want)
			}
			if label == "" {
				t.Fatal("label is empty; the dot needs a non-color signal")
			}
		})
	}
}

func TestSourceDevicePGNSortingAndControls(t *testing.T) {
	now := time.Now().UTC()
	rows := []sourceDevicePGNRow{
		{PGN: 100, messagesPerSec: 1, trafficShare: 50, payloadBytesMean: 8, lastSeen: now.Add(-time.Second)},
		{PGN: 200, messagesPerSec: 10, trafficShare: 30, payloadBytesMean: 16, lastSeen: now.Add(-2 * time.Second)},
		{PGN: 300, messagesPerSec: 5, trafficShare: 20, payloadBytesMean: 4, lastSeen: now},
	}
	cases := []struct {
		key       string
		direction string
		want      []uint32
	}{
		{sourceDevicePGNSortPGN, sourceDeviceSortAsc, []uint32{100, 200, 300}},
		{sourceDevicePGNSortPGN, sourceDeviceSortDesc, []uint32{300, 200, 100}},
		{sourceDevicePGNSortRates, sourceDeviceSortAsc, []uint32{100, 300, 200}},
		{sourceDevicePGNSortRates, sourceDeviceSortDesc, []uint32{200, 300, 100}},
		{sourceDevicePGNSortTraffic, sourceDeviceSortAsc, []uint32{300, 200, 100}},
		{sourceDevicePGNSortTraffic, sourceDeviceSortDesc, []uint32{100, 200, 300}},
		{sourceDevicePGNSortPayload, sourceDeviceSortAsc, []uint32{300, 100, 200}},
		{sourceDevicePGNSortPayload, sourceDeviceSortDesc, []uint32{200, 100, 300}},
		{sourceDevicePGNSortActivity, sourceDeviceSortAsc, []uint32{200, 100, 300}},
		{sourceDevicePGNSortActivity, sourceDeviceSortDesc, []uint32{300, 100, 200}},
	}
	for _, tc := range cases {
		t.Run(tc.key+"_"+tc.direction, func(t *testing.T) {
			got := append([]sourceDevicePGNRow(nil), rows...)
			sortSourceDevicePGNRows(got, sourceDevicePGNSortFromQuery(tc.key, tc.direction))
			for i, want := range tc.want {
				if got[i].PGN != want {
					t.Fatalf("row %d PGN = %d, want %d; rows = %+v", i, got[i].PGN, want, got)
				}
			}
		})
	}

	const base = "/frag/sources/src1/overview"
	deviceSort := sourceDeviceSortFromQuery(sourceDeviceSortBytes, sourceDeviceSortDesc)
	defaultSort := sourceDevicePGNSortFromQuery("", "")
	if defaultSort.key != sourceDevicePGNSortPGN || defaultSort.direction != sourceDeviceSortAsc {
		t.Fatalf("default PGN sort = %+v, want PGN ascending", defaultSort)
	}
	controls := sourceDevicePGNSortControls(base, deviceSort, 12, defaultSort)
	if controls.PGN.AriaSort != "ascending" || controls.PGN.Indicator != "↑" {
		t.Fatalf("default PGN control = %+v", controls.PGN)
	}
	if controls.PGN.Href != base+"?device=12&dir=desc&pgn_dir=desc&pgn_sort=pgn&sort=bytes" {
		t.Fatalf("PGN toggle href = %q", controls.PGN.Href)
	}
	if controls.Rates.Href != base+"?device=12&dir=desc&pgn_dir=desc&pgn_sort=rates&sort=bytes" {
		t.Fatalf("rates initial href = %q", controls.Rates.Href)
	}
}
