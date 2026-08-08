package ui

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	n2k "github.com/open-ships/n2k"
	"github.com/open-ships/n2k/pgn"

	"github.com/open-ships/beacon/internal/stats"
)

// sourceDeviceRow summarizes every observed PGN stream for one source address.
// Address and Device NAME are deliberately separate: the address orders the
// table and identifies the current sender slot, while NAME is the stable
// ISO 11783 device identity that survives address changes.
type sourceDeviceRow struct {
	Address                 uint8
	DeviceName              string
	IdentityNumber          string
	Manufacturer            string
	ManufacturerCode        string
	ManufacturerInformation string
	Model                   string
	ProductCode             string
	Role                    string
	Instances               string
	Software                string
	Serial                  string
	TrafficShareText        string
	MessagesPerSecText      string
	BytesPerSecText         string
	BusLoadText             string
	LastSeenText            string
	LastSeenTitle           string
	messagesPerSec          float64
	bytesPerSec             float64
	trafficShare            float64
	lastSeen                time.Time
	Selected                bool
	DetailHref              string
}

type sourceDeviceAggregate struct {
	sourceDeviceRow
	observedBytes  float64
	messagesPerSec float64
	bytesPerSec    float64
	busLoad        float64
	lastSeen       time.Time
}

type sourceDeviceSort struct {
	key       string
	direction string
}

type sourceDeviceSortControl struct {
	Href       string
	AriaSort   string
	Indicator  string
	Accessible string
}

type sourceDeviceSortView struct {
	Address  sourceDeviceSortControl
	Messages sourceDeviceSortControl
	Bytes    sourceDeviceSortControl
	Share    sourceDeviceSortControl
	LastSeen sourceDeviceSortControl
}

const (
	sourceDeviceSortAddress  = "address"
	sourceDeviceSortMessages = "messages"
	sourceDeviceSortBytes    = "bytes"
	sourceDeviceSortShare    = "share"
	sourceDeviceSortLastSeen = "last_seen"
	sourceDeviceSortAsc      = "asc"
	sourceDeviceSortDesc     = "desc"
)

// sourceDeviceRows groups source/PGN metrics into one device row per observed
// source address. It uses process-lifetime payload bytes for traffic share so
// the percentages remain meaningful after an idle source's rate window decays.
func sourceDeviceRows(metrics []stats.SourcePGNMetric, sorting sourceDeviceSort) []sourceDeviceRow {
	byAddress := make(map[uint8]*sourceDeviceAggregate)
	totalObservedBytes := 0.0
	for _, metric := range metrics {
		if !metric.Observed {
			continue
		}
		device := byAddress[metric.SourceAddress]
		if device == nil {
			device = &sourceDeviceAggregate{sourceDeviceRow: sourceDeviceRow{Address: metric.SourceAddress}}
			byAddress[metric.SourceAddress] = device
		}

		observedBytes := float64(metric.Messages) * metric.PayloadBytesMean
		device.observedBytes += observedBytes
		totalObservedBytes += observedBytes
		device.messagesPerSec += metric.RecentMessagesPerSec
		device.bytesPerSec += metric.RecentBytesPerSec
		device.busLoad += metric.EstimatedBusLoadPercent
		if metric.LastSeen.After(device.lastSeen) {
			device.lastSeen = metric.LastSeen
		}

		if name, ok := sourceMetricDeviceName(metric); ok {
			applySourceDeviceName(device, name)
		}
		if metric.ManufacturerCode != nil && device.ManufacturerCode == "" {
			applySourceManufacturer(device, *metric.ManufacturerCode)
		}
		switch metric.PGN {
		case 126996:
			applySourceProductInformation(device, metric.Fields)
		case 126998:
			applySourceConfigurationInformation(device, metric.Fields)
		}
	}

	addresses := make([]uint8, 0, len(byAddress))
	for address := range byAddress {
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(i, j int) bool {
		return addresses[i] < addresses[j]
	})

	rows := make([]sourceDeviceRow, 0, len(addresses))
	for _, address := range addresses {
		device := byAddress[address]
		share := 0.0
		if totalObservedBytes > 0 {
			share = device.observedBytes / totalObservedBytes * 100
		}
		device.sourceDeviceRow.messagesPerSec = device.messagesPerSec
		device.sourceDeviceRow.bytesPerSec = device.bytesPerSec
		device.trafficShare = share
		device.sourceDeviceRow.lastSeen = device.lastSeen
		device.TrafficShareText = fmt.Sprintf("%.1f%%", share)
		device.MessagesPerSecText = fmt.Sprintf("%.2f", device.messagesPerSec)
		device.BytesPerSecText = humanizeBytes(device.bytesPerSec, "/s")
		device.BusLoadText = fmt.Sprintf("%.3f%%", device.busLoad)
		device.LastSeenText = formatAge(time.Since(device.lastSeen).Seconds())
		device.LastSeenTitle = device.lastSeen.UTC().Format(time.RFC3339Nano)
		rows = append(rows, device.sourceDeviceRow)
	}
	sortSourceDeviceRows(rows, sorting)
	return rows
}

func sourceDeviceSortFromQuery(key, direction string) sourceDeviceSort {
	switch key {
	case sourceDeviceSortMessages, sourceDeviceSortBytes, sourceDeviceSortShare, sourceDeviceSortLastSeen:
	default:
		key = sourceDeviceSortAddress
	}
	if direction != sourceDeviceSortAsc && direction != sourceDeviceSortDesc {
		direction = defaultSourceDeviceSortDirection(key)
	}
	return sourceDeviceSort{key: key, direction: direction}
}

func defaultSourceDeviceSortDirection(key string) string {
	if key == sourceDeviceSortAddress {
		return sourceDeviceSortAsc
	}
	return sourceDeviceSortDesc
}

func sourceDeviceSortURL(base string, sorting sourceDeviceSort, selectedAddress *uint8, pgnSorting sourceDevicePGNSort) string {
	query := url.Values{}
	query.Set("sort", sorting.key)
	query.Set("dir", sorting.direction)
	if selectedAddress != nil {
		query.Set("device", strconv.FormatUint(uint64(*selectedAddress), 10))
		query.Set("pgn_sort", pgnSorting.key)
		query.Set("pgn_dir", pgnSorting.direction)
	}
	separator := "?"
	if strings.Contains(base, "?") {
		separator = "&"
	}
	return base + separator + query.Encode()
}

func sourceDeviceSortControls(base string, current sourceDeviceSort, selectedAddress *uint8, pgnSorting sourceDevicePGNSort) sourceDeviceSortView {
	return sourceDeviceSortView{
		Address:  sourceDeviceSortControlFor(base, current, sourceDeviceSortAddress, "address", selectedAddress, pgnSorting),
		Messages: sourceDeviceSortControlFor(base, current, sourceDeviceSortMessages, "messages per second", selectedAddress, pgnSorting),
		Bytes:    sourceDeviceSortControlFor(base, current, sourceDeviceSortBytes, "bytes per second", selectedAddress, pgnSorting),
		Share:    sourceDeviceSortControlFor(base, current, sourceDeviceSortShare, "traffic share", selectedAddress, pgnSorting),
		LastSeen: sourceDeviceSortControlFor(base, current, sourceDeviceSortLastSeen, "last seen", selectedAddress, pgnSorting),
	}
}

func sourceDeviceSortControlFor(base string, current sourceDeviceSort, key, label string, selectedAddress *uint8, pgnSorting sourceDevicePGNSort) sourceDeviceSortControl {
	nextDirection := defaultSourceDeviceSortDirection(key)
	control := sourceDeviceSortControl{AriaSort: "none", Indicator: "↕"}
	if current.key == key {
		control.AriaSort = "ascending"
		control.Indicator = "↑"
		nextDirection = sourceDeviceSortDesc
		if current.direction == sourceDeviceSortDesc {
			control.AriaSort = "descending"
			control.Indicator = "↓"
			nextDirection = sourceDeviceSortAsc
		}
	}
	control.Href = sourceDeviceSortURL(base, sourceDeviceSort{key: key, direction: nextDirection}, selectedAddress, pgnSorting)
	control.Accessible = fmt.Sprintf("Sort by %s %s", label, sortDirectionWord(nextDirection))
	return control
}

func sourceDeviceAddressFromQuery(raw string) *uint8 {
	address, err := strconv.ParseUint(raw, 10, 8)
	if err != nil {
		return nil
	}
	value := uint8(address)
	return &value
}

func decorateSourceDeviceRows(rows []sourceDeviceRow, base string, sorting sourceDeviceSort, selectedAddress *uint8, pgnSorting sourceDevicePGNSort) {
	for i := range rows {
		address := rows[i].Address
		rows[i].DetailHref = sourceDeviceSortURL(base, sorting, &address, pgnSorting)
		rows[i].Selected = selectedAddress != nil && address == *selectedAddress
	}
}

func sortDirectionWord(direction string) string {
	if direction == sourceDeviceSortDesc {
		return "descending"
	}
	return "ascending"
}

func sortSourceDeviceRows(rows []sourceDeviceRow, sorting sourceDeviceSort) {
	sort.SliceStable(rows, func(i, j int) bool {
		comparison := 0
		switch sorting.key {
		case sourceDeviceSortMessages:
			comparison = compareFloat(rows[i].messagesPerSec, rows[j].messagesPerSec)
		case sourceDeviceSortBytes:
			comparison = compareFloat(rows[i].bytesPerSec, rows[j].bytesPerSec)
		case sourceDeviceSortShare:
			comparison = compareFloat(rows[i].trafficShare, rows[j].trafficShare)
		case sourceDeviceSortLastSeen:
			comparison = compareTime(rows[i].lastSeen, rows[j].lastSeen)
		default:
			comparison = compareInt(int(rows[i].Address), int(rows[j].Address))
		}
		if comparison == 0 {
			return rows[i].Address < rows[j].Address
		}
		if sorting.direction == sourceDeviceSortDesc {
			return comparison > 0
		}
		return comparison < 0
	})
}

func compareFloat(left, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareInt(left, right int) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func sourceMetricDeviceName(metric stats.SourcePGNMetric) (uint64, bool) {
	if metric.DeviceName != nil {
		return *metric.DeviceName, true
	}
	if metric.DeviceNameHex != "" {
		if name, err := strconv.ParseUint(strings.TrimPrefix(metric.DeviceNameHex, "0x"), 16, 64); err == nil {
			return name, true
		}
	}
	if metric.PGN != 60928 || metric.Raw == nil {
		return 0, false
	}
	raw, err := hex.DecodeString(metric.Raw.LastHex)
	if err != nil || len(raw) != 8 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(raw), true
}

func applySourceDeviceName(device *sourceDeviceAggregate, rawName uint64) {
	name := n2k.UnpackDeviceName(rawName)
	device.DeviceName = fmt.Sprintf("0x%016X", rawName)
	device.IdentityNumber = strconv.FormatUint(uint64(name.IdentityNumber), 10)
	applySourceManufacturer(device, name.ManufacturerCode)

	className := pgn.DeviceClassConst(name.DeviceClass).String()
	functionName := ""
	if functions := pgn.DeviceFunctionConstMap[int(name.DeviceClass)]; functions != nil {
		functionName = functions[int(name.DeviceFunction)]
	}
	if functionName == "" {
		functionName = fmt.Sprintf("function %d", name.DeviceFunction)
	}
	device.Role = strings.TrimSpace(className + " / " + functionName)
	device.Instances = fmt.Sprintf("device %d / system %d", name.DeviceInstance, name.SystemInstance)
}

func applySourceManufacturer(device *sourceDeviceAggregate, code uint16) {
	device.ManufacturerCode = strconv.FormatUint(uint64(code), 10)
	device.Manufacturer = pgn.ManufacturerCodeConst(code).String()
}

func applySourceProductInformation(device *sourceDeviceAggregate, fields []stats.FieldDistribution) {
	values := sourceDeviceFieldValues(fields)
	device.Model = values["modelId"]
	device.ProductCode = values["productCode"]
	device.Serial = values["modelSerialCode"]
	device.Software = strings.TrimSpace(strings.Join(nonemptyStrings(
		values["softwareVersionCode"],
		values["modelVersion"],
	), " · "))
}

func applySourceConfigurationInformation(device *sourceDeviceAggregate, fields []stats.FieldDistribution) {
	values := sourceDeviceFieldValues(fields)
	device.ManufacturerInformation = values["manufacturerInformation"]
}

func sourceDeviceFieldValues(fields []stats.FieldDistribution) map[string]string {
	values := make(map[string]string, len(fields))
	for _, field := range fields {
		if field.Last != "" {
			values[field.Field] = strings.TrimSpace(field.Last)
		}
	}
	return values
}

func nonemptyStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			out = append(out, value)
		}
	}
	return out
}
