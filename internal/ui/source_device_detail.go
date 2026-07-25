package ui

import (
	"fmt"
	"sort"
	"time"

	"github.com/open-ships/beacon/internal/stats"
)

type sourceDeviceDetailView struct {
	Device      sourceDeviceRow
	PGNs        []sourceDevicePGNRow
	Sort        sourceDevicePGNSortView
	RefreshHref string
	CloseHref   string
}

type sourceDevicePGNRow struct {
	PGN              uint32
	PGNName          string
	MessagesPerSec   string
	BytesPerSec      string
	DeviceTraffic    string
	MedianPeriod     string
	PeriodP90        string
	PeriodP99        string
	Payload          string
	LastSeen         string
	LastSeenTitle    string
	ActivityLevel    string
	ActivityLabel    string
	EstimatedBusLoad string
	variant          string
	messagesPerSec   float64
	trafficShare     float64
	payloadBytesMean float64
	lastSeen         time.Time
}

type sourceDevicePGNSort struct {
	key       string
	direction string
}

type sourceDevicePGNSortView struct {
	PGN      sourceDeviceSortControl
	Rates    sourceDeviceSortControl
	Traffic  sourceDeviceSortControl
	Payload  sourceDeviceSortControl
	Activity sourceDeviceSortControl
}

const (
	sourceDevicePGNSortPGN      = "pgn"
	sourceDevicePGNSortRates    = "rates"
	sourceDevicePGNSortTraffic  = "traffic"
	sourceDevicePGNSortPayload  = "payload"
	sourceDevicePGNSortActivity = "activity"
)

func sourceDeviceDetail(
	metrics []stats.SourcePGNMetric,
	devices []sourceDeviceRow,
	selectedAddress *uint8,
	base string,
	deviceSorting sourceDeviceSort,
	pgnSorting sourceDevicePGNSort,
) *sourceDeviceDetailView {
	if selectedAddress == nil {
		return nil
	}

	var device *sourceDeviceRow
	for i := range devices {
		if devices[i].Address == *selectedAddress {
			copy := devices[i]
			device = &copy
			break
		}
	}
	if device == nil {
		return nil
	}

	deviceMetrics := make([]stats.SourcePGNMetric, 0)
	totalObservedBytes := 0.0
	for _, metric := range metrics {
		if !metric.Observed || metric.SourceAddress != *selectedAddress {
			continue
		}
		deviceMetrics = append(deviceMetrics, metric)
		totalObservedBytes += float64(metric.Messages) * metric.PayloadBytesMean
	}
	rows := make([]sourceDevicePGNRow, 0, len(deviceMetrics))
	for _, metric := range deviceMetrics {
		trafficShare := 0.0
		if totalObservedBytes > 0 {
			trafficShare = float64(metric.Messages) * metric.PayloadBytesMean / totalObservedBytes * 100
		}
		learnedTiming := metric.ExpectedPeriodSeconds > 0
		activityLevel, activityLabel := pgnActivityGrade(metric.AgeSeconds, metric.PeriodP90Seconds)
		rows = append(rows, sourceDevicePGNRow{
			PGN:              metric.PGN,
			PGNName:          metric.PGNName,
			MessagesPerSec:   fmt.Sprintf("%.2f", metric.RecentMessagesPerSec),
			BytesPerSec:      humanizeBytes(metric.RecentBytesPerSec, "/s"),
			DeviceTraffic:    fmt.Sprintf("%.1f%%", trafficShare),
			MedianPeriod:     formatPanelDuration(metric.ExpectedPeriodSeconds, learnedTiming),
			PeriodP90:        formatPanelDuration(metric.PeriodP90Seconds, learnedTiming),
			PeriodP99:        formatPanelDuration(metric.PeriodP99Seconds, learnedTiming),
			Payload:          formatPayloadSize(metric.PayloadBytesLast, metric.PayloadBytesMean),
			LastSeen:         formatAge(metric.AgeSeconds),
			LastSeenTitle:    metric.LastSeen.UTC().Format(time.RFC3339Nano),
			ActivityLevel:    activityLevel,
			ActivityLabel:    activityLabel,
			EstimatedBusLoad: fmt.Sprintf("%.3f%%", metric.EstimatedBusLoadPercent),
			variant:          metric.Variant,
			messagesPerSec:   metric.RecentMessagesPerSec,
			trafficShare:     trafficShare,
			payloadBytesMean: metric.PayloadBytesMean,
			lastSeen:         metric.LastSeen,
		})
	}
	sortSourceDevicePGNRows(rows, pgnSorting)

	return &sourceDeviceDetailView{
		Device:      *device,
		PGNs:        rows,
		Sort:        sourceDevicePGNSortControls(base, deviceSorting, *selectedAddress, pgnSorting),
		RefreshHref: sourceDeviceSortURL(base, deviceSorting, selectedAddress, pgnSorting),
		CloseHref:   sourceDeviceSortURL(base, deviceSorting, nil, pgnSorting),
	}
}

func sourceDevicePGNSortFromQuery(key, direction string) sourceDevicePGNSort {
	switch key {
	case sourceDevicePGNSortRates, sourceDevicePGNSortTraffic, sourceDevicePGNSortPayload, sourceDevicePGNSortActivity:
	default:
		key = sourceDevicePGNSortPGN
	}
	if direction != sourceDeviceSortAsc && direction != sourceDeviceSortDesc {
		direction = defaultSourceDevicePGNSortDirection(key)
	}
	return sourceDevicePGNSort{key: key, direction: direction}
}

func defaultSourceDevicePGNSortDirection(key string) string {
	if key == sourceDevicePGNSortPGN {
		return sourceDeviceSortAsc
	}
	return sourceDeviceSortDesc
}

func sourceDevicePGNSortControls(
	base string,
	deviceSorting sourceDeviceSort,
	selectedAddress uint8,
	current sourceDevicePGNSort,
) sourceDevicePGNSortView {
	return sourceDevicePGNSortView{
		PGN:      sourceDevicePGNSortControlFor(base, deviceSorting, selectedAddress, current, sourceDevicePGNSortPGN, "PGN number"),
		Rates:    sourceDevicePGNSortControlFor(base, deviceSorting, selectedAddress, current, sourceDevicePGNSortRates, "message rate"),
		Traffic:  sourceDevicePGNSortControlFor(base, deviceSorting, selectedAddress, current, sourceDevicePGNSortTraffic, "traffic percentage"),
		Payload:  sourceDevicePGNSortControlFor(base, deviceSorting, selectedAddress, current, sourceDevicePGNSortPayload, "mean payload size"),
		Activity: sourceDevicePGNSortControlFor(base, deviceSorting, selectedAddress, current, sourceDevicePGNSortActivity, "last activity"),
	}
}

func sourceDevicePGNSortControlFor(
	base string,
	deviceSorting sourceDeviceSort,
	selectedAddress uint8,
	current sourceDevicePGNSort,
	key string,
	label string,
) sourceDeviceSortControl {
	nextDirection := defaultSourceDevicePGNSortDirection(key)
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
	control.Href = sourceDeviceSortURL(
		base,
		deviceSorting,
		&selectedAddress,
		sourceDevicePGNSort{key: key, direction: nextDirection},
	)
	control.Accessible = fmt.Sprintf("Sort by %s %s", label, sortDirectionWord(nextDirection))
	return control
}

func sortSourceDevicePGNRows(rows []sourceDevicePGNRow, sorting sourceDevicePGNSort) {
	sort.SliceStable(rows, func(i, j int) bool {
		comparison := 0
		switch sorting.key {
		case sourceDevicePGNSortRates:
			comparison = compareFloat(rows[i].messagesPerSec, rows[j].messagesPerSec)
		case sourceDevicePGNSortTraffic:
			comparison = compareFloat(rows[i].trafficShare, rows[j].trafficShare)
		case sourceDevicePGNSortPayload:
			comparison = compareFloat(rows[i].payloadBytesMean, rows[j].payloadBytesMean)
		case sourceDevicePGNSortActivity:
			comparison = compareTime(rows[i].lastSeen, rows[j].lastSeen)
		default:
			comparison = compareInt(int(rows[i].PGN), int(rows[j].PGN))
		}
		if comparison == 0 {
			if rows[i].PGN == rows[j].PGN {
				return rows[i].variant < rows[j].variant
			}
			return rows[i].PGN < rows[j].PGN
		}
		if sorting.direction == sourceDeviceSortDesc {
			return comparison > 0
		}
		return comparison < 0
	})
}

func compareTime(left, right time.Time) int {
	switch {
	case left.Before(right):
		return -1
	case left.After(right):
		return 1
	default:
		return 0
	}
}

func formatPanelDuration(seconds float64, learned bool) string {
	if !learned {
		return "learning"
	}
	if seconds <= 0 {
		return "0s"
	}
	return formatDuration(seconds)
}

func formatPayloadSize(last int64, mean float64) string {
	return fmt.Sprintf("%d B last · %.1f B mean", last, mean)
}

// pgnActivityGrade rates how recently a PGN was seen against its own learned
// p90 period, so a 10 Hz PGN and a once-a-minute PGN are judged on their own
// cadence. The half-second slack keeps sub-second PGNs from flapping between
// colors on the panel's 500ms refresh.
func pgnActivityGrade(ageSeconds, periodP90Seconds float64) (level, label string) {
	if periodP90Seconds <= 0 {
		return "warming", "Cadence still being learned"
	}
	switch {
	case ageSeconds <= periodP90Seconds+pgnActivitySlackSeconds:
		return "fresh", "On cadence · seen within its p90 period"
	case ageSeconds <= pgnActivityStaleFactor*periodP90Seconds+pgnActivitySlackSeconds:
		return "late", "Late · overdue past its p90 period"
	default:
		return "stale", "Stale · missed several periods"
	}
}

const (
	pgnActivitySlackSeconds = 0.5
	pgnActivityStaleFactor  = 3
)
