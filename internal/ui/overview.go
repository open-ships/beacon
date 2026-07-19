package ui

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
)

const overviewEventLimit = 24

type configRow struct {
	Label string
	Value string
	Code  bool
}

type overviewPageData struct {
	pageData
	Kind        string
	Heading     string
	EditHref    string
	ConfigRows  []configRow
	LiveDOMID   string
	LiveHref    string
	Description string
}

type overviewLogRow struct {
	Level   string
	Message string
}

type overviewEventRow struct {
	TimeText      string
	Stage         string
	ConnectorID   string
	PGN           uint32
	PGNName       string
	Source        uint8
	Dest          uint8
	Priority      uint8
	MessageTime   string
	Payload       string
	SizeBytesText string
}

type overviewLiveData struct {
	DOMID             string
	RefreshHref       string
	Kind              string
	ID                string
	State             string
	Err               string
	Logs              []overviewLogRow
	Snapshot          stats.Snapshot
	BytesPerSecText   string
	TotalBytesText    string
	QueueBytesText    string
	RetainedBytesText string
	Events            []overviewEventRow
	SourcePGNs        []sourcePGNRow
	SourceEvents      []sourceMetricEventRow
	BaselineCount     int
	EmptyStream       string
}

type sourcePGNRow struct {
	Observed        bool
	Status          string
	PGN             uint32
	PGNName         string
	SourceAddress   uint8
	DeviceNameHex   string
	FrequencyText   string
	PeriodText      string
	LastSeenText    string
	LastSeenTitle   string
	GapText         string
	PayloadText     string
	PayloadDetail   string
	Messages        int64
	AnomalyText     string
	Fields          []sourceFieldRow
	DecodeStatus    string
	DecodeDetail    string
	DecodeMetadata  string
	Destinations    string
	Priorities      string
	RateDetail      string
	TrafficText     string
	TrafficDetail   string
	BaselineStatus  string
	BaselineIssues  []string
	Raw             *stats.RawPayloadDiagnostics
	RawFingerprints []sourceRawFingerprintRow
	RawBytes        []sourceRawByteRow
	RawSamples      []sourceRawSampleRow
}

type sourceFieldRow struct {
	Name             string
	Summary          string
	Samples          int64
	Anomalous        bool
	Anomalies        int64
	AvailabilityText string
	QualityText      string
}

type sourceRawByteRow struct {
	Offset  int
	Range   string
	Mode    string
	Entropy string
	Changed string
	BitMask string
}

type sourceRawFingerprintRow struct {
	Fingerprint string
	Count       int64
	Share       string
	Length      int
}

type sourceRawSampleRow struct {
	Time        string
	Hex         string
	Fingerprint string
	Length      int
}

type sourceMetricEventRow struct {
	Time          string
	Kind          string
	Severity      string
	Summary       string
	PGN           uint32
	SourceAddress uint8
}

func sourceOverviewPageData(version string, s model.Source) overviewPageData {
	title := displayName(s.Name, s.ID)
	return overviewPageData{
		pageData: newPageData(title, version, "sources").withBreadcrumbs(
			breadcrumbItem{Label: "Sources", Href: "/ui/sources"},
			breadcrumbItem{Label: title},
		),
		Kind:        "source",
		Heading:     title,
		EditHref:    "/ui/sources/" + s.ID + "/edit",
		ConfigRows:  sourceConfigRows(s),
		LiveDOMID:   "source-overview-live",
		LiveHref:    "/ui/frag/sources/" + s.ID + "/overview",
		Description: "Received message activity and runtime health for this source.",
	}
}

func sinkOverviewPageData(version string, s model.Sink) overviewPageData {
	title := displayName(s.Name, s.ID)
	return overviewPageData{
		pageData: newPageData(title, version, "sinks").withBreadcrumbs(
			breadcrumbItem{Label: "Sinks", Href: "/ui/sinks"},
			breadcrumbItem{Label: title},
		),
		Kind:        "sink",
		Heading:     title,
		EditHref:    "/ui/sinks/" + s.ID + "/edit",
		ConfigRows:  sinkConfigRows(s),
		LiveDOMID:   "sink-overview-live",
		LiveHref:    "/ui/frag/sinks/" + s.ID + "/overview",
		Description: "Sent message activity and runtime health for this sink.",
	}
}

func connectorOverviewPageData(version string, c model.Connector) overviewPageData {
	title := displayName(c.Name, c.ID)
	return overviewPageData{
		pageData: newPageData(title, version, "connectors").withBreadcrumbs(
			breadcrumbItem{Label: "Connectors", Href: "/ui/connectors"},
			breadcrumbItem{Label: title},
		),
		Kind:        "connector",
		Heading:     title,
		EditHref:    "/ui/connectors/" + c.ID + "/edit",
		ConfigRows:  connectorConfigRows(c),
		LiveDOMID:   "connector-overview-live",
		LiveHref:    "/ui/frag/connectors/" + c.ID + "/overview",
		Description: "Matched and delivered message activity for this connector.",
	}
}

func sourceConfigRows(s model.Source) []configRow {
	rows := []configRow{
		{"ID", s.ID, true},
		{"Name", displayName(s.Name, s.ID), false},
		{"Type", string(s.Type), true},
		{"Enabled", boolText(s.Enabled), false},
	}
	add := func(label, value string, code bool) {
		if strings.TrimSpace(value) != "" {
			rows = append(rows, configRow{label, value, code})
		}
	}
	add("Interface", s.Interface, true)
	add("Port", s.Port, true)
	add("URL", s.URL, true)
	add("Topic", s.Topic, true)
	add("File path", s.FilePath, true)
	add("Address", s.Address, true)
	add("Format", s.Format, true)
	if len(s.Headers) > 0 {
		rows = append(rows, configRow{"Headers", strconv.Itoa(len(s.Headers)), false})
	}
	return rows
}

func sinkConfigRows(s model.Sink) []configRow {
	rows := []configRow{
		{"ID", s.ID, true},
		{"Name", displayName(s.Name, s.ID), false},
		{"Type", string(s.Type), true},
		{"Enabled", boolText(s.Enabled), false},
	}
	add := func(label, value string, code bool) {
		if strings.TrimSpace(value) != "" {
			rows = append(rows, configRow{label, value, code})
		}
	}
	add("Interface", s.Interface, true)
	add("Port", s.Port, true)
	add("Path", s.Path, true)
	add("Address", s.Address, true)
	add("URL", s.URL, true)
	add("Topic", s.Topic, true)
	add("File path", s.FilePath, true)
	add("Format", s.Format, true)
	if s.MaxFileBytes != 0 {
		rows = append(rows, configRow{"Max file bytes", strconv.FormatInt(s.MaxFileBytes, 10), false})
	}
	if s.MaxFiles != 0 {
		rows = append(rows, configRow{"Max files", strconv.Itoa(s.MaxFiles), false})
	}
	return rows
}

func connectorConfigRows(c model.Connector) []configRow {
	rows := []configRow{
		{"ID", c.ID, true},
		{"Name", displayName(c.Name, c.ID), false},
		{"Enabled", boolText(c.Enabled), false},
		{"Source", c.SourceID, true},
		{"Sink", c.SinkID, true},
		{"Bridge mode", string(c.EffectiveMode()), true},
		{"Forward management", boolText(c.ForwardManagement), false},
		{"Filters", strconv.Itoa(len(c.Filters)), false},
	}
	if len(c.Filters) > 0 {
		rows = append(rows, configRow{"Filter expression", strings.Join(c.Filters, " && "), true})
	}
	if c.Buffer.MaxMessages != 0 {
		rows = append(rows, configRow{"Max messages", strconv.FormatInt(c.Buffer.MaxMessages, 10), false})
	}
	if c.Buffer.MaxAge != 0 {
		rows = append(rows, configRow{"Max age", time.Duration(c.Buffer.MaxAge).String(), false})
	}
	if c.Buffer.MaxBytes != 0 {
		rows = append(rows, configRow{"Max bytes", strconv.FormatInt(c.Buffer.MaxBytes, 10), false})
	}
	return rows
}

func boolText(v bool) string {
	if v {
		return "enabled"
	}
	return "disabled"
}

func overviewStatus(statuses []supervisor.Status, kind, id string, enabled bool) (state, errText string) {
	for _, s := range statuses {
		if s.Kind == kind && s.ID == id {
			return s.State, s.Err
		}
	}
	if !enabled {
		return "disabled", ""
	}
	return "restarting", ""
}

func overviewLogs(kind, state, errText string) []overviewLogRow {
	if errText != "" {
		return []overviewLogRow{{Level: "error", Message: errText}}
	}
	switch state {
	case "up":
		return nil
	case "disabled":
		return []overviewLogRow{{Level: "info", Message: kind + " is disabled"}}
	case "restarting":
		return []overviewLogRow{{Level: "warn", Message: kind + " is enabled but not currently running"}}
	default:
		return []overviewLogRow{{Level: "warn", Message: kind + " state is " + state}}
	}
}

func overviewEvents(events []stats.Event) []overviewEventRow {
	rows := make([]overviewEventRow, 0, len(events))
	for _, e := range events {
		row := overviewEventRow{
			TimeText:      formatEventTime(e.Time),
			Stage:         e.Stage,
			ConnectorID:   e.ConnectorID,
			PGN:           e.PGN,
			PGNName:       e.PGNName,
			Source:        e.Source,
			Dest:          e.Dest,
			Priority:      e.Priority,
			MessageTime:   formatEventTime(e.Timestamp),
			Payload:       e.Payload,
			SizeBytesText: humanizeBytes(float64(e.SizeBytes), ""),
		}
		rows = append(rows, row)
	}
	return rows
}

func formatEventTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("15:04:05")
}

func sourceOverviewLiveData(s model.Source, reg *stats.Registry, statuses []supervisor.Status) overviewLiveData {
	snap, _ := reg.SourceSnapshot(s.ID)
	state, errText := overviewStatus(statuses, "source", s.ID, s.Enabled)
	return overviewLiveData{
		DOMID:             "source-overview-live",
		RefreshHref:       "/ui/frag/sources/" + s.ID + "/overview",
		Kind:              "source",
		ID:                s.ID,
		State:             state,
		Err:               errText,
		Logs:              overviewLogs("source", state, errText),
		Snapshot:          snap,
		BytesPerSecText:   humanizeBytes(snap.BytesPerSec, "/s"),
		TotalBytesText:    humanizeBytes(float64(snap.TotalBytes), ""),
		QueueBytesText:    humanizeBytes(float64(snap.QueueBytes), ""),
		RetainedBytesText: humanizeBytes(float64(snap.RetainedBytes), ""),
		Events:            overviewEvents(reg.Recent("source", s.ID, overviewEventLimit)),
		SourcePGNs:        sourcePGNRows(reg.SourcePGNMetrics(s.ID)),
		SourceEvents:      sourceMetricEventRows(reg.SourceMetricEvents(s.ID, 30)),
		BaselineCount:     len(reg.SourceTrafficBaselines(s.ID)),
		EmptyStream:       "No decoded messages have been received from this source in this process.",
	}
}

func sourcePGNRows(metrics []stats.SourcePGNMetric) []sourcePGNRow {
	rows := make([]sourcePGNRow, 0, len(metrics))
	for _, metric := range metrics {
		row := sourcePGNRow{
			Observed: metric.Observed, Status: metric.Status, PGN: metric.PGN, PGNName: metric.PGNName,
			SourceAddress: metric.SourceAddress, DeviceNameHex: metric.DeviceNameHex,
			FrequencyText: formatFrequency(metric.FrequencyHz),
			PeriodText:    formatMetricDuration(metric.ExpectedPeriodSeconds),
			LastSeenText:  formatAge(metric.AgeSeconds),
			LastSeenTitle: metric.LastSeen.UTC().Format(time.RFC3339Nano),
			GapText:       sourceGapText(metric),
			PayloadText:   strconv.FormatInt(metric.PayloadBytesLast, 10) + " B",
			PayloadDetail: fmt.Sprintf("mean %s B · range %d–%d B",
				formatMetricFloat(metric.PayloadBytesMean), metric.PayloadBytesMin, metric.PayloadBytesMax),
			Messages:       metric.Messages,
			DecodeStatus:   metric.DecodeStatus,
			DecodeDetail:   sourceDecodeDetail(metric),
			DecodeMetadata: sourceDecodeMetadata(metric),
			Destinations:   sortedMetricKeys(metric.DestinationCounts),
			Priorities:     sortedMetricKeys(metric.PriorityCounts),
			RateDetail: fmt.Sprintf("p95 %s · p99 %s · jitter %s (%.1f%%) · %d bursts",
				formatDuration(metric.PeriodP95Seconds), formatDuration(metric.PeriodP99Seconds),
				formatDuration(metric.JitterMADSeconds), metric.JitterPercent, metric.BurstCount),
			TrafficText: fmt.Sprintf("%s/s · %.1f%% of source", humanizeBytes(metric.RecentBytesPerSec, ""), metric.TrafficSharePercent),
			TrafficDetail: fmt.Sprintf("%.2f msg/s · ~%.3f%% bus load · %d messages",
				metric.RecentMessagesPerSec, metric.EstimatedBusLoadPercent, metric.Messages),
			BaselineStatus: metric.BaselineStatus, BaselineIssues: metric.BaselineIssues,
			Raw: metric.Raw,
		}
		if !metric.Observed {
			row.LastSeenText = "not observed"
			row.LastSeenTitle = ""
			row.GapText = "expected by approved baseline"
			row.PayloadText = "-"
			row.PayloadDetail = "awaiting traffic"
			row.TrafficText = "-"
			row.TrafficDetail = "no observations in this process"
			row.DecodeDetail = "approved expectation"
		}
		if metric.RecentAnomaly {
			row.AnomalyText = metric.AnomalyField
			if metric.AnomalyReason != "" {
				row.AnomalyText += ": " + metric.AnomalyReason
			}
		}
		for _, field := range metric.Fields {
			row.Fields = append(row.Fields, sourceFieldDistributionRow(field))
		}
		if metric.Raw != nil {
			for _, fingerprint := range metric.Raw.Fingerprints {
				row.RawFingerprints = append(row.RawFingerprints, sourceRawFingerprintRow{
					Fingerprint: fingerprint.Fingerprint, Count: fingerprint.Count,
					Share: fmt.Sprintf("%.1f%%", fingerprint.Share*100), Length: fingerprint.Length,
				})
			}
			for _, rawByte := range metric.Raw.Bytes {
				row.RawBytes = append(row.RawBytes, sourceRawByteRow{
					Offset:  rawByte.Offset,
					Range:   fmt.Sprintf("%02x–%02x", rawByte.Minimum, rawByte.Maximum),
					Mode:    fmt.Sprintf("%02x (%.0f%%)", rawByte.MostCommon, rawByte.MostCommonShare*100),
					Entropy: fmt.Sprintf("%.2f bits", rawByte.EntropyBits),
					Changed: fmt.Sprintf("%.0f%%", rawByte.ChangedShare*100), BitMask: rawByte.ChangedBitMaskHex,
				})
			}
			for _, sample := range metric.Raw.Samples {
				row.RawSamples = append(row.RawSamples, sourceRawSampleRow{
					Time: sample.ObservedAt.Format("15:04:05"), Hex: sample.Hex,
					Fingerprint: sample.Fingerprint, Length: sample.Length,
				})
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func sourceDecodeDetail(metric stats.SourcePGNMetric) string {
	parts := []string{fmt.Sprintf("%d complete", metric.DecodeComplete)}
	if metric.DecodeIncomplete > 0 {
		parts = append(parts, fmt.Sprintf("%d incomplete", metric.DecodeIncomplete))
	}
	if metric.DecodeFallback > 0 {
		parts = append(parts, fmt.Sprintf("%d fallback", metric.DecodeFallback))
	}
	if metric.UnknownMessages > 0 {
		parts = append(parts, fmt.Sprintf("%d unknown", metric.UnknownMessages))
	}
	if len(metric.MissingDecodedFields) > 0 {
		parts = append(parts, "missing "+sortedMetricKeys(metric.MissingDecodedFields))
	}
	return strings.Join(parts, " · ")
}

func sourceDecodeMetadata(metric stats.SourcePGNMetric) string {
	parts := make([]string, 0, 3)
	if metric.Variant != "" {
		parts = append(parts, metric.Variant)
	}
	if metric.Transport != "" {
		parts = append(parts, metric.Transport)
	}
	if metric.ManufacturerCode != nil {
		parts = append(parts, fmt.Sprintf("manufacturer %d", *metric.ManufacturerCode))
	}
	return strings.Join(parts, " · ")
}

func sortedMetricKeys(values map[string]int64) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left, leftErr := strconv.Atoi(keys[i])
		right, rightErr := strconv.Atoi(keys[j])
		if leftErr == nil && rightErr == nil && left != right {
			return left < right
		}
		return keys[i] < keys[j]
	})
	if len(keys) == 0 {
		return "-"
	}
	return strings.Join(keys, ", ")
}

func formatFrequency(hz float64) string {
	if hz <= 0 {
		return "learning"
	}
	if hz >= 100 {
		return fmt.Sprintf("%.0f Hz", hz)
	}
	if hz >= 10 {
		return fmt.Sprintf("%.1f Hz", hz)
	}
	if hz >= 1 {
		return fmt.Sprintf("%.2f Hz", hz)
	}
	return fmt.Sprintf("%.3f Hz", hz)
}

func formatMetricDuration(seconds float64) string {
	if seconds <= 0 {
		return "period learning"
	}
	return formatDuration(seconds) + " period"
}

func formatDuration(seconds float64) string {
	if seconds <= 0 {
		return "learning"
	}
	d := time.Duration(seconds * float64(time.Second))
	if d < time.Millisecond {
		return d.Round(time.Microsecond).String()
	}
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(100 * time.Millisecond).String()
}

func formatAge(seconds float64) string {
	if seconds < 0 {
		seconds = 0
	}
	d := time.Duration(seconds * float64(time.Second))
	if d < time.Second {
		return "now"
	}
	if d < time.Minute {
		return d.Round(time.Second).String() + " ago"
	}
	if d < time.Hour {
		return d.Round(time.Minute).String() + " ago"
	}
	return d.Round(time.Hour).String() + " ago"
}

func sourceGapText(metric stats.SourcePGNMetric) string {
	if metric.GapActive {
		return fmt.Sprintf("%.1f× expected now", metric.GapRatio)
	}
	if metric.GapCount == 0 {
		return "none detected"
	}
	return fmt.Sprintf("%d prior · max %s", metric.GapCount,
		formatDuration(metric.LongestGapSeconds))
}

func sourceFieldDistributionRow(field stats.FieldDistribution) sourceFieldRow {
	row := sourceFieldRow{Name: field.Field, Samples: field.Samples,
		Anomalous: field.Anomalous, Anomalies: field.AnomalyCount,
		AvailabilityText: fmt.Sprintf("%.1f%% available · %s unchanged", field.AvailabilityPercent, formatDuration(field.StuckSeconds)),
		QualityText: fmt.Sprintf("%d missing · %d invalid · %d out of range · %d novel values",
			field.MissingMessages, field.InvalidCount, field.OutOfRangeCount, field.NovelValueCount)}
	if field.Kind == "number" && field.LastNumeric != nil && field.Mean != nil &&
		field.Minimum != nil && field.Maximum != nil && field.StdDev != nil {
		unit := ""
		if field.Unit != "" {
			unit = " " + field.Unit
		}
		row.Summary = fmt.Sprintf("last %s%s · mean %s ± %s%s · range %s–%s%s",
			formatMetricFloat(*field.LastNumeric), unit,
			formatMetricFloat(*field.Mean), formatMetricFloat(*field.StdDev), unit,
			formatMetricFloat(*field.Minimum), formatMetricFloat(*field.Maximum), unit)
		if field.LastChange != nil {
			row.Summary += " · Δ " + formatMetricFloat(*field.LastChange) + unit
		}
		if field.P05 != nil && field.P50 != nil && field.P95 != nil && field.P99 != nil {
			row.Summary += fmt.Sprintf(" · p05/p50/p95/p99 %s/%s/%s/%s%s",
				formatMetricFloat(*field.P05), formatMetricFloat(*field.P50),
				formatMetricFloat(*field.P95), formatMetricFloat(*field.P99), unit)
		}
		if field.LastRateOfChange != nil {
			row.Summary += " · rate " + formatMetricFloat(*field.LastRateOfChange) + unit + "/s"
		}
		return row
	}
	type categoryCount struct {
		value string
		count int64
	}
	counts := make([]categoryCount, 0, len(field.Values))
	for value, count := range field.Values {
		counts = append(counts, categoryCount{value: value, count: count})
	}
	sort.Slice(counts, func(i, j int) bool {
		if counts[i].count != counts[j].count {
			return counts[i].count > counts[j].count
		}
		return counts[i].value < counts[j].value
	})
	parts := make([]string, 0, len(counts)+1)
	for _, count := range counts {
		parts = append(parts, fmt.Sprintf("%s × %d", count.value, count.count))
	}
	if field.Other > 0 {
		parts = append(parts, fmt.Sprintf("other × %d", field.Other))
	}
	row.Summary = strings.Join(parts, " · ")
	return row
}

func sourceMetricEventRows(events []stats.SourceMetricEvent) []sourceMetricEventRow {
	rows := make([]sourceMetricEventRow, 0, len(events))
	for _, event := range events {
		rows = append(rows, sourceMetricEventRow{Time: event.Time.Format("2006-01-02 15:04:05"),
			Kind: event.Kind, Severity: event.Severity, Summary: event.Summary,
			PGN: event.PGN, SourceAddress: event.SourceAddress})
	}
	return rows
}

func formatMetricFloat(value float64) string {
	if math.IsInf(value, 0) {
		return "∞"
	}
	return strconv.FormatFloat(value, 'g', 4, 64)
}

func sinkOverviewLiveData(s model.Sink, reg *stats.Registry, statuses []supervisor.Status) overviewLiveData {
	snap, _ := reg.SinkSnapshot(s.ID)
	state, errText := overviewStatus(statuses, "sink", s.ID, s.Enabled)
	return overviewLiveData{
		DOMID:             "sink-overview-live",
		RefreshHref:       "/ui/frag/sinks/" + s.ID + "/overview",
		Kind:              "sink",
		ID:                s.ID,
		State:             state,
		Err:               errText,
		Logs:              overviewLogs("sink", state, errText),
		Snapshot:          snap,
		BytesPerSecText:   humanizeBytes(snap.BytesPerSec, "/s"),
		TotalBytesText:    humanizeBytes(float64(snap.TotalBytes), ""),
		QueueBytesText:    humanizeBytes(float64(snap.QueueBytes), ""),
		RetainedBytesText: humanizeBytes(float64(snap.RetainedBytes), ""),
		Events:            overviewEvents(reg.Recent("sink", s.ID, overviewEventLimit)),
		EmptyStream:       "No messages have been sent to this sink in this process.",
	}
}

func connectorOverviewLiveData(c model.Connector, reg *stats.Registry, statuses []supervisor.Status) overviewLiveData {
	snap, _ := reg.Snapshot(c.ID)
	state, errText := overviewStatus(statuses, "connector", c.ID, c.Enabled)
	return overviewLiveData{
		DOMID:             "connector-overview-live",
		RefreshHref:       "/ui/frag/connectors/" + c.ID + "/overview",
		Kind:              "connector",
		ID:                c.ID,
		State:             state,
		Err:               errText,
		Logs:              overviewLogs("connector", state, errText),
		Snapshot:          snap,
		BytesPerSecText:   humanizeBytes(snap.BytesPerSec, "/s"),
		TotalBytesText:    humanizeBytes(float64(snap.TotalBytes), ""),
		QueueBytesText:    humanizeBytes(float64(snap.QueueBytes), ""),
		RetainedBytesText: humanizeBytes(float64(snap.RetainedBytes), ""),
		Events:            overviewEvents(reg.Recent("connector", c.ID, overviewEventLimit)),
		EmptyStream:       "No messages have moved through this connector in this process.",
	}
}

func handleSourceOverviewPage(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		source, err := svc.GetSource(r.Context(), r.PathValue("id"))
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get source failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		_ = reg
		_ = statuses
		renderPage(w, log, "overview", sourceOverviewPageData(version, source))
	}
}

func handleSinkOverviewPage(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sink, err := svc.GetSink(r.Context(), r.PathValue("id"))
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get sink failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		_ = reg
		_ = statuses
		renderPage(w, log, "overview", sinkOverviewPageData(version, sink))
	}
}

func handleConnectorOverviewPage(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		connector, err := svc.GetConnector(r.Context(), r.PathValue("id"))
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: get connector failed", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		_ = reg
		_ = statuses
		renderPage(w, log, "overview", connectorOverviewPageData(version, connector))
	}
}

func handleSourceOverviewFrag(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		source, err := svc.GetSource(r.Context(), r.PathValue("id"))
		if err != nil {
			renderOverviewFragErr(w, log, err)
			return
		}
		renderFragment(w, log, "overview-live", sourceOverviewLiveData(source, reg, statuses()))
	}
}

func handleSourceTrafficBaselineCommit(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		source, err := svc.GetSource(r.Context(), r.PathValue("id"))
		if err != nil {
			renderOverviewFragErr(w, log, err)
			return
		}
		if _, err := reg.CommitSourceTrafficBaseline(r.Context(), source.ID); err != nil {
			log.Error("ui: commit source traffic baseline", "source", source.ID, "err", err)
			http.Error(w, "traffic baseline failed", http.StatusInternalServerError)
			return
		}
		renderFragment(w, log, "overview-live", sourceOverviewLiveData(source, reg, statuses()))
	}
}

func handleSourceTrafficBaselineClear(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		source, err := svc.GetSource(r.Context(), r.PathValue("id"))
		if err != nil {
			renderOverviewFragErr(w, log, err)
			return
		}
		if err := reg.ClearSourceTrafficBaseline(r.Context(), source.ID); err != nil {
			log.Error("ui: clear source traffic baseline", "source", source.ID, "err", err)
			http.Error(w, "traffic baseline clear failed", http.StatusInternalServerError)
			return
		}
		renderFragment(w, log, "overview-live", sourceOverviewLiveData(source, reg, statuses()))
	}
}

func handleSinkOverviewFrag(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sink, err := svc.GetSink(r.Context(), r.PathValue("id"))
		if err != nil {
			renderOverviewFragErr(w, log, err)
			return
		}
		renderFragment(w, log, "overview-live", sinkOverviewLiveData(sink, reg, statuses()))
	}
}

func handleConnectorOverviewFrag(svc *config.Service, reg *stats.Registry, statuses func() []supervisor.Status, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		connector, err := svc.GetConnector(r.Context(), r.PathValue("id"))
		if err != nil {
			renderOverviewFragErr(w, log, err)
			return
		}
		renderFragment(w, log, "overview-live", connectorOverviewLiveData(connector, reg, statuses()))
	}
}

func renderOverviewFragErr(w http.ResponseWriter, log *slog.Logger, err error) {
	if errors.Is(err, config.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	log.Error("ui: overview fragment failed", "err", err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

type configPageData struct {
	pageData
	ConfigJSON string
	ConfigRows int
	Mode       string
	Alert      *alertData
}

func handleConfigPage(svc *config.Service, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		renderConfigPage(w, r, svc, version, log, nil)
	}
}

func handleConfigImport(svc *config.Service, version string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			renderConfigPage(w, r, svc, version, log, &alertData{Kind: "error", Message: "invalid form submission: " + err.Error()})
			return
		}
		var cfg model.Config
		raw := r.PostFormValue("config_json")
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			renderConfigPageWithJSON(w, r, svc, version, log, raw, r.PostFormValue("mode"), &alertData{Kind: "error", Message: "invalid JSON: " + err.Error()})
			return
		}
		replace := r.PostFormValue("mode") != "merge"
		if err := svc.Import(r.Context(), cfg, replace); err != nil {
			renderConfigPageWithJSON(w, r, svc, version, log, raw, r.PostFormValue("mode"), &alertData{Kind: "error", Message: entityWriteErrorMessage(err, "configuration", "json")})
			return
		}
		renderConfigPage(w, r, svc, version, log, &alertData{Kind: "success", Message: "configuration loaded"})
	}
}

func renderConfigPage(w http.ResponseWriter, r *http.Request, svc *config.Service, version string, log *slog.Logger, alert *alertData) {
	cfg, err := svc.Export(r.Context())
	if err != nil {
		log.Error("ui: export config failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Error("ui: marshal config failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	renderConfigPageWithJSON(w, r, svc, version, log, string(b), "replace", alert)
}

// configTextareaRows sizes the config editor to its content so the whole
// document renders on load without an inner scrollbar. A floor keeps small
// configs looking like a proper editor; there is deliberately no ceiling —
// the entire configuration should be visible, however large.
func configTextareaRows(raw string) int {
	const minRows = 20
	rows := strings.Count(raw, "\n") + 1
	if rows < minRows {
		return minRows
	}
	return rows
}

func renderConfigPageWithJSON(w http.ResponseWriter, _ *http.Request, _ *config.Service, version string, log *slog.Logger, raw, mode string, alert *alertData) {
	if mode == "" {
		mode = "replace"
	}
	data := configPageData{
		pageData: newPageData("Configuration JSON", version, "config").withBreadcrumbs(
			breadcrumbItem{Label: "Configuration JSON"},
		),
		ConfigJSON: raw,
		ConfigRows: configTextareaRows(raw),
		Mode:       mode,
		Alert:      alert,
	}
	renderPage(w, log, "config", data)
}

func redirectToTrailingSlash(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently) // #nosec G710 -- the target is the same-origin request path with a slash appended.
}
