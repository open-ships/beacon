package stats

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (r *Registry) sourceBaselinesFor(source string) []SourceTrafficBaseline {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]SourceTrafficBaseline, 0)
	for key, baseline := range r.sourceBaselines {
		if key.source == source {
			out = append(out, baseline)
		}
	}
	return out
}

func applySourceBaselines(metrics []SourcePGNMetric, baselines []SourceTrafficBaseline, now, startedAt time.Time) []SourcePGNMetric {
	matched := make(map[string]bool)
	byKey := make(map[string]SourceTrafficBaseline, len(baselines))
	for _, baseline := range baselines {
		byKey[baseline.Identity+fmt.Sprintf(":%d", baseline.PGN)] = baseline
	}
	for i := range metrics {
		identity := sourceBaselineIdentity(metrics[i].DeviceNameHex, metrics[i].SourceAddress)
		key := identity + fmt.Sprintf(":%d", metrics[i].PGN)
		baseline, ok := byKey[key]
		if !ok && metrics[i].DeviceNameHex != "" {
			fallback := sourceBaselineIdentity("", metrics[i].SourceAddress) + fmt.Sprintf(":%d", metrics[i].PGN)
			baseline, ok = byKey[fallback]
			key = fallback
		}
		if !ok {
			metrics[i].BaselineStatus = "not_baselined"
			continue
		}
		matched[key] = true
		compareSourceBaseline(&metrics[i], baseline)
	}
	for _, baseline := range baselines {
		key := baseline.Identity + fmt.Sprintf(":%d", baseline.PGN)
		if matched[key] {
			continue
		}
		grace := 10 * time.Second
		if baseline.ExpectedFrequencyHz > 0 {
			candidate := time.Duration(3 / baseline.ExpectedFrequencyHz * float64(time.Second))
			if candidate > grace {
				grace = candidate
			}
		}
		status := "awaiting"
		gap := false
		if now.Sub(startedAt) > grace {
			status = "missing"
			gap = true
		}
		approved := baseline.ApprovedAt
		metrics = append(metrics, SourcePGNMetric{
			Observed: false, SourceID: baseline.SourceID, PGN: baseline.PGN, PGNName: baseline.PGNName,
			Variant: baseline.Variant, Transport: baseline.Transport, DecodeStatus: baseline.DecodeStatus,
			SourceAddress: baseline.SourceAddress, DeviceNameHex: baseline.DeviceNameHex,
			FrequencyHz:           baseline.ExpectedFrequencyHz,
			ExpectedPeriodSeconds: baselinePeriodSeconds(baseline.ExpectedFrequencyHz),
			Expected:              true, BaselineStatus: status,
			BaselineFrequencyHz:      baseline.ExpectedFrequencyHz,
			BaselineTolerancePercent: baseline.FrequencyTolerancePercent,
			BaselineApprovedAt:       &approved, BaselineIssues: []string{"expected stream has not been observed"},
			GapActive: gap, Status: status,
		})
	}
	return metrics
}

func compareSourceBaseline(metric *SourcePGNMetric, baseline SourceTrafficBaseline) {
	metric.Expected = true
	metric.BaselineStatus = "matching"
	metric.BaselineFrequencyHz = baseline.ExpectedFrequencyHz
	metric.BaselineTolerancePercent = baseline.FrequencyTolerancePercent
	approved := baseline.ApprovedAt
	metric.BaselineApprovedAt = &approved
	issues := make([]string, 0)
	if baseline.ExpectedFrequencyHz > 0 && metric.FrequencyHz > 0 {
		metric.FrequencyDriftPercent = (metric.FrequencyHz - baseline.ExpectedFrequencyHz) / baseline.ExpectedFrequencyHz * 100
		if mathAbs(metric.FrequencyDriftPercent) > baseline.FrequencyTolerancePercent {
			issues = append(issues, fmt.Sprintf("frequency drift %.1f%%", metric.FrequencyDriftPercent))
		}
	}
	if metric.PayloadBytesLast > 0 && !containsInt(baseline.PayloadLengths, int(metric.PayloadBytesLast)) {
		issues = append(issues, fmt.Sprintf("unexpected payload length %d B", metric.PayloadBytesLast))
	}
	if baseline.DecodeStatus != "" && metric.DecodeStatus != baseline.DecodeStatus {
		issues = append(issues, "decode status changed")
	}
	if baseline.Variant != "" && metric.Variant != baseline.Variant {
		issues = append(issues, "decode variant changed")
	}
	if baseline.Transport != "" && metric.Transport != baseline.Transport {
		issues = append(issues, "transport changed")
	}
	for destination := range metric.DestinationCounts {
		if !containsUint8String(baseline.Destinations, destination) {
			issues = append(issues, fmt.Sprintf("new destination %s", destination))
		}
	}
	for priority := range metric.PriorityCounts {
		if !containsUint8String(baseline.Priorities, priority) {
			issues = append(issues, fmt.Sprintf("new priority %s", priority))
		}
	}
	if baseline.DeviceNameHex != "" && metric.SourceAddress != baseline.SourceAddress {
		issues = append(issues, fmt.Sprintf("source address changed from %d", baseline.SourceAddress))
	}
	for _, field := range metric.Fields {
		expected, ok := baseline.Fields[field.Field]
		if !ok || field.LastNumeric == nil {
			continue
		}
		if *field.LastNumeric < expected.Minimum || *field.LastNumeric > expected.Maximum {
			issues = append(issues, fmt.Sprintf("%s outside approved range", field.Field))
		}
	}
	compareRawBaseline(metric, baseline, &issues)
	sort.Strings(issues)
	metric.BaselineIssues = issues
	if len(issues) > 0 {
		metric.BaselineStatus = "changed"
		if metric.Status != "gap" && metric.Status != "anomaly" {
			metric.Status = "changed"
		}
	}
}

func compareRawBaseline(metric *SourcePGNMetric, baseline SourceTrafficBaseline, issues *[]string) {
	if metric.Raw == nil || len(baseline.RawBytes) == 0 {
		return
	}
	expected := make(map[int]BaselineRawByte, len(baseline.RawBytes))
	for _, rawByte := range baseline.RawBytes {
		expected[rawByte.Offset] = rawByte
	}
	outside := make([]string, 0)
	newChangeBits := make([]string, 0)
	for _, observed := range metric.Raw.Bytes {
		approved, ok := expected[observed.Offset]
		if !ok || observed.Minimum < approved.Minimum || observed.Maximum > approved.Maximum {
			outside = append(outside, strconv.Itoa(observed.Offset))
		}
		observedMask, observedErr := strconv.ParseUint(observed.ChangedBitMaskHex, 16, 8)
		approvedMask, approvedErr := strconv.ParseUint(approved.ChangedBitMaskHex, 16, 8)
		if ok && observedErr == nil && approvedErr == nil && observedMask&^approvedMask != 0 {
			newChangeBits = append(newChangeBits, strconv.Itoa(observed.Offset))
		}
	}
	if len(outside) > 0 {
		*issues = append(*issues, "raw byte outside approved range at offset "+strings.Join(outside, ", "))
	}
	if len(newChangeBits) > 0 {
		*issues = append(*issues, "new raw bit changes at offset "+strings.Join(newChangeBits, ", "))
	}
}

func baselinePeriodSeconds(frequencyHz float64) float64 {
	if frequencyHz <= 0 {
		return 0
	}
	return 1 / frequencyHz
}

func containsInt(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsUint8String(values []int, want string) bool {
	for _, value := range values {
		if fmt.Sprintf("%d", value) == want {
			return true
		}
	}
	return false
}
