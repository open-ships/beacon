package stats

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/msg"
)

const (
	sourceIntervalSamples = 64
	maxSourceFields       = 128
	maxCategoryValues     = 16
)

// FieldDistribution is the process-local distribution of one decoded field
// on a source/PGN/sender stream. Numeric fields expose descriptive statistics;
// category fields expose bounded value counts (overflow is counted in Other).
type FieldDistribution struct {
	Field               string           `json:"field"`
	Kind                string           `json:"kind"`
	Unit                string           `json:"unit,omitempty"`
	Samples             int64            `json:"samples"`
	Last                string           `json:"last"`
	Minimum             *float64         `json:"minimum,omitempty"`
	Maximum             *float64         `json:"maximum,omitempty"`
	Mean                *float64         `json:"mean,omitempty"`
	StdDev              *float64         `json:"stddev,omitempty"`
	LastNumeric         *float64         `json:"last_numeric,omitempty"`
	LastChange          *float64         `json:"last_change,omitempty"`
	P05                 *float64         `json:"p05,omitempty"`
	P50                 *float64         `json:"p50,omitempty"`
	P95                 *float64         `json:"p95,omitempty"`
	P99                 *float64         `json:"p99,omitempty"`
	LastRateOfChange    *float64         `json:"last_rate_of_change,omitempty"`
	StuckSeconds        float64          `json:"stuck_seconds,omitempty"`
	PresentMessages     int64            `json:"present_messages"`
	MissingMessages     int64            `json:"missing_messages"`
	AvailabilityPercent float64          `json:"availability_percent"`
	InvalidCount        int64            `json:"invalid_count,omitempty"`
	NovelValueCount     int64            `json:"novel_value_count,omitempty"`
	CatalogMinimum      *float64         `json:"catalog_minimum,omitempty"`
	CatalogMaximum      *float64         `json:"catalog_maximum,omitempty"`
	Values              map[string]int64 `json:"values,omitempty"`
	Other               int64            `json:"other,omitempty"`
}

// SourcePGNMetric describes one distinct stream on a configured source. The
// CAN source address is part of the identity so two devices sending the same
// PGN remain independently observable when one stops transmitting.
type SourcePGNMetric struct {
	Observed                bool                   `json:"observed"`
	SourceID                string                 `json:"source_id"`
	PGN                     uint32                 `json:"pgn"`
	PGNName                 string                 `json:"pgn_name,omitempty"`
	Variant                 string                 `json:"variant,omitempty"`
	Transport               string                 `json:"transport,omitempty"`
	ManufacturerCode        *uint16                `json:"manufacturer_code,omitempty"`
	DecodeStatus            string                 `json:"decode_status"`
	DecodeStatuses          map[string]int64       `json:"decode_statuses"`
	DecodeComplete          int64                  `json:"decode_complete"`
	DecodeIncomplete        int64                  `json:"decode_incomplete"`
	DecodeFallback          int64                  `json:"decode_fallback"`
	UnknownMessages         int64                  `json:"unknown_messages"`
	MissingDecodedFields    map[string]int64       `json:"missing_decoded_fields,omitempty"`
	SourceAddress           uint8                  `json:"source_address"`
	DeviceName              *uint64                `json:"device_name,omitempty"`
	DeviceNameHex           string                 `json:"device_name_hex,omitempty"`
	Messages                int64                  `json:"messages"`
	FirstSeen               time.Time              `json:"first_seen"`
	LastSeen                time.Time              `json:"last_seen"`
	AgeSeconds              float64                `json:"age_seconds"`
	FrequencyHz             float64                `json:"frequency_hz"`
	ExpectedPeriodSeconds   float64                `json:"expected_period_seconds"`
	ShortestPeriodSeconds   float64                `json:"shortest_period_seconds,omitempty"`
	LongestPeriodSeconds    float64                `json:"longest_period_seconds,omitempty"`
	PeriodP90Seconds        float64                `json:"period_p90_seconds,omitempty"`
	PeriodP95Seconds        float64                `json:"period_p95_seconds,omitempty"`
	PeriodP99Seconds        float64                `json:"period_p99_seconds,omitempty"`
	JitterMADSeconds        float64                `json:"jitter_mad_seconds,omitempty"`
	JitterPercent           float64                `json:"jitter_percent,omitempty"`
	BurstCount              int64                  `json:"burst_count"`
	RecentMessagesPerSec    float64                `json:"recent_messages_per_sec"`
	RecentBytesPerSec       float64                `json:"recent_bytes_per_sec"`
	EstimatedBusLoadPercent float64                `json:"estimated_bus_load_percent"`
	TrafficSharePercent     float64                `json:"traffic_share_percent"`
	PayloadBytesLast        int64                  `json:"payload_bytes_last"`
	PayloadBytesMin         int64                  `json:"payload_bytes_min"`
	PayloadBytesMax         int64                  `json:"payload_bytes_max"`
	PayloadBytesMean        float64                `json:"payload_bytes_mean"`
	GapActive               bool                   `json:"gap_active"`
	GapRatio                float64                `json:"gap_ratio,omitempty"`
	GapCount                int64                  `json:"gap_count"`
	LastGapAt               *time.Time             `json:"last_gap_at,omitempty"`
	LongestGapSeconds       float64                `json:"longest_gap_seconds,omitempty"`
	Status                  string                 `json:"status"`
	DestinationCounts       map[string]int64       `json:"destination_counts"`
	PriorityCounts          map[string]int64       `json:"priority_counts"`
	IdentityChanges         int64                  `json:"identity_changes"`
	Raw                     *RawPayloadDiagnostics `json:"raw,omitempty"`
	Fields                  []FieldDistribution    `json:"fields,omitempty"`
}

type sourceStreamKey struct {
	source  string
	pgn     uint32
	address uint8
}

type sourceAddressKey struct {
	source  string
	address uint8
}

type sourceDeviceKey struct {
	source string
	name   uint64
}

type sourceEventState struct {
	messages        int64
	gapCount        int64
	identityChanges int64
	decodeStatus    string
	payloadLengths  int
	novelValues     int64
}

type runningDistribution struct {
	count int64
	last  float64
	min   float64
	max   float64
	mean  float64
	m2    float64
}

func (d *runningDistribution) add(value float64) {
	d.last = value
	if d.count == 0 {
		d.count = 1
		d.min = value
		d.max = value
		d.mean = value
		return
	}
	d.count++
	if value < d.min {
		d.min = value
	}
	if value > d.max {
		d.max = value
	}
	delta := value - d.mean
	d.mean += delta / float64(d.count)
	d.m2 += delta * (value - d.mean)
}

func (d *runningDistribution) stddev() float64 {
	if d.count < 2 {
		return 0
	}
	return math.Sqrt(d.m2 / float64(d.count-1))
}

type sourceField struct {
	kind             string
	unit             string
	numeric          runningDistribution
	changes          runningDistribution
	values           map[string]int64
	lastCategory     string
	other            int64
	lowerBound       *float64
	upperBound       *float64
	numericSamples   [sourceIntervalSamples]float64
	numericHead      int
	numericLen       int
	presentMessages  int64
	missingMessages  int64
	invalidCount     int64
	novelValueCount  int64
	lastSeen         time.Time
	lastChanged      time.Time
	lastRateOfChange float64
}

type sourceStream struct {
	mu sync.Mutex

	key                  sourceStreamKey
	pgnName              string
	variant              string
	transport            string
	manufacturerCode     *uint16
	deviceName           *uint64
	identityChanges      int64
	messages             int64
	firstSeen            time.Time
	lastSeen             time.Time
	intervals            [sourceIntervalSamples]time.Duration
	intHead              int
	intLen               int
	payload              runningDistribution
	fields               map[string]*sourceField
	destinations         map[uint8]int64
	priorities           map[uint8]int64
	decodeStatuses       map[string]int64
	lastDecodeStatus     string
	decodeComplete       int64
	decodeIncomplete     int64
	decodeFallback       int64
	unknownMessages      int64
	missingDecodedFields map[string]int64
	burstCount           int64
	wire                 sourceWireStats
	rate                 sourceRateStats

	gapCount   int64
	lastGap    time.Time
	longestGap time.Duration
}

func (s *sourceStream) addInterval(value time.Duration) {
	s.intervals[s.intHead] = value
	s.intHead = (s.intHead + 1) % sourceIntervalSamples
	if s.intLen < sourceIntervalSamples {
		s.intLen++
	}
}

func (s *sourceStream) sourceEventState() sourceEventState {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := sourceEventState{
		messages: s.messages, gapCount: s.gapCount,
		identityChanges: s.identityChanges, decodeStatus: s.lastDecodeStatus,
		payloadLengths: len(s.wire.lengths),
	}
	for _, field := range s.fields {
		state.novelValues += field.novelValueCount
	}
	return state
}

func (s *sourceStream) intervalValues() []time.Duration {
	out := make([]time.Duration, s.intLen)
	start := (s.intHead - s.intLen + sourceIntervalSamples) % sourceIntervalSamples
	for i := range out {
		out[i] = s.intervals[(start+i)%sourceIntervalSamples]
	}
	return out
}

func medianInterval(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]time.Duration(nil), values...)
	sort.Slice(copyValues, func(i, j int) bool { return copyValues[i] < copyValues[j] })
	mid := len(copyValues) / 2
	if len(copyValues)%2 == 1 {
		return copyValues[mid]
	}
	return copyValues[mid-1]/2 + copyValues[mid]/2
}

func durationPercentile(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]time.Duration(nil), values...)
	sort.Slice(copyValues, func(i, j int) bool { return copyValues[i] < copyValues[j] })
	index := int(math.Ceil(percentile*float64(len(copyValues)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(copyValues) {
		index = len(copyValues) - 1
	}
	return copyValues[index]
}

func intervalMAD(values []time.Duration, median time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	deviations := make([]time.Duration, 0, len(values))
	for _, value := range values {
		delta := value - median
		if delta < 0 {
			delta = -delta
		}
		deviations = append(deviations, delta)
	}
	return medianInterval(deviations)
}

func gapThreshold(expected time.Duration) time.Duration {
	if expected <= 0 {
		return 0
	}
	threshold := 3 * expected
	if floor := expected + 500*time.Millisecond; threshold < floor {
		threshold = floor
	}
	return threshold
}

func sourcePayloadSize(e *msg.Envelope) int {
	if len(e.Raw) > 0 {
		return len(e.Raw)
	}
	return len(e.Payload)
}

func (s *sourceStream) record(now time.Time, e *msg.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.messages == 0 {
		s.firstSeen = now
	}
	if !s.lastSeen.IsZero() && now.After(s.lastSeen) {
		interval := now.Sub(s.lastSeen)
		expectedBefore := medianInterval(s.intervalValues())
		if s.intLen >= 3 && interval > gapThreshold(expectedBefore) {
			s.gapCount++
			s.lastGap = now
			if interval > s.longestGap {
				s.longestGap = interval
			}
		}
		if s.intLen >= 3 && expectedBefore > 0 && interval < expectedBefore/3 {
			s.burstCount++
		}
		s.addInterval(interval)
	}
	s.messages++
	s.lastSeen = now
	if e.PGNName != "" {
		s.pgnName = e.PGNName
	}
	if e.Variant != "" {
		s.variant = e.Variant
	}
	if e.Transport != "" {
		s.transport = e.Transport
	}
	if e.ManufacturerCode != nil {
		manufacturer := *e.ManufacturerCode
		s.manufacturerCode = &manufacturer
	}
	if e.DeviceName != nil {
		name := *e.DeviceName
		if s.deviceName != nil && *s.deviceName != name {
			s.identityChanges++
		}
		s.deviceName = &name
	}
	payloadSize := sourcePayloadSize(e)
	s.payload.add(float64(payloadSize))
	s.rate.record(now, payloadSize, e.Transport == "Fast" || e.Transport == "fast")
	s.wire.record(now, e.Raw)
	if s.destinations == nil {
		s.destinations = make(map[uint8]int64)
		s.priorities = make(map[uint8]int64)
		s.decodeStatuses = make(map[string]int64)
		s.missingDecodedFields = make(map[string]int64)
	}
	s.destinations[e.Dest]++
	s.priorities[e.Priority]++
	decodeStatus := normalizeDecodeStatus(e.Decode.Status)
	s.decodeStatuses[decodeStatus]++
	s.lastDecodeStatus = decodeStatus
	if e.Decode.Complete {
		s.decodeComplete++
	} else {
		s.decodeIncomplete++
	}
	if e.Decode.Fallback {
		s.decodeFallback++
	}
	if decodeStatus == "unknown" {
		s.unknownMessages++
	}
	for _, missing := range e.Decode.Missing {
		s.missingDecodedFields[missing]++
	}

	observedFields := make(map[string]bool)
	physicalNames := make([]string, 0, len(e.Physical))
	for name := range e.Physical {
		physicalNames = append(physicalNames, name)
	}
	sort.Strings(physicalNames)
	for _, name := range physicalNames {
		value := e.Physical[name]
		field := s.field(name, "number", value.Unit)
		if field == nil {
			continue
		}
		field.lowerBound = cloneFloat(value.Minimum)
		field.upperBound = cloneFloat(value.Maximum)
		observedFields[name] = true
		field.recordNumeric(now, value.Value)
	}

	values := map[string]any{}
	if len(e.Payload) > 0 && string(e.Payload) != "null" {
		_ = json.Unmarshal(e.Payload, &values)
	}
	flat := make(map[string]any)
	flattenScalars("", values, flat)
	fieldNames := make([]string, 0, len(flat))
	for name := range flat {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)
	for _, name := range fieldNames {
		if _, described := e.Physical[name]; described {
			continue
		}
		switch value := flat[name].(type) {
		case float64:
			field := s.field(name, "number", "")
			if field != nil {
				observedFields[name] = true
				field.recordNumeric(now, value)
			}
		case string:
			if field := s.field(name, "category", ""); field != nil {
				observedFields[name] = true
				field.recordCategory(now, value)
			}
		case bool:
			if field := s.field(name, "category", ""); field != nil {
				observedFields[name] = true
				field.recordCategory(now, strconv.FormatBool(value))
			}
		}
	}
	for name, field := range s.fields {
		if !observedFields[name] {
			field.missingMessages++
		}
	}
}

func normalizeDecodeStatus(status string) string {
	switch status {
	case "decoded", "unknown", "partial", "fallback", "error":
		return status
	case "":
		return "unspecified"
	default:
		return "other"
	}
}

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func flattenScalars(prefix string, value any, out map[string]any) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			name := key
			if prefix != "" {
				name = prefix + "." + key
			}
			flattenScalars(name, typed[key], out)
		}
	case []any:
		for i, item := range typed {
			flattenScalars(prefix+"["+strconv.Itoa(i)+"]", item, out)
		}
	case float64, string, bool:
		if prefix != "" {
			out[prefix] = typed
		}
	}
}

func (s *sourceStream) field(name, kind, unit string) *sourceField {
	if field := s.fields[name]; field != nil {
		if unit != "" {
			field.unit = unit
		}
		return field
	}
	if len(s.fields) >= maxSourceFields {
		return nil
	}
	field := &sourceField{kind: kind, unit: unit}
	if kind == "category" {
		field.values = make(map[string]int64)
	}
	s.fields[name] = field
	return field
}

func (f *sourceField) recordNumeric(now time.Time, value float64) {
	f.presentMessages++
	if math.IsNaN(value) || math.IsInf(value, 0) {
		f.invalidCount++
		f.lastSeen = now
		return
	}
	if f.numeric.count > 0 {
		change := math.Abs(value - f.numeric.last)
		f.changes.add(change)
		if now.After(f.lastSeen) {
			f.lastRateOfChange = (value - f.numeric.last) / now.Sub(f.lastSeen).Seconds()
		}
		if change > math.Max(math.Abs(f.numeric.last)*1e-9, 1e-12) {
			f.lastChanged = now
		}
	} else {
		f.lastChanged = now
	}
	f.numeric.add(value)
	f.numericSamples[f.numericHead] = value
	f.numericHead = (f.numericHead + 1) % sourceIntervalSamples
	if f.numericLen < sourceIntervalSamples {
		f.numericLen++
	}
	f.lastSeen = now
}

func (f *sourceField) recordCategory(now time.Time, value string) {
	if len(value) > 64 {
		value = value[:64] + "…"
	}
	_, known := f.values[value]
	if known || len(f.values) < maxCategoryValues {
		f.values[value]++
	} else {
		f.other++
	}
	if f.presentMessages > 0 && !known {
		f.novelValueCount++
	}
	if f.presentMessages == 0 || f.lastCategory != value {
		f.lastChanged = now
	}
	f.presentMessages++
	f.lastCategory = value
	f.lastSeen = now
}

func (s *sourceStream) snapshot(now time.Time) SourcePGNMetric {
	s.mu.Lock()
	defer s.mu.Unlock()

	intervals := s.intervalValues()
	expected := medianInterval(intervals)
	periodP90 := durationPercentile(intervals, 0.90)
	periodP95 := durationPercentile(intervals, 0.95)
	periodP99 := durationPercentile(intervals, 0.99)
	jitterMAD := intervalMAD(intervals, expected)
	var shortest, longest time.Duration
	for _, interval := range intervals {
		if shortest == 0 || interval < shortest {
			shortest = interval
		}
		if interval > longest {
			longest = interval
		}
	}
	age := now.Sub(s.lastSeen)
	if age < 0 {
		age = 0
	}
	gapActive := len(intervals) >= 3 && age > gapThreshold(expected)
	gapRatio := 0.0
	if expected > 0 {
		gapRatio = float64(age) / float64(expected)
	}
	recentMessages, recentBytes, busLoad := s.rate.snapshot(now)
	status := "active"
	if len(intervals) < 3 {
		status = "warming"
	}
	if gapActive {
		status = "gap"
	}

	out := SourcePGNMetric{
		Observed: true, SourceID: s.key.source, PGN: s.key.pgn, PGNName: s.pgnName,
		Variant: s.variant, Transport: s.transport, DecodeStatus: s.lastDecodeStatus,
		DecodeStatuses: cloneStringTotals(s.decodeStatuses), DecodeComplete: s.decodeComplete,
		DecodeIncomplete: s.decodeIncomplete, DecodeFallback: s.decodeFallback,
		UnknownMessages: s.unknownMessages, MissingDecodedFields: cloneStringTotals(s.missingDecodedFields),
		SourceAddress: s.key.address, Messages: s.messages,
		FirstSeen: s.firstSeen, LastSeen: s.lastSeen, AgeSeconds: age.Seconds(),
		ExpectedPeriodSeconds: expected.Seconds(), ShortestPeriodSeconds: shortest.Seconds(),
		LongestPeriodSeconds: longest.Seconds(), PeriodP90Seconds: periodP90.Seconds(),
		PeriodP95Seconds: periodP95.Seconds(),
		PeriodP99Seconds: periodP99.Seconds(), JitterMADSeconds: jitterMAD.Seconds(),
		BurstCount: s.burstCount, RecentMessagesPerSec: recentMessages,
		RecentBytesPerSec: recentBytes, EstimatedBusLoadPercent: busLoad,
		GapActive: gapActive, GapRatio: gapRatio,
		GapCount: s.gapCount, LastGapAt: timePtr(s.lastGap), LongestGapSeconds: s.longestGap.Seconds(),
		Status: status, DestinationCounts: cloneUint8Totals(s.destinations),
		PriorityCounts: cloneUint8Totals(s.priorities), IdentityChanges: s.identityChanges,
		Raw: s.wire.snapshot(now),
	}
	if expected > 0 {
		out.JitterPercent = float64(jitterMAD) / float64(expected) * 100
	}
	if expected > 0 {
		out.FrequencyHz = 1 / expected.Seconds()
	}
	if s.payload.count > 0 {
		out.PayloadBytesLast = int64(s.payload.last)
		out.PayloadBytesMin = int64(s.payload.min)
		out.PayloadBytesMax = int64(s.payload.max)
		out.PayloadBytesMean = s.payload.mean
	}
	if s.deviceName != nil {
		name := *s.deviceName
		out.DeviceName = &name
		out.DeviceNameHex = fmt.Sprintf("%016x", name)
	}
	if s.manufacturerCode != nil {
		manufacturer := *s.manufacturerCode
		out.ManufacturerCode = &manufacturer
	}

	fieldNames := make([]string, 0, len(s.fields))
	for name := range s.fields {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)
	for _, name := range fieldNames {
		out.Fields = append(out.Fields, fieldSnapshot(name, s.fields[name], now, s.messages))
	}
	return out
}

func fieldSnapshot(name string, field *sourceField, now time.Time, messages int64) FieldDistribution {
	out := FieldDistribution{
		Field: name, Kind: field.kind, Unit: field.unit,
		PresentMessages: field.presentMessages,
		MissingMessages: field.missingMessages, InvalidCount: field.invalidCount,
		NovelValueCount: field.novelValueCount,
		CatalogMinimum:  cloneFloat(field.lowerBound), CatalogMaximum: cloneFloat(field.upperBound),
	}
	if messages > 0 {
		out.AvailabilityPercent = float64(field.presentMessages) / float64(messages) * 100
	}
	if !field.lastChanged.IsZero() {
		out.StuckSeconds = max(0, now.Sub(field.lastChanged).Seconds())
	}
	if field.kind == "number" && field.numeric.count > 0 {
		last, minValue, maxValue := field.numeric.last, field.numeric.min, field.numeric.max
		mean, stddev := field.numeric.mean, field.numeric.stddev()
		out.Samples = field.numeric.count
		out.Last = formatMetricNumber(last)
		out.LastNumeric = &last
		out.Minimum = &minValue
		out.Maximum = &maxValue
		out.Mean = &mean
		out.StdDev = &stddev
		values := make([]float64, field.numericLen)
		start := (field.numericHead - field.numericLen + sourceIntervalSamples) % sourceIntervalSamples
		for i := range values {
			values[i] = field.numericSamples[(start+i)%sourceIntervalSamples]
		}
		p05, p50 := floatPercentile(values, 0.05), floatPercentile(values, 0.50)
		p95, p99 := floatPercentile(values, 0.95), floatPercentile(values, 0.99)
		out.P05, out.P50, out.P95, out.P99 = &p05, &p50, &p95, &p99
		if field.changes.count > 0 {
			change := field.changes.last
			out.LastChange = &change
			rate := field.lastRateOfChange
			out.LastRateOfChange = &rate
		}
		return out
	}
	out.Values = make(map[string]int64, len(field.values))
	for value, count := range field.values {
		out.Values[value] = count
		out.Samples += count
	}
	out.Other = field.other
	out.Samples += field.other
	out.Last = field.lastCategory
	return out
}

func cloneStringTotals(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneUint8Totals(in map[uint8]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[strconv.Itoa(int(key))] = value
	}
	return out
}

func formatMetricNumber(value float64) string {
	return strconv.FormatFloat(value, 'g', 6, 64)
}

// SourcePGNMetrics returns every observed PGN/sender stream for one source.
// Results are sorted with active problems first, then by PGN and address.
func (r *Registry) SourcePGNMetrics(source string) []SourcePGNMetric {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	streams := make([]*sourceStream, 0)
	for key, stream := range r.sourceStreams {
		if key.source == source {
			streams = append(streams, stream)
		}
	}
	r.mu.Unlock()
	now := r.now()
	out := make([]SourcePGNMetric, 0, len(streams))
	for _, stream := range streams {
		out = append(out, stream.snapshot(now))
	}
	applyTrafficShares(out)
	sortSourcePGNMetrics(out)
	return out
}

// AllSourcePGNMetrics returns source metrics keyed by configured source id.
func (r *Registry) AllSourcePGNMetrics() map[string][]SourcePGNMetric {
	out := map[string][]SourcePGNMetric{}
	if r == nil {
		return out
	}
	r.mu.Lock()
	streams := make([]*sourceStream, 0, len(r.sourceStreams))
	for _, stream := range r.sourceStreams {
		streams = append(streams, stream)
	}
	r.mu.Unlock()
	now := r.now()
	for _, stream := range streams {
		metric := stream.snapshot(now)
		out[metric.SourceID] = append(out[metric.SourceID], metric)
	}
	for source := range out {
		applyTrafficShares(out[source])
		sortSourcePGNMetrics(out[source])
	}
	return out
}

func applyTrafficShares(metrics []SourcePGNMetric) {
	total := 0.0
	for _, metric := range metrics {
		total += metric.RecentBytesPerSec
	}
	if total <= 0 {
		return
	}
	for i := range metrics {
		metrics[i].TrafficSharePercent = metrics[i].RecentBytesPerSec / total * 100
	}
}

func sortSourcePGNMetrics(metrics []SourcePGNMetric) {
	priority := map[string]int{"missing": 0, "gap": 1, "changed": 2, "awaiting": 3, "warming": 4, "active": 5}
	sort.Slice(metrics, func(i, j int) bool {
		left, right := priority[metrics[i].Status], priority[metrics[j].Status]
		if left != right {
			return left < right
		}
		if metrics[i].PGN != metrics[j].PGN {
			return metrics[i].PGN < metrics[j].PGN
		}
		return metrics[i].SourceAddress < metrics[j].SourceAddress
	})
}

func (r *Registry) getSourceStream(source string, e *msg.Envelope) (*sourceStream, bool) {
	key := sourceStreamKey{source: source, pgn: e.PGN, address: e.Source}
	r.mu.Lock()
	defer r.mu.Unlock()
	stream := r.sourceStreams[key]
	created := false
	if stream == nil {
		stream = &sourceStream{key: key, fields: make(map[string]*sourceField)}
		r.sourceStreams[key] = stream
		created = true
	}
	return stream, created
}

func (r *Registry) sourceLifecycleEvents(now time.Time, source string, e *msg.Envelope, created bool, before, after sourceEventState) []SourceMetricEvent {
	base := SourceMetricEvent{Time: now.UTC(), SourceID: source, PGN: e.PGN,
		SourceAddress: e.Source, DeviceNameHex: e.DeviceNameHex}
	if base.DeviceNameHex == "" && e.DeviceName != nil {
		base.DeviceNameHex = fmt.Sprintf("%016x", *e.DeviceName)
	}
	var events []SourceMetricEvent
	add := func(kind, severity, summary string, details map[string]string) {
		event := base
		event.Kind, event.Severity, event.Summary, event.Details = kind, severity, summary, details
		events = append(events, event)
	}
	if created {
		kind, severity := "new_stream", "info"
		if normalizeDecodeStatus(e.Decode.Status) == "unknown" {
			kind, severity = "unknown_stream", "warning"
		}
		add(kind, severity, fmt.Sprintf("First observed PGN %d from address %d", e.PGN, e.Source), nil)
	}
	if after.gapCount > before.gapCount {
		add("gap_recovered", "warning", fmt.Sprintf("PGN %d resumed after a sending gap", e.PGN), nil)
	}
	if before.messages > 0 && after.decodeStatus != before.decodeStatus {
		add("decode_status_changed", "warning", fmt.Sprintf("PGN %d decode status changed from %s to %s", e.PGN, before.decodeStatus, after.decodeStatus),
			map[string]string{"before": before.decodeStatus, "after": after.decodeStatus})
	}
	if before.messages > 0 && after.payloadLengths > before.payloadLengths {
		add("payload_length_changed", "warning", fmt.Sprintf("PGN %d introduced a new payload length", e.PGN),
			map[string]string{"length_bytes": strconv.Itoa(sourcePayloadSize(e))})
	}
	if after.identityChanges > before.identityChanges {
		add("device_name_changed", "error", fmt.Sprintf("Address %d changed Device NAME while sending PGN %d", e.Source, e.PGN), nil)
	}
	if after.novelValues > before.novelValues {
		add("new_categorical_value", "info", fmt.Sprintf("PGN %d introduced a new decoded categorical value", e.PGN), nil)
	}
	return events
}

func (r *Registry) sourceIdentityEvents(now time.Time, source string, e *msg.Envelope) []SourceMetricEvent {
	if e.DeviceName == nil {
		return nil
	}
	name := *e.DeviceName
	addressKey := sourceAddressKey{source: source, address: e.Source}
	deviceKey := sourceDeviceKey{source: source, name: name}
	r.mu.Lock()
	previousName, addressKnown := r.sourceAddressNames[addressKey]
	previousAddress, deviceKnown := r.sourceDeviceAddresses[deviceKey]
	r.sourceAddressNames[addressKey] = name
	r.sourceDeviceAddresses[deviceKey] = e.Source
	r.mu.Unlock()
	base := SourceMetricEvent{Time: now.UTC(), SourceID: source, PGN: e.PGN,
		SourceAddress: e.Source, DeviceNameHex: fmt.Sprintf("%016x", name)}
	var events []SourceMetricEvent
	if addressKnown && previousName != name {
		event := base
		event.Kind, event.Severity = "address_conflict", "error"
		event.Summary = fmt.Sprintf("Source address %d changed from Device NAME %016x to %016x", e.Source, previousName, name)
		events = append(events, event)
	}
	if deviceKnown && previousAddress != e.Source {
		event := base
		event.Kind, event.Severity = "source_address_changed", "warning"
		event.Summary = fmt.Sprintf("Device NAME %016x moved from source address %d to %d", name, previousAddress, e.Source)
		event.Details = map[string]string{"before": strconv.Itoa(int(previousAddress)), "after": strconv.Itoa(int(e.Source))}
		events = append(events, event)
	}
	return events
}
