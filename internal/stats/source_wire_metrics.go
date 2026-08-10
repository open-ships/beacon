package stats

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"time"
)

const (
	// Raw diagnostics retain only a bounded prefix. NMEA 2000 Envelopes remain
	// canonical and complete elsewhere; these limits affect descriptive UI/MCP
	// snapshots only.
	maxAnalyzedPayloadBytes  = 32
	maxRetainedRawBytes      = 256
	maxTrackedPayloadLengths = 16
	maxTrackedByteValues     = 16
	maxRawSamples            = 4
	maxTrackedFingerprints   = 32
	maxReportedFingerprints  = 8
	sourceRateBuckets        = 60
)

// RawPayloadSample is a bounded recent wire-payload example. It is exposed in
// the UI and MCP, but never emitted as a Prometheus label.
type RawPayloadSample struct {
	ObservedAt   time.Time `json:"observed_at"`
	Hex          string    `json:"hex"`
	Fingerprint  string    `json:"fingerprint"`
	Length       int       `json:"length"`
	HexTruncated bool      `json:"hex_truncated,omitempty"`
}

type PayloadFingerprint struct {
	Fingerprint string    `json:"fingerprint"`
	Count       int64     `json:"count"`
	Share       float64   `json:"share"`
	Length      int       `json:"length"`
	LastSeen    time.Time `json:"last_seen"`
}

// ByteDistribution describes one raw payload byte position. Counts stay
// internal; only bounded descriptive values leave the registry.
type ByteDistribution struct {
	Offset            int     `json:"offset"`
	Samples           int64   `json:"samples"`
	Minimum           uint8   `json:"minimum"`
	Maximum           uint8   `json:"maximum"`
	MostCommon        uint8   `json:"most_common"`
	MostCommonShare   float64 `json:"most_common_share"`
	EntropyBits       float64 `json:"entropy_bits"`
	ChangedShare      float64 `json:"changed_share"`
	ChangedBitMaskHex string  `json:"changed_bit_mask_hex"`
	OtherSamples      int64   `json:"other_samples,omitempty"`
}

type RawPayloadDiagnostics struct {
	LastHex                 string               `json:"last_hex,omitempty"`
	LastFingerprint         string               `json:"last_fingerprint,omitempty"`
	LastLength              int                  `json:"last_length,omitempty"`
	LastHexTruncated        bool                 `json:"last_hex_truncated,omitempty"`
	RetainedByteLimit       int                  `json:"retained_byte_limit"`
	TruncatedSamples        int64                `json:"truncated_samples,omitempty"`
	LengthCounts            map[string]int64     `json:"length_counts,omitempty"`
	LengthCountOverflow     int64                `json:"length_count_overflow,omitempty"`
	DistinctPayloads        int                  `json:"distinct_payloads"`
	DistinctPayloadOverflow int64                `json:"distinct_payload_overflow,omitempty"`
	UnchangedSeconds        float64              `json:"unchanged_seconds,omitempty"`
	HammingDistanceMean     float64              `json:"hamming_distance_mean,omitempty"`
	HammingDistanceP95      float64              `json:"hamming_distance_p95,omitempty"`
	LastChangedBytes        []int                `json:"last_changed_bytes,omitempty"`
	LastChangeOutsidePrefix bool                 `json:"last_change_outside_prefix,omitempty"`
	Fingerprints            []PayloadFingerprint `json:"top_fingerprints,omitempty"`
	Bytes                   []ByteDistribution   `json:"byte_distributions,omitempty"`
	Samples                 []RawPayloadSample   `json:"recent_samples,omitempty"`
}

type sourceByteDistribution struct {
	samples     int64
	counts      map[uint8]uint32
	other       uint32
	minimum     uint8
	maximum     uint8
	comparisons int64
	changed     int64
	bitChanges  [8]int64
}

type trackedFingerprint struct {
	digest   string
	count    int64
	length   int
	lastSeen time.Time
}

type sourceWireStats struct {
	previous                []byte
	lastChanged             time.Time
	lastChangedBytes        []int
	lengths                 map[int]int64
	lengthOverflow          int64
	fingerprints            map[string]*trackedFingerprint
	fingerprintOverflow     int64
	bytes                   []sourceByteDistribution
	hamming                 runningDistribution
	hammingSamples          [sourceIntervalSamples]float64
	hammingHead             int
	hammingLen              int
	samples                 [maxRawSamples]RawPayloadSample
	sampleHead              int
	sampleLen               int
	lastHex                 string
	lastFingerprint         string
	lastLength              int
	lastHexTruncated        bool
	truncatedSamples        int64
	lastChangeOutsidePrefix bool
}

func payloadFingerprint(retained []byte, originalLength int) string {
	if len(retained) == originalLength {
		digest := sha256.Sum256(retained)
		return hex.EncodeToString(digest[:8])
	}
	if originalLength < 0 {
		originalLength = 0
	}
	// Include original length when hashing a prefix so different-length
	// payloads cannot collapse merely because their retained bytes match.
	var sample [maxRetainedRawBytes + 8]byte
	// originalLength is non-negative here and an int always fits in uint64.
	binary.BigEndian.PutUint64(sample[:8], uint64(originalLength)) //nolint:gosec // guarded lossless conversion
	copy(sample[8:], retained)
	digest := sha256.Sum256(sample[:8+len(retained)])
	return hex.EncodeToString(digest[:8])
}

func (w *sourceWireStats) record(now time.Time, raw []byte) {
	if len(raw) == 0 {
		return
	}
	if w.lengths == nil {
		w.lengths = make(map[int]int64)
	}
	if count, ok := w.lengths[len(raw)]; ok {
		w.lengths[len(raw)] = count + 1
	} else if len(w.lengths) < maxTrackedPayloadLengths {
		w.lengths[len(raw)] = 1
	} else {
		w.lengthOverflow++
	}
	retained := raw
	w.lastHexTruncated = len(raw) > maxRetainedRawBytes
	if w.lastHexTruncated {
		retained = raw[:maxRetainedRawBytes]
		w.truncatedSamples++
	}
	previousFingerprint := w.lastFingerprint
	fingerprint := payloadFingerprint(retained, len(raw))
	w.lastHex = hex.EncodeToString(retained)
	w.lastFingerprint = fingerprint
	w.lastLength = len(raw)
	if w.fingerprints == nil {
		w.fingerprints = make(map[string]*trackedFingerprint)
	}
	if item := w.fingerprints[fingerprint]; item != nil {
		item.count++
		item.lastSeen = now
	} else if len(w.fingerprints) < maxTrackedFingerprints {
		w.fingerprints[fingerprint] = &trackedFingerprint{
			digest: fingerprint, count: 1, length: len(raw), lastSeen: now,
		}
	} else {
		w.fingerprintOverflow++
	}

	changedBytes := make([]int, 0)
	if len(w.previous) > 0 {
		maxLength := max(len(w.previous), len(retained))
		distance := 0
		for i := 0; i < maxLength; i++ {
			var before, after byte
			if i < len(w.previous) {
				before = w.previous[i]
			}
			if i < len(retained) {
				after = retained[i]
			}
			if before != after || i >= len(w.previous) || i >= len(retained) {
				changedBytes = append(changedBytes, i)
			}
			distance += bits.OnesCount8(before ^ after)
		}
		w.hamming.add(float64(distance))
		w.hammingSamples[w.hammingHead] = float64(distance)
		w.hammingHead = (w.hammingHead + 1) % sourceIntervalSamples
		if w.hammingLen < sourceIntervalSamples {
			w.hammingLen++
		}
	}
	changed := previousFingerprint == "" || previousFingerprint != fingerprint
	if changed {
		w.lastChanged = now
		w.lastChangedBytes = append([]int(nil), changedBytes...)
		w.lastChangeOutsidePrefix = previousFingerprint != "" && len(changedBytes) == 0
		sample := RawPayloadSample{
			ObservedAt: now, Hex: w.lastHex, Fingerprint: fingerprint,
			Length: len(raw), HexTruncated: w.lastHexTruncated,
		}
		w.samples[w.sampleHead] = sample
		w.sampleHead = (w.sampleHead + 1) % maxRawSamples
		if w.sampleLen < maxRawSamples {
			w.sampleLen++
		}
	}

	analyzed := min(len(retained), maxAnalyzedPayloadBytes)
	for len(w.bytes) < analyzed {
		w.bytes = append(w.bytes, sourceByteDistribution{counts: make(map[uint8]uint32)})
	}
	for i := 0; i < analyzed; i++ {
		value := retained[i]
		byteStats := &w.bytes[i]
		if byteStats.samples == 0 {
			byteStats.minimum, byteStats.maximum = value, value
		} else {
			if value < byteStats.minimum {
				byteStats.minimum = value
			}
			if value > byteStats.maximum {
				byteStats.maximum = value
			}
		}
		byteStats.samples++
		if count, ok := byteStats.counts[value]; ok {
			byteStats.counts[value] = count + 1
		} else if len(byteStats.counts) < maxTrackedByteValues {
			byteStats.counts[value] = 1
		} else {
			byteStats.other++
		}
		if i < len(w.previous) {
			byteStats.comparisons++
			delta := w.previous[i] ^ value
			if delta != 0 {
				byteStats.changed++
			}
			for bit := 0; bit < 8; bit++ {
				if delta&(1<<bit) != 0 {
					byteStats.bitChanges[bit]++
				}
			}
		}
	}
	w.previous = append(w.previous[:0], retained...)
}

func (w *sourceWireStats) snapshot(now time.Time) *RawPayloadDiagnostics {
	if len(w.previous) == 0 {
		return nil
	}
	out := &RawPayloadDiagnostics{
		LastHex: w.lastHex, LastFingerprint: w.lastFingerprint,
		LastLength: w.lastLength, LastHexTruncated: w.lastHexTruncated,
		RetainedByteLimit: maxRetainedRawBytes, TruncatedSamples: w.truncatedSamples,
		LengthCounts: make(map[string]int64, len(w.lengths)), LengthCountOverflow: w.lengthOverflow,
		DistinctPayloads: len(w.fingerprints), DistinctPayloadOverflow: w.fingerprintOverflow,
		HammingDistanceMean:     w.hamming.mean,
		LastChangedBytes:        append([]int(nil), w.lastChangedBytes...),
		LastChangeOutsidePrefix: w.lastChangeOutsidePrefix,
	}
	if !w.lastChanged.IsZero() {
		out.UnchangedSeconds = max(0, now.Sub(w.lastChanged).Seconds())
	}
	for length, count := range w.lengths {
		out.LengthCounts[strconv.Itoa(length)] = count
	}
	hammingValues := make([]float64, w.hammingLen)
	start := (w.hammingHead - w.hammingLen + sourceIntervalSamples) % sourceIntervalSamples
	for i := range hammingValues {
		hammingValues[i] = w.hammingSamples[(start+i)%sourceIntervalSamples]
	}
	out.HammingDistanceP95 = floatPercentile(hammingValues, 0.95)

	fingerprints := make([]PayloadFingerprint, 0, len(w.fingerprints))
	total := int64(0)
	for _, item := range w.fingerprints {
		total += item.count
	}
	total += w.fingerprintOverflow
	for _, item := range w.fingerprints {
		share := 0.0
		if total > 0 {
			share = float64(item.count) / float64(total)
		}
		fingerprints = append(fingerprints, PayloadFingerprint{
			Fingerprint: item.digest, Count: item.count, Share: share,
			Length: item.length, LastSeen: item.lastSeen,
		})
	}
	sort.Slice(fingerprints, func(i, j int) bool {
		if fingerprints[i].Count != fingerprints[j].Count {
			return fingerprints[i].Count > fingerprints[j].Count
		}
		return fingerprints[i].Fingerprint < fingerprints[j].Fingerprint
	})
	if len(fingerprints) > maxReportedFingerprints {
		fingerprints = fingerprints[:maxReportedFingerprints]
	}
	out.Fingerprints = fingerprints

	for offset, value := range w.bytes {
		if value.samples == 0 {
			continue
		}
		var mode uint8
		var modeCount uint32
		entropy := 0.0
		for candidate, count := range value.counts {
			if count > modeCount {
				mode, modeCount = candidate, count
			}
			probability := float64(count) / float64(value.samples)
			entropy -= probability * math.Log2(probability)
		}
		if value.other > 0 {
			// Overflow is one aggregate bucket: entropy remains bounded-cost and
			// intentionally underestimates diversity beyond the tracked values.
			probability := float64(value.other) / float64(value.samples)
			entropy -= probability * math.Log2(probability)
		}
		changedShare := 0.0
		var changedMask uint8
		if value.comparisons > 0 {
			changedShare = float64(value.changed) / float64(value.comparisons)
			for bit, count := range value.bitChanges {
				if count > 0 {
					changedMask |= 1 << bit
				}
			}
		}
		out.Bytes = append(out.Bytes, ByteDistribution{
			Offset: offset, Samples: value.samples, Minimum: value.minimum, Maximum: value.maximum,
			MostCommon: mode, MostCommonShare: float64(modeCount) / float64(value.samples),
			EntropyBits: entropy, ChangedShare: changedShare,
			ChangedBitMaskHex: hex.EncodeToString([]byte{changedMask}), OtherSamples: int64(value.other),
		})
	}
	start = (w.sampleHead - w.sampleLen + maxRawSamples) % maxRawSamples
	for i := 0; i < w.sampleLen; i++ {
		out.Samples = append(out.Samples, w.samples[(start+i)%maxRawSamples])
	}
	return out
}

func floatPercentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	return floatPercentileSorted(copyValues, percentile)
}

func floatPercentileSorted(sorted []float64, percentile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

type sourceRateBucket struct {
	second   int64
	messages int64
	bytes    int64
	wireBits int64
}

type sourceRateStats struct {
	buckets [sourceRateBuckets]sourceRateBucket
}

func (r *sourceRateStats) record(now time.Time, payloadBytes int, fastPacket bool) {
	second := now.Unix()
	index := int(second % sourceRateBuckets)
	bucket := &r.buckets[index]
	if bucket.second != second {
		*bucket = sourceRateBucket{second: second}
	}
	bucket.messages++
	bucket.bytes += int64(payloadBytes)
	frames := 1
	if fastPacket || payloadBytes > 8 {
		remaining := max(payloadBytes-6, 0)
		frames += (remaining + 6) / 7
	}
	bucket.wireBits += int64(frames * 130) // extended CAN frame including framing/stuffing allowance
}

func (r *sourceRateStats) snapshot(now time.Time) (messagesPerSecond, bytesPerSecond, busLoadPercent float64) {
	cutoff := now.Unix() - sourceRateBuckets + 1
	var messages, bytesCount, wireBits int64
	for _, bucket := range r.buckets {
		if bucket.second < cutoff || bucket.second > now.Unix() {
			continue
		}
		messages += bucket.messages
		bytesCount += bucket.bytes
		wireBits += bucket.wireBits
	}
	messagesPerSecond = float64(messages) / sourceRateBuckets
	bytesPerSecond = float64(bytesCount) / sourceRateBuckets
	busLoadPercent = float64(wireBits) / (250_000 * sourceRateBuckets) * 100
	return
}
