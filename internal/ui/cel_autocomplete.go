package ui

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/open-ships/n2k/pgn"
)

// celCompletionCatalog is the schema-backed portion of the connector form's
// CEL completion data. Envelope fields and CEL functions live in app.js; PGN
// numbers and decoded payload keys come from the same n2k metadata used by the
// decoder so UI suggestions cannot silently drift from the messages Beacon
// actually produces.
type celCompletionCatalog struct {
	PayloadFields []celCompletionItem `json:"payloadFields"`
	PGNs          []celCompletionItem `json:"pgns"`
}

type celCompletionItem struct {
	Label  string `json:"label"`
	Detail string `json:"detail"`
}

type payloadFieldSummary struct {
	names map[string]struct{}
	pgns  map[uint32]struct{}
}

var (
	celCatalogOnce sync.Once
	celCatalog     celCompletionCatalog
)

// handleCELCompletions serves immutable-for-this-process metadata as JSON.
// The response is deliberately revalidated rather than cached immutably: its
// URL is not asset-versioned, and a browser may remain open across a Beacon
// binary upgrade that brings a newer n2k schema.
func handleCELCompletions(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(getCELCompletionCatalog())
}

func getCELCompletionCatalog() celCompletionCatalog {
	celCatalogOnce.Do(func() {
		celCatalog = buildCELCompletionCatalog()
	})
	return celCatalog
}

func buildCELCompletionCatalog() celCompletionCatalog {
	fieldSummaries := make(map[string]*payloadFieldSummary)
	pgnDescriptions := make(map[uint32]map[string]struct{})

	for pgnNumber, variants := range pgn.PgnInfoLookup {
		for _, info := range variants {
			if info == nil {
				continue
			}
			if _, ok := pgnDescriptions[pgnNumber]; !ok {
				pgnDescriptions[pgnNumber] = make(map[string]struct{})
			}
			if info.Description != "" {
				pgnDescriptions[pgnNumber][info.Description] = struct{}{}
			}

			for order, field := range info.Fields {
				if field == nil || repeatingField(info, order) || !celIdentifier(field.SourceID) {
					continue
				}
				if field.SourceType == "RESERVED" || field.SourceType == "SPARE" || field.SourceID == "reserved" {
					continue
				}
				addPayloadField(fieldSummaries, field.SourceID, field.Name, pgnNumber)
			}

			// Repeating field-set members are nested below these actual JSON
			// payload keys, not flattened at the payload root.
			if info.RepeatingFieldSet1StartField != nil {
				addPayloadField(fieldSummaries, "repeating1", "Repeating field set", pgnNumber)
			}
			if info.RepeatingFieldSet2StartField != nil {
				addPayloadField(fieldSummaries, "repeating2", "Repeating field set", pgnNumber)
			}
		}
	}

	catalog := celCompletionCatalog{
		PayloadFields: make([]celCompletionItem, 0, len(fieldSummaries)),
		PGNs:          make([]celCompletionItem, 0, len(pgnDescriptions)),
	}
	for field, summary := range fieldSummaries {
		catalog.PayloadFields = append(catalog.PayloadFields, celCompletionItem{
			Label:  "msg.payload." + field,
			Detail: payloadFieldDetail(summary),
		})
	}
	for pgnNumber, descriptions := range pgnDescriptions {
		catalog.PGNs = append(catalog.PGNs, celCompletionItem{
			Label:  strconv.FormatUint(uint64(pgnNumber), 10),
			Detail: pgnDetail(descriptions),
		})
	}

	sort.Slice(catalog.PayloadFields, func(i, j int) bool {
		return strings.ToLower(catalog.PayloadFields[i].Label) < strings.ToLower(catalog.PayloadFields[j].Label)
	})
	sort.Slice(catalog.PGNs, func(i, j int) bool {
		left, _ := strconv.ParseUint(catalog.PGNs[i].Label, 10, 32)
		right, _ := strconv.ParseUint(catalog.PGNs[j].Label, 10, 32)
		return left < right
	})
	return catalog
}

func addPayloadField(fields map[string]*payloadFieldSummary, id, name string, pgnNumber uint32) {
	summary, ok := fields[id]
	if !ok {
		summary = &payloadFieldSummary{names: make(map[string]struct{}), pgns: make(map[uint32]struct{})}
		fields[id] = summary
	}
	if name != "" {
		summary.names[name] = struct{}{}
	}
	summary.pgns[pgnNumber] = struct{}{}
}

func repeatingField(info *pgn.PgnInfo, order int) bool {
	starts := []*int{info.RepeatingFieldSet1StartField, info.RepeatingFieldSet2StartField}
	for _, start := range starts {
		if start != nil && order >= *start {
			return true
		}
	}
	return false
}

func celIdentifier(s string) bool {
	if s == "" || !asciiLetterOrUnderscore(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		if !asciiLetterOrUnderscore(s[i]) && (s[i] < '0' || s[i] > '9') {
			return false
		}
	}
	return true
}

func asciiLetterOrUnderscore(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func payloadFieldDetail(summary *payloadFieldSummary) string {
	names := sortedStrings(summary.names)
	detail := "payload field"
	if len(names) > 0 {
		detail = names[0]
	}
	return detail + " · " + summarizePGNs(summary.pgns)
}

func pgnDetail(descriptions map[string]struct{}) string {
	items := sortedStrings(descriptions)
	if len(items) == 0 {
		return "NMEA 2000 PGN"
	}
	if len(items) == 1 {
		return items[0]
	}
	return items[0] + " · " + strconv.Itoa(len(items)) + " variants"
}

func summarizePGNs(values map[uint32]struct{}) string {
	pgns := make([]uint32, 0, len(values))
	for value := range values {
		pgns = append(pgns, value)
	}
	sort.Slice(pgns, func(i, j int) bool { return pgns[i] < pgns[j] })
	if len(pgns) == 1 {
		return "PGN " + strconv.FormatUint(uint64(pgns[0]), 10)
	}
	if len(pgns) <= 3 {
		labels := make([]string, len(pgns))
		for i, value := range pgns {
			labels[i] = strconv.FormatUint(uint64(value), 10)
		}
		return "PGNs " + strings.Join(labels, ", ")
	}
	return strconv.Itoa(len(pgns)) + " PGNs"
}

func sortedStrings(values map[string]struct{}) []string {
	items := make([]string, 0, len(values))
	for value := range values {
		items = append(items, value)
	}
	sort.Strings(items)
	return items
}
