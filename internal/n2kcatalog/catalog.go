// Package n2kcatalog owns Beacon's runtime view of the NMEA 2000 PGN and
// field catalog, including unit-scaled values for decoded messages.
package n2kcatalog

import (
	"reflect"
	"sort"
	"strings"
	"unicode"

	"github.com/open-ships/n2k/pgn"
)

type PGN struct {
	Number   uint32         `json:"pgn"`
	Variants []*pgn.PgnInfo `json:"variants"`
}

type MessageMetadata struct {
	Name             string   `json:"name,omitempty"`
	Variant          string   `json:"variant,omitempty"`
	Transport        string   `json:"transport,omitempty"`
	Complete         bool     `json:"complete"`
	Fallback         bool     `json:"fallback"`
	Missing          []string `json:"missing,omitempty"`
	ManufacturerCode *uint16  `json:"manufacturer_code,omitempty"`
}

type PhysicalField struct {
	Value            float64  `json:"value"`
	Unit             string   `json:"unit,omitempty"`
	PhysicalQuantity string   `json:"physical_quantity,omitempty"`
	Minimum          *float64 `json:"minimum,omitempty"`
	Maximum          *float64 `json:"maximum,omitempty"`
}

func List() []PGN {
	numbers := make([]uint32, 0, len(pgn.PgnInfoLookup))
	for number := range pgn.PgnInfoLookup {
		numbers = append(numbers, number)
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] < numbers[j] })
	out := make([]PGN, 0, len(numbers))
	for _, number := range numbers {
		out = append(out, PGN{Number: number, Variants: pgn.PgnInfoLookup[number]})
	}
	return out
}

func Lookup(number uint32) (PGN, bool) {
	variants, ok := pgn.PgnInfoLookup[number]
	return PGN{Number: number, Variants: variants}, ok
}

func Describe(message pgn.PGN, raw []byte) (MessageMetadata, map[string]PhysicalField) {
	info := selectVariant(message)
	if info == nil {
		return MessageMetadata{}, nil
	}
	metadata := MessageMetadata{
		Name: info.Description, Variant: info.SourceID, Transport: info.Type,
		Complete: info.Complete, Fallback: info.Fallback,
		Missing: append([]string(nil), info.Missing...),
	}
	if pgn.IsProprietaryPGN(message.PGNNumber()) {
		if manufacturer, _, err := pgn.GetProprietaryInfo(raw); err == nil {
			value := uint16(manufacturer)
			metadata.ManufacturerCode = &value
		}
	}
	physical := map[string]PhysicalField{}
	orders := make([]int, 0, len(info.Fields))
	for order := range info.Fields {
		orders = append(orders, order)
	}
	sort.Ints(orders)
	for _, order := range orders {
		field := info.Fields[order]
		if field == nil || field.Match != nil {
			continue
		}
		value, unit, ok, err := pgn.PhysicalValue(message, order)
		if err != nil || !ok {
			continue
		}
		physical[field.SourceID] = PhysicalField{
			Value: value, Unit: unit, PhysicalQuantity: field.PhysicalQuantity,
			Minimum: field.RangeMin, Maximum: field.RangeMax,
		}
	}
	if len(physical) == 0 {
		physical = nil
	}
	return metadata, physical
}

func DescribeUnknown(number uint32, raw []byte) MessageMetadata {
	metadata := MessageMetadata{}
	if infos := pgn.PgnInfoLookup[number]; len(infos) > 0 {
		info := infos[0]
		metadata = MessageMetadata{Name: info.Description, Variant: info.SourceID,
			Transport: info.Type, Complete: info.Complete, Fallback: info.Fallback,
			Missing: append([]string(nil), info.Missing...)}
	}
	if pgn.IsProprietaryPGN(number) {
		if manufacturer, _, err := pgn.GetProprietaryInfo(raw); err == nil {
			value := uint16(manufacturer)
			metadata.ManufacturerCode = &value
		}
	}
	return metadata
}

func selectVariant(message pgn.PGN) *pgn.PgnInfo {
	infos := pgn.PgnInfoLookup[message.PGNNumber()]
	if len(infos) == 0 {
		return nil
	}
	typeName := reflect.TypeOf(message)
	if typeName.Kind() == reflect.Pointer {
		typeName = typeName.Elem()
	}
	want := normalize(typeName.Name())
	for _, info := range infos {
		if normalize(info.Id) == want || normalize(info.SourceID) == want {
			return info
		}
	}
	for _, info := range infos {
		if !info.Fallback {
			return info
		}
	}
	return infos[0]
}

func normalize(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, value)
}
