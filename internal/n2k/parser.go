// Package n2k provides the ParsedMessage type and helpers for converting
// decoded NMEA 2000 structs (from github.com/open-ships/n2k) into a
// canonical form suitable for buffering, filtering, and forwarding.
package n2k

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/open-ships/n2k/pgn"
)

// ParsedMessage is the canonical message type flowing through the system.
type ParsedMessage struct {
	// ID is the SQLite row id, set when the message is read back from the buffer.
	ID int64 `json:"id,omitempty"`

	PGN       uint32          `json:"pgn"`
	Source    uint8           `json:"source"`
	Dest      uint8           `json:"dest"`
	Priority  uint8           `json:"priority"`
	Timestamp time.Time       `json:"timestamp"`
	// Payload is the decoded struct serialized to JSON for storage/filtering.
	Payload json.RawMessage `json:"payload"`
	// Raw is the original assembled bytes (for pass-through sinks).
	Raw []byte `json:"raw,omitempty"`
}

// FromDecoded converts a decoded n2k struct (yielded by n2k.Receive or Scanner)
// into a ParsedMessage.
func FromDecoded(result any) (*ParsedMessage, error) {
	switch v := result.(type) {
	case pgn.UnknownPGN:
		return &ParsedMessage{
			PGN:       v.Info.PGN,
			Source:    v.Info.SourceId,
			Dest:      v.Info.TargetId,
			Priority:  v.Info.Priority,
			Timestamp: v.Info.Timestamp,
			Payload:   json.RawMessage("null"),
			Raw:       append([]byte(nil), v.Data...),
		}, nil

	default:
		info := extractInfo(v)
		payload, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal PGN struct: %w", err)
		}
		return &ParsedMessage{
			PGN:       info.PGN,
			Source:    info.SourceId,
			Dest:      info.TargetId,
			Priority:  info.Priority,
			Timestamp: info.Timestamp,
			Payload:   json.RawMessage(payload),
		}, nil
	}
}

var messageInfoType = reflect.TypeOf(pgn.MessageInfo{})

// extractInfo uses reflection to pull MessageInfo from a generated PGN struct.
func extractInfo(v any) pgn.MessageInfo {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return pgn.MessageInfo{Timestamp: time.Now()}
	}
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		if rt.Field(i).Type == messageInfoType {
			if info, ok := rv.Field(i).Interface().(pgn.MessageInfo); ok {
				return info
			}
		}
	}
	return pgn.MessageInfo{Timestamp: time.Now()}
}
