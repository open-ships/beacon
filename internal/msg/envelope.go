// Package msg defines the canonical message envelope that flows through
// queues, HTTP wire formats, and CEL filters.
package msg

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/open-ships/n2k/pgn"
)

type Envelope struct {
	Seq         int64           `json:"id,omitempty"`
	ConnectorID string          `json:"connector,omitempty"`
	PGN         uint32          `json:"pgn"`
	Source      uint8           `json:"source"`
	Dest        uint8           `json:"dest"`
	Priority    uint8           `json:"priority"`
	Timestamp   time.Time       `json:"timestamp"`
	Payload     json.RawMessage `json:"payload"`
	Raw         []byte          `json:"raw,omitempty"`

	payloadOnce sync.Once
	payloadMap  map[string]any // lazy cache for CEL
}

const (
	defaultPriority = 6
	broadcastDest   = 255
)

// FromPGN converts a decoded n2k message into an Envelope. Known PGNs get
// their payload marshaled to JSON and Raw set to the canonical re-encoding;
// UnknownPGN keeps its original bytes and a null payload.
func FromPGN(m pgn.Message) (*Envelope, error) {
	// Handle UnknownPGN separately since it doesn't implement pgn.PGN
	if u, isUnknown := m.(*pgn.UnknownPGN); isUnknown {
		e := &Envelope{
			PGN:       u.Info.PGN,
			Source:    u.Info.SourceId,
			Dest:      broadcastDest,
			Priority:  defaultPriority,
			Timestamp: u.Info.Timestamp,
			Payload:   json.RawMessage("null"),
			Raw:       append([]byte(nil), u.Data...),
		}
		if e.Timestamp.IsZero() {
			e.Timestamp = time.Now()
		}
		if u.Info.TargetId != nil {
			e.Dest = *u.Info.TargetId
		}
		if u.Info.Priority != nil {
			e.Priority = *u.Info.Priority
		}
		return e, nil
	}

	p, ok := m.(pgn.PGN)
	if !ok {
		return nil, fmt.Errorf("%T does not implement pgn.PGN", m)
	}
	info := p.MessageInfo()
	e := &Envelope{
		PGN:       info.PGN,
		Source:    info.SourceId,
		Dest:      broadcastDest,
		Priority:  defaultPriority,
		Timestamp: info.Timestamp,
	}
	if e.PGN == 0 {
		e.PGN = m.PGNNumber()
	}
	if info.TargetId != nil {
		e.Dest = *info.TargetId
	}
	if info.Priority != nil {
		e.Priority = *info.Priority
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}

	payload, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal PGN %d payload: %w", e.PGN, err)
	}
	payload, err = stripInfo(payload)
	if err != nil {
		return nil, fmt.Errorf("strip info from PGN %d payload: %w", e.PGN, err)
	}
	e.Payload = payload
	raw, err := pgn.EncodeMessage(m)
	if err != nil {
		// Still deliverable to HTTP sinks; CAN sinks will skip it.
		e.Raw = nil
	} else {
		e.Raw = raw
	}
	return e, nil
}

// stripInfo removes the redundant top-level "info" key that pgn.PGN types
// embed in their JSON encoding (MessageInfo: timestamp, priority, pgn,
// source/target id). The envelope header fields already carry that data, so
// keeping it in the payload too would duplicate it on the wire. Done once at
// envelope creation, not per read.
func stripInfo(payload json.RawMessage) (json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	delete(m, "info")
	return json.Marshal(m)
}

// Info rebuilds the MessageInfo for encoding this envelope back onto a CAN bus.
func (e *Envelope) Info() pgn.MessageInfo {
	return pgn.MessageInfo{
		Timestamp: e.Timestamp,
		Priority:  pgn.Priority(e.Priority),
		PGN:       e.PGN,
		SourceId:  e.Source,
		TargetId:  pgn.Target(e.Dest),
	}
}

// PayloadMap returns the payload as a map for CEL evaluation, decoded once
// and cached. Callers must treat the returned map as read-only — it is
// shared across all subscribers of this envelope.
func (e *Envelope) PayloadMap() map[string]any {
	e.payloadOnce.Do(func() {
		m := map[string]any{}
		if len(e.Payload) > 0 && string(e.Payload) != "null" {
			_ = json.Unmarshal(e.Payload, &m)
		}
		e.payloadMap = m
	})
	return e.payloadMap
}

// SizeBytes approximates the stored size for buffer byte-limit accounting.
func (e *Envelope) SizeBytes() int {
	return len(e.Payload) + len(e.Raw) + 64
}
