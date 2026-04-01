// Package n2k wraps github.com/boatkit-io/n2k to convert raw CAN frames into
// decoded NMEA 2000 ParsedMessage values.
package n2k

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/boatkit-io/n2k/pkg/adapter/canadapter"
	"github.com/boatkit-io/n2k/pkg/pgn"
	"github.com/boatkit-io/n2k/pkg/pkt"
	"github.com/brutella/can"
	"github.com/sirupsen/logrus"
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
	// Payload is the boatkit-io decoded struct serialized to JSON for storage/filtering.
	Payload json.RawMessage `json:"payload"`
	// Raw is the original assembled bytes (for pass-through sinks).
	Raw []byte `json:"raw,omitempty"`
}

// structHandler collects the result from the PacketStruct pipeline.
type structHandler struct {
	ch chan any
}

func (h *structHandler) HandleStruct(s any) {
	select {
	case h.ch <- s:
	default:
	}
}

// Parser wraps the boatkit-io pipeline to convert CAN frames into ParsedMessages.
type Parser struct {
	mu      sync.Mutex
	adapter *canadapter.CANAdapter
	pktStr  *pkt.PacketStruct
	handler structHandler
}

// NewParser creates a new Parser.
func NewParser() *Parser {
	log := logrus.New()
	log.SetOutput(io.Discard)

	ca := canadapter.NewCANAdapter(log)
	ps := pkt.NewPacketStruct()
	ca.SetOutput(ps)

	sh := structHandler{ch: make(chan any, 1)}
	ps.SetOutput(&sh)

	return &Parser{
		adapter: ca,
		pktStr:  ps,
		handler: sh,
	}
}

// Parse converts a single CAN frame into a ParsedMessage.
// Fast-packet fragments that are not yet complete return nil, nil.
func (p *Parser) Parse(frame *can.Frame) (*ParsedMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Drain any stale result
	select {
	case <-p.handler.ch:
	default:
	}

	p.adapter.HandleMessage(frame)

	select {
	case result := <-p.handler.ch:
		return buildParsedMessage(result)
	default:
		// Fast packet fragment not yet complete
		return nil, nil
	}
}

func buildParsedMessage(result any) (*ParsedMessage, error) {
	switch v := result.(type) {
	case pgn.UnknownPGN:
		msg := &ParsedMessage{
			PGN:       v.Info.PGN,
			Source:    v.Info.SourceId,
			Dest:      v.Info.TargetId,
			Priority:  v.Info.Priority,
			Timestamp: v.Info.Timestamp,
			Payload:   json.RawMessage("null"),
			Raw:       append([]byte(nil), v.Data...),
		}
		return msg, nil

	default:
		// Known PGN struct — all generated structs have Info MessageInfo as first field.
		info := extractInfo(v)
		payload, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal PGN struct: %w", err)
		}
		msg := &ParsedMessage{
			PGN:       info.PGN,
			Source:    info.SourceId,
			Dest:      info.TargetId,
			Priority:  info.Priority,
			Timestamp: info.Timestamp,
			Payload:   json.RawMessage(payload),
		}
		return msg, nil
	}
}

var messageInfoType = reflect.TypeOf(pgn.MessageInfo{})

// extractInfo uses reflection to pull MessageInfo from a generated PGN struct.
// All generated structs have `Info MessageInfo` as their first exported field.
func extractInfo(v any) pgn.MessageInfo {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return pgn.MessageInfo{Timestamp: time.Now()}
	}
	// Look for first field of type pgn.MessageInfo
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
