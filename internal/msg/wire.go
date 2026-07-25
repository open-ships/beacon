package msg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/open-ships/n2k/pgn"

	"github.com/open-ships/beacon/internal/n2kcatalog"
)

// Metadata holds everything Beacon adds to an N2K message. The native n2k
// struct and raw CAN bytes are separate siblings.
type Metadata struct {
	Seq              int64                               `json:"id,omitempty"`
	ConnectorID      string                              `json:"connector,omitempty"`
	ObservedAt       time.Time                           `json:"observed_at"`
	Ingress          string                              `json:"ingress,omitempty"`
	OriginIngress    string                              `json:"origin_ingress,omitempty"`
	DeviceName       *uint64                             `json:"device_name,omitempty"`
	DeviceNameHex    string                              `json:"device_name_hex,omitempty"`
	PGNName          string                              `json:"pgn_name,omitempty"`
	Variant          string                              `json:"variant,omitempty"`
	Transport        string                              `json:"transport,omitempty"`
	ManufacturerCode *uint16                             `json:"manufacturer_code,omitempty"`
	Decode           DecodeInfo                          `json:"decode"`
	Physical         map[string]n2kcatalog.PhysicalField `json:"physical,omitempty"`
}

type wireEnvelope struct {
	Payload  json.RawMessage `json:"payload"`
	Metadata Metadata        `json:"metadata"`
	Raw      []byte          `json:"raw"`
}

// MarshalJSON emits the consumer contract shared by MQTT, SSE, WebSocket,
// TCP, NDJSON files, and persisted route buffers. Only payload and metadata
// plus the raw CAN bytes exist at the top level. Payload is the verbatim JSON
// representation of the native n2k struct.
func (e *Envelope) MarshalJSON() ([]byte, error) {
	payload, err := e.consumerPayload()
	if err != nil {
		return nil, err
	}
	return json.Marshal(wireEnvelope{
		Payload: payload,
		Metadata: Metadata{
			Seq:              e.Seq,
			ConnectorID:      e.ConnectorID,
			ObservedAt:       e.ObservedAt,
			Ingress:          e.Ingress,
			OriginIngress:    e.OriginIngress,
			DeviceName:       e.DeviceName,
			DeviceNameHex:    e.DeviceNameHex,
			PGNName:          e.PGNName,
			Variant:          e.Variant,
			Transport:        e.Transport,
			ManufacturerCode: e.ManufacturerCode,
			Decode:           e.Decode,
			Physical:         e.Physical,
		},
		Raw: e.Raw,
	})
}

// UnmarshalJSON accepts the same three-key consumer contract. CAN identity is
// restored from payload.info, so an upstream Beacon can ingest the document
// without duplicating N2K fields into Beacon metadata.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	var wire struct {
		Payload  json.RawMessage `json:"payload"`
		Metadata json.RawMessage `json:"metadata"`
		Raw      json.RawMessage `json:"raw"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if len(wire.Payload) == 0 {
		return errors.New(`beacon envelope is missing "payload"`)
	}
	if len(wire.Metadata) == 0 {
		return errors.New(`beacon envelope is missing "metadata"`)
	}
	if len(wire.Raw) == 0 {
		return errors.New(`beacon envelope is missing "raw"`)
	}

	var metadata Metadata
	if err := json.Unmarshal(wire.Metadata, &metadata); err != nil {
		return fmt.Errorf("decode beacon metadata: %w", err)
	}
	var payloadHeader struct {
		Info *pgn.MessageInfo `json:"info"`
	}
	if err := json.Unmarshal(wire.Payload, &payloadHeader); err != nil {
		return fmt.Errorf("decode N2K payload header: %w", err)
	}
	if payloadHeader.Info == nil {
		return errors.New(`beacon envelope payload is missing N2K "info"`)
	}
	var raw []byte
	if err := json.Unmarshal(wire.Raw, &raw); err != nil {
		return fmt.Errorf("decode raw CAN payload: %w", err)
	}

	info := *payloadHeader.Info
	priority := uint8(defaultPriority)
	if info.Priority != nil {
		priority = *info.Priority
	}
	dest := uint8(broadcastDest)
	if info.TargetId != nil {
		dest = *info.TargetId
	}

	*e = Envelope{
		Seq:              metadata.Seq,
		ConnectorID:      metadata.ConnectorID,
		PGN:              info.PGN,
		Source:           info.SourceId,
		Dest:             dest,
		Priority:         priority,
		Timestamp:        info.Timestamp,
		ObservedAt:       metadata.ObservedAt,
		Ingress:          metadata.Ingress,
		OriginIngress:    metadata.OriginIngress,
		DeviceName:       metadata.DeviceName,
		DeviceNameHex:    metadata.DeviceNameHex,
		PGNName:          metadata.PGNName,
		Variant:          metadata.Variant,
		Transport:        metadata.Transport,
		ManufacturerCode: metadata.ManufacturerCode,
		Decode:           metadata.Decode,
		Physical:         metadata.Physical,
		Payload:          bytes.Clone(wire.Payload),
		Raw:              bytes.Clone(raw),
	}
	return nil
}

// consumerPayload preserves a native n2k payload byte-for-byte. Hand-built
// envelopes used by internal callers may omit info; add it in that case so no
// producer can emit a payload that consumers cannot deserialize as an n2k
// struct.
func (e *Envelope) consumerPayload() (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if len(e.Payload) != 0 {
		if err := json.Unmarshal(e.Payload, &fields); err != nil {
			return nil, fmt.Errorf("decode N2K payload: %w", err)
		}
	}
	if fields == nil {
		fields = make(map[string]json.RawMessage)
	}
	if info, ok := fields["info"]; !ok || bytes.Equal(bytes.TrimSpace(info), []byte("null")) {
		rawInfo, err := json.Marshal(e.Info())
		if err != nil {
			return nil, fmt.Errorf("encode N2K payload info: %w", err)
		}
		fields["info"] = rawInfo
		return json.Marshal(fields)
	}
	return bytes.Clone(e.Payload), nil
}
