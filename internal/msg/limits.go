package msg

import "fmt"

// Remote Envelope limits are deliberately generous relative to NMEA 2000's
// 1,785-byte ISO-TP payload maximum. They bound memory before untrusted
// software-facing sources can reach CEL, diagnostics, or retained storage.
const (
	MaxWireEnvelopeBytes = 256 << 10
	MaxPayloadBytes      = 128 << 10
	MaxRawPayloadBytes   = 1785
	MaxPhysicalFields    = 256
	MaxMissingFields     = 256
	MaxMetadataTextBytes = 1024
)

// ValidateRemote rejects an Envelope whose decoded shape exceeds the
// appliance-safe ingress budget. encodedBytes must be the original MQTT,
// WebSocket, or SSE JSON document size when known.
func ValidateRemote(e *Envelope, encodedBytes int) error {
	if e == nil {
		return fmt.Errorf("nil envelope")
	}
	if encodedBytes > MaxWireEnvelopeBytes {
		return fmt.Errorf("envelope is %d bytes; maximum is %d", encodedBytes, MaxWireEnvelopeBytes)
	}
	if len(e.Payload) > MaxPayloadBytes {
		return fmt.Errorf("payload is %d bytes; maximum is %d", len(e.Payload), MaxPayloadBytes)
	}
	if len(e.Raw) > MaxRawPayloadBytes {
		return fmt.Errorf("raw NMEA 2000 payload is %d bytes; maximum is %d", len(e.Raw), MaxRawPayloadBytes)
	}
	if len(e.Physical) > MaxPhysicalFields {
		return fmt.Errorf("physical field count is %d; maximum is %d", len(e.Physical), MaxPhysicalFields)
	}
	if len(e.Decode.Missing) > MaxMissingFields {
		return fmt.Errorf("missing-field count is %d; maximum is %d", len(e.Decode.Missing), MaxMissingFields)
	}
	for name, value := range map[string]string{
		"ingress": e.Ingress, "origin_ingress": e.OriginIngress,
		"device_name_hex": e.DeviceNameHex, "pgn_name": e.PGNName,
		"variant": e.Variant, "transport": e.Transport, "decode_status": e.Decode.Status,
	} {
		if len(value) > MaxMetadataTextBytes {
			return fmt.Errorf("%s is %d bytes; maximum is %d", name, len(value), MaxMetadataTextBytes)
		}
	}
	for name, field := range e.Physical {
		if len(name) > MaxMetadataTextBytes || len(field.Unit) > MaxMetadataTextBytes ||
			len(field.PhysicalQuantity) > MaxMetadataTextBytes {
			return fmt.Errorf("physical field metadata exceeds %d bytes", MaxMetadataTextBytes)
		}
	}
	for _, name := range e.Decode.Missing {
		if len(name) > MaxMetadataTextBytes {
			return fmt.Errorf("missing-field name exceeds %d bytes", MaxMetadataTextBytes)
		}
	}
	return nil
}
