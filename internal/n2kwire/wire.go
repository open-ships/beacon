// Package n2kwire reconstructs NMEA 2000 CAN frames from canonical message
// envelopes without changing their source address or payload.
package n2kwire

import (
	"errors"
	"fmt"

	"github.com/brutella/can"
	"github.com/open-ships/n2k/pgn"

	"github.com/open-ships/beacon/internal/msg"
)

const MaxFastPacketPayload = 6 + 31*7

var ErrUnsupportedTransport = errors.New("message requires an unsupported NMEA 2000 transport")

type streamKey struct {
	pgn    uint32
	source uint8
}

type Fragmenter struct{ sequence map[streamKey]uint8 }

func NewFragmenter() *Fragmenter { return &Fragmenter{sequence: map[streamKey]uint8{}} }

func (f *Fragmenter) Frames(e *msg.Envelope) ([]can.Frame, error) {
	if len(e.Raw) == 0 {
		return nil, fmt.Errorf("pgn %d has no raw payload", e.PGN)
	}
	id := BuildCANID(e.Priority, e.PGN, e.Source, e.Dest)
	if len(e.Raw) <= 8 && !IsFastPacket(e.PGN, len(e.Raw)) {
		return []can.Frame{frameSingle(id, e.Raw)}, nil
	}
	if len(e.Raw) > MaxFastPacketPayload {
		return nil, fmt.Errorf("pgn %d payload is %d bytes: %w", e.PGN, len(e.Raw), ErrUnsupportedTransport)
	}
	key := streamKey{pgn: e.PGN, source: e.Source}
	seq := f.sequence[key]
	f.sequence[key] = (seq + 1) % 8
	return FrameFastPacket(id, e.Raw, seq), nil
}

func BuildCANID(priority uint8, pgnNum uint32, source, dest uint8) uint32 {
	id := uint32(priority&0x07)<<26 | pgnNum<<8 | uint32(source)
	if uint8((pgnNum>>8)&0xFF) < 240 {
		id |= uint32(dest) << 8
	}
	return id
}

func IsFastPacket(pgnNum uint32, payloadLen int) bool {
	if infos, ok := pgn.PgnInfoLookup[pgnNum]; ok {
		for _, info := range infos {
			if info.Fast {
				return true
			}
		}
		return false
	}
	// The two proprietary fast-packet ranges are knowable even when the
	// manufacturer-specific variant is absent from the catalog.
	if pgnNum >= 0x1EF00 && pgnNum <= 0x1EFFF {
		return true
	}
	if pgnNum >= 0x1FF00 && pgnNum <= 0x1FFFF {
		return true
	}
	return payloadLen > 8
}

func frameSingle(id uint32, payload []byte) can.Frame {
	f := can.Frame{ID: id, Length: 8}
	for i := range f.Data {
		f.Data[i] = 0xFF
	}
	copy(f.Data[:], payload)
	return f
}

func FrameFastPacket(id uint32, payload []byte, sequence uint8) []can.Frame {
	first := can.Frame{ID: id, Length: 8}
	for i := range first.Data {
		first.Data[i] = 0xFF
	}
	first.Data[0] = (sequence & 7) << 5
	first.Data[1] = uint8(len(payload))
	n := min(len(payload), 6)
	copy(first.Data[2:], payload[:n])
	frames := []can.Frame{first}
	for offset, number := n, uint8(1); offset < len(payload); number++ {
		frame := can.Frame{ID: id, Length: 8}
		for i := range frame.Data {
			frame.Data[i] = 0xFF
		}
		frame.Data[0] = (sequence&7)<<5 | (number & 0x1F)
		n = min(len(payload)-offset, 7)
		copy(frame.Data[1:], payload[offset:offset+n])
		offset += n
		frames = append(frames, frame)
	}
	return frames
}

func IsManagementPGN(pgnNum uint32) bool {
	switch pgnNum {
	case 59392, 59904, 60160, 60416, 60928, 126208, 126464, 126993, 126996, 126998:
		return true
	default:
		return false
	}
}
