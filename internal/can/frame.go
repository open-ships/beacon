package can

import (
	"encoding/binary"

	"github.com/brutella/can"
)

const (
	canEFFFlag = 0x80000000
	canRTRFlag = 0x40000000
	canERRFlag = 0x20000000
	canEFFMask = 0x1FFFFFFF
)

// ParseRawFrame parses a raw CAN frame byte sequence into a can.Frame.
// It returns the frame and a boolean indicating whether it is an error frame.
func ParseRawFrame(buf []byte) (*can.Frame, bool) {
	id := binary.LittleEndian.Uint32(buf[0:4])

	if id&canERRFlag != 0 {
		return nil, true
	}

	actualID := id
	if id&canEFFFlag != 0 {
		actualID = id & canEFFMask
	}

	dlc := buf[4]
	var data [8]uint8
	copy(data[:], buf[8:8+dlc])

	frame := &can.Frame{
		ID:     actualID,
		Length: dlc,
		Data:   data,
	}
	return frame, false
}
