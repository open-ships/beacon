package can

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRawBuf(id uint32, dlc uint8, data []byte) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[0:4], id)
	buf[4] = dlc
	copy(buf[8:], data)
	return buf
}

func TestParseRawFrameNormal(t *testing.T) {
	id := uint32(0x18FF0001) | canEFFFlag
	buf := makeRawBuf(id, 8, []byte{1, 2, 3, 4, 5, 6, 7, 8})

	frame, isErr := ParseRawFrame(buf)
	require.False(t, isErr)
	require.NotNil(t, frame)
	assert.Equal(t, uint32(0x18FF0001), frame.ID)
	assert.Equal(t, uint8(8), frame.Length)
	for i := 0; i < 8; i++ {
		assert.Equal(t, byte(i+1), frame.Data[i], "Data[%d]", i)
	}
}

func TestParseRawFrameStandardID(t *testing.T) {
	buf := makeRawBuf(0x123, 3, []byte{0xAA, 0xBB, 0xCC})

	frame, isErr := ParseRawFrame(buf)
	require.False(t, isErr)
	require.NotNil(t, frame)
	assert.Equal(t, uint32(0x123), frame.ID)
	assert.Equal(t, uint8(3), frame.Length)
}

func TestParseRawFrameErrorFrame(t *testing.T) {
	buf := makeRawBuf(canERRFlag|0x01, 8, nil)
	_, isErr := ParseRawFrame(buf)
	assert.True(t, isErr)
}
