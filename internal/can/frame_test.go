package can

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRawFrame_ZeroLengthData(t *testing.T) {
	buf := makeRawBuf(0x100, 0, nil)
	frame, isErr := ParseRawFrame(buf)
	require.False(t, isErr)
	require.NotNil(t, frame)
	assert.Equal(t, uint8(0), frame.Length)
}

func TestParseRawFrame_EFFAndERRFlags(t *testing.T) {
	// Both flags set — ERR takes priority
	buf := makeRawBuf(canEFFFlag|canERRFlag, 0, nil)
	_, isErr := ParseRawFrame(buf)
	assert.True(t, isErr)
}

func TestParseRawFrame_DataCopied(t *testing.T) {
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04}
	buf := makeRawBuf(0x42, 8, data)
	frame, isErr := ParseRawFrame(buf)
	require.False(t, isErr)
	require.NotNil(t, frame)
	var want [8]uint8
	copy(want[:], data)
	assert.Equal(t, want, frame.Data)
}
