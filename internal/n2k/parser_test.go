package n2k

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/boatkit-io/n2k/pkg/pgn"
	"github.com/brutella/can"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildParsedMessageUnknownPGN(t *testing.T) {
	ts := time.Now()
	unknown := pgn.UnknownPGN{
		Info: pgn.MessageInfo{
			PGN:       129029,
			SourceId:  1,
			TargetId:  255,
			Priority:  3,
			Timestamp: ts,
		},
		Data: []byte{0x01, 0x02, 0x03},
	}

	msg, err := buildParsedMessage(unknown)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, uint32(129029), msg.PGN)
	assert.Equal(t, uint8(1), msg.Source)
	assert.Equal(t, uint8(255), msg.Dest)
	assert.Equal(t, uint8(3), msg.Priority)
	assert.Equal(t, json.RawMessage("null"), msg.Payload)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, msg.Raw)
}

func TestBuildParsedMessageKnownPGN(t *testing.T) {
	type testPGN struct {
		Info    pgn.MessageInfo `json:"-"`
		Heading float64         `json:"heading"`
	}

	ts := time.Now()
	s := testPGN{
		Info: pgn.MessageInfo{
			PGN:       127250,
			SourceId:  1,
			TargetId:  255,
			Priority:  2,
			Timestamp: ts,
		},
		Heading: 1.57,
	}

	msg, err := buildParsedMessage(s)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, uint32(127250), msg.PGN)
	assert.Equal(t, uint8(1), msg.Source)
	assert.NotNil(t, msg.Payload)

	var p map[string]any
	require.NoError(t, json.Unmarshal(msg.Payload, &p))
	assert.Equal(t, 1.57, p["heading"])
}

func TestExtractInfoFromStruct(t *testing.T) {
	type testStruct struct {
		Info pgn.MessageInfo
		Val  float64
	}

	ts := time.Now()
	s := testStruct{
		Info: pgn.MessageInfo{
			PGN:       127250,
			SourceId:  2,
			TargetId:  255,
			Priority:  2,
			Timestamp: ts,
		},
		Val: 1.57,
	}

	info := extractInfo(s)
	assert.Equal(t, uint32(127250), info.PGN)
	assert.Equal(t, uint8(2), info.SourceId)
	assert.Equal(t, ts, info.Timestamp)
}

func TestExtractInfoFromPointer(t *testing.T) {
	type testStruct struct {
		Info pgn.MessageInfo
	}

	s := &testStruct{Info: pgn.MessageInfo{PGN: 128259}}
	info := extractInfo(s)
	assert.Equal(t, uint32(128259), info.PGN)
}

func TestExtractInfoNonStruct(t *testing.T) {
	info := extractInfo("not a struct")
	assert.False(t, info.Timestamp.IsZero(), "should return default with non-zero timestamp")
}

func TestNewParser(t *testing.T) {
	p := NewParser()
	assert.NotNil(t, p)
}

func TestParseNilResult(t *testing.T) {
	p := NewParser()
	frame := &can.Frame{
		ID:     0x18EEFF01,
		Length: 8,
		Data:   [8]uint8{0x40, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
	}

	msg, err := p.Parse(frame)
	assert.NoError(t, err)
	// Either nil (incomplete fast-packet) or a valid message — both acceptable
	_ = msg
}

func TestParsedMessageJSON(t *testing.T) {
	ts := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	msg := &ParsedMessage{
		ID:        42,
		PGN:       127250,
		Source:    1,
		Dest:      255,
		Priority:  2,
		Timestamp: ts,
		Payload:   json.RawMessage(`{"heading":1.57}`),
	}

	data, err := json.Marshal(msg)
	require.NoError(t, err)

	var got ParsedMessage
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, int64(42), got.ID)
	assert.Equal(t, uint32(127250), got.PGN)
	assert.Equal(t, uint8(1), got.Source)
	assert.True(t, got.Timestamp.Equal(ts))

	var p map[string]any
	require.NoError(t, json.Unmarshal(got.Payload, &p))
	assert.Equal(t, 1.57, p["heading"])
}
