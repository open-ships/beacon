package filter_test

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/n2k"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeMsg(pgn uint32, source, dest, priority uint8, payload map[string]any) *n2k.ParsedMessage {
	p, _ := json.Marshal(payload)
	return &n2k.ParsedMessage{
		PGN:       pgn,
		Source:    source,
		Dest:      dest,
		Priority:  priority,
		Timestamp: time.Now(),
		Payload:   json.RawMessage(p),
	}
}

var testLog = slog.Default()

func TestCELFilter(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		msg     *n2k.ParsedMessage
		want    bool
		wantErr bool
	}{
		{
			name: "PGN equality match",
			expr: "msg.pgn == 127250",
			msg:  makeMsg(127250, 1, 255, 2, nil),
			want: true,
		},
		{
			name: "PGN equality no match",
			expr: "msg.pgn == 127250",
			msg:  makeMsg(128259, 1, 255, 2, nil),
			want: false,
		},
		{
			name: "PGN in list match",
			expr: "msg.pgn in [127250, 128259, 129026]",
			msg:  makeMsg(128259, 1, 255, 2, nil),
			want: true,
		},
		{
			name: "PGN in list no match",
			expr: "msg.pgn in [127250, 128259, 129026]",
			msg:  makeMsg(129029, 1, 255, 2, nil),
			want: false,
		},
		{
			name: "payload field threshold above",
			expr: `double(msg.payload.speed) > 2.0`,
			msg:  makeMsg(128259, 1, 255, 2, map[string]any{"speed": 3.5}),
			want: true,
		},
		{
			name: "payload field threshold below",
			expr: `double(msg.payload.speed) > 2.0`,
			msg:  makeMsg(128259, 1, 255, 2, map[string]any{"speed": 1.0}),
			want: false,
		},
		{
			name: "boolean AND match",
			expr: "msg.pgn == 127250 && msg.source == 1",
			msg:  makeMsg(127250, 1, 255, 2, nil),
			want: true,
		},
		{
			name: "boolean AND no match",
			expr: "msg.pgn == 127250 && msg.source == 2",
			msg:  makeMsg(127250, 1, 255, 2, nil),
			want: false,
		},
		{
			name: "boolean OR",
			expr: "msg.pgn == 127250 || msg.pgn == 128259",
			msg:  makeMsg(128259, 1, 255, 2, nil),
			want: true,
		},
		{
			name: "priority filter",
			expr: "msg.priority < 3",
			msg:  makeMsg(127250, 1, 255, 2, nil),
			want: true,
		},
		{
			name: "source exclusion",
			expr: "msg.source != 3",
			msg:  makeMsg(127250, 1, 255, 2, nil),
			want: true,
		},
		{
			name: "unknown payload field",
			expr: `has(msg.payload.nonexistent)`,
			msg:  makeMsg(127250, 1, 255, 2, nil),
			want: false,
		},
		{
			name:    "invalid expression",
			expr:    "msg.pgn === 127250",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := filter.NewCELFilter(tt.expr, testLog)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, f.Match(tt.msg))
		})
	}
}

func TestChainAND(t *testing.T) {
	chain, err := filter.Parse([]string{
		"msg.pgn == 127250",
		"msg.source == 1",
	}, testLog, nil, "")
	require.NoError(t, err)

	assert.True(t, chain.Match(makeMsg(127250, 1, 255, 2, nil)), "both filters should match")
	assert.False(t, chain.Match(makeMsg(127250, 2, 255, 2, nil)), "chain should fail when one filter fails")
}

func TestEmptyChainMatchesAll(t *testing.T) {
	chain, err := filter.Parse([]string{}, testLog, nil, "")
	require.NoError(t, err)
	assert.True(t, chain.Match(makeMsg(127250, 1, 255, 2, nil)))
}

func TestChainInvalidExpr(t *testing.T) {
	_, err := filter.Parse([]string{"msg.pgn === 127250"}, testLog, nil, "")
	assert.Error(t, err)
}

func TestCELFilter_NilPayload(t *testing.T) {
	f, err := filter.NewCELFilter("msg.pgn == 127250", testLog)
	require.NoError(t, err)

	msg := &n2k.ParsedMessage{PGN: 127250, Payload: nil}
	assert.True(t, f.Match(msg))
}
