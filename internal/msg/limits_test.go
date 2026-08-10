package msg

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/open-ships/beacon/internal/n2kcatalog"
)

func TestValidateRemoteEnvelopeLimits(t *testing.T) {
	valid := &Envelope{Payload: json.RawMessage(`{"info":{"pgn":1,"sourceId":1}}`), Raw: []byte{1}}
	if err := ValidateRemote(valid, 100); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}

	tests := []struct {
		name string
		env  *Envelope
		size int
	}{
		{"wire bytes", valid, MaxWireEnvelopeBytes + 1},
		{"payload", &Envelope{Payload: json.RawMessage(strings.Repeat(" ", MaxPayloadBytes+1))}, 0},
		{"raw", &Envelope{Raw: make([]byte, MaxRawPayloadBytes+1)}, 0},
		{"physical", &Envelope{Physical: func() map[string]n2kcatalog.PhysicalField {
			fields := make(map[string]n2kcatalog.PhysicalField, MaxPhysicalFields+1)
			for i := 0; i <= MaxPhysicalFields; i++ {
				fields[string(rune(i+1))] = n2kcatalog.PhysicalField{}
			}
			return fields
		}()}, 0},
		{"metadata", &Envelope{Ingress: strings.Repeat("x", MaxMetadataTextBytes+1)}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateRemote(tc.env, tc.size); err == nil {
				t.Fatal("oversized envelope accepted")
			}
		})
	}
}
