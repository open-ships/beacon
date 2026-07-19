package n2kwire

import (
	"errors"
	"testing"

	"github.com/open-ships/beacon/internal/msg"
)

func TestFramesPreserveCANIdentityAndUnknownPayload(t *testing.T) {
	f := NewFragmenter()
	e := &msg.Envelope{PGN: 130999, Priority: 2, Source: 12, Dest: 255,
		Raw: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}}
	frames, err := f.Frames(e)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2", len(frames))
	}
	if frames[0].ID != BuildCANID(2, 130999, 12, 255) {
		t.Fatalf("id = %08x", frames[0].ID)
	}
	if frames[0].Data[2] != 1 || frames[1].Data[1] != 7 || frames[1].Data[3] != 9 {
		t.Fatalf("payload not preserved: %+v %+v", frames[0].Data, frames[1].Data)
	}
}

func TestFramesRejectUnsupportedLargeTransport(t *testing.T) {
	_, err := NewFragmenter().Frames(&msg.Envelope{PGN: 130000, Raw: make([]byte, 224)})
	if !errors.Is(err, ErrUnsupportedTransport) {
		t.Fatalf("err = %v", err)
	}
}

func TestFrameFastPacketRejectsOversizedPayload(t *testing.T) {
	if frames := FrameFastPacket(1, make([]byte, MaxFastPacketPayload+1), 0); frames != nil {
		t.Fatalf("frames = %d, want nil for oversized payload", len(frames))
	}
}
