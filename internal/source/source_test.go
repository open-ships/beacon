package source

import (
	"testing"

	"github.com/open-ships/beacon/internal/msg"
)

func TestHubFanOutAndUnsubscribe(t *testing.T) {
	h := newHub()
	a, unsubA := h.subscribe(4)
	b, _ := h.subscribe(4)

	h.publish(&msg.Envelope{PGN: 1})
	if e := <-a; e.PGN != 1 {
		t.Fatal("a missed message")
	}
	if e := <-b; e.PGN != 1 {
		t.Fatal("b missed message")
	}

	unsubA()
	h.publish(&msg.Envelope{PGN: 2})
	if e := <-b; e.PGN != 2 {
		t.Fatal("b missed second message")
	}
	select {
	case _, ok := <-a:
		if ok {
			t.Fatal("a received after unsubscribe")
		}
	default:
	}
}

func TestHubDropsWhenSubscriberFull(t *testing.T) {
	h := newHub()
	ch, _ := h.subscribe(1)
	h.publish(&msg.Envelope{PGN: 1})
	h.publish(&msg.Envelope{PGN: 2}) // must not block
	if e := <-ch; e.PGN != 1 {
		t.Fatal("first message lost")
	}
	select {
	case e := <-ch:
		t.Fatalf("unexpected second message %d (buffer 1 should have dropped it)", e.PGN)
	default:
	}
}
