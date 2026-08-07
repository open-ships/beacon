package stats

import (
	"encoding/json"
	"sync"

	"github.com/open-ships/beacon/internal/msg"
)

// SubscribeStream subscribes to future source-received or sink-sent
// envelopes. It is an observability tap: publishers never block and drop a
// older preview data when a subscriber cannot keep up. Each message is the
// canonical three-key consumer envelope JSON.
func (r *Registry) SubscribeStream(kind, id string, buffer int) (<-chan []byte, func()) {
	if buffer < 1 {
		buffer = 1
	}
	if r == nil || id == "" || (kind != "source" && kind != "sink") {
		ch := make(chan []byte)
		close(ch)
		return ch, func() {}
	}

	key := streamKey(kind, id)
	r.mu.Lock()
	subscriberID := r.nextStreamSubscriber
	r.nextStreamSubscriber++
	ch := make(chan []byte, buffer)
	subscribers := r.streamSubscribers[key]
	if subscribers == nil {
		subscribers = map[uint64]chan []byte{}
		r.streamSubscribers[key] = subscribers
	}
	subscribers[subscriberID] = ch
	r.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			r.mu.Lock()
			subscribers := r.streamSubscribers[key]
			if current, ok := subscribers[subscriberID]; ok {
				delete(subscribers, subscriberID)
				close(current)
			}
			if len(subscribers) == 0 {
				delete(r.streamSubscribers, key)
			}
			r.mu.Unlock()
		})
	}
}

func (r *Registry) publishStream(kind, id string, envelope *msg.Envelope) {
	if r == nil || envelope == nil {
		return
	}
	key := streamKey(kind, id)

	// Avoid serialization on the normal data path when nobody is watching.
	r.mu.Lock()
	hasSubscribers := len(r.streamSubscribers[key]) != 0
	r.mu.Unlock()
	if !hasSubscribers {
		return
	}

	document, err := json.Marshal(envelope)
	if err != nil {
		return
	}

	r.mu.Lock()
	for _, subscriber := range r.streamSubscribers[key] {
		select {
		case subscriber <- document:
		default:
			// Preview streams are latest-biased: discard one stale document and
			// offer the newest without ever applying backpressure to traffic.
			select {
			case <-subscriber:
			default:
			}
			select {
			case subscriber <- document:
			default:
			}
		}
	}
	r.mu.Unlock()
}

func (r *Registry) closeStreamLocked(kind, id string) {
	key := streamKey(kind, id)
	for subscriberID, subscriber := range r.streamSubscribers[key] {
		delete(r.streamSubscribers[key], subscriberID)
		close(subscriber)
	}
	delete(r.streamSubscribers, key)
}

func streamKey(kind, id string) string {
	return kind + ":" + id
}
