// Package retry provides the bounded reconnect policy shared by long-lived
// vessel links. Equal jitter prevents several routes from hammering the same
// endpoint in lockstep after a long outage while retaining a deterministic
// upper bound on network and wake-up frequency.
package retry

import (
	"context"
	"math/rand"
	"time"
)

type Backoff struct {
	base    time.Duration
	max     time.Duration
	current time.Duration
	random  func(int64) int64
}

func NewBackoff(base, max time.Duration) *Backoff {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	return &Backoff{
		base: base, max: max, current: base,
		random: rand.Int63n, //nolint:gosec // scheduling jitter is not security-sensitive
	}
}

// Next returns equal-jitter delay in [current/2, current], then doubles the
// next ceiling up to max. A Backoff is owned by one retry loop; it is not
// intended for concurrent use.
func (b *Backoff) Next() time.Duration {
	ceiling := b.current
	half := ceiling / 2
	delay := half
	if span := int64(ceiling - half); span > 0 {
		delay += time.Duration(b.random(span + 1))
	}
	if b.current < b.max {
		if b.current > b.max/2 {
			b.current = b.max
		} else {
			b.current *= 2
		}
	}
	return delay
}

func (b *Backoff) Reset() { b.current = b.base }

func Sleep(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
