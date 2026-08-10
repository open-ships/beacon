// Package netlimit bounds the number of connections accepted across one or
// more listeners. Pending clients remain in the kernel listen backlog and do
// not consume a process file descriptor until a shared slot is available.
package netlimit

import (
	"net"
	"sync"
	"sync/atomic"
)

// Limiter is a shared accepted-connection budget. Wrap every listener that
// should draw from the same process-wide allowance.
type Limiter struct {
	slots chan struct{}
	inUse atomic.Int64
}

func New(max int) *Limiter {
	if max < 1 {
		panic("netlimit: maximum must be positive")
	}
	return &Limiter{slots: make(chan struct{}, max)}
}

func (l *Limiter) Capacity() int { return cap(l.slots) }
func (l *Limiter) InUse() int    { return int(l.inUse.Load()) }

// Wrap returns a listener whose accepted connections hold one shared slot
// until Close. The slot is acquired before Accept, so accepted process FDs can
// never exceed Capacity even when multiple wrapped listeners are serving.
func (l *Limiter) Wrap(inner net.Listener) net.Listener {
	return &listener{Listener: inner, limiter: l, closed: make(chan struct{})}
}

type listener struct {
	net.Listener
	limiter *Limiter
	closed  chan struct{}
	once    sync.Once
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case l.limiter.slots <- struct{}{}:
	case <-l.closed:
		return nil, net.ErrClosed
	}

	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.limiter.slots
		return nil, err
	}
	l.limiter.inUse.Add(1)
	return &connWithSlot{Conn: conn, release: func() {
		l.limiter.inUse.Add(-1)
		<-l.limiter.slots
	}}, nil
}

func (l *listener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return l.Listener.Close()
}

type connWithSlot struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *connWithSlot) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
