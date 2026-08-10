package netlimit

import (
	"net"
	"testing"
	"time"
)

func TestLimiterSharesCapacityAcrossListeners(t *testing.T) {
	limiter := New(1)
	firstRaw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	secondRaw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = firstRaw.Close()
		t.Fatal(err)
	}
	first := limiter.Wrap(firstRaw)
	second := limiter.Wrap(secondRaw)
	t.Cleanup(func() {
		_ = first.Close()
		_ = second.Close()
	})

	acceptedFirst := make(chan net.Conn, 1)
	acceptedSecond := make(chan net.Conn, 1)
	go func() {
		conn, _ := first.Accept()
		acceptedFirst <- conn
	}()

	client1, err := net.Dial("tcp", first.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client1.Close() })
	server1 := <-acceptedFirst
	if server1 == nil || limiter.InUse() != 1 {
		t.Fatalf("first accept = %v, in use = %d", server1, limiter.InUse())
	}
	go func() {
		conn, _ := second.Accept()
		acceptedSecond <- conn
	}()

	client2, err := net.Dial("tcp", second.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client2.Close() })
	select {
	case conn := <-acceptedSecond:
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatal("second listener accepted beyond the shared capacity")
	case <-time.After(50 * time.Millisecond):
	}

	if err := server1.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case server2 := <-acceptedSecond:
		if server2 == nil {
			t.Fatal("second accept returned nil after capacity was released")
		}
		_ = server2.Close()
	case <-time.After(time.Second):
		t.Fatal("second listener did not resume after capacity was released")
	}
}

func TestClosingListenerUnblocksCapacityWait(t *testing.T) {
	limiter := New(1)
	limiter.slots <- struct{}{}

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	limited := limiter.Wrap(raw)
	done := make(chan error, 1)
	go func() {
		_, err := limited.Accept()
		done <- err
	}()
	if err := limited.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Accept returned nil after listener close")
		}
	case <-time.After(time.Second):
		t.Fatal("listener close did not unblock a capacity wait")
	}
	<-limiter.slots
}
