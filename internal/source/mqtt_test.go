package source

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
)

func TestMQTTHandshakeTimeoutClosesStalledConnection(t *testing.T) {
	oldTimeout := mqttHandshakeTimeout
	mqttHandshakeTimeout = 100 * time.Millisecond
	t.Cleanup(func() { mqttHandshakeTimeout = oldTimeout })
	for _, stage := range []string{"connect", "subscribe"} {
		t.Run(stage, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			closed := make(chan struct{})
			go func() {
				defer close(closed)
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				r := bufio.NewReader(conn)
				for {
					header, _, err := readMQTTPacket(r)
					if err != nil || header>>4 == 14 {
						return
					}
					if header>>4 == 1 && stage == "subscribe" {
						_, _ = conn.Write([]byte{0x20, 2, 0, 0})
					}
					if header>>4 == 12 {
						_, _ = conn.Write([]byte{0xD0, 0})
					}
				}
			}()
			connected := false
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = runMQTT(ctx, model.Source{ID: "timeout", URL: "mqtt://" + ln.Addr().String(), Topic: "beacon/input"}, func(*msg.Envelope) {}, func() { connected = true })
			if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil || connected {
				t.Fatalf("handshake = %v, parent error %v, connected %v", err, ctx.Err(), connected)
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("timed-out client retained broker connection")
			}
		})
	}
}

func TestMQTTStopWaitsForActiveDeliveryCallback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		r := bufio.NewReader(conn)
		for {
			header, body, err := readMQTTPacket(r)
			if err != nil || header>>4 == 14 {
				return
			}
			switch header >> 4 {
			case 1:
				_, _ = conn.Write([]byte{0x20, 2, 0, 0})
			case 8:
				if len(body) < 2 {
					return
				}
				_, _ = conn.Write([]byte{0x90, 3, body[0], body[1], 0})
				payload := append([]byte{0, 1, 't'}, envelopeJSON(127250)...)
				_, _ = conn.Write(mqttPacket(0x30, payload))
			}
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		done <- runMQTT(ctx, model.Source{ID: "callback", URL: "mqtt://" + ln.Addr().String(), Topic: "t"}, func(*msg.Envelope) {
			close(entered)
			<-release
		}, func() {})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback never started")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("source returned while delivery callback could still write metrics: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("source did not finish after callback retired")
	}
}

func readMQTTPacket(r *bufio.Reader) (byte, []byte, error) {
	header, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	remaining, multiplier := 0, 1
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		remaining += int(b&127) * multiplier
		if b&128 == 0 {
			break
		}
		multiplier *= 128
	}
	payload := make([]byte, remaining)
	_, err = io.ReadFull(r, payload)
	return header, payload, err
}

func mqttPacket(header byte, payload []byte) []byte {
	out := []byte{header}
	remaining := len(payload)
	for {
		b := byte(remaining % 128)
		remaining /= 128
		if remaining > 0 {
			b |= 128
		}
		out = append(out, b)
		if remaining == 0 {
			return append(out, payload...)
		}
	}
}

func TestMQTTSourceConnectsSubscribesAndReceives(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		r := bufio.NewReader(conn)
		header, _, err := readMQTTPacket(r)
		if err != nil || header>>4 != 1 { // CONNECT
			serverErr <- err
			return
		}
		if _, err := conn.Write([]byte{0x20, 0x02, 0x00, 0x00}); err != nil { // CONNACK
			serverErr <- err
			return
		}
		header, subscribe, err := readMQTTPacket(r)
		if err != nil || header>>4 != 8 || len(subscribe) < 2 { // SUBSCRIBE
			serverErr <- err
			return
		}
		if _, err := conn.Write([]byte{0x90, 0x03, subscribe[0], subscribe[1], 0x00}); err != nil { // SUBACK QoS 0
			serverErr <- err
			return
		}
		topic := []byte("beacon/input")
		body := make([]byte, 2+len(topic))
		binary.BigEndian.PutUint16(body[:2], uint16(len(topic)))
		copy(body[2:], topic)
		body = append(body, envelopeJSON(128259)...)
		if _, err := conn.Write(mqttPacket(0x30, body)); err != nil { // QoS 0 PUBLISH
			serverErr <- err
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Source{
		ID: "mqtt-in", Type: model.SourceMQTT,
		URL: "mqtt://" + ln.Addr().String(), Topic: "beacon/input",
	}, nil, slog.Default(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	ch, unsub := rt.Subscribe(1)
	defer unsub()

	select {
	case e := <-ch:
		if e.PGN != 128259 {
			t.Fatalf("PGN = %d, want 128259", e.PGN)
		}
	case err := <-serverErr:
		if err != nil {
			t.Fatalf("broker: %v", err)
		}
		t.Fatal("broker completed without source message")
	case <-time.After(2 * time.Second):
		t.Fatal("source did not receive MQTT publish")
	}
}
