package source

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
)

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
