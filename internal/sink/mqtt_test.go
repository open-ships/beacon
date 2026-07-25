package sink

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

type mqttPacket struct {
	header  byte
	payload []byte
}

func readMQTTPacket(r *bufio.Reader) (mqttPacket, error) {
	header, err := r.ReadByte()
	if err != nil {
		return mqttPacket{}, err
	}
	remaining, multiplier := 0, 1
	for {
		b, err := r.ReadByte()
		if err != nil {
			return mqttPacket{}, err
		}
		remaining += int(b&127) * multiplier
		if b&128 == 0 {
			break
		}
		multiplier *= 128
	}
	payload := make([]byte, remaining)
	_, err = io.ReadFull(r, payload)
	return mqttPacket{header: header, payload: payload}, err
}

func mqttPublishBody(p mqttPacket) (topic string, body []byte, ok bool) {
	if p.header>>4 != 3 || len(p.payload) < 2 {
		return "", nil, false
	}
	n := int(binary.BigEndian.Uint16(p.payload[:2]))
	if len(p.payload) < 2+n {
		return "", nil, false
	}
	return string(p.payload[2 : 2+n]), p.payload[2+n:], true
}

func TestMQTTSinkBroadcastWithoutBrokerReportsDegraded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Sink{
		ID: "mqtt", Name: "MQTT", Type: model.SinkMQTT, Enabled: true,
		URL: "mqtt://127.0.0.1:1", Topic: "beacon/test",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	rt.(Broadcaster).Broadcast([]queue.Entry{entry("", 0, 127250)})
	state, stateErr := rt.State()
	if state != "degraded" || stateErr == nil {
		t.Fatalf("state = %q/%v, want degraded/non-nil error", state, stateErr)
	}
}

func TestMQTTSinkConnectsAndPublishes(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	published := make(chan mqttPacket, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		r := bufio.NewReader(conn)
		packet, err := readMQTTPacket(r)
		if err != nil || packet.header>>4 != 1 { // CONNECT
			return
		}
		if _, err := conn.Write([]byte{0x20, 0x02, 0x00, 0x00}); err != nil { // CONNACK
			return
		}
		for {
			packet, err = readMQTTPacket(r)
			if err != nil {
				return
			}
			if packet.header>>4 == 3 { // PUBLISH
				published <- packet
				return
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Sink{
		ID: "mqtt-live", Type: model.SinkMQTT,
		URL: "mqtt://" + ln.Addr().String(), Topic: "beacon/test",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if state, _ := rt.State(); state == "up" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if state, stateErr := rt.State(); state != "up" {
		t.Fatalf("state = %q/%v, want up", state, stateErr)
	}

	rt.(Broadcaster).Broadcast([]queue.Entry{entry("", 0, 127250)})
	select {
	case packet := <-published:
		topic, body, ok := mqttPublishBody(packet)
		if !ok {
			t.Fatalf("invalid publish packet: %#v", packet)
		}
		if topic != "beacon/test" {
			t.Fatalf("publish = topic %q body %s", topic, body)
		}
		var event map[string]any
		if err := json.Unmarshal(body, &event); err != nil {
			t.Fatalf("MQTT body is not JSON: %v", err)
		}
		if consumerEnvelopePGN(t, event) != 127250 {
			t.Fatalf("publish = topic %q body %s", topic, body)
		}
		if _, ok := event["raw"].(string); !ok {
			t.Fatalf("MQTT raw CAN bytes are not a top-level base64 value: %s", body)
		}
		assertNativeMessageInfoPreserved(t, event)
	case <-time.After(time.Second):
		t.Fatal("broker did not receive publish")
	}
}

func TestMQTTSinkStopCancelsInitialConnectRetry(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	rt, err := New(context.Background(), model.Sink{
		ID: "mqtt-stopped", Type: model.SinkMQTT,
		URL: "mqtt://" + addr, Topic: "beacon/test",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	rt.Stop()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if err := ln.(*net.TCPListener).SetDeadline(time.Now().Add(2300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	conn, err := ln.Accept()
	if err == nil {
		_ = conn.Close()
		t.Fatal("stopped MQTT sink retried and connected")
	}
	if nerr, ok := err.(net.Error); !ok || !nerr.Timeout() {
		t.Fatalf("accept error = %v, want timeout", err)
	}
}
