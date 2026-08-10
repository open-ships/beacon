package sink

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
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

func mqttPublishBody(p mqttPacket) (topic string, packetID uint16, qos byte, body []byte, ok bool) {
	if p.header>>4 != 3 || len(p.payload) < 2 {
		return "", 0, 0, nil, false
	}
	n := int(binary.BigEndian.Uint16(p.payload[:2]))
	if len(p.payload) < 2+n {
		return "", 0, 0, nil, false
	}
	qos = (p.header >> 1) & 0x03
	offset := 2 + n
	if qos > 0 {
		if len(p.payload) < offset+2 {
			return "", 0, 0, nil, false
		}
		packetID = binary.BigEndian.Uint16(p.payload[offset : offset+2])
		offset += 2
	}
	return string(p.payload[2 : 2+n]), packetID, qos, p.payload[offset:], true
}

func TestMQTTSinkPushWithoutBrokerReportsDegraded(t *testing.T) {
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

	if got := DeliveryClassOf(rt); got != DeliveryConfirmed {
		t.Fatalf("delivery class = %q, want %q", got, DeliveryConfirmed)
	}
	if err := rt.(Pusher).Push(context.Background(), entry("", 0, 127250).Env); err == nil {
		t.Fatal("Push without a broker succeeded")
	}
	state, stateErr := rt.State()
	if state != "degraded" || stateErr == nil {
		t.Fatalf("state = %q/%v, want degraded/non-nil error", state, stateErr)
	}
}

func TestMQTTSinkWaitsForQoS1PUBACK(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	published := make(chan mqttPacket, 1)
	ack := make(chan struct{})
	var ackOnce sync.Once
	releaseACK := func() { ackOnce.Do(func() { close(ack) }) }
	defer releaseACK()
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
				<-ack
				_, packetID, qos, _, ok := mqttPublishBody(packet)
				if !ok || qos != 1 {
					return
				}
				_, _ = conn.Write([]byte{0x40, 0x02, byte(packetID >> 8), byte(packetID)}) // PUBACK
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

	if got := DeliveryClassOf(rt); got != DeliveryConfirmed {
		t.Fatalf("delivery class = %q, want %q", got, DeliveryConfirmed)
	}
	pushDone := make(chan error, 1)
	go func() {
		pushDone <- rt.(Pusher).Push(context.Background(), entry("", 0, 127250).Env)
	}()
	select {
	case packet := <-published:
		topic, packetID, qos, body, ok := mqttPublishBody(packet)
		if !ok {
			t.Fatalf("invalid publish packet: %#v", packet)
		}
		if qos != 1 || packetID == 0 {
			t.Fatalf("publish QoS/packet id = %d/%d, want QoS 1 and a packet id", qos, packetID)
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
		select {
		case err := <-pushDone:
			t.Fatalf("Push completed before PUBACK: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		releaseACK()
		select {
		case err := <-pushDone:
			if err != nil {
				t.Fatalf("Push after PUBACK = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Push did not complete after PUBACK")
		}
	case <-time.After(time.Second):
		t.Fatal("broker did not receive publish")
	}
}

func TestMQTTSinkConnectionLossCannotConfirmPublishWithoutPUBACK(t *testing.T) {
	oldMin, oldMax := mqttReconnectMin, mqttReconnectMax
	mqttReconnectMin, mqttReconnectMax = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { mqttReconnectMin, mqttReconnectMax = oldMin, oldMax })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	firstPublish := make(chan mqttPacket, 1)
	secondPublish := make(chan mqttPacket, 1)
	secondACK := make(chan struct{})
	brokerErr := make(chan error, 1)
	reportBrokerErr := func(err error) {
		select {
		case brokerErr <- err:
		default:
		}
	}
	handshake := func(conn net.Conn) (*bufio.Reader, error) {
		r := bufio.NewReader(conn)
		packet, err := readMQTTPacket(r)
		if err != nil {
			return nil, err
		}
		if packet.header>>4 != 1 {
			return nil, io.ErrUnexpectedEOF
		}
		if _, err := conn.Write([]byte{0x20, 0x02, 0x00, 0x00}); err != nil {
			return nil, err
		}
		return r, nil
	}
	go func() {
		first, err := ln.Accept()
		if err != nil {
			reportBrokerErr(err)
			return
		}
		r, err := handshake(first)
		if err != nil {
			_ = first.Close()
			reportBrokerErr(err)
			return
		}
		for {
			packet, err := readMQTTPacket(r)
			if err != nil {
				_ = first.Close()
				reportBrokerErr(err)
				return
			}
			if packet.header>>4 == 3 {
				firstPublish <- packet
				_ = first.Close() // lose the connection without PUBACK
				break
			}
		}

		second, err := ln.Accept()
		if err != nil {
			reportBrokerErr(err)
			return
		}
		defer func() { _ = second.Close() }()
		r, err = handshake(second)
		if err != nil {
			reportBrokerErr(err)
			return
		}
		for {
			packet, err := readMQTTPacket(r)
			if err != nil {
				reportBrokerErr(err)
				return
			}
			if packet.header>>4 != 3 {
				continue
			}
			secondPublish <- packet
			<-secondACK
			_, packetID, qos, _, ok := mqttPublishBody(packet)
			if !ok || qos != 1 {
				reportBrokerErr(io.ErrUnexpectedEOF)
				return
			}
			_, err = second.Write([]byte{0x40, 0x02, byte(packetID >> 8), byte(packetID)})
			if err != nil {
				reportBrokerErr(err)
			}
			return
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Sink{
		ID: "mqtt-loss", Type: model.SinkMQTT,
		URL: "mqtt://" + ln.Addr().String(), Topic: "beacon/test",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	waitFor(t, time.Second, "initial MQTT connection", func() bool {
		state, _ := rt.State()
		return state == "up"
	})
	sink := rt.(*mqttSink)
	firstGeneration := sink.activeGeneration()
	if firstGeneration == nil {
		t.Fatal("initial MQTT connection has no active generation")
	}

	firstResult := make(chan error, 1)
	go func() { firstResult <- rt.(Pusher).Push(context.Background(), entry("", 0, 127250).Env) }()
	select {
	case <-firstPublish:
	case err := <-brokerErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("broker did not receive first publish")
	}
	select {
	case err := <-firstResult:
		if err == nil {
			t.Fatal("connection loss confirmed publish without PUBACK")
		}
	case <-time.After(time.Second):
		t.Fatal("connection loss did not fail the ambiguous publish")
	}
	waitFor(t, time.Second, "MQTT reconnect after ambiguous publish", func() bool {
		state, _ := rt.State()
		return state == "up"
	})
	secondGeneration := sink.activeGeneration()
	if secondGeneration == nil {
		t.Fatal("reconnected MQTT sink has no active generation")
	}
	if secondGeneration.id <= firstGeneration.id {
		t.Fatalf("reconnect generation = %d, want > %d", secondGeneration.id, firstGeneration.id)
	}
	if secondGeneration.client == firstGeneration.client {
		t.Fatal("MQTT reconnect reused the previous Paho client")
	}

	secondResult := make(chan error, 1)
	go func() { secondResult <- rt.(Pusher).Push(context.Background(), entry("", 0, 127250).Env) }()
	select {
	case <-secondPublish:
	case err := <-brokerErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("broker did not receive retried publish")
	}
	select {
	case err := <-secondResult:
		t.Fatalf("retried Push completed before PUBACK: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(secondACK)
	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatalf("retried Push after PUBACK = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retried Push did not complete after PUBACK")
	}
}

func TestMQTTSinkRepeatedPublishLossRacesUseFreshGenerations(t *testing.T) {
	oldMin, oldMax := mqttReconnectMin, mqttReconnectMax
	mqttReconnectMin, mqttReconnectMax = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { mqttReconnectMin, mqttReconnectMax = oldMin, oldMax })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	const droppedPublishes = 5
	published := make(chan int, droppedPublishes+1)
	finalACK := make(chan struct{})
	var finalACKOnce sync.Once
	releaseFinalACK := func() { finalACKOnce.Do(func() { close(finalACK) }) }
	defer releaseFinalACK()
	brokerErr := make(chan error, 1)
	brokerDone := make(chan struct{})
	reportBrokerErr := func(err error) {
		select {
		case brokerErr <- err:
		default:
		}
	}

	go func() {
		defer close(brokerDone)
		for connection := 0; connection <= droppedPublishes; connection++ {
			conn, err := ln.Accept()
			if err != nil {
				reportBrokerErr(err)
				return
			}
			r := bufio.NewReader(conn)
			packet, err := readMQTTPacket(r)
			if err != nil || packet.header>>4 != 1 {
				_ = conn.Close()
				if err == nil {
					err = io.ErrUnexpectedEOF
				}
				reportBrokerErr(err)
				return
			}
			if _, err := conn.Write([]byte{0x20, 0x02, 0x00, 0x00}); err != nil {
				_ = conn.Close()
				reportBrokerErr(err)
				return
			}

			for {
				packet, err = readMQTTPacket(r)
				if err != nil {
					_ = conn.Close()
					reportBrokerErr(err)
					return
				}
				if packet.header>>4 != 3 {
					continue
				}
				published <- connection
				if connection < droppedPublishes {
					// Close immediately after accepting the bytes but before PUBACK.
					// This repeatedly exercises Paho's loss-cleanup/token-registration
					// race, which must fail the Push and retire this generation.
					_ = conn.Close()
					break
				}

				<-finalACK
				_, packetID, qos, _, ok := mqttPublishBody(packet)
				if !ok || qos != 1 || packetID == 0 {
					_ = conn.Close()
					reportBrokerErr(io.ErrUnexpectedEOF)
					return
				}
				_, err = conn.Write([]byte{0x40, 0x02, byte(packetID >> 8), byte(packetID)})
				_ = conn.Close()
				if err != nil {
					reportBrokerErr(err)
				}
				return
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := New(ctx, model.Sink{
		ID: "mqtt-repeated-loss", Type: model.SinkMQTT,
		URL: "mqtt://" + ln.Addr().String(), Topic: "beacon/test",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()
	sink := rt.(*mqttSink)
	waitFor(t, time.Second, "initial MQTT connection", func() bool {
		state, _ := sink.State()
		return state == "up"
	})
	previous := sink.activeGeneration()
	if previous == nil {
		t.Fatal("initial MQTT connection has no active generation")
	}

	for attempt := 0; attempt < droppedPublishes; attempt++ {
		result := make(chan error, 1)
		go func() {
			result <- sink.Push(context.Background(), entry("", 0, 127250).Env)
		}()
		select {
		case got := <-published:
			if got != attempt {
				t.Fatalf("broker publish connection = %d, want %d", got, attempt)
			}
		case err := <-brokerErr:
			t.Fatal(err)
		case <-time.After(time.Second):
			t.Fatalf("broker did not receive publish %d", attempt)
		}
		select {
		case err := <-result:
			if err == nil {
				t.Fatalf("publish %d was confirmed without PUBACK", attempt)
			}
		case <-time.After(time.Second):
			t.Fatalf("publish %d did not fail after connection loss", attempt)
		}

		prior := previous
		waitFor(t, time.Second, "fresh MQTT generation", func() bool {
			state, _ := sink.State()
			candidate := sink.activeGeneration()
			if state != "up" || candidate == nil || candidate.id <= prior.id {
				return false
			}
			previous = candidate
			return true
		})
		if previous.client == prior.client {
			t.Fatalf("reconnect %d reused generation %d's Paho client", attempt, prior.id)
		}
	}

	result := make(chan error, 1)
	go func() {
		result <- sink.Push(context.Background(), entry("", 0, 127250).Env)
	}()
	select {
	case got := <-published:
		if got != droppedPublishes {
			t.Fatalf("broker final publish connection = %d, want %d", got, droppedPublishes)
		}
	case err := <-brokerErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("broker did not receive final publish")
	}
	select {
	case err := <-result:
		t.Fatalf("final Push completed before PUBACK: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseFinalACK()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("final Push after PUBACK = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("final Push did not complete after PUBACK")
	}
	select {
	case err := <-brokerErr:
		t.Fatal(err)
	case <-brokerDone:
	case <-time.After(time.Second):
		t.Fatal("broker did not finish")
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
