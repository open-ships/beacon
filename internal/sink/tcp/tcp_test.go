package tcp_test

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/n2k"
	tcpsink "github.com/open-ships/beacon/internal/sink/tcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startTCPSink(t *testing.T) (*tcpsink.TCPSink, string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	ln.Close()

	sink := tcpsink.NewTCPSink(addr, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	go sink.Start(ctx)
	<-sink.Ready()

	return sink, addr
}

func TestTCPSink(t *testing.T) {
	sink, addr := startTCPSink(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for client to register
	require.Eventually(t, func() bool {
		return sink.ClientCount() == 1
	}, time.Second, 10*time.Millisecond)

	msg := &n2k.ParsedMessage{
		PGN:       127250,
		Source:    1,
		Dest:      255,
		Priority:  2,
		Timestamp: time.Now(),
		Payload:   json.RawMessage(`{"heading":1.57}`),
	}
	require.NoError(t, sink.Send(msg))

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(conn)
	require.True(t, scanner.Scan(), "expected to read a line")

	var got n2k.ParsedMessage
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &got))
	assert.Equal(t, uint32(127250), got.PGN)
}

func TestTCPSinkMultipleClients(t *testing.T) {
	sink, addr := startTCPSink(t)

	conn1, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn2.Close()

	require.Eventually(t, func() bool {
		return sink.ClientCount() == 2
	}, time.Second, 10*time.Millisecond)

	msg := &n2k.ParsedMessage{
		PGN:     127250,
		Payload: json.RawMessage(`{}`),
	}
	require.NoError(t, sink.Send(msg))

	for _, conn := range []net.Conn{conn1, conn2} {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		scanner := bufio.NewScanner(conn)
		require.True(t, scanner.Scan())
		var got n2k.ParsedMessage
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &got))
		assert.Equal(t, uint32(127250), got.PGN)
	}
}

func TestTCPSinkHealthCheck_NotStarted(t *testing.T) {
	sink := tcpsink.NewTCPSink("127.0.0.1:0", slog.Default())
	assert.Error(t, sink.HealthCheck(context.Background()))
}

func TestTCPSinkHealthCheck_Started(t *testing.T) {
	sink, _ := startTCPSink(t)
	assert.NoError(t, sink.HealthCheck(context.Background()))
}

func TestTCPSinkClientCount(t *testing.T) {
	sink, addr := startTCPSink(t)
	assert.Equal(t, 0, sink.ClientCount())

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	assert.Eventually(t, func() bool {
		return sink.ClientCount() == 1
	}, time.Second, 10*time.Millisecond)
}
