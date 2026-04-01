package sse_test

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/n2k"
	ssesink "github.com/open-ships/beacon/internal/sink/sse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startSSESink(t *testing.T) (*ssesink.SSESink, string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	ln.Close()

	sink := ssesink.NewSSESink(addr, "/events", slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	go sink.Start(ctx)

	// Wait for listener to be ready
	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 2*time.Second, 20*time.Millisecond, "SSE sink did not start in time")

	return sink, addr
}

func TestSSESink(t *testing.T) {
	sink, addr := startSSESink(t)

	url := "http://" + addr + "/events"
	clientCtx, clientCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer clientCancel()

	req, err := http.NewRequestWithContext(clientCtx, "GET", url, nil)
	require.NoError(t, err)

	resp, err := (&http.Client{}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream"),
		"expected text/event-stream, got %q", resp.Header.Get("Content-Type"))

	// Wait for client to register
	time.Sleep(50 * time.Millisecond)

	msg := &n2k.ParsedMessage{
		PGN:       128259,
		Source:    1,
		Dest:      255,
		Priority:  2,
		Timestamp: time.Now(),
		Payload:   json.RawMessage(`{"speed":3.5}`),
	}
	require.NoError(t, sink.Send(msg))

	lineCh := make(chan string, 20)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
	}()

	var dataLine, eventLine string
	deadline := time.After(3 * time.Second)
loop:
	for {
		select {
		case line := <-lineCh:
			if strings.HasPrefix(line, "data: ") {
				dataLine = strings.TrimPrefix(line, "data: ")
			}
			if strings.HasPrefix(line, "event: ") {
				eventLine = strings.TrimPrefix(line, "event: ")
			}
			if dataLine != "" && eventLine != "" {
				break loop
			}
		case <-deadline:
			t.Fatal("timeout waiting for SSE data")
		}
	}

	assert.Equal(t, "128259", eventLine)

	var got n2k.ParsedMessage
	require.NoError(t, json.Unmarshal([]byte(dataLine), &got))
	assert.Equal(t, uint32(128259), got.PGN)
}

func TestSSESinkHealthCheck_NotStarted(t *testing.T) {
	sink := ssesink.NewSSESink("127.0.0.1:0", "/events", slog.Default())
	assert.Error(t, sink.HealthCheck(context.Background()))
}

func TestSSESinkHealthCheck_Started(t *testing.T) {
	sink, _ := startSSESink(t)
	assert.NoError(t, sink.HealthCheck(context.Background()))
}

func TestSSESinkClientCount(t *testing.T) {
	sink, addr := startSSESink(t)

	assert.Equal(t, 0, sink.ClientCount())

	url := "http://" + addr + "/events"
	clientCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(clientCtx, "GET", url, nil)
	resp, err := (&http.Client{}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Eventually(t, func() bool {
		return sink.ClientCount() == 1
	}, time.Second, 10*time.Millisecond)
}
