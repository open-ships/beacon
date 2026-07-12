package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/queue"
)

func TestTCPLiveTail(t *testing.T) {
	rt, err := New(context.Background(), model.Sink{
		ID: "tcp", Name: "TCP", Type: model.SinkTCP, Enabled: true, Address: "127.0.0.1:0",
	}, nil, nil, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Stop()

	addr := rt.(interface{ Addr() string }).Addr()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(100 * time.Millisecond)

	rt.(Broadcaster).Broadcast([]queue.Entry{entry("nav", 7, 127250)})

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatal(err)
	}
	if m["pgn"].(float64) != 127250 || m["connector"].(string) != "nav" {
		t.Fatalf("line = %s", line)
	}
}
