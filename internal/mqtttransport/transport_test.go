package mqtttransport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestGuardedConnectionsSupportTLSAndWebSocket(t *testing.T) {
	for _, scheme := range []string{"ssl", "ws", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"mqtt"}})
				if err != nil {
					return
				}
				defer func() { _ = ws.CloseNow() }()
				_ = ws.Write(context.Background(), websocket.MessageBinary, []byte{0xD0, 0})
			})
			server := httptest.NewTLSServer(handler)
			defer server.Close()
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			broker := strings.Replace(server.URL, "https://", scheme+"://", 1)
			switch scheme {
			case "ssl":
				ln, err := tls.Listen("tcp", "127.0.0.1:0", server.TLS)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = ln.Close() }()
				broker = "ssl://" + ln.Addr().String()
				go func() {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					defer func() { _ = conn.Close() }()
					_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
					_, _ = conn.Write([]byte{0xD0, 0})
				}()
			case "ws":
				plain := httptest.NewServer(handler)
				defer plain.Close()
				broker = strings.Replace(plain.URL, "http://", "ws://", 1)
			}
			opts := mqtt.NewClientOptions().SetTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})
			cleanup := Configure(opts)
			defer cleanup()
			uri, err := url.Parse(broker)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := opts.CustomOpenConnectionFn(uri, *opts)
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			packet, err := packets.ReadPacket(conn)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := packet.(*packets.PingrespPacket); !ok {
				t.Fatalf("decoded %T", packet)
			}
			cleanup()
			if _, err := opts.CustomOpenConnectionFn(uri, *opts); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("retired transport reopened: %v", err)
			}
		})
	}
}

type memoryConn struct {
	net.Conn
	*bytes.Reader
	closed bool
}

func (c *memoryConn) Read(p []byte) (int, error) { return c.Reader.Read(p) }
func (c *memoryConn) Close() error               { c.closed = true; return nil }

func packetHeader(length int) []byte {
	out := []byte{0x30}
	for {
		b := byte(length % 128)
		length /= 128
		if length > 0 {
			b |= 128
		}
		out = append(out, b)
		if length == 0 {
			return out
		}
	}
}

func TestRejectsOversizedPacketBeforeDecoderReadsBody(t *testing.T) {
	for _, length := range []int{maxPacketBodyBytes + 1, 16 << 20, 268435455} {
		inner := &memoryConn{Reader: bytes.NewReader(packetHeader(length))}
		conn := &packetConn{Conn: inner}
		// The broker supplies only the length. Paho must see the limit error,
		// not allocate the body and then hit EOF trying to fill it.
		if _, err := packets.ReadPacket(conn); !errors.Is(err, ErrPacketTooLarge) {
			t.Fatalf("length %d: decoder error = %v", length, err)
		}
		if !inner.closed {
			t.Fatal("oversized connection left open")
		}
		if n, err := conn.Read(make([]byte, 1)); n != 0 || !errors.Is(err, ErrPacketTooLarge) {
			t.Fatalf("repeated read = %d, %v", n, err)
		}
	}
}

func TestPacketFramingSurvivesFragmentedReads(t *testing.T) {
	for _, size := range []int{1, 2, 7, 4096} {
		body := bytes.Repeat([]byte{0xFF}, maxPacketBodyBytes)
		wire := append([]byte{0xD0, 0}, packetHeader(len(body))...)
		wire = append(wire, body...)
		wire = append(wire, 0xD0, 0, 0x20, 2, 0, 0)
		inner := &memoryConn{Reader: bytes.NewReader(wire)}
		conn := &packetConn{Conn: inner}
		var got bytes.Buffer
		buf := make([]byte, size)
		for {
			n, err := conn.Read(buf)
			got.Write(buf[:n])
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(got.Bytes(), wire) {
			t.Fatalf("read size %d changed packet stream", size)
		}
	}
}

func TestMalformedAndTruncatedHeadersAreRejected(t *testing.T) {
	for _, wire := range [][]byte{{0x30}, {0x30, 0x80}, {0x30, 0x80, 0x80, 0x80, 0x80}} {
		inner := &memoryConn{Reader: bytes.NewReader(wire)}
		conn := &packetConn{Conn: inner}
		if n, err := conn.Read(make([]byte, 5)); n != 0 || err == nil || !inner.closed {
			t.Fatalf("header %x: read %d, %v, closed %v", wire, n, err, inner.closed)
		}
	}
}
