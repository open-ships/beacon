// Package mqtttransport bounds inbound MQTT packets before Paho's decoder
// allocates their advertised body. Application payload checks run too late to
// protect that allocation. The wrapper preserves MQTT over TCP, TLS and WS.
package mqtttransport

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/net/proxy"

	"github.com/open-ships/beacon/internal/msg"
)

// A PUBLISH can add a two-byte topic length, a 65,535-byte topic, and a
// two-byte QoS packet ID. Payload validation still enforces the tighter
// Envelope limit after decoding; every packet type has this allocation cap.
const maxPacketBodyBytes = msg.MaxWireEnvelopeBytes + 2 + 65535 + 2

const ConnectTimeout = 15 * time.Second

var ErrPacketTooLarge = errors.New("MQTT packet exceeds inbound size limit")

// Configure installs the same framing guard for sources and sinks and returns
// cleanup for this client's transport. Paho's Disconnect can return before an
// in-progress CONNECT finishes, so its owner must also close the socket.
func Configure(opts *mqtt.ClientOptions) func() {
	var mu sync.Mutex
	var active net.Conn
	closed := false
	opts.SetConnectTimeout(ConnectTimeout)
	opts.SetCustomOpenConnectionFn(func(uri *url.URL, options mqtt.ClientOptions) (net.Conn, error) {
		mu.Lock()
		stopped := closed
		mu.Unlock()
		if stopped {
			return nil, net.ErrClosed
		}
		conn, err := openConnection(uri, options)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		if closed {
			_ = conn.Close()
			return nil, net.ErrClosed
		}
		active = conn
		return conn, nil
	})
	return func() {
		mu.Lock()
		defer mu.Unlock()
		closed = true
		if active != nil {
			_ = active.Close()
			active = nil
		}
	}
}

func openConnection(uri *url.URL, opts mqtt.ClientOptions) (net.Conn, error) {
	var conn net.Conn
	var err error
	dialer := &net.Dialer{Timeout: opts.ConnectTimeout, KeepAlive: 30 * time.Second}
	switch uri.Scheme {
	case "ws", "wss":
		target := *uri
		target.User = nil // credentials belong to the MQTT CONNECT packet
		var tlsConfig *tls.Config
		if uri.Scheme == "wss" {
			tlsConfig = opts.TLSConfig
		}
		// Paho's WS adapter uses streaming NextReader, so a WebSocket message
		// does not allocate its advertised length ahead of our framing guard.
		conn, err = mqtt.NewWebsocket(target.String(), tlsConfig, opts.ConnectTimeout, opts.HTTPHeaders, opts.WebsocketOptions)
	case "tcp", "mqtt", "ssl", "mqtts":
		conn, err = proxy.FromEnvironmentUsing(dialer).Dial("tcp", uri.Host)
		if err == nil && (uri.Scheme == "ssl" || uri.Scheme == "mqtts") {
			config := &tls.Config{MinVersion: tls.VersionTLS12}
			if opts.TLSConfig != nil {
				config = opts.TLSConfig.Clone()
			}
			if config.ServerName == "" {
				config.ServerName = uri.Hostname()
			}
			secure := tls.Client(conn, config)
			if err = secure.SetDeadline(time.Now().Add(opts.ConnectTimeout)); err == nil {
				err = secure.Handshake()
			}
			if err != nil {
				_ = conn.Close()
				return nil, err
			}
			conn = secure
		}
	default:
		return nil, fmt.Errorf("unsupported MQTT transport %q", uri.Scheme)
	}
	if err != nil {
		return nil, err
	}
	return &packetConn{Conn: conn}, nil
}

type packetConn struct {
	net.Conn
	mu        sync.Mutex // serializes Read, never prevents Close or deadlines
	header    [5]byte
	ready     []byte
	remaining int
	err       error
}

func (c *packetConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return 0, c.err
	}
	if len(c.ready) == 0 && c.remaining == 0 {
		if err := c.readHeader(); err != nil {
			c.err = err
			_ = c.Close()
			return 0, err
		}
	}
	if len(c.ready) > 0 {
		n := copy(p, c.ready)
		c.ready = c.ready[n:]
		return n, nil
	}
	n, err := c.Conn.Read(p[:min(len(p), c.remaining)])
	c.remaining -= n
	return n, err
}

func (c *packetConn) readHeader() error {
	if _, err := io.ReadFull(c.Conn, c.header[:1]); err != nil {
		return err
	}
	length, multiplier := 0, 1
	for i := 1; i < len(c.header); i++ {
		if _, err := io.ReadFull(c.Conn, c.header[i:i+1]); err != nil {
			return err
		}
		b := c.header[i]
		length += int(b&127) * multiplier
		if length > maxPacketBodyBytes {
			return ErrPacketTooLarge
		}
		if b&128 == 0 {
			c.remaining = length
			// Release no header bytes to Paho until the full length is checked.
			c.ready = c.header[:i+1]
			return nil
		}
		multiplier *= 128
	}
	return errors.New("malformed MQTT remaining length")
}
