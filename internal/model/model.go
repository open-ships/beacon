// Package model defines beacon's configuration entities: sources, sinks,
// and connectors. Validation here is structural; CEL filter compilation is
// checked by the filter package at apply time.
package model

import (
	"encoding/json"
	"fmt"
	"time"
)

type SourceType string

const (
	SourceSocketCAN SourceType = "socketcan"
	SourceUSBCAN    SourceType = "usbcan"
	SourceHTTPSSE   SourceType = "http_sse"
	SourceHTTPWS    SourceType = "http_ws"
)

type SinkType string

const (
	SinkSocketCAN SinkType = "socketcan"
	SinkUSBCAN    SinkType = "usbcan"
	SinkHTTPSSE   SinkType = "http_sse"
	SinkHTTPWS    SinkType = "http_ws"
	SinkTCP       SinkType = "tcp"
)

// ReservedPathPrefixes cannot be used by HTTP sink paths.
var ReservedPathPrefixes = []string{"/api", "/ui", "/docs", "/metrics", "/health"}

// Duration marshals as a Go duration string ("90s", "24h").
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

type Source struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Type      SourceType        `json:"type"`
	Enabled   bool              `json:"enabled"`
	Interface string            `json:"interface,omitempty"` // socketcan
	Port      string            `json:"port,omitempty"`      // usbcan (serial device path)
	URL       string            `json:"url,omitempty"`       // http_sse / http_ws
	Headers   map[string]string `json:"headers,omitempty"`   // http_sse / http_ws
}

type Sink struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Type      SinkType `json:"type"`
	Enabled   bool     `json:"enabled"`
	Interface string   `json:"interface,omitempty"` // socketcan
	Port      string   `json:"port,omitempty"`      // usbcan
	Path      string   `json:"path,omitempty"`      // http_sse / http_ws (served on data server)
	Address   string   `json:"address,omitempty"`   // tcp listen address
}

type BufferLimits struct {
	MaxMessages int64    `json:"max_messages,omitempty"`
	MaxAge      Duration `json:"max_age,omitempty"`
	MaxBytes    int64    `json:"max_bytes,omitempty"`
}

// ApplyDefaults returns l with the spec default (max_messages=100000)
// applied when no limit at all is set.
func (l BufferLimits) ApplyDefaults() BufferLimits {
	if l.MaxMessages == 0 && l.MaxAge == 0 && l.MaxBytes == 0 {
		l.MaxMessages = 100000
	}
	return l
}

type Connector struct {
	ID       string       `json:"id"`
	Name     string       `json:"name"`
	SourceID string       `json:"source_id"`
	SinkID   string       `json:"sink_id"`
	Filters  []string     `json:"filters,omitempty"`
	Buffer   BufferLimits `json:"buffer"`
	Enabled  bool         `json:"enabled"`
}

type Config struct {
	Sources    []Source    `json:"sources"`
	Sinks      []Sink      `json:"sinks"`
	Connectors []Connector `json:"connectors"`
}
