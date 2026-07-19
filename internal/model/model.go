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
	SourceMQTT      SourceType = "mqtt"
	SourceFile      SourceType = "file" // replay an NMEA-2000 capture log (candump/canboat/YD/Actisense)
	SourceTCP       SourceType = "tcp"  // ingest from a TCP NMEA-2000 gateway (Yacht Devices / Actisense)
	SourceUDP       SourceType = "udp"  // ingest from a UDP NMEA-2000 gateway
)

// Gateway stream formats (Source.Format, source types tcp/udp only).
const (
	StreamFormatYDRaw     = "ydraw"     // Yacht Devices RAW ASCII line protocol
	StreamFormatActisense = "actisense" // Actisense binary stream protocol
)

type SinkType string

const (
	SinkSocketCAN  SinkType = "socketcan"
	SinkUSBCAN     SinkType = "usbcan"
	SinkHTTPSSE    SinkType = "http_sse"
	SinkHTTPWS     SinkType = "http_ws"
	SinkTCP        SinkType = "tcp" // TCP listener serving NDJSON to connecting clients
	SinkFile       SinkType = "file"
	SinkMQTT       SinkType = "mqtt"
	SinkTCPGateway SinkType = "tcp_gateway" // transmit onto an NMEA-2000 bus via a TCP gateway (YD / Actisense)
)

// File sink formats (Sink.Format, sink type file only).
const (
	FileFormatNDJSON  = "ndjson"
	FileFormatCANDump = "candump"
)

// Defaults applied by the file sink when MaxFileBytes/MaxFiles are left
// unset (0). MaxFiles counts the active file plus rotated backups, so the
// default keeps the active file and 4 rotated backups.
const (
	DefaultMaxFileBytes int64 = 100 << 20 // 100 MiB
	DefaultMaxFiles           = 5
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
	URL       string            `json:"url,omitempty"`       // http_sse / http_ws / mqtt broker
	Topic     string            `json:"topic,omitempty"`     // mqtt
	Headers   map[string]string `json:"headers,omitempty"`   // http_sse / http_ws
	FilePath  string            `json:"file_path,omitempty"` // file: capture log to replay
	Address   string            `json:"address,omitempty"`   // tcp/udp: gateway host:port
	Format    string            `json:"format,omitempty"`    // tcp/udp: "ydraw" or "actisense"
}

type Sink struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Type         SinkType `json:"type"`
	Enabled      bool     `json:"enabled"`
	Interface    string   `json:"interface,omitempty"`      // socketcan
	Port         string   `json:"port,omitempty"`           // usbcan
	Path         string   `json:"path,omitempty"`           // http_sse / http_ws (served on data server)
	Address      string   `json:"address,omitempty"`        // tcp listen address; tcp_gateway: gateway host:port
	URL          string   `json:"url,omitempty"`            // mqtt broker
	Topic        string   `json:"topic,omitempty"`          // mqtt
	FilePath     string   `json:"file_path,omitempty"`      // file: absolute output path
	Format       string   `json:"format,omitempty"`         // file: "ndjson"/"candump"; tcp_gateway: "ydraw"/"actisense"
	MaxFileBytes int64    `json:"max_file_bytes,omitempty"` // file: rotate threshold, 0 = default
	MaxFiles     int      `json:"max_files,omitempty"`      // file: total files kept, 0 = default
}

type BufferLimits struct {
	MaxMessages int64    `json:"max_messages,omitempty"`
	MaxAge      Duration `json:"max_age,omitempty"`
	MaxBytes    int64    `json:"max_bytes,omitempty"`
}

type BridgeMode string

const (
	BridgeSemantic    BridgeMode = "semantic"
	BridgeTransparent BridgeMode = "transparent"
	BridgeObserve     BridgeMode = "observe"
)

// ApplyDefaults returns l with the spec default (max_messages=100000)
// applied when no limit at all is set.
func (l BufferLimits) ApplyDefaults() BufferLimits {
	if l.MaxMessages == 0 && l.MaxAge == 0 && l.MaxBytes == 0 {
		l.MaxMessages = 100000
	}
	return l
}

type Connector struct {
	ID                string       `json:"id"`
	Name              string       `json:"name"`
	SourceID          string       `json:"source_id"`
	SinkID            string       `json:"sink_id"`
	Filters           []string     `json:"filters,omitempty"`
	Buffer            BufferLimits `json:"buffer"`
	Enabled           bool         `json:"enabled"`
	Mode              BridgeMode   `json:"mode,omitempty"`
	ForwardManagement bool         `json:"forward_management,omitempty"`
}

func (c Connector) EffectiveMode() BridgeMode {
	if c.Mode == "" {
		return BridgeSemantic
	}
	return c.Mode
}

type Config struct {
	Sources    []Source    `json:"sources"`
	Sinks      []Sink      `json:"sinks"`
	Connectors []Connector `json:"connectors"`
}
