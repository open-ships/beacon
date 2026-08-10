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

// Configuration text limits are byte counts. The aggregate covers every
// operator-authored string stored in Source, Sink, and Connector values; fixed
// JSON syntax and numeric/boolean settings are not charged to it.
const (
	MaxEntityIDBytes           = 128
	MaxEntityNameBytes         = 256
	MaxEndpointTextBytes       = 8 << 10
	MaxTopicBytes              = 4 << 10
	MaxHeaderNameBytes         = 256
	MaxHeaderValueBytes        = 8 << 10
	MaxAuthoredConfigTextBytes = 256 << 10
	MaxSources                 = 32
	MaxSinks                   = 32
	MaxConnectors              = 64
	MaxEndpointHeaders         = 32
	MaxConnectorFilters        = 32
	MaxFilterExpressionLen     = 8 << 10
)

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
	SinkHTTPPost   SinkType = "http_post" // POST confirmed JSON envelope batches to an HTTP(S) endpoint
	SinkTCP        SinkType = "tcp"       // TCP listener serving NDJSON to connecting clients
	SinkFile       SinkType = "file"
	SinkMQTT       SinkType = "mqtt"
	SinkPostgres   SinkType = "postgres"    // confirmed envelope batches persisted to PostgreSQL / TimescaleDB
	SinkTCPGateway SinkType = "tcp_gateway" // transmit onto an NMEA-2000 bus via a TCP gateway (YD / Actisense)
	SinkNull       SinkType = "null"        // accept and discard messages without external delivery
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
	MaxFileCount              = 128
)

// HTTP POST sink defaults and bounds. BatchSize is a maximum: a route sends
// a smaller batch immediately when fewer pending envelopes are available.
const (
	DefaultHTTPPostBatchSize      = 100
	MaxHTTPPostBatchSize          = 1000
	DefaultHTTPPostRequestTimeout = Duration(10 * time.Second)
)

// PostgreSQL sink defaults and bounds. PostgreSQL's bind-parameter limit is
// comfortably above MaxPostgresBatchSize times the sink's column count, while
// the cap keeps one confirmed write from becoming unreasonably large.
const (
	DefaultPostgresTable        = "public.beacon_envelopes"
	DefaultPostgresBatchSize    = 100
	MaxPostgresBatchSize        = 1000
	DefaultPostgresWriteTimeout = Duration(10 * time.Second)
	MaxPostgresWriteTimeout     = Duration(time.Minute)
)

// ReservedPathPrefixes cannot be used by HTTP sink paths.
var ReservedPathPrefixes = []string{
	"/api", "/assets", "/cel-completions", "/config", "/connectors",
	"/dashboard", "/docs", "/frag", "/health", "/mcp", "/metrics",
	"/n2k", "/sinks", "/sources",
}

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
	FilePath  string            `json:"file_path,omitempty"` // file: capture log to replay; gzip is transparent
	Address   string            `json:"address,omitempty"`   // tcp/udp: gateway host:port
	Format    string            `json:"format,omitempty"`    // tcp/udp: "ydraw" or "actisense"
}

type Sink struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Type            SinkType          `json:"type"`
	Enabled         bool              `json:"enabled"`
	Interface       string            `json:"interface,omitempty"`         // socketcan
	Port            string            `json:"port,omitempty"`              // usbcan
	Path            string            `json:"path,omitempty"`              // http_sse / http_ws (served on data server)
	Address         string            `json:"address,omitempty"`           // tcp listen address; tcp_gateway: gateway host:port
	URL             string            `json:"url,omitempty"`               // mqtt broker, http_post endpoint, or postgres connection URL
	Topic           string            `json:"topic,omitempty"`             // mqtt
	Headers         map[string]string `json:"headers,omitempty"`           // http_post authentication and custom headers
	BatchSize       int               `json:"batch_size,omitempty"`        // http_post/postgres maximum envelopes per batch, 0 = default
	RequestTimeout  Duration          `json:"request_timeout,omitempty"`   // http_post request timeout, 0 = default
	Gzip            bool              `json:"gzip,omitempty"`              // http_post compress request bodies with gzip
	FilePath        string            `json:"file_path,omitempty"`         // file: absolute output path
	Format          string            `json:"format,omitempty"`            // file: "ndjson"/"candump"; tcp_gateway: "ydraw"/"actisense"
	MaxFileBytes    int64             `json:"max_file_bytes,omitempty"`    // file: rotate threshold, 0 = default
	MaxFiles        int               `json:"max_files,omitempty"`         // file: total files kept, 0 = default
	Table           string            `json:"table,omitempty"`             // postgres: schema-qualified destination table, blank = default
	AutoCreateTable bool              `json:"auto_create_table,omitempty"` // postgres: create/verify the destination schema at runtime
	TimescaleDB     bool              `json:"timescaledb,omitempty"`       // postgres: convert the table to a TimescaleDB hypertable
	WriteTimeout    Duration          `json:"write_timeout,omitempty"`     // postgres: schema/write timeout, 0 = default
}

func (s Sink) EffectiveHTTPPostBatchSize() int {
	if s.BatchSize == 0 {
		return DefaultHTTPPostBatchSize
	}
	return s.BatchSize
}

func (s Sink) EffectiveHTTPPostRequestTimeout() time.Duration {
	if s.RequestTimeout == 0 {
		return time.Duration(DefaultHTTPPostRequestTimeout)
	}
	return time.Duration(s.RequestTimeout)
}

func (s Sink) EffectivePostgresTable() string {
	if s.Table == "" {
		return DefaultPostgresTable
	}
	return s.Table
}

func (s Sink) EffectivePostgresBatchSize() int {
	if s.BatchSize == 0 {
		return DefaultPostgresBatchSize
	}
	return s.BatchSize
}

func (s Sink) EffectivePostgresWriteTimeout() time.Duration {
	if s.WriteTimeout == 0 {
		return time.Duration(DefaultPostgresWriteTimeout)
	}
	return time.Duration(s.WriteTimeout)
}

type BufferLimits struct {
	MaxMessages int64    `json:"max_messages,omitempty"`
	MaxAge      Duration `json:"max_age,omitempty"`
	MaxBytes    int64    `json:"max_bytes,omitempty"`
}

// Connector route and appliance storage defaults are hard safety budgets, not
// outage promises. Operators should size them from the documented Envelope
// rate × disconnected-duration calculation.
const (
	DefaultMaxMessages       int64 = 10_000
	DefaultBufferMaxBytes    int64 = 64 << 20 // 64 MiB per connector route
	MaxBufferMessages        int64 = 10_000_000
	MaxBufferBytes           int64 = 8 << 30
	MaxBufferAge                   = Duration(365 * 24 * time.Hour)
	DefaultMaxDatabaseBytes  int64 = 1 << 30 // includes queue rows, indexes, and config
	DefaultDatabaseReserve   int64 = 128 << 20
	DefaultMaxFileStoreBytes int64 = 2 << 30
	MinDatabaseBytes         int64 = 64 << 20
	MaxDatabaseBytes         int64 = 64 << 30
)

type BridgeMode string

const (
	BridgeSemantic    BridgeMode = "semantic"
	BridgeTransparent BridgeMode = "transparent"
	BridgeObserve     BridgeMode = "observe"
)

// ApplyDefaults always supplies independent count and logical-byte guards.
// Zero therefore means "use the appliance-safe default", never unbounded.
func (l BufferLimits) ApplyDefaults() BufferLimits {
	if l.MaxMessages == 0 {
		l.MaxMessages = DefaultMaxMessages
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = DefaultBufferMaxBytes
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

// ObservabilityConfig controls optional, resource-intensive diagnostic
// exports. The ordinary low-cardinality delivery, queue, component, and source
// counters are always exported; PrometheusSourceDetails adds the per-PGN,
// per-field, and raw-byte source series.
type ObservabilityConfig struct {
	PrometheusSourceDetails bool `json:"prometheus_source_details,omitempty"`
}

// ResourceConfig is the process-wide physical storage budget. Connector route
// limits are logical JSON sizes; DatabaseReserveBytes leaves room for SQLite
// indexes, config, inventory, and maintenance inside MaxDatabaseBytes.
type ResourceConfig struct {
	MaxDatabaseBytes     int64 `json:"max_database_bytes,omitempty"`
	DatabaseReserveBytes int64 `json:"database_reserve_bytes,omitempty"`
	MaxFileStoreBytes    int64 `json:"max_file_store_bytes,omitempty"`
}

func (r ResourceConfig) ApplyDefaults() ResourceConfig {
	if r.MaxDatabaseBytes == 0 {
		r.MaxDatabaseBytes = DefaultMaxDatabaseBytes
	}
	if r.DatabaseReserveBytes == 0 {
		r.DatabaseReserveBytes = DefaultDatabaseReserve
	}
	if r.MaxFileStoreBytes == 0 {
		r.MaxFileStoreBytes = DefaultMaxFileStoreBytes
	}
	return r
}

// Settings holds process-wide appliance settings. Its pointer-valued sections
// let merge imports distinguish an omitted section from an explicitly supplied
// zero-valued section, and leave one document that future resource policies can
// extend without changing the entity tables.
type Settings struct {
	Observability *ObservabilityConfig `json:"observability,omitempty"`
	Resources     *ResourceConfig      `json:"resources,omitempty"`
}

type Config struct {
	Sources    []Source    `json:"sources"`
	Sinks      []Sink      `json:"sinks"`
	Connectors []Connector `json:"connectors"`
	Settings   *Settings   `json:"settings,omitempty"`
}

// PrometheusSourceDetailsEnabled reports the effective rich-source-metrics
// setting. A missing observability block is the backward-compatible default:
// disabled.
func (c Config) PrometheusSourceDetailsEnabled() bool {
	return c.Settings != nil && c.Settings.Observability != nil &&
		c.Settings.Observability.PrometheusSourceDetails
}

func (c Config) EffectiveResources() ResourceConfig {
	if c.Settings == nil || c.Settings.Resources == nil {
		return (ResourceConfig{}).ApplyDefaults()
	}
	return c.Settings.Resources.ApplyDefaults()
}
