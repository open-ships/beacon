// Package mcpserver exposes Beacon's local configuration and operational
// state through the Model Context Protocol. It is a thin transport over the
// same config.Service and stats.Registry used by the REST API and web UI, so
// MCP writes get identical validation, persistence, and hot reconciliation.
package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
)

const (
	EndpointPath       = "/mcp"
	maxMCPRequestBytes = 1 << 20 // 1 MiB; comfortably above normal config writes.

	defaultRichDiagnosticResults = 16
	maxRichDiagnosticResults     = 32
	defaultSourceEventResults    = 10
	maxSourceEventResults        = 20
)

// ToolInfo is the stable, human-readable catalog shared by the MCP server and
// the onboard /mcp/info reference page.
type ToolInfo struct {
	Name        string
	Access      string
	Description string
}

var toolCatalog = []ToolInfo{
	{Name: "get_config", Access: "read", Description: "Read every configured source, sink, and connector route."},
	{Name: "put_source", Access: "write", Description: "Create or update a source by id, then hot-apply it."},
	{Name: "put_sink", Access: "write", Description: "Create or update a sink by id, then hot-apply it."},
	{Name: "put_connector", Access: "write", Description: "Create or update a connector route by id, including CEL filters and buffer limits."},
	{Name: "delete_source", Access: "delete", Description: "Delete an unreferenced source."},
	{Name: "delete_sink", Access: "delete", Description: "Delete an unreferenced sink."},
	{Name: "delete_connector", Access: "delete", Description: "Delete a connector route and its route-owned queue."},
	{Name: "get_health", Access: "read", Description: "Read rolled-up health and live source, sink, and connector states."},
	{Name: "get_delivery_metrics", Access: "read", Description: "Read delivery rates, totals, pending delivery, retained history, limits, drops, and errors."},
	{Name: "get_source_metrics", Access: "read", Description: "Inspect a bounded, filterable set of sensor/PGN streams, including sampled raw payload diagnostics, timing, load, decode quality, addressing, gaps, field distributions, and lifecycle events."},
	{Name: "get_latest_payloads", Access: "read", Description: "Read a bounded, filterable set of latest decoded sensor/PGN payloads."},
}

// Catalog returns a copy so callers cannot mutate the registered tool list.
func Catalog() []ToolInfo {
	out := make([]ToolInfo, len(toolCatalog))
	copy(out, toolCatalog)
	return out
}

// NewServer creates Beacon's MCP server. The returned server can be used with
// an in-memory transport in tests or the Streamable HTTP handler returned by
// Handler in production.
func NewServer(svc *config.Service, reg *stats.Registry, version string, log *slog.Logger) *sdkmcp.Server {
	if log == nil {
		log = slog.Default()
	}
	if version == "" {
		version = "dev"
	}

	server := sdkmcp.NewServer(&sdkmcp.Implementation{
		Name:    "beacon",
		Title:   "Beacon",
		Version: version,
	}, &sdkmcp.ServerOptions{
		Instructions: "Manage this Beacon appliance's local sources, sinks, and connector routes. Read current configuration before changing it. Connector routes must reference existing source and sink ids. Writes persist immediately and hot-apply without a restart.",
		Logger:       log,
		Capabilities: &sdkmcp.ServerCapabilities{},
	})

	registerTools(server, svc, reg, log)
	return server
}

// Handler serves the current MCP Streamable HTTP transport. Beacon's tools are
// synchronous and keep no cross-call state, so stateless mode avoids retaining
// one server session per client. Request bodies are bounded before the SDK
// decodes JSON. Cross-origin protection enforces the MCP transport's Origin
// requirement while continuing to allow non-browser local/LAN agents.
func Handler(svc *config.Service, reg *stats.Registry, version string, log *slog.Logger) http.Handler {
	server := NewServer(svc, reg, version, log)
	transport := sdkmcp.NewStreamableHTTPHandler(func(*http.Request) *sdkmcp.Server {
		return server
	}, &sdkmcp.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
		Logger:       log,
	})
	return allMethodOriginGuard(http.NewCrossOriginProtection().Handler(limitMCPRequestBody(transport)))
}

func limitMCPRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if r.ContentLength > maxMCPRequestBytes {
				http.Error(w, "MCP request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxMCPRequestBytes)
		}
		next.ServeHTTP(w, r)
	})
}

// allMethodOriginGuard extends net/http's cross-origin protection to safe
// methods too. MCP's Streamable HTTP endpoint uses GET for server streams, and
// the MCP transport specification requires Origin validation on every incoming
// connection rather than only state-changing browser requests.
func allMethodOriginGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Sec-Fetch-Site"), "cross-site") {
			http.Error(w, "cross-origin MCP request denied", http.StatusForbidden)
			return
		}
		if rawOrigin := r.Header.Get("Origin"); rawOrigin != "" {
			origin, err := url.Parse(rawOrigin)
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			if err != nil || origin.Scheme != scheme || !strings.EqualFold(origin.Host, r.Host) {
				http.Error(w, "cross-origin MCP request denied", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

type emptyInput struct{}

type sourceDefinition struct {
	ID        string            `json:"id" jsonschema:"Stable source id; lowercase letters, digits, underscores, and hyphens."`
	Name      string            `json:"name" jsonschema:"Human-readable source name."`
	Type      string            `json:"type" jsonschema:"Source type: socketcan, usbcan, http_sse, http_ws, mqtt, file, tcp, or udp."`
	Enabled   bool              `json:"enabled" jsonschema:"Whether Beacon should run this source."`
	Interface string            `json:"interface,omitempty" jsonschema:"SocketCAN interface, required for socketcan."`
	Port      string            `json:"port,omitempty" jsonschema:"Serial device path, required for usbcan."`
	URL       string            `json:"url,omitempty" jsonschema:"Remote URL for HTTP or MQTT sources."`
	Topic     string            `json:"topic,omitempty" jsonschema:"MQTT subscription topic."`
	Headers   map[string]string `json:"headers,omitempty" jsonschema:"HTTP headers for SSE or WebSocket ingestion."`
	FilePath  string            `json:"file_path,omitempty" jsonschema:"Absolute capture path for file replay; gzip content and a missing path's .gz counterpart are handled automatically."`
	Address   string            `json:"address,omitempty" jsonschema:"host:port for tcp or udp gateway ingestion."`
	Format    string            `json:"format,omitempty" jsonschema:"Gateway stream format: ydraw or actisense."`
}

func sourceDefinitionFromModel(v model.Source) sourceDefinition {
	return sourceDefinition{
		ID: v.ID, Name: v.Name, Type: string(v.Type), Enabled: v.Enabled,
		Interface: v.Interface, Port: v.Port, URL: v.URL, Topic: v.Topic,
		Headers: v.Headers, FilePath: v.FilePath, Address: v.Address, Format: v.Format,
	}
}

func (v sourceDefinition) model() model.Source {
	return model.Source{
		ID: v.ID, Name: v.Name, Type: model.SourceType(v.Type), Enabled: v.Enabled,
		Interface: v.Interface, Port: v.Port, URL: v.URL, Topic: v.Topic,
		Headers: v.Headers, FilePath: v.FilePath, Address: v.Address, Format: v.Format,
	}
}

type sinkDefinition struct {
	ID              string            `json:"id" jsonschema:"Stable sink id; lowercase letters, digits, underscores, and hyphens."`
	Name            string            `json:"name" jsonschema:"Human-readable sink name."`
	Type            string            `json:"type" jsonschema:"Sink type: socketcan, usbcan, http_sse, http_ws, http_post, tcp, file, mqtt, postgres, tcp_gateway, or null."`
	Enabled         bool              `json:"enabled" jsonschema:"Whether Beacon should run this sink."`
	Interface       string            `json:"interface,omitempty" jsonschema:"SocketCAN interface, required for socketcan."`
	Port            string            `json:"port,omitempty" jsonschema:"Serial device path, required for usbcan."`
	Path            string            `json:"path,omitempty" jsonschema:"Local data-server path for http_sse or http_ws."`
	Address         string            `json:"address,omitempty" jsonschema:"Listen or gateway host:port for tcp sinks."`
	URL             string            `json:"url,omitempty" jsonschema:"MQTT broker URL, HTTP(S) endpoint for http_post, or postgres/postgresql connection URL."`
	Topic           string            `json:"topic,omitempty" jsonschema:"MQTT publish topic."`
	Headers         map[string]string `json:"headers,omitempty" jsonschema:"Authentication and custom request headers for http_post."`
	BatchSize       int               `json:"batch_size,omitempty" jsonschema:"Maximum envelopes per http_post request or postgres write; zero uses Beacon's type-specific default."`
	RequestTimeout  string            `json:"request_timeout,omitempty" jsonschema:"HTTP POST timeout as a Go duration such as 10s; empty uses Beacon's default."`
	Gzip            bool              `json:"gzip,omitempty" jsonschema:"Compress http_post request bodies with gzip."`
	FilePath        string            `json:"file_path,omitempty" jsonschema:"Absolute output path for file sinks."`
	Format          string            `json:"format,omitempty" jsonschema:"File format ndjson/candump or gateway format ydraw/actisense."`
	MaxFileBytes    int64             `json:"max_file_bytes,omitempty" jsonschema:"File rotation threshold in bytes; zero uses Beacon's default."`
	MaxFiles        int               `json:"max_files,omitempty" jsonschema:"Total active plus rotated files to retain; zero uses Beacon's default."`
	Table           string            `json:"table,omitempty" jsonschema:"PostgreSQL destination as table or schema.table; empty uses public.beacon_envelopes."`
	AutoCreateTable bool              `json:"auto_create_table,omitempty" jsonschema:"Create the PostgreSQL table and indexes automatically."`
	TimescaleDB     bool              `json:"timescaledb,omitempty" jsonschema:"Convert the PostgreSQL table to a TimescaleDB hypertable; the extension must already be installed."`
	WriteTimeout    string            `json:"write_timeout,omitempty" jsonschema:"PostgreSQL schema and batch-write timeout as a Go duration such as 10s; empty uses Beacon's default."`
}

func sinkDefinitionFromModel(v model.Sink) sinkDefinition {
	requestTimeout := ""
	if v.RequestTimeout != 0 {
		requestTimeout = time.Duration(v.RequestTimeout).String()
	}
	writeTimeout := ""
	if v.WriteTimeout != 0 {
		writeTimeout = time.Duration(v.WriteTimeout).String()
	}
	return sinkDefinition{
		ID: v.ID, Name: v.Name, Type: string(v.Type), Enabled: v.Enabled,
		Interface: v.Interface, Port: v.Port, Path: v.Path, Address: v.Address,
		URL: v.URL, Topic: v.Topic, Headers: v.Headers, BatchSize: v.BatchSize, Gzip: v.Gzip,
		RequestTimeout: requestTimeout, FilePath: v.FilePath, Format: v.Format,
		MaxFileBytes: v.MaxFileBytes, MaxFiles: v.MaxFiles,
		Table: v.Table, AutoCreateTable: v.AutoCreateTable, TimescaleDB: v.TimescaleDB,
		WriteTimeout: writeTimeout,
	}
}

func (v sinkDefinition) model() (model.Sink, error) {
	var requestTimeout time.Duration
	if v.RequestTimeout != "" {
		parsed, err := time.ParseDuration(v.RequestTimeout)
		if err != nil {
			return model.Sink{}, fmt.Errorf("invalid sink.request_timeout %q: %w", v.RequestTimeout, err)
		}
		requestTimeout = parsed
	}
	var writeTimeout time.Duration
	if v.WriteTimeout != "" {
		parsed, err := time.ParseDuration(v.WriteTimeout)
		if err != nil {
			return model.Sink{}, fmt.Errorf("invalid sink.write_timeout %q: %w", v.WriteTimeout, err)
		}
		writeTimeout = parsed
	}
	return model.Sink{
		ID: v.ID, Name: v.Name, Type: model.SinkType(v.Type), Enabled: v.Enabled,
		Interface: v.Interface, Port: v.Port, Path: v.Path, Address: v.Address,
		URL: v.URL, Topic: v.Topic, Headers: v.Headers, BatchSize: v.BatchSize, Gzip: v.Gzip,
		RequestTimeout: model.Duration(requestTimeout), FilePath: v.FilePath, Format: v.Format,
		MaxFileBytes: v.MaxFileBytes, MaxFiles: v.MaxFiles,
		Table: v.Table, AutoCreateTable: v.AutoCreateTable, TimescaleDB: v.TimescaleDB,
		WriteTimeout: model.Duration(writeTimeout),
	}, nil
}

type bufferDefinition struct {
	MaxMessages int64  `json:"max_messages,omitempty" jsonschema:"Maximum retained messages; zero or omitted uses the independent 10000-message safety default."`
	MaxAge      string `json:"max_age,omitempty" jsonschema:"Maximum retained age as a Go duration such as 24h or 90s."`
	MaxBytes    int64  `json:"max_bytes,omitempty" jsonschema:"Maximum retained logical bytes; zero or omitted uses the independent 67108864-byte (64 MiB) safety default."`
}

type connectorDefinition struct {
	ID                string           `json:"id" jsonschema:"Stable connector route id; lowercase letters, digits, underscores, and hyphens."`
	Name              string           `json:"name" jsonschema:"Human-readable connector route name."`
	SourceID          string           `json:"source_id" jsonschema:"Id of an existing source."`
	SinkID            string           `json:"sink_id" jsonschema:"Id of an existing sink."`
	Filters           []string         `json:"filters,omitempty" jsonschema:"CEL expressions combined with AND semantics."`
	Buffer            bufferDefinition `json:"buffer" jsonschema:"Durable queue retention limits."`
	Enabled           bool             `json:"enabled" jsonschema:"Whether Beacon should run this connector route."`
	Mode              string           `json:"mode,omitempty" jsonschema:"Bridge mode: semantic (default), transparent, or observe."`
	ForwardManagement bool             `json:"forward_management,omitempty" jsonschema:"Allow NMEA network-management PGNs in transparent mode."`
}

type connectorView struct {
	ID                string           `json:"id"`
	Name              string           `json:"name"`
	SourceID          string           `json:"source_id"`
	SinkID            string           `json:"sink_id"`
	Filters           []string         `json:"filters,omitempty"`
	Buffer            bufferDefinition `json:"buffer"`
	EffectiveBuffer   bufferDefinition `json:"effective_buffer" jsonschema:"Effective durable queue limits after Beacon defaults are applied."`
	Enabled           bool             `json:"enabled"`
	Mode              string           `json:"mode,omitempty"`
	ForwardManagement bool             `json:"forward_management,omitempty"`
}

func bufferDefinitionFromModel(v model.BufferLimits) bufferDefinition {
	maxAge := ""
	if v.MaxAge != 0 {
		maxAge = time.Duration(v.MaxAge).String()
	}
	return bufferDefinition{MaxMessages: v.MaxMessages, MaxAge: maxAge, MaxBytes: v.MaxBytes}
}

func connectorViewFromModel(v model.Connector) connectorView {
	return connectorView{
		ID: v.ID, Name: v.Name, SourceID: v.SourceID, SinkID: v.SinkID,
		Filters: v.Filters, Buffer: bufferDefinitionFromModel(v.Buffer),
		EffectiveBuffer: bufferDefinitionFromModel(v.Buffer.ApplyDefaults()),
		Enabled:         v.Enabled, Mode: string(v.Mode), ForwardManagement: v.ForwardManagement,
	}
}

func (v connectorDefinition) model() (model.Connector, error) {
	var maxAge time.Duration
	if v.Buffer.MaxAge != "" {
		parsed, err := time.ParseDuration(v.Buffer.MaxAge)
		if err != nil {
			return model.Connector{}, fmt.Errorf("invalid buffer.max_age %q: %w", v.Buffer.MaxAge, err)
		}
		maxAge = parsed
	}
	return model.Connector{
		ID: v.ID, Name: v.Name, SourceID: v.SourceID, SinkID: v.SinkID,
		Filters: v.Filters, Buffer: model.BufferLimits{
			MaxMessages: v.Buffer.MaxMessages, MaxAge: model.Duration(maxAge), MaxBytes: v.Buffer.MaxBytes,
		},
		Enabled: v.Enabled, Mode: model.BridgeMode(v.Mode), ForwardManagement: v.ForwardManagement,
	}, nil
}

type getConfigOutput struct {
	Sources    []sourceDefinition `json:"sources"`
	Sinks      []sinkDefinition   `json:"sinks"`
	Connectors []connectorView    `json:"connectors"`
	Settings   *model.Settings    `json:"settings,omitempty" jsonschema:"Optional process-wide appliance settings; absent values use low-resource defaults."`
}

type putSourceInput struct {
	Source sourceDefinition `json:"source" jsonschema:"Complete source definition to create or update."`
}

type putSourceOutput struct {
	Source sourceDefinition    `json:"source"`
	Status []supervisor.Status `json:"status"`
}

type putSinkInput struct {
	Sink sinkDefinition `json:"sink" jsonschema:"Complete sink definition to create or update."`
}

type putSinkOutput struct {
	Sink   sinkDefinition      `json:"sink"`
	Status []supervisor.Status `json:"status"`
}

type putConnectorInput struct {
	Connector connectorDefinition `json:"connector" jsonschema:"Complete connector route definition to create or update."`
}

type putConnectorOutput struct {
	Connector connectorView       `json:"connector"`
	Status    []supervisor.Status `json:"status"`
}

type deleteInput struct {
	ID string `json:"id" jsonschema:"Id of the entity to delete."`
}

type deleteOutput struct {
	Deleted bool   `json:"deleted"`
	Kind    string `json:"kind"`
	ID      string `json:"id"`
}

type healthOutput struct {
	Status     string              `json:"status"`
	Components []supervisor.Status `json:"components"`
}

type deliveryMetricsInput struct {
	ConnectorID string `json:"connector_id,omitempty" jsonschema:"Optional connector route id. Omit to return every configured connector."`
}

type sourceMetricsInput struct {
	SourceID      string  `json:"source_id,omitempty" jsonschema:"Optional configured source id. Omit to inspect every source."`
	PGN           *uint32 `json:"pgn,omitempty" jsonschema:"Optional PGN number filter."`
	SourceAddress *uint8  `json:"source_address,omitempty" jsonschema:"Optional NMEA 2000 source-address (sensor/sender id) filter."`
	DeviceNameHex string  `json:"device_name_hex,omitempty" jsonschema:"Optional stable 64-bit NMEA 2000 Device NAME filter, as hexadecimal with or without a 0x prefix."`
	EventLimit    int     `json:"event_limit,omitempty" jsonschema:"Recent lifecycle events per source to include; zero defaults to 10 and values are capped at 20."`
	Limit         int     `json:"limit,omitempty" jsonschema:"Maximum rich PGN/sender streams returned across all sources; zero defaults to 16 and values are capped at 32. Use filters for additional streams."`
}

type sourceMetricsOutput struct {
	GeneratedAt time.Time                             `json:"generated_at"`
	Sources     map[string][]stats.SourcePGNMetric    `json:"sources"`
	Events      map[string][]stats.SourceMetricEvent  `json:"events"`
	Capacity    map[string]stats.SourceMetricCapacity `json:"capacity"`
	Truncated   bool                                  `json:"truncated,omitempty"`
}

type latestPayloadsInput struct {
	SourceID      string  `json:"source_id,omitempty" jsonschema:"Optional configured source id. Omit to inspect every source."`
	SensorID      string  `json:"sensor_id,omitempty" jsonschema:"Optional sensor_id returned by this tool: a stable Device NAME hex value or address:<source_address>."`
	PGN           *uint32 `json:"pgn,omitempty" jsonschema:"Optional PGN number filter."`
	SourceAddress *uint8  `json:"source_address,omitempty" jsonschema:"Optional NMEA 2000 source-address (sensor/sender id) filter."`
	DeviceNameHex string  `json:"device_name_hex,omitempty" jsonschema:"Optional stable 64-bit NMEA 2000 Device NAME filter, as hexadecimal with or without a 0x prefix."`
	Limit         int     `json:"limit,omitempty" jsonschema:"Maximum latest payloads returned; zero defaults to 16 and values are capped at 32. Use filters for additional payloads."`
}

type latestPayloadView struct {
	SourceID      string    `json:"source_id"`
	SensorID      string    `json:"sensor_id" jsonschema:"Stable Device NAME when known; otherwise address:<source_address>."`
	SourceAddress uint8     `json:"source_address"`
	DeviceNameHex string    `json:"device_name_hex,omitempty"`
	PGN           uint32    `json:"pgn"`
	PGNName       string    `json:"pgn_name,omitempty"`
	LastSeen      time.Time `json:"last_seen"`
	Payload       any       `json:"payload"`
}

type latestPayloadsOutput struct {
	GeneratedAt time.Time           `json:"generated_at"`
	Payloads    []latestPayloadView `json:"payloads"`
	Truncated   bool                `json:"truncated,omitempty"`
}

func latestPayloadViewFromStats(v stats.SourcePGNLastPayload) (latestPayloadView, error) {
	var payload any
	if err := json.Unmarshal(v.Payload, &payload); err != nil {
		return latestPayloadView{}, fmt.Errorf("decode latest payload for source %s address %d PGN %d: %w",
			v.SourceID, v.SourceAddress, v.PGN, err)
	}
	sensorID := "address:" + strconv.Itoa(int(v.SourceAddress))
	if v.DeviceNameHex != "" {
		sensorID = v.DeviceNameHex
	}
	return latestPayloadView{
		SourceID: v.SourceID, SensorID: sensorID, SourceAddress: v.SourceAddress,
		DeviceNameHex: v.DeviceNameHex, PGN: v.PGN, PGNName: v.PGNName,
		LastSeen: v.LastSeen, Payload: payload,
	}, nil
}

func normalizeDeviceNameHex(raw string) (string, error) {
	trimmed := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(raw)), "0x")
	name, err := strconv.ParseUint(trimmed, 16, 64)
	if err != nil {
		return "", fmt.Errorf("invalid Device NAME %q: expected up to 16 hexadecimal digits", raw)
	}
	return fmt.Sprintf("%016x", name), nil
}

func boundedDiagnosticLimit(requested, fallback, maximum int) int {
	if requested <= 0 {
		return fallback
	}
	return min(requested, maximum)
}

func latestPayloadFilter(in latestPayloadsInput) (stats.SourcePGNMetricFilter, error) {
	filter := stats.SourcePGNMetricFilter{
		PGN: in.PGN, SourceAddress: in.SourceAddress, DeviceNameHex: in.DeviceNameHex,
	}
	if filter.DeviceNameHex != "" {
		normalized, err := normalizeDeviceNameHex(filter.DeviceNameHex)
		if err != nil {
			return stats.SourcePGNMetricFilter{}, err
		}
		filter.DeviceNameHex = normalized
	}
	if in.SensorID == "" {
		return filter, nil
	}
	if rawAddress, ok := strings.CutPrefix(strings.ToLower(strings.TrimSpace(in.SensorID)), "address:"); ok {
		parsed, err := strconv.ParseUint(rawAddress, 10, 8)
		if err != nil {
			return stats.SourcePGNMetricFilter{}, fmt.Errorf("invalid sensor_id %q: expected address:<0-255>", in.SensorID)
		}
		address := uint8(parsed)
		if filter.SourceAddress != nil && *filter.SourceAddress != address {
			return stats.SourcePGNMetricFilter{}, fmt.Errorf("sensor_id %q conflicts with source_address %d", in.SensorID, *filter.SourceAddress)
		}
		filter.SourceAddress = &address
		return filter, nil
	}
	normalizedName, err := normalizeDeviceNameHex(in.SensorID)
	if err != nil {
		return stats.SourcePGNMetricFilter{}, fmt.Errorf("invalid sensor_id %q: expected Device NAME hex or address:<0-255>", in.SensorID)
	}
	if filter.DeviceNameHex != "" {
		configuredName, err := normalizeDeviceNameHex(filter.DeviceNameHex)
		if err != nil || configuredName != normalizedName {
			return stats.SourcePGNMetricFilter{}, fmt.Errorf("sensor_id %q conflicts with device_name_hex %q", in.SensorID, filter.DeviceNameHex)
		}
	}
	filter.DeviceNameHex = normalizedName
	return filter, nil
}

type deliveryMetricsOutput struct {
	Connectors map[string]deliveryMetrics `json:"connectors"`
}

type deliveryMetrics struct {
	TotalMessages    int64            `json:"total_messages"`
	TotalBytes       int64            `json:"total_bytes"`
	MsgPerSec        float64          `json:"msg_per_sec"`
	BytesPerSec      float64          `json:"bytes_per_sec"`
	PendingMessages  int64            `json:"pending_messages"`
	PendingBytes     int64            `json:"pending_bytes"`
	RetainedMessages int64            `json:"retained_messages"`
	RetainedBytes    int64            `json:"retained_bytes"`
	QueueCursor      int64            `json:"queue_cursor"`
	QueueTail        int64            `json:"queue_tail"`
	OldestPending    string           `json:"oldest_pending,omitempty"`
	OldestRetained   string           `json:"oldest_retained,omitempty"`
	LimitMessages    int64            `json:"limit_messages,omitempty"`
	LimitBytes       int64            `json:"limit_bytes,omitempty"`
	HeadroomMessages int64            `json:"headroom_messages,omitempty"`
	HeadroomBytes    int64            `json:"headroom_bytes,omitempty"`
	DeliveryClass    string           `json:"delivery_class,omitempty"`
	State            string           `json:"state,omitempty"`
	LastError        string           `json:"last_error,omitempty"`
	Drops            int64            `json:"drops"`
	StageTotals      map[string]int64 `json:"stage_totals,omitempty"`
	DepthHistory     []int64          `json:"pending_history,omitempty"`
}

func deliveryMetricsFromSnapshot(v stats.Snapshot) deliveryMetrics {
	out := deliveryMetrics{
		TotalMessages: v.TotalMessages, TotalBytes: v.TotalBytes,
		MsgPerSec: v.MsgPerSec, BytesPerSec: v.BytesPerSec,
		PendingMessages: v.QueueDepth, PendingBytes: v.QueueBytes,
		RetainedMessages: v.RetainedDepth, RetainedBytes: v.RetainedBytes,
		QueueCursor: v.QueueCursor, QueueTail: v.QueueTail,
		LimitMessages: v.LimitMessages, LimitBytes: v.LimitBytes,
		HeadroomMessages: v.HeadroomMessages, HeadroomBytes: v.HeadroomBytes,
		DeliveryClass: v.DeliveryClass, State: v.State, LastError: v.LastError,
		Drops: v.Drops, StageTotals: v.StageTotals, DepthHistory: v.DepthHistory,
	}
	if v.OldestPending != nil {
		out.OldestPending = v.OldestPending.UTC().Format(time.RFC3339Nano)
	}
	if v.OldestRetained != nil {
		out.OldestRetained = v.OldestRetained.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func registerTools(server *sdkmcp.Server, svc *config.Service, reg *stats.Registry, log *slog.Logger) {
	// Rich snapshots clone and sort bounded-but-nontrivial diagnostic maps. Keep
	// only one such construction active per MCP server; ordinary configuration,
	// health, and delivery tools remain fully concurrent.
	richDiagnosticGate := make(chan struct{}, 1)
	acquireRichDiagnostics := func(ctx context.Context) (func(), error) {
		select {
		case richDiagnosticGate <- struct{}{}:
			return func() { <-richDiagnosticGate }, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	readAnnotations := toolAnnotations(true, false, false)
	writeAnnotations := toolAnnotations(false, true, true)
	deleteAnnotations := toolAnnotations(false, true, true)

	sdkmcp.AddTool(server, tool("get_config", "Get configuration", readAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, _ emptyInput) (*sdkmcp.CallToolResult, getConfigOutput, error) {
			cfg, err := svc.Export(ctx)
			if err != nil {
				return nil, getConfigOutput{}, publicError(log, err)
			}
			out := getConfigOutput{
				Sources:    make([]sourceDefinition, 0, len(cfg.Sources)),
				Sinks:      make([]sinkDefinition, 0, len(cfg.Sinks)),
				Connectors: make([]connectorView, 0, len(cfg.Connectors)),
				Settings:   cfg.Settings,
			}
			for _, v := range cfg.Sources {
				out.Sources = append(out.Sources, sourceDefinitionFromModel(v))
			}
			for _, v := range cfg.Sinks {
				out.Sinks = append(out.Sinks, sinkDefinitionFromModel(v))
			}
			for _, v := range cfg.Connectors {
				out.Connectors = append(out.Connectors, connectorViewFromModel(v))
			}
			return nil, out, nil
		})

	sdkmcp.AddTool(server, tool("put_source", "Create or update source", writeAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in putSourceInput) (*sdkmcp.CallToolResult, putSourceOutput, error) {
			v := in.Source.model()
			isCreate, err := sourceIsNew(ctx, svc, v.ID)
			if err != nil {
				return nil, putSourceOutput{}, publicError(log, err)
			}
			if err := svc.PutSource(ctx, v, isCreate); err != nil {
				return nil, putSourceOutput{}, publicError(log, err)
			}
			return nil, putSourceOutput{Source: sourceDefinitionFromModel(v), Status: statusesFor(svc.Statuses(), v.ID)}, nil
		})

	sdkmcp.AddTool(server, tool("put_sink", "Create or update sink", writeAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in putSinkInput) (*sdkmcp.CallToolResult, putSinkOutput, error) {
			v, err := in.Sink.model()
			if err != nil {
				return nil, putSinkOutput{}, err
			}
			isCreate, err := sinkIsNew(ctx, svc, v.ID)
			if err != nil {
				return nil, putSinkOutput{}, publicError(log, err)
			}
			if err := svc.PutSink(ctx, v, isCreate); err != nil {
				return nil, putSinkOutput{}, publicError(log, err)
			}
			return nil, putSinkOutput{Sink: sinkDefinitionFromModel(v), Status: statusesFor(svc.Statuses(), v.ID)}, nil
		})

	sdkmcp.AddTool(server, tool("put_connector", "Create or update connector", writeAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in putConnectorInput) (*sdkmcp.CallToolResult, putConnectorOutput, error) {
			v, err := in.Connector.model()
			if err != nil {
				return nil, putConnectorOutput{}, err
			}
			isCreate, err := connectorIsNew(ctx, svc, v.ID)
			if err != nil {
				return nil, putConnectorOutput{}, publicError(log, err)
			}
			if err := svc.PutConnector(ctx, v, isCreate); err != nil {
				return nil, putConnectorOutput{}, publicError(log, err)
			}
			return nil, putConnectorOutput{Connector: connectorViewFromModel(v), Status: statusesFor(svc.Statuses(), v.ID)}, nil
		})

	sdkmcp.AddTool(server, tool("delete_source", "Delete source", deleteAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in deleteInput) (*sdkmcp.CallToolResult, deleteOutput, error) {
			if err := svc.DeleteSource(ctx, in.ID); err != nil {
				return nil, deleteOutput{}, publicError(log, err)
			}
			return nil, deleteOutput{Deleted: true, Kind: "source", ID: in.ID}, nil
		})

	sdkmcp.AddTool(server, tool("delete_sink", "Delete sink", deleteAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in deleteInput) (*sdkmcp.CallToolResult, deleteOutput, error) {
			if err := svc.DeleteSink(ctx, in.ID); err != nil {
				return nil, deleteOutput{}, publicError(log, err)
			}
			return nil, deleteOutput{Deleted: true, Kind: "sink", ID: in.ID}, nil
		})

	sdkmcp.AddTool(server, tool("delete_connector", "Delete connector", deleteAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in deleteInput) (*sdkmcp.CallToolResult, deleteOutput, error) {
			if err := svc.DeleteConnector(ctx, in.ID); err != nil {
				return nil, deleteOutput{}, publicError(log, err)
			}
			return nil, deleteOutput{Deleted: true, Kind: "connector", ID: in.ID}, nil
		})

	sdkmcp.AddTool(server, tool("get_health", "Get health", readAnnotations),
		func(context.Context, *sdkmcp.CallToolRequest, emptyInput) (*sdkmcp.CallToolResult, healthOutput, error) {
			components := svc.Statuses()
			if components == nil {
				components = []supervisor.Status{}
			}
			return nil, healthOutput{Status: supervisor.RollupHealth(components), Components: components}, nil
		})

	sdkmcp.AddTool(server, tool("get_delivery_metrics", "Get delivery metrics", readAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in deliveryMetricsInput) (*sdkmcp.CallToolResult, deliveryMetricsOutput, error) {
			out := deliveryMetricsOutput{Connectors: map[string]deliveryMetrics{}}
			if in.ConnectorID != "" {
				if _, err := svc.GetConnector(ctx, in.ConnectorID); err != nil {
					return nil, deliveryMetricsOutput{}, publicError(log, err)
				}
				snap, _ := reg.Snapshot(in.ConnectorID)
				out.Connectors[in.ConnectorID] = deliveryMetricsFromSnapshot(snap)
				return nil, out, nil
			}
			connectors, err := svc.ListConnectors(ctx)
			if err != nil {
				return nil, deliveryMetricsOutput{}, publicError(log, err)
			}
			all := reg.All()
			for _, connector := range connectors {
				out.Connectors[connector.ID] = deliveryMetricsFromSnapshot(all[connector.ID])
			}
			return nil, out, nil
		})

	sdkmcp.AddTool(server, tool("get_source_metrics", "Get source PGN metrics", readAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in sourceMetricsInput) (*sdkmcp.CallToolResult, sourceMetricsOutput, error) {
			release, err := acquireRichDiagnostics(ctx)
			if err != nil {
				return nil, sourceMetricsOutput{}, err
			}
			defer release()
			out := sourceMetricsOutput{
				GeneratedAt: time.Now().UTC(),
				Sources:     map[string][]stats.SourcePGNMetric{},
				Events:      map[string][]stats.SourceMetricEvent{},
				Capacity:    map[string]stats.SourceMetricCapacity{},
			}
			eventLimit := boundedDiagnosticLimit(in.EventLimit, defaultSourceEventResults, maxSourceEventResults)
			resultLimit := boundedDiagnosticLimit(in.Limit, defaultRichDiagnosticResults, maxRichDiagnosticResults)
			if in.DeviceNameHex != "" {
				normalized, err := normalizeDeviceNameHex(in.DeviceNameHex)
				if err != nil {
					return nil, sourceMetricsOutput{}, err
				}
				in.DeviceNameHex = normalized
			}
			metricFilter := stats.SourcePGNMetricFilter{
				PGN: in.PGN, SourceAddress: in.SourceAddress, DeviceNameHex: in.DeviceNameHex,
			}
			if in.SourceID != "" {
				if _, err := svc.GetSource(ctx, in.SourceID); err != nil {
					return nil, sourceMetricsOutput{}, publicError(log, err)
				}
				metrics, truncated := reg.SourcePGNMetricsFilteredLimit(in.SourceID, metricFilter, resultLimit)
				out.Truncated = truncated
				out.Sources[in.SourceID] = metrics
				out.Events[in.SourceID] = filterSourceMetricEvents(reg.SourceMetricEvents(in.SourceID, eventLimit), in)
				out.Capacity[in.SourceID] = reg.SourceMetricCapacity(in.SourceID)
				return nil, out, nil
			}
			sources, err := svc.ListSources(ctx)
			if err != nil {
				return nil, sourceMetricsOutput{}, publicError(log, err)
			}
			remaining := resultLimit
			for _, source := range sources {
				metrics, truncated := reg.SourcePGNMetricsFilteredLimit(source.ID, metricFilter, remaining)
				out.Truncated = out.Truncated || truncated
				out.Sources[source.ID] = metrics
				out.Events[source.ID] = filterSourceMetricEvents(reg.SourceMetricEvents(source.ID, eventLimit), in)
				out.Capacity[source.ID] = reg.SourceMetricCapacity(source.ID)
				remaining -= len(metrics)
				if remaining == 0 {
					// Later sources may contain matching streams. Stop before cloning
					// their rich diagnostic state and make the output bound explicit.
					out.Truncated = out.Truncated || len(sources) > len(out.Sources)
					break
				}
			}
			return nil, out, nil
		})

	sdkmcp.AddTool(server, tool("get_latest_payloads", "Get latest sensor payloads", readAnnotations),
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, in latestPayloadsInput) (*sdkmcp.CallToolResult, latestPayloadsOutput, error) {
			release, err := acquireRichDiagnostics(ctx)
			if err != nil {
				return nil, latestPayloadsOutput{}, err
			}
			defer release()
			out := latestPayloadsOutput{GeneratedAt: time.Now().UTC(), Payloads: []latestPayloadView{}}
			resultLimit := boundedDiagnosticLimit(in.Limit, defaultRichDiagnosticResults, maxRichDiagnosticResults)
			filter, err := latestPayloadFilter(in)
			if err != nil {
				return nil, latestPayloadsOutput{}, err
			}
			appendSource := func(sourceID string) error {
				remaining := resultLimit - len(out.Payloads)
				if remaining <= 0 {
					out.Truncated = true
					return nil
				}
				payloads, truncated := reg.SourcePGNLastPayloadsFilteredLimit(sourceID, filter, remaining)
				out.Truncated = out.Truncated || truncated
				for _, payload := range payloads {
					view, err := latestPayloadViewFromStats(payload)
					if err != nil {
						return err
					}
					out.Payloads = append(out.Payloads, view)
				}
				return nil
			}
			if in.SourceID != "" {
				if _, err := svc.GetSource(ctx, in.SourceID); err != nil {
					return nil, latestPayloadsOutput{}, publicError(log, err)
				}
				if err := appendSource(in.SourceID); err != nil {
					return nil, latestPayloadsOutput{}, publicError(log, err)
				}
				return nil, out, nil
			}
			sources, err := svc.ListSources(ctx)
			if err != nil {
				return nil, latestPayloadsOutput{}, publicError(log, err)
			}
			for _, source := range sources {
				if err := appendSource(source.ID); err != nil {
					return nil, latestPayloadsOutput{}, publicError(log, err)
				}
				if out.Truncated {
					break
				}
			}
			return nil, out, nil
		})
}

func filterSourceMetricEvents(events []stats.SourceMetricEvent, in sourceMetricsInput) []stats.SourceMetricEvent {
	out := make([]stats.SourceMetricEvent, 0, len(events))
	for _, event := range events {
		if in.PGN != nil && event.PGN != *in.PGN {
			continue
		}
		if in.SourceAddress != nil && event.SourceAddress != *in.SourceAddress {
			continue
		}
		if in.DeviceNameHex != "" {
			want := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(in.DeviceNameHex)), "0x")
			got := strings.TrimPrefix(strings.ToLower(event.DeviceNameHex), "0x")
			if got != want {
				continue
			}
		}
		out = append(out, event)
	}
	return out
}

func tool(name, title string, annotations *sdkmcp.ToolAnnotations) *sdkmcp.Tool {
	for _, info := range toolCatalog {
		if info.Name == name {
			return &sdkmcp.Tool{Name: name, Title: title, Description: info.Description, Annotations: annotations}
		}
	}
	panic("MCP tool missing from catalog: " + name)
}

func toolAnnotations(readOnly, idempotent, destructive bool) *sdkmcp.ToolAnnotations {
	openWorld := false
	return &sdkmcp.ToolAnnotations{
		ReadOnlyHint: readOnly, IdempotentHint: idempotent,
		DestructiveHint: &destructive, OpenWorldHint: &openWorld,
	}
}

func sourceIsNew(ctx context.Context, svc *config.Service, id string) (bool, error) {
	_, err := svc.GetSource(ctx, id)
	return isNew(err)
}

func sinkIsNew(ctx context.Context, svc *config.Service, id string) (bool, error) {
	_, err := svc.GetSink(ctx, id)
	return isNew(err)
}

func connectorIsNew(ctx context.Context, svc *config.Service, id string) (bool, error) {
	_, err := svc.GetConnector(ctx, id)
	return isNew(err)
}

func isNew(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if errors.Is(err, config.ErrNotFound) {
		return true, nil
	}
	return false, err
}

func statusesFor(all []supervisor.Status, id string) []supervisor.Status {
	out := make([]supervisor.Status, 0, 1)
	for _, status := range all {
		if status.ID == id {
			out = append(out, status)
		}
	}
	return out
}

func publicError(log *slog.Logger, err error) error {
	var validation *config.ValidationError
	if errors.As(err, &validation) || errors.Is(err, config.ErrNotFound) ||
		errors.Is(err, config.ErrExists) || errors.Is(err, config.ErrInUse) {
		return err
	}
	log.Error("MCP internal error", "err", err)
	return errors.New("internal error")
}
