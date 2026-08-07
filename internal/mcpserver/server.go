// Package mcpserver exposes Beacon's local configuration and operational
// state through the Model Context Protocol. It is a thin transport over the
// same config.Service and stats.Registry used by the REST API and web UI, so
// MCP writes get identical validation, persistence, and hot reconciliation.
package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/stats"
	"github.com/open-ships/beacon/internal/supervisor"
)

const EndpointPath = "/mcp"

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
	{Name: "get_source_metrics", Access: "read", Description: "Inspect all PGNs by source and sender, including unknown raw payloads, timing, load, decode quality, addressing, gaps, field distributions, and lifecycle events."},
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

// Handler serves the current MCP Streamable HTTP transport. It uses ordinary
// JSON responses because Beacon's tools complete synchronously and do not need
// server-to-client streaming. Sessions are still negotiated normally for broad
// client compatibility. Cross-origin protection enforces the MCP transport's
// Origin requirement while continuing to allow non-browser local/LAN agents.
func Handler(svc *config.Service, reg *stats.Registry, version string, log *slog.Logger) http.Handler {
	server := NewServer(svc, reg, version, log)
	transport := sdkmcp.NewStreamableHTTPHandler(func(*http.Request) *sdkmcp.Server {
		return server
	}, &sdkmcp.StreamableHTTPOptions{
		JSONResponse:   true,
		Logger:         log,
		SessionTimeout: 30 * time.Minute,
	})
	return allMethodOriginGuard(http.NewCrossOriginProtection().Handler(transport))
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
	ID           string `json:"id" jsonschema:"Stable sink id; lowercase letters, digits, underscores, and hyphens."`
	Name         string `json:"name" jsonschema:"Human-readable sink name."`
	Type         string `json:"type" jsonschema:"Sink type: socketcan, usbcan, http_sse, http_ws, tcp, file, mqtt, tcp_gateway, or null."`
	Enabled      bool   `json:"enabled" jsonschema:"Whether Beacon should run this sink."`
	Interface    string `json:"interface,omitempty" jsonschema:"SocketCAN interface, required for socketcan."`
	Port         string `json:"port,omitempty" jsonschema:"Serial device path, required for usbcan."`
	Path         string `json:"path,omitempty" jsonschema:"Local data-server path for http_sse or http_ws."`
	Address      string `json:"address,omitempty" jsonschema:"Listen or gateway host:port for tcp sinks."`
	URL          string `json:"url,omitempty" jsonschema:"MQTT broker URL."`
	Topic        string `json:"topic,omitempty" jsonschema:"MQTT publish topic."`
	FilePath     string `json:"file_path,omitempty" jsonschema:"Absolute output path for file sinks."`
	Format       string `json:"format,omitempty" jsonschema:"File format ndjson/candump or gateway format ydraw/actisense."`
	MaxFileBytes int64  `json:"max_file_bytes,omitempty" jsonschema:"File rotation threshold in bytes; zero uses Beacon's default."`
	MaxFiles     int    `json:"max_files,omitempty" jsonschema:"Total active plus rotated files to retain; zero uses Beacon's default."`
}

func sinkDefinitionFromModel(v model.Sink) sinkDefinition {
	return sinkDefinition{
		ID: v.ID, Name: v.Name, Type: string(v.Type), Enabled: v.Enabled,
		Interface: v.Interface, Port: v.Port, Path: v.Path, Address: v.Address,
		URL: v.URL, Topic: v.Topic, FilePath: v.FilePath, Format: v.Format,
		MaxFileBytes: v.MaxFileBytes, MaxFiles: v.MaxFiles,
	}
}

func (v sinkDefinition) model() model.Sink {
	return model.Sink{
		ID: v.ID, Name: v.Name, Type: model.SinkType(v.Type), Enabled: v.Enabled,
		Interface: v.Interface, Port: v.Port, Path: v.Path, Address: v.Address,
		URL: v.URL, Topic: v.Topic, FilePath: v.FilePath, Format: v.Format,
		MaxFileBytes: v.MaxFileBytes, MaxFiles: v.MaxFiles,
	}
}

type bufferDefinition struct {
	MaxMessages int64  `json:"max_messages,omitempty" jsonschema:"Maximum retained messages; all-zero limits default to 10000 messages."`
	MaxAge      string `json:"max_age,omitempty" jsonschema:"Maximum retained age as a Go duration such as 24h or 90s."`
	MaxBytes    int64  `json:"max_bytes,omitempty" jsonschema:"Maximum retained bytes."`
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

func connectorDefinitionFromModel(v model.Connector) connectorDefinition {
	maxAge := ""
	if v.Buffer.MaxAge != 0 {
		maxAge = time.Duration(v.Buffer.MaxAge).String()
	}
	return connectorDefinition{
		ID: v.ID, Name: v.Name, SourceID: v.SourceID, SinkID: v.SinkID,
		Filters: v.Filters, Buffer: bufferDefinition{
			MaxMessages: v.Buffer.MaxMessages, MaxAge: maxAge, MaxBytes: v.Buffer.MaxBytes,
		},
		Enabled: v.Enabled, Mode: string(v.Mode), ForwardManagement: v.ForwardManagement,
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
	Sources    []sourceDefinition    `json:"sources"`
	Sinks      []sinkDefinition      `json:"sinks"`
	Connectors []connectorDefinition `json:"connectors"`
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
	Connector connectorDefinition `json:"connector"`
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
	SourceAddress *uint8  `json:"source_address,omitempty" jsonschema:"Optional NMEA 2000 source-address filter."`
	EventLimit    int     `json:"event_limit,omitempty" jsonschema:"Recent lifecycle events per source to include; zero defaults to 50."`
}

type sourceMetricsOutput struct {
	GeneratedAt time.Time                            `json:"generated_at"`
	Sources     map[string][]stats.SourcePGNMetric   `json:"sources"`
	Events      map[string][]stats.SourceMetricEvent `json:"events"`
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
				Connectors: make([]connectorDefinition, 0, len(cfg.Connectors)),
			}
			for _, v := range cfg.Sources {
				out.Sources = append(out.Sources, sourceDefinitionFromModel(v))
			}
			for _, v := range cfg.Sinks {
				out.Sinks = append(out.Sinks, sinkDefinitionFromModel(v))
			}
			for _, v := range cfg.Connectors {
				out.Connectors = append(out.Connectors, connectorDefinitionFromModel(v))
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
			v := in.Sink.model()
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
			return nil, putConnectorOutput{Connector: connectorDefinitionFromModel(v), Status: statusesFor(svc.Statuses(), v.ID)}, nil
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
			out := sourceMetricsOutput{
				GeneratedAt: time.Now().UTC(),
				Sources:     map[string][]stats.SourcePGNMetric{},
				Events:      map[string][]stats.SourceMetricEvent{},
			}
			eventLimit := in.EventLimit
			if eventLimit <= 0 || eventLimit > 200 {
				eventLimit = 50
			}
			if in.SourceID != "" {
				if _, err := svc.GetSource(ctx, in.SourceID); err != nil {
					return nil, sourceMetricsOutput{}, publicError(log, err)
				}
				out.Sources[in.SourceID] = filterSourceMetrics(reg.SourcePGNMetrics(in.SourceID), in)
				out.Events[in.SourceID] = filterSourceMetricEvents(reg.SourceMetricEvents(in.SourceID, eventLimit), in)
				return nil, out, nil
			}
			sources, err := svc.ListSources(ctx)
			if err != nil {
				return nil, sourceMetricsOutput{}, publicError(log, err)
			}
			all := reg.AllSourcePGNMetrics()
			for _, source := range sources {
				out.Sources[source.ID] = filterSourceMetrics(all[source.ID], in)
				out.Events[source.ID] = filterSourceMetricEvents(reg.SourceMetricEvents(source.ID, eventLimit), in)
			}
			return nil, out, nil
		})
}

func filterSourceMetrics(metrics []stats.SourcePGNMetric, in sourceMetricsInput) []stats.SourcePGNMetric {
	out := make([]stats.SourcePGNMetric, 0, len(metrics))
	for _, metric := range metrics {
		if in.PGN != nil && metric.PGN != *in.PGN {
			continue
		}
		if in.SourceAddress != nil && metric.SourceAddress != *in.SourceAddress {
			continue
		}
		out = append(out, metric)
	}
	return out
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
