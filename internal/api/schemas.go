package api

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/open-ships/beacon/internal/model"
)

// durationSchema describes the JSON representation without coupling the domain
// model to an HTTP framework. Register the alias before any parent schema is built.
type durationSchema string

func (durationSchema) Schema(huma.Registry) *huma.Schema {
	return &huma.Schema{
		Type:        huma.TypeString,
		Description: "Go duration string (for example 10s, 1m30s, or 24h). Zero is written as 0s.",
		Examples:    []any{"10s", "24h"},
	}
}

func configureSchemas(r huma.Registry) {
	r.RegisterTypeAlias(reflect.TypeFor[model.Duration](), reflect.TypeFor[durationSchema]())
	r.Schema(reflect.TypeFor[model.Config](), false, "")
	schemas := r.Map()

	// Limits come from the same constants used by domain validation. String
	// budgets are UTF-8 bytes, not JSON Schema's Unicode-character maxLength.
	for _, name := range []string{"Source", "Sink", "Connector"} {
		s := schemas[name]
		for field, description := range map[string]string{
			"id":      "Stable ID within this entity kind. Lowercase letters, digits, underscores and hyphens; must start with a letter or digit. PUT body ID must match the path.",
			"name":    "Operator-facing name. An empty name displays the ID.",
			"enabled": "Whether the appliance should run this entity. Disabling preserves configuration and retained route history.",
		} {
			s.Properties[field].Description = description
		}
		s.Properties["id"].Pattern = "^[a-z0-9][a-z0-9_-]*$"
		textBudget(s.Properties["id"], model.MaxEntityIDBytes)
		textBudget(s.Properties["name"], model.MaxEntityNameBytes)
		for _, field := range []string{"interface", "port", "url", "path", "address", "file_path"} {
			if p := s.Properties[field]; p != nil {
				textBudget(p, model.MaxEndpointTextBytes)
			}
		}
		if p := s.Properties["topic"]; p != nil {
			textBudget(p, model.MaxTopicBytes)
		}
		if p := s.Properties["headers"]; p != nil {
			p.Description = "Custom HTTP headers, including authentication. Stored in configuration and exports; protect these as secrets. Redirects are not followed."
			p.MaxProperties = ptr(model.MaxEndpointHeaders)
			p.Description += fmt.Sprintf(" Header names: at most %d UTF-8 bytes; values: at most %d UTF-8 bytes.", model.MaxHeaderNameBytes, model.MaxHeaderValueBytes)
			textBudget(p.AdditionalProperties.(*huma.Schema), model.MaxHeaderValueBytes)
		}
	}

	source := schemas["Source"]
	describe(source, map[string]string{
		"type":      "Input transport. Type-specific required fields are described by oneOf.",
		"interface": "SocketCAN interface, required for socketcan (for example can0).",
		"port":      "Serial device path, required for usbcan.",
		"url":       "Remote endpoint, required for http_sse, http_ws, and mqtt. Use the final endpoint URL; redirects are not followed.",
		"topic":     "MQTT subscription topic, required for mqtt; subscription wildcards are supported.",
		"file_path": "Absolute capture-log path, required for file. Gzip is detected automatically.",
		"address":   "Gateway host:port, required for tcp and udp.",
		"format":    "Required for tcp/udp: ydraw (Yacht Devices RAW) or actisense.",
	})
	variants(source, map[string][]string{
		string(model.SourceSocketCAN): {"interface"}, string(model.SourceUSBCAN): {"port"},
		string(model.SourceHTTPSSE): {"url"}, string(model.SourceHTTPWS): {"url"},
		string(model.SourceMQTT): {"url", "topic"}, string(model.SourceFile): {"file_path"},
		string(model.SourceTCP): {"address", "format"}, string(model.SourceUDP): {"address", "format"},
	})

	sink := schemas["Sink"]
	describe(sink, map[string]string{
		"type":      "Output transport. Type-specific required fields are described by oneOf.",
		"interface": "SocketCAN interface, required for socketcan.", "port": "Serial device path, required for usbcan.",
		"path":              "Unique data-server path, required for http_sse/http_ws. Must start with / and must not use a reserved admin prefix.",
		"address":           "Listen host:port for tcp, or destination gateway host:port for tcp_gateway.",
		"url":               "Destination URL for http_post, mqtt, or postgres. PostgreSQL accepts postgres:// and postgresql://. Credentials are included in configuration exports.",
		"topic":             "MQTT publish topic, required for mqtt. Publish wildcards are forbidden.",
		"batch_size":        fmt.Sprintf("Maximum envelopes per HTTP POST or PostgreSQL batch. 0 or omitted uses %d; maximum %d. Smaller available batches send immediately.", model.DefaultHTTPPostBatchSize, model.MaxHTTPPostBatchSize),
		"request_timeout":   fmt.Sprintf("HTTP POST request timeout; nonnegative Go duration. 0s or omitted uses %s.", time.Duration(model.DefaultHTTPPostRequestTimeout)),
		"gzip":              "HTTP POST only: compress the JSON request body with gzip. Default false.",
		"file_path":         "Absolute output path, required for file.",
		"format":            "Required for file: ndjson or candump. Required for tcp_gateway: ydraw or actisense.",
		"max_file_bytes":    fmt.Sprintf("File rotation threshold in bytes. 0 or omitted uses %d.", model.DefaultMaxFileBytes),
		"max_files":         fmt.Sprintf("Total active and rotated files. 0 or omitted uses %d; maximum %d.", model.DefaultMaxFiles, model.MaxFileCount),
		"table":             "PostgreSQL table or schema.table; omitted/empty uses " + model.DefaultPostgresTable + ". The schema must already exist.",
		"auto_create_table": "PostgreSQL: create and verify the destination table. Default false; otherwise wait for an operator-created table.",
		"timescaledb":       "PostgreSQL: use a hypertable partitioned by observed_at. The TimescaleDB extension must already be installed. Default false.",
		"write_timeout":     fmt.Sprintf("PostgreSQL initialization/write timeout. Nonnegative Go duration up to %s; 0s or omitted uses %s.", time.Duration(model.MaxPostgresWriteTimeout), time.Duration(model.DefaultPostgresWriteTimeout)),
	})
	variants(sink, map[string][]string{
		string(model.SinkSocketCAN): {"interface"}, string(model.SinkUSBCAN): {"port"},
		string(model.SinkHTTPSSE): {"path"}, string(model.SinkHTTPWS): {"path"},
		string(model.SinkHTTPPost): {"url"}, string(model.SinkTCP): {"address"},
		string(model.SinkFile): {"file_path", "format"}, string(model.SinkMQTT): {"url", "topic"},
		string(model.SinkPostgres): {"url"}, string(model.SinkTCPGateway): {"address", "format"}, string(model.SinkNull): {},
	})
	for _, s := range []*huma.Schema{source, sink} {
		for _, variant := range s.OneOf {
			typ := variant.Properties["type"].Enum[0].(string)
			switch typ {
			case "tcp", "udp", "tcp_gateway":
				if s == source || typ == "tcp_gateway" {
					variant.Properties["format"] = &huma.Schema{Type: huma.TypeString, Enum: []any{model.StreamFormatYDRaw, model.StreamFormatActisense}}
				}
			case "file":
				if s == sink {
					variant.Properties["format"] = &huma.Schema{Type: huma.TypeString, Enum: []any{model.FileFormatNDJSON, model.FileFormatCANDump}}
					variant.Properties["max_file_bytes"] = &huma.Schema{Type: huma.TypeInteger, Minimum: ptr(float64(0))}
					variant.Properties["max_files"] = &huma.Schema{Type: huma.TypeInteger, Minimum: ptr(float64(0)), Maximum: ptr(float64(model.MaxFileCount))}
				}
			case "http_post":
				variant.Properties["batch_size"] = &huma.Schema{Type: huma.TypeInteger, Minimum: ptr(float64(0)), Maximum: ptr(float64(model.MaxHTTPPostBatchSize))}
			case "postgres":
				variant.Properties["batch_size"] = &huma.Schema{Type: huma.TypeInteger, Minimum: ptr(float64(0)), Maximum: ptr(float64(model.MaxPostgresBatchSize))}
			}
		}
	}

	connector := schemas["Connector"]
	describe(connector, map[string]string{
		"source_id": "ID of an existing source.", "sink_id": "ID of an existing sink (also required in observe mode).",
		"filters":            "CEL boolean expressions, ANDed in order. An omitted/empty list accepts every envelope.",
		"buffer":             "Independent route retention limits. Count and byte defaults always apply; age is optional.",
		"mode":               "Bridge policy. Omitted/empty means semantic. Transparent requires a SocketCAN sink and cannot forward from the same SocketCAN interface. Observe retains without forwarding.",
		"forward_management": "Permit management PGNs in transparent mode only. Default false.",
	})
	connector.Properties["mode"].Enum = []any{"", model.BridgeSemantic, model.BridgeTransparent, model.BridgeObserve}
	for _, key := range []string{"source_id", "sink_id"} {
		connector.Properties[key].MinLength = ptr(1)
		textBudget(connector.Properties[key], model.MaxEntityIDBytes)
	}
	connector.Properties["filters"].MaxItems = ptr(model.MaxConnectorFilters)
	textBudget(connector.Properties["filters"].Items, model.MaxFilterExpressionLen)
	describe(schemas["BufferLimits"], map[string]string{
		"max_messages": fmt.Sprintf("Retained message count. 0 or omitted uses %d.", model.DefaultMaxMessages),
		"max_bytes":    fmt.Sprintf("Logical retained-envelope bytes. 0 or omitted uses %d.", model.DefaultBufferMaxBytes),
		"max_age":      fmt.Sprintf("Nonnegative retention age up to %s. 0s or omitted disables age expiration.", time.Duration(model.MaxBufferAge)),
	})
	bounds(schemas["BufferLimits"].Properties["max_messages"], 0, float64(model.MaxBufferMessages))
	bounds(schemas["BufferLimits"].Properties["max_bytes"], 0, float64(model.MaxBufferBytes))
	describe(schemas["ResourceConfig"], map[string]string{
		"max_database_bytes":     fmt.Sprintf("Physical SQLite budget; 0 or omitted uses %d. Nonzero range: %d to %d bytes.", model.DefaultMaxDatabaseBytes, model.MinDatabaseBytes, model.MaxDatabaseBytes),
		"database_reserve_bytes": fmt.Sprintf("Reserve for non-queue SQLite storage; 0 or omitted uses %d. Must be smaller than max_database_bytes.", model.DefaultDatabaseReserve),
		"max_file_store_bytes":   fmt.Sprintf("Combined file-sink budget; 0 or omitted uses %d bytes.", model.DefaultMaxFileStoreBytes),
	})
	resources := schemas["ResourceConfig"].Properties
	resources["max_database_bytes"].AnyOf = []*huma.Schema{
		{Type: huma.TypeInteger, Enum: []any{0}},
		{Type: huma.TypeInteger, Minimum: ptr(float64(model.MinDatabaseBytes)), Maximum: ptr(float64(model.MaxDatabaseBytes))},
	}
	resources["database_reserve_bytes"].Minimum = ptr(float64(0))
	bounds(resources["max_file_store_bytes"], 0, float64(model.MaxDatabaseBytes))
	describe(schemas["Settings"], map[string]string{
		"observability": "Optional diagnostic exports. An omitted section is preserved by merge import.",
		"resources":     "Process-wide storage budgets. An omitted section is preserved by merge import.",
	})
	describe(schemas["ObservabilityConfig"], map[string]string{
		"prometheus_source_details": "Export per-PGN/field/raw-byte Prometheus metrics. Default false; increases resource use.",
	})
	describe(schemas["Config"], map[string]string{
		"sources":    "Configured input endpoints. IDs are unique within sources.",
		"sinks":      "Configured output endpoints. IDs are unique within sinks.",
		"connectors": "Configured routes. IDs are unique within connectors; source_id/sink_id must resolve.",
		"settings":   "Optional appliance settings. Merge preserves omitted sections; replace applies defaults for omitted settings.",
	})
	for field, max := range map[string]int{"sources": model.MaxSources, "sinks": model.MaxSinks, "connectors": model.MaxConnectors} {
		schemas["Config"].Properties[field].MaxItems = ptr(max)
	}
	schemas["Config"].Description = fmt.Sprintf("Sources, sinks, connector routes and appliance settings. Operator-authored entity strings together must not exceed %d UTF-8 bytes. Writes validate the whole resulting configuration before persistence.", model.MaxAuthoredConfigTextBytes)
	for _, s := range schemas {
		s.PrecomputeMessages()
	}
}

func ptr[T any](v T) *T { return &v }

func describe(s *huma.Schema, fields map[string]string) {
	for name, description := range fields {
		p := s.Properties[name]
		if p.Description != "" {
			p.Description += " "
		}
		p.Description += description
	}
}

func textBudget(s *huma.Schema, max int) {
	s.Description += fmt.Sprintf(" Maximum %d UTF-8 bytes.", max)
	s.Extensions = map[string]any{"x-max-bytes": max}
}

func bounds(s *huma.Schema, min, max float64) {
	s.Minimum, s.Maximum = &min, &max
}

// Each variant only constrains the discriminator and required fields. Other
// fields remain legal, matching the model's handling of inactive type settings.
func variants(s *huma.Schema, required map[string][]string) {
	for _, typ := range slices.Sorted(maps.Keys(required)) {
		s.Properties["type"].Enum = append(s.Properties["type"].Enum, typ)
		v := &huma.Schema{Type: huma.TypeObject, Required: append([]string{"type"}, required[typ]...), Properties: map[string]*huma.Schema{
			"type": {Type: huma.TypeString, Enum: []any{typ}},
		}}
		for _, field := range required[typ] {
			v.Properties[field] = &huma.Schema{Type: huma.TypeString, MinLength: ptr(1)}
		}
		s.OneOf = append(s.OneOf, v)
	}
}
