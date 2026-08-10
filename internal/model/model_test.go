package model

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		Sources: []Source{
			{ID: "can0", Name: "Main bus", Type: SourceSocketCAN, Enabled: true, Interface: "can0"},
			{ID: "remote", Name: "Remote beacon", Type: SourceHTTPSSE, Enabled: true, URL: "http://10.0.0.2:8080/events"},
		},
		Sinks: []Sink{
			{ID: "nav-sse", Name: "Nav stream", Type: SinkHTTPSSE, Enabled: true, Path: "/nav"},
			{ID: "can2", Name: "Second bus", Type: SinkSocketCAN, Enabled: true, Interface: "can2"},
		},
		Connectors: []Connector{
			{ID: "nav", Name: "Nav to SSE", SourceID: "can0", SinkID: "nav-sse", Enabled: true,
				Filters: []string{"msg.pgn == 127250"},
				Buffer:  BufferLimits{MaxMessages: 1000}},
		},
	}
}

func TestValidateOK(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestPrometheusSourceDetailsConfigCompatibility(t *testing.T) {
	legacy := []byte(`{"sources":[],"sinks":[],"connectors":[]}`)
	var cfg Config
	if err := json.Unmarshal(legacy, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.PrometheusSourceDetailsEnabled() {
		t.Fatal("legacy config enabled rich Prometheus source details")
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(legacy) {
		t.Fatalf("legacy config JSON = %s, want %s", encoded, legacy)
	}

	cfg.Settings = &Settings{Observability: &ObservabilityConfig{PrometheusSourceDetails: true}}
	if !cfg.PrometheusSourceDetailsEnabled() {
		t.Fatal("explicit opt-in did not enable rich Prometheus source details")
	}
	encoded, err = json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"settings":{"observability":{"prometheus_source_details":true}}`) {
		t.Fatalf("opt-in config JSON = %s", encoded)
	}
}

func TestTransparentBridgeValidation(t *testing.T) {
	cfg := validConfig()
	cfg.Connectors[0].SinkID = "can2"
	cfg.Connectors[0].Mode = BridgeTransparent
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid transparent bridge rejected: %v", err)
	}

	cfg.Sinks[1].Interface = "can0"
	if err := cfg.Validate(); err == nil {
		t.Fatal("same-interface transparent loop accepted")
	}
}

func TestTransparentBridgeRequiresSocketCANSink(t *testing.T) {
	cfg := validConfig()
	cfg.Connectors[0].Mode = BridgeTransparent
	if err := cfg.Validate(); err == nil {
		t.Fatal("transparent bridge to SSE accepted")
	}
}

// A file sink with zero MaxFileBytes/MaxFiles (meaning "use defaults") must
// validate cleanly — zero is not the same as negative.
func TestValidateFileSinkOK(t *testing.T) {
	cfg := validConfig()
	cfg.Sinks = append(cfg.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
		FilePath: filepath.Join(t.TempDir(), "nav.jsonl"), Format: FileFormatNDJSON})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid file sink rejected: %v", err)
	}
}

func TestValidateMQTTSourceAndSinkOK(t *testing.T) {
	cfg := validConfig()
	cfg.Sources = append(cfg.Sources, Source{
		ID: "mqtt-in", Name: "MQTT input", Type: SourceMQTT, Enabled: true,
		URL: "mqtt://broker.local:1883", Topic: "vessels/main/engine/#",
	})
	cfg.Sinks = append(cfg.Sinks, Sink{
		ID: "mqtt-out", Name: "MQTT output", Type: SinkMQTT, Enabled: true,
		URL: "mqtts://broker.local:8883", Topic: "vessels/main/engine/json",
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid mqtt source/sink rejected: %v", err)
	}
	if got := NormalizeMQTTBrokerURL("mqtts://broker.local:8883"); got != "ssl://broker.local:8883" {
		t.Fatalf("NormalizeMQTTBrokerURL = %q", got)
	}
}

func TestValidateResourceCardinalityCaps(t *testing.T) {
	cfg := validConfig()
	for i := len(cfg.Sources); i <= MaxSources; i++ {
		cfg.Sources = append(cfg.Sources, Source{ID: fmt.Sprintf("source-%d", i), Type: SourceSocketCAN, Interface: "can0"})
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("configuration above source cap validated")
	}

	connector := Connector{ID: "bounded", SourceID: "src", SinkID: "sink"}
	connector.Filters = make([]string, MaxConnectorFilters+1)
	if err := connector.Validate(); err == nil {
		t.Fatal("connector above filter count cap validated")
	}
	connector.Filters = []string{strings.Repeat("x", MaxFilterExpressionLen+1)}
	if err := connector.Validate(); err == nil {
		t.Fatal("connector above filter byte cap validated")
	}
}

func TestDirectValidationTextBoundaries(t *testing.T) {
	name := strings.Repeat("n", MaxEntityNameBytes)
	endpoint := strings.Repeat("e", MaxEndpointTextBytes)
	topic := strings.Repeat("t", MaxTopicBytes)
	headerName := strings.Repeat("X", MaxHeaderNameBytes)
	headerValue := strings.Repeat("v", MaxHeaderValueBytes)

	newSource := func() Source {
		return Source{
			ID: "source", Name: name, Type: SourceSocketCAN, Interface: endpoint,
			Port: endpoint, URL: endpoint, Topic: topic, FilePath: endpoint,
			Address: endpoint, Headers: map[string]string{headerName: headerValue},
		}
	}
	if err := newSource().Validate(); err != nil {
		t.Fatalf("source at text boundaries rejected: %v", err)
	}
	sourceCases := []struct {
		name   string
		mutate func(*Source)
	}{
		{"name", func(s *Source) { s.Name += "x" }},
		{"interface", func(s *Source) { s.Interface += "x" }},
		{"port", func(s *Source) { s.Port += "x" }},
		{"url", func(s *Source) { s.URL += "x" }},
		{"file_path", func(s *Source) { s.FilePath += "x" }},
		{"address", func(s *Source) { s.Address += "x" }},
		{"topic", func(s *Source) { s.Topic += "x" }},
		{"header_name", func(s *Source) { s.Headers = map[string]string{headerName + "x": headerValue} }},
		{"header_value", func(s *Source) { s.Headers = map[string]string{headerName: headerValue + "x"} }},
	}
	for _, tc := range sourceCases {
		t.Run("source_"+tc.name, func(t *testing.T) {
			source := newSource()
			tc.mutate(&source)
			if err := source.Validate(); err == nil {
				t.Fatal("over-limit source text accepted")
			}
		})
	}

	newSink := func() Sink {
		return Sink{
			ID: "sink", Name: name, Type: SinkNull, Interface: endpoint,
			Port: endpoint, Path: endpoint, Address: endpoint, URL: endpoint,
			Topic: topic, FilePath: endpoint, Headers: map[string]string{headerName: headerValue},
		}
	}
	if err := newSink().Validate(); err != nil {
		t.Fatalf("sink at text boundaries rejected: %v", err)
	}
	sinkCases := []struct {
		name   string
		mutate func(*Sink)
	}{
		{"name", func(s *Sink) { s.Name += "x" }},
		{"interface", func(s *Sink) { s.Interface += "x" }},
		{"port", func(s *Sink) { s.Port += "x" }},
		{"path", func(s *Sink) { s.Path += "x" }},
		{"address", func(s *Sink) { s.Address += "x" }},
		{"url", func(s *Sink) { s.URL += "x" }},
		{"file_path", func(s *Sink) { s.FilePath += "x" }},
		{"topic", func(s *Sink) { s.Topic += "x" }},
		{"header_name", func(s *Sink) { s.Headers = map[string]string{headerName + "x": headerValue} }},
		{"header_value", func(s *Sink) { s.Headers = map[string]string{headerName: headerValue + "x"} }},
	}
	for _, tc := range sinkCases {
		t.Run("sink_"+tc.name, func(t *testing.T) {
			sink := newSink()
			tc.mutate(&sink)
			if err := sink.Validate(); err == nil {
				t.Fatal("over-limit sink text accepted")
			}
		})
	}

	connector := Connector{
		ID: "connector", Name: name,
		SourceID: strings.Repeat("s", MaxEntityIDBytes),
		SinkID:   strings.Repeat("k", MaxEntityIDBytes),
	}
	if err := connector.Validate(); err != nil {
		t.Fatalf("connector at text boundaries rejected: %v", err)
	}
	connector.Name += "x"
	if err := connector.Validate(); err == nil {
		t.Fatal("over-limit connector name accepted")
	}
	connector.Name = name
	connector.SourceID += "x"
	if err := connector.Validate(); err == nil {
		t.Fatal("over-limit connector reference accepted")
	}
}

func TestAuthoredConfigTextBudgetBoundary(t *testing.T) {
	cfg := validConfig()
	cfg.Connectors[0].Filters = nil
	used, ok := authoredConfigTextBytes(&cfg, MaxAuthoredConfigTextBytes)
	if !ok {
		t.Fatalf("small valid config unexpectedly exceeds authored-text budget")
	}
	remaining := MaxAuthoredConfigTextBytes - used
	for remaining > 0 {
		chunk := min(remaining, MaxFilterExpressionLen)
		cfg.Connectors[0].Filters = append(cfg.Connectors[0].Filters, strings.Repeat("x", chunk))
		remaining -= chunk
	}
	if len(cfg.Connectors[0].Filters) > MaxConnectorFilters {
		t.Fatalf("test fixture needs %d filters, maximum is %d", len(cfg.Connectors[0].Filters), MaxConnectorFilters)
	}
	if got, within := authoredConfigTextBytes(&cfg, MaxAuthoredConfigTextBytes); !within || got != MaxAuthoredConfigTextBytes {
		t.Fatalf("authored text = %d/%t, want exact limit %d", got, within, MaxAuthoredConfigTextBytes)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("configuration at authored-text boundary rejected: %v", err)
	}

	cfg.Sources[0].Name += "x"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "authored text") {
		t.Fatalf("configuration above authored-text boundary error = %v", err)
	}
}

func TestAuthoredConfigTextCountDoesNotAllocate(t *testing.T) {
	cfg := validConfig()
	if allocs := testing.AllocsPerRun(100, func() {
		_, _ = authoredConfigTextBytes(&cfg, MaxAuthoredConfigTextBytes)
	}); allocs != 0 {
		t.Fatalf("authored config text count allocated %v objects per call", allocs)
	}
}

func TestValidateNullSinkOK(t *testing.T) {
	cfg := validConfig()
	cfg.Sinks = append(cfg.Sinks, Sink{
		ID: "discard", Name: "Discard", Type: SinkNull, Enabled: true,
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid null sink rejected: %v", err)
	}
}

func TestValidateHTTPPostSink(t *testing.T) {
	for _, endpoint := range []string{"http://collector.local/v1/envelopes", "https://collector.local/v1/envelopes?vessel=main"} {
		t.Run(endpoint, func(t *testing.T) {
			s := Sink{
				ID: "webhook", Name: "Webhook", Type: SinkHTTPPost, Enabled: true,
				URL: endpoint, BatchSize: 250, RequestTimeout: Duration(15 * time.Second),
				Headers: map[string]string{"Authorization": "Bearer token", "X-API-Key": "secret"}, Gzip: true,
			}
			if err := s.Validate(); err != nil {
				t.Fatalf("valid HTTP POST sink rejected: %v", err)
			}
		})
	}

	base := Sink{ID: "webhook", Type: SinkHTTPPost, URL: "https://collector.local/events"}
	cases := []struct {
		name   string
		mutate func(*Sink)
	}{
		{"missing host", func(s *Sink) { s.URL = "https:///events" }},
		{"unsupported scheme", func(s *Sink) { s.URL = "ftp://collector.local/events" }},
		{"fragment", func(s *Sink) { s.URL += "#ignored" }},
		{"url credentials", func(s *Sink) { s.URL = "https://user:pass@collector.local/events" }},
		{"negative batch", func(s *Sink) { s.BatchSize = -1 }},
		{"oversize batch", func(s *Sink) { s.BatchSize = MaxHTTPPostBatchSize + 1 }},
		{"negative timeout", func(s *Sink) { s.RequestTimeout = -1 }},
		{"invalid header name", func(s *Sink) { s.Headers = map[string]string{"Bad Header": "value"} }},
		{"padded header name", func(s *Sink) { s.Headers = map[string]string{" Authorization ": "value"} }},
		{"duplicate header", func(s *Sink) { s.Headers = map[string]string{"X-API-Key": "one", "x-api-key": "two"} }},
		{"header newline", func(s *Sink) { s.Headers = map[string]string{"Authorization": "Bearer ok\r\nX-Bad: yes"} }},
		{"managed header", func(s *Sink) { s.Headers = map[string]string{"Idempotency-Key": "fixed"} }},
		{"managed encoding header", func(s *Sink) { s.Headers = map[string]string{"Content-Encoding": "br"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := base
			tc.mutate(&s)
			if err := s.Validate(); err == nil {
				t.Fatal("invalid HTTP POST sink accepted")
			}
		})
	}
}

func TestHTTPPostDefaults(t *testing.T) {
	s := Sink{Type: SinkHTTPPost}
	if got := s.EffectiveHTTPPostBatchSize(); got != DefaultHTTPPostBatchSize {
		t.Fatalf("effective batch size = %d, want %d", got, DefaultHTTPPostBatchSize)
	}
	if got := s.EffectiveHTTPPostRequestTimeout(); got != time.Duration(DefaultHTTPPostRequestTimeout) {
		t.Fatalf("effective request timeout = %v, want %v", got, time.Duration(DefaultHTTPPostRequestTimeout))
	}
}

func TestValidatePostgresSink(t *testing.T) {
	for _, endpoint := range []string{
		"postgres://beacon:secret@db.local:5432/vessel?sslmode=require",
		"postgresql://beacon@db.local/vessel",
	} {
		t.Run(endpoint, func(t *testing.T) {
			s := Sink{
				ID: "telemetry-db", Name: "Telemetry database", Type: SinkPostgres, Enabled: true,
				URL: endpoint, Table: "telemetry.envelopes", BatchSize: 250,
				WriteTimeout: Duration(15 * time.Second), AutoCreateTable: true, TimescaleDB: true,
			}
			if err := s.Validate(); err != nil {
				t.Fatalf("valid PostgreSQL sink rejected: %v", err)
			}
		})
	}

	base := Sink{ID: "telemetry-db", Type: SinkPostgres, URL: "postgres://db.local/vessel"}
	cases := []struct {
		name   string
		mutate func(*Sink)
	}{
		{"missing host", func(s *Sink) { s.URL = "postgres:///vessel" }},
		{"unsupported scheme", func(s *Sink) { s.URL = "mysql://db.local/vessel" }},
		{"fragment", func(s *Sink) { s.URL += "#ignored" }},
		{"too many table components", func(s *Sink) { s.Table = "one.two.three" }},
		{"unsafe table", func(s *Sink) { s.Table = "public.envelopes;drop" }},
		{"negative batch", func(s *Sink) { s.BatchSize = -1 }},
		{"oversize batch", func(s *Sink) { s.BatchSize = MaxPostgresBatchSize + 1 }},
		{"negative timeout", func(s *Sink) { s.WriteTimeout = -1 }},
		{"oversize timeout", func(s *Sink) { s.WriteTimeout = MaxPostgresWriteTimeout + 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := base
			tc.mutate(&s)
			if err := s.Validate(); err == nil {
				t.Fatal("invalid PostgreSQL sink accepted")
			}
		})
	}
}

func TestPostgresDefaults(t *testing.T) {
	s := Sink{Type: SinkPostgres}
	if got := s.EffectivePostgresTable(); got != DefaultPostgresTable {
		t.Fatalf("effective table = %q, want %q", got, DefaultPostgresTable)
	}
	if got := s.EffectivePostgresBatchSize(); got != DefaultPostgresBatchSize {
		t.Fatalf("effective batch size = %d, want %d", got, DefaultPostgresBatchSize)
	}
	if got := s.EffectivePostgresWriteTimeout(); got != time.Duration(DefaultPostgresWriteTimeout) {
		t.Fatalf("effective write timeout = %v, want %v", got, time.Duration(DefaultPostgresWriteTimeout))
	}
}

func TestValidateFileAndGatewaySourcesOK(t *testing.T) {
	cfg := validConfig()
	cfg.Sources = append(cfg.Sources,
		Source{ID: "replay", Name: "Capture replay", Type: SourceFile, Enabled: true, FilePath: filepath.Join(t.TempDir(), "capture.log")},
		Source{ID: "gw-tcp", Name: "YD gateway", Type: SourceTCP, Enabled: true, Address: "192.168.4.1:1457", Format: StreamFormatYDRaw},
		Source{ID: "gw-udp", Name: "Actisense UDP", Type: SourceUDP, Enabled: true, Address: "0.0.0.0:2000", Format: StreamFormatActisense},
	)
	cfg.Sinks = append(cfg.Sinks, Sink{
		ID: "gw-out", Name: "YD gateway out", Type: SinkTCPGateway, Enabled: true,
		Address: "192.168.4.1:1457", Format: StreamFormatYDRaw,
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid file/tcp/udp sources or gateway sink rejected: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	absoluteFilePath := filepath.Join(t.TempDir(), "beacon.log")
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"duplicate id", func(c *Config) { c.Sources = append(c.Sources, c.Sources[0]) }},
		{"bad slug", func(c *Config) { c.Sources[0].ID = "Can 0!" }},
		{"socketcan without interface", func(c *Config) { c.Sources[0].Interface = "" }},
		{"http source without url", func(c *Config) { c.Sources[1].URL = "" }},
		{"mqtt source without broker", func(c *Config) {
			c.Sources = append(c.Sources, Source{ID: "mqtt-in", Name: "MQTT", Type: SourceMQTT, Enabled: true, Topic: "vessels/#"})
		}},
		{"mqtt source without topic", func(c *Config) {
			c.Sources = append(c.Sources, Source{ID: "mqtt-in", Name: "MQTT", Type: SourceMQTT, Enabled: true, URL: "mqtt://broker.local:1883"})
		}},
		{"file source without path", func(c *Config) {
			c.Sources = append(c.Sources, Source{ID: "replay", Name: "R", Type: SourceFile, Enabled: true})
		}},
		{"file source path not absolute", func(c *Config) {
			c.Sources = append(c.Sources, Source{ID: "replay", Name: "R", Type: SourceFile, Enabled: true, FilePath: "capture.log"})
		}},
		{"tcp source without address", func(c *Config) {
			c.Sources = append(c.Sources, Source{ID: "gw", Name: "G", Type: SourceTCP, Enabled: true, Format: StreamFormatYDRaw})
		}},
		{"tcp source bad format", func(c *Config) {
			c.Sources = append(c.Sources, Source{ID: "gw", Name: "G", Type: SourceTCP, Enabled: true, Address: "192.168.4.1:1457", Format: "nmea0183"})
		}},
		{"gateway sink without address", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "gw-out", Name: "G", Type: SinkTCPGateway, Enabled: true, Format: StreamFormatYDRaw})
		}},
		{"gateway sink bad format", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "gw-out", Name: "G", Type: SinkTCPGateway, Enabled: true, Address: "192.168.4.1:1457", Format: "candump"})
		}},
		{"sse sink without path", func(c *Config) { c.Sinks[0].Path = "" }},
		{"sink path not absolute", func(c *Config) { c.Sinks[0].Path = "nav" }},
		{"sink path reserved", func(c *Config) { c.Sinks[0].Path = "/api/v1/x" }},
		{"sink path duplicate", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "dup", Name: "d", Type: SinkHTTPWS, Enabled: true, Path: "/nav"})
		}},
		{"connector unknown source", func(c *Config) { c.Connectors[0].SourceID = "nope" }},
		{"connector unknown sink", func(c *Config) { c.Connectors[0].SinkID = "nope" }},
		{"unknown source type", func(c *Config) { c.Sources[0].Type = "carrier-pigeon" }},
		{"negative buffer limit", func(c *Config) { c.Connectors[0].Buffer.MaxMessages = -1 }},
		{"file sink without file_path", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
				FilePath: "", Format: FileFormatNDJSON})
		}},
		{"file sink path not absolute", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
				FilePath: "relative/path.log", Format: FileFormatNDJSON})
		}},
		{"file sink bad format", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
				FilePath: absoluteFilePath, Format: "xml"})
		}},
		{"file sink negative max_file_bytes", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
				FilePath: absoluteFilePath, Format: FileFormatNDJSON, MaxFileBytes: -1})
		}},
		{"file sink negative max_files", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
				FilePath: absoluteFilePath, Format: FileFormatNDJSON, MaxFiles: -1})
		}},
		{"file sink excessive max_files", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
				FilePath: absoluteFilePath, Format: FileFormatNDJSON, MaxFiles: MaxFileCount + 1})
		}},
		{"file sinks duplicate path", func(c *Config) {
			c.Sinks = append(c.Sinks,
				Sink{ID: "log-a", Name: "A", Type: SinkFile, Enabled: true, FilePath: absoluteFilePath, Format: FileFormatNDJSON},
				Sink{ID: "log-b", Name: "B", Type: SinkFile, Enabled: true, FilePath: absoluteFilePath, Format: FileFormatNDJSON})
		}},
		{"file sinks overlapping rotation paths", func(c *Config) {
			c.Sinks = append(c.Sinks,
				Sink{ID: "log-a", Name: "A", Type: SinkFile, Enabled: true, FilePath: absoluteFilePath, Format: FileFormatNDJSON},
				Sink{ID: "log-b", Name: "B", Type: SinkFile, Enabled: true, FilePath: absoluteFilePath + ".1", Format: FileFormatNDJSON})
		}},
		{"mqtt sink without broker", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "mqtt-out", Name: "MQTT", Type: SinkMQTT, Enabled: true, Topic: "vessels/main/engine/json"})
		}},
		{"mqtt sink without topic", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "mqtt-out", Name: "MQTT", Type: SinkMQTT, Enabled: true, URL: "mqtt://broker.local:1883"})
		}},
		{"mqtt sink publish wildcard", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "mqtt-out", Name: "MQTT", Type: SinkMQTT, Enabled: true,
				URL: "mqtt://broker.local:1883", Topic: "vessels/#"})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestBufferDefaults(t *testing.T) {
	l := BufferLimits{}.ApplyDefaults()
	if l.MaxMessages != DefaultMaxMessages || l.MaxBytes != DefaultBufferMaxBytes {
		t.Fatalf("default buffer = %+v, want messages=%d bytes=%d", l, DefaultMaxMessages, DefaultBufferMaxBytes)
	}
	// Explicit values are preserved while independent safety guards are added.
	l = BufferLimits{MaxAge: Duration(time.Hour)}.ApplyDefaults()
	if l.MaxMessages != DefaultMaxMessages || l.MaxBytes != DefaultBufferMaxBytes || time.Duration(l.MaxAge) != time.Hour {
		t.Fatalf("explicit limits mangled: %+v", l)
	}
}

func TestEntityIDLengthIsBoundedAtDiagnosticIdentityLimit(t *testing.T) {
	if err := validSlug(strings.Repeat("a", MaxEntityIDBytes)); err != nil {
		t.Fatalf("maximum-length id rejected: %v", err)
	}
	if err := validSlug(strings.Repeat("a", MaxEntityIDBytes+1)); err == nil {
		t.Fatal("overlong id accepted")
	}
}

func TestResourceDefaultsAndAggregateBudgets(t *testing.T) {
	cfg := validConfig()
	resources := cfg.EffectiveResources()
	if resources.MaxDatabaseBytes != DefaultMaxDatabaseBytes ||
		resources.DatabaseReserveBytes != DefaultDatabaseReserve ||
		resources.MaxFileStoreBytes != DefaultMaxFileStoreBytes {
		t.Fatalf("resource defaults = %+v", resources)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default resource budget rejected: %v", err)
	}

	cfg.Settings = &Settings{Resources: &ResourceConfig{
		MaxDatabaseBytes: MinDatabaseBytes, DatabaseReserveBytes: MinDatabaseBytes / 2,
		MaxFileStoreBytes: DefaultMaxFileStoreBytes,
	}}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "connector route max_bytes total") {
		t.Fatalf("undersized aggregate database budget error = %v", err)
	}
}

func TestDurationJSON(t *testing.T) {
	var l BufferLimits
	if err := json.Unmarshal([]byte(`{"max_age":"90s"}`), &l); err != nil {
		t.Fatal(err)
	}
	if time.Duration(l.MaxAge) != 90*time.Second {
		t.Fatalf("MaxAge = %v", l.MaxAge)
	}
	out, _ := json.Marshal(l)
	if string(out) != `{"max_age":"1m30s"}` {
		t.Fatalf("marshal = %s", out)
	}
}
