package model

import (
	"encoding/json"
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

// A file sink with zero MaxFileBytes/MaxFiles (meaning "use defaults") must
// validate cleanly — zero is not the same as negative.
func TestValidateFileSinkOK(t *testing.T) {
	cfg := validConfig()
	cfg.Sinks = append(cfg.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
		FilePath: "/var/log/beacon/nav.jsonl", Format: FileFormatNDJSON})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid file sink rejected: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"duplicate id", func(c *Config) { c.Sources = append(c.Sources, c.Sources[0]) }},
		{"bad slug", func(c *Config) { c.Sources[0].ID = "Can 0!" }},
		{"socketcan without interface", func(c *Config) { c.Sources[0].Interface = "" }},
		{"http source without url", func(c *Config) { c.Sources[1].URL = "" }},
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
				FilePath: "/var/log/beacon.log", Format: "xml"})
		}},
		{"file sink negative max_file_bytes", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
				FilePath: "/var/log/beacon.log", Format: FileFormatNDJSON, MaxFileBytes: -1})
		}},
		{"file sink negative max_files", func(c *Config) {
			c.Sinks = append(c.Sinks, Sink{ID: "log", Name: "Log", Type: SinkFile, Enabled: true,
				FilePath: "/var/log/beacon.log", Format: FileFormatNDJSON, MaxFiles: -1})
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
	if l.MaxMessages != 100000 {
		t.Fatalf("default MaxMessages = %d, want 100000", l.MaxMessages)
	}
	// explicit limit is preserved, no default injected
	l = BufferLimits{MaxAge: Duration(time.Hour)}.ApplyDefaults()
	if l.MaxMessages != 0 || time.Duration(l.MaxAge) != time.Hour {
		t.Fatalf("explicit limits mangled: %+v", l)
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
