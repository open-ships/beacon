package model

import (
	"encoding/json"
	"path/filepath"
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

func TestValidateNullSinkOK(t *testing.T) {
	cfg := validConfig()
	cfg.Sinks = append(cfg.Sinks, Sink{
		ID: "discard", Name: "Discard", Type: SinkNull, Enabled: true,
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid null sink rejected: %v", err)
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
