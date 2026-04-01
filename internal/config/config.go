package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// AppConfig holds application-level settings.
type AppConfig struct {
	LogLevel string `mapstructure:"log_level" json:"log_level"`
}

// CANConfig holds CAN bus interface settings.
type CANConfig struct {
	Interface string   `mapstructure:"interface"  json:"interface"`
	Bitrate   int      `mapstructure:"bitrate"    json:"bitrate"`
	AutoUp    bool     `mapstructure:"auto_up"    json:"auto_up"`
	RestartMS int      `mapstructure:"restart_ms" json:"restart_ms"`
	USBPorts  []string `mapstructure:"usb_ports"  json:"usb_ports"`
}

// BufferConfig holds SQLite buffer settings.
type BufferConfig struct {
	Path         string `mapstructure:"path"          json:"path"`
	MaxRows      int    `mapstructure:"max_rows"      json:"max_rows"`
	CheckpointMS int    `mapstructure:"checkpoint_ms" json:"checkpoint_ms"`
}

// SSESinkConfig holds SSE sink settings.
type SSESinkConfig struct {
	Enabled bool     `mapstructure:"enabled" json:"enabled"`
	Address string   `mapstructure:"address" json:"address"`
	Path    string   `mapstructure:"path"    json:"path"`
	Filters []string `mapstructure:"filters" json:"filters"`
}

// TCPSinkConfig holds TCP sink settings.
type TCPSinkConfig struct {
	Enabled bool     `mapstructure:"enabled" json:"enabled"`
	Address string   `mapstructure:"address" json:"address"`
	Filters []string `mapstructure:"filters" json:"filters"`
}

// SinkConfig holds all sink configurations.
type SinkConfig struct {
	SSE SSESinkConfig `mapstructure:"sse" json:"sse"`
	TCP TCPSinkConfig `mapstructure:"tcp" json:"tcp"`
}

// AdminConfig holds admin server settings.
type AdminConfig struct {
	Address string `mapstructure:"address" json:"address"`
}

// Config is the top-level application configuration.
type Config struct {
	App    AppConfig    `mapstructure:"app"    json:"app"`
	CAN    CANConfig    `mapstructure:"can"    json:"can"`
	Buffer BufferConfig `mapstructure:"buffer" json:"buffer"`
	Sinks  SinkConfig   `mapstructure:"sinks"  json:"sinks"`
	Admin  AdminConfig  `mapstructure:"admin"  json:"admin"`
}

// Load reads the config file at path and returns a validated Config.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)

	// Environment variable support
	v.SetEnvPrefix("N2KBRIDGE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("app.log_level", "info")
	v.SetDefault("can.bitrate", 250000)
	v.SetDefault("can.auto_up", false)
	v.SetDefault("can.restart_ms", 100)
	v.SetDefault("buffer.path", "/data/beacon.db")
	v.SetDefault("buffer.max_rows", 100000)
	v.SetDefault("buffer.checkpoint_ms", 500)
	v.SetDefault("sinks.sse.enabled", false)
	v.SetDefault("sinks.sse.address", "0.0.0.0:8080")
	v.SetDefault("sinks.sse.path", "/events")
	v.SetDefault("sinks.tcp.enabled", false)
	v.SetDefault("sinks.tcp.address", "0.0.0.0:9090")
	v.SetDefault("admin.address", "0.0.0.0:2112")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("reading config file %q: %w", path, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

func validate(cfg *Config) error {
	if cfg.CAN.Interface == "" && len(cfg.CAN.USBPorts) == 0 {
		return fmt.Errorf("can.interface or can.usb_ports is required")
	}
	if cfg.Buffer.MaxRows <= 0 {
		return fmt.Errorf("buffer.max_rows must be positive")
	}
	if cfg.Buffer.CheckpointMS <= 0 {
		return fmt.Errorf("buffer.checkpoint_ms must be positive")
	}
	if cfg.Sinks.SSE.Enabled && cfg.Sinks.SSE.Address == "" {
		return fmt.Errorf("sinks.sse.address is required when SSE sink is enabled")
	}
	if cfg.Sinks.TCP.Enabled && cfg.Sinks.TCP.Address == "" {
		return fmt.Errorf("sinks.tcp.address is required when TCP sink is enabled")
	}
	return nil
}
