package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/open-ships/beacon/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	return path
}

func TestLoadConfig(t *testing.T) {
	content := `
[app]
log_level = "debug"

[can]
interface  = "can0"
bitrate    = 250000
auto_up    = true
restart_ms = 200

[buffer]
path          = "/tmp/test.db"
max_rows      = 50000
checkpoint_ms = 300

[sinks.sse]
enabled = true
address = "0.0.0.0:8080"
path    = "/events"
filters = ["msg.pgn == 127250"]

[sinks.tcp]
enabled = true
address = "0.0.0.0:9090"
filters = []

[admin]
address = "0.0.0.0:2112"
`
	cfg, err := config.Load(writeConfig(t, content))
	require.NoError(t, err)

	assert.Equal(t, "debug", cfg.App.LogLevel)
	assert.Equal(t, "can0", cfg.CAN.Interface)
	assert.Equal(t, 250000, cfg.CAN.Bitrate)
	assert.True(t, cfg.CAN.AutoUp)
	assert.Equal(t, 200, cfg.CAN.RestartMS)
	assert.Equal(t, "/tmp/test.db", cfg.Buffer.Path)
	assert.Equal(t, 50000, cfg.Buffer.MaxRows)
	assert.Equal(t, 300, cfg.Buffer.CheckpointMS)
	assert.True(t, cfg.Sinks.SSE.Enabled)
	assert.Equal(t, "0.0.0.0:8080", cfg.Sinks.SSE.Address)
	assert.Equal(t, "/events", cfg.Sinks.SSE.Path)
	assert.Equal(t, []string{"msg.pgn == 127250"}, cfg.Sinks.SSE.Filters)
	assert.True(t, cfg.Sinks.TCP.Enabled)
	assert.Equal(t, "0.0.0.0:9090", cfg.Sinks.TCP.Address)
	assert.Equal(t, "0.0.0.0:2112", cfg.Admin.Address)
}

func TestLoadConfigDefaults(t *testing.T) {
	content := `
[can]
interface = "can0"

[buffer]
path = "/tmp/test.db"
`
	cfg, err := config.Load(writeConfig(t, content))
	require.NoError(t, err)

	assert.Equal(t, "info", cfg.App.LogLevel)
	assert.Equal(t, 250000, cfg.CAN.Bitrate)
	assert.False(t, cfg.CAN.AutoUp)
	assert.Equal(t, 100, cfg.CAN.RestartMS)
	assert.Equal(t, 100000, cfg.Buffer.MaxRows)
	assert.Equal(t, 500, cfg.Buffer.CheckpointMS)
	assert.False(t, cfg.Sinks.SSE.Enabled)
	assert.False(t, cfg.Sinks.TCP.Enabled)
}

func TestLoadConfigMissingInterface(t *testing.T) {
	content := `
[can]
interface = ""

[buffer]
max_rows = 1000
`
	_, err := config.Load(writeConfig(t, content))
	assert.ErrorContains(t, err, "can.interface is required")
}

func TestLoadConfigMissingFile(t *testing.T) {
	_, err := config.Load("/nonexistent/path/config.toml")
	assert.Error(t, err)
}

func TestLoadConfigInvalidMaxRows(t *testing.T) {
	content := `
[can]
interface = "can0"

[buffer]
path     = "/tmp/test.db"
max_rows = 0
`
	_, err := config.Load(writeConfig(t, content))
	assert.ErrorContains(t, err, "buffer.max_rows must be positive")
}

func TestLoadConfigInvalidCheckpointMS(t *testing.T) {
	content := `
[can]
interface = "can0"

[buffer]
path          = "/tmp/test.db"
checkpoint_ms = 0
`
	_, err := config.Load(writeConfig(t, content))
	assert.ErrorContains(t, err, "buffer.checkpoint_ms must be positive")
}

func TestLoadConfigSSEEnabledWithoutAddress(t *testing.T) {
	content := `
[can]
interface = "can0"

[buffer]
path = "/tmp/test.db"

[sinks.sse]
enabled = true
address = ""
`
	_, err := config.Load(writeConfig(t, content))
	assert.ErrorContains(t, err, "sinks.sse.address is required")
}

func TestLoadConfigTCPEnabledWithoutAddress(t *testing.T) {
	content := `
[can]
interface = "can0"

[buffer]
path = "/tmp/test.db"

[sinks.tcp]
enabled = true
address = ""
`
	_, err := config.Load(writeConfig(t, content))
	assert.ErrorContains(t, err, "sinks.tcp.address is required")
}
