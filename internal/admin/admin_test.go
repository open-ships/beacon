package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/admin"
	"github.com/open-ships/beacon/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type okChecker struct{}

func (o *okChecker) HealthCheck(_ context.Context) error { return nil }

type failChecker struct{ msg string }

func (f *failChecker) HealthCheck(_ context.Context) error { return errors.New(f.msg) }

func TestHealthHandlerAllOK(t *testing.T) {
	checks := map[string]admin.HealthChecker{
		"can_reader": &okChecker{},
		"buffer":     &okChecker{},
	}
	h := admin.HealthHandler(checks)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp struct {
		Healthy bool `json:"healthy"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp.Healthy)
}

func TestHealthHandlerOneFailing(t *testing.T) {
	checks := map[string]admin.HealthChecker{
		"can_reader": &failChecker{"bus-off"},
		"buffer":     &okChecker{},
	}
	h := admin.HealthHandler(checks)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)

	var resp struct {
		Healthy bool `json:"healthy"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.False(t, resp.Healthy)
}

func TestHealthHandlerEmpty(t *testing.T) {
	h := admin.HealthHandler(map[string]admin.HealthChecker{})
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Healthy bool `json:"healthy"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp.Healthy)
}

func TestAdminServerMetrics(t *testing.T) {
	_, _, err := admin.InitMetrics()
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	ln.Close()

	srv := admin.NewAdminServer(addr, map[string]admin.HealthChecker{"test": &okChecker{}}, &config.Config{}, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go srv.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotEmpty(t, string(body))
}

func TestAdminServerHealthz(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	ln.Close()

	srv := admin.NewAdminServer(addr, map[string]admin.HealthChecker{"db": &okChecker{}}, &config.Config{}, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go srv.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://%s/health", addr))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var healthResp struct {
		Healthy bool `json:"healthy"`
	}
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, &healthResp))
	assert.True(t, healthResp.Healthy)
}

func TestConfigHandler(t *testing.T) {
	cfg := &config.Config{
		App: config.AppConfig{LogLevel: "debug"},
		CAN: config.CANConfig{Interface: "can0"},
		Buffer: config.BufferConfig{
			Path:         "beacon.db",
			MaxRows:      100000,
			CheckpointMS: 500,
		},
		Sinks: config.SinkConfig{
			SSE: config.SSESinkConfig{
				Enabled: true,
				Address: "0.0.0.0:8080",
				Path:    "/events",
				Filters: []string{"msg.pgn == 127250"},
			},
			TCP: config.TCPSinkConfig{
				Enabled: false,
				Address: "0.0.0.0:9090",
			},
		},
		Admin: config.AdminConfig{Address: "0.0.0.0:2112"},
	}

	h := admin.ConfigHandler(cfg)
	req := httptest.NewRequest("GET", "/config", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var got config.Config
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "debug", got.App.LogLevel)
	assert.Equal(t, "can0", got.CAN.Interface)
	assert.Equal(t, "beacon.db", got.Buffer.Path)
	assert.True(t, got.Sinks.SSE.Enabled)
	assert.Equal(t, []string{"msg.pgn == 127250"}, got.Sinks.SSE.Filters)
	assert.False(t, got.Sinks.TCP.Enabled)
}
