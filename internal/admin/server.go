package admin

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/open-ships/beacon/internal/config"
)

// AdminServer serves /health, /metrics, and /config.
type AdminServer struct {
	address string
	log     *slog.Logger
	checks  map[string]HealthChecker
	cfg     *config.Config
	server  *http.Server
}

// NewAdminServer creates a new AdminServer.
func NewAdminServer(address string, checks map[string]HealthChecker, cfg *config.Config, log *slog.Logger) *AdminServer {
	return &AdminServer{
		address: address,
		checks:  checks,
		cfg:     cfg,
		log:     log,
	}
}

// Start begins serving admin endpoints. It blocks until ctx is cancelled.
func (s *AdminServer) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/health", HealthHandler(s.checks))
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/config", ConfigHandler(s.cfg))

	s.server = &http.Server{
		Addr:    s.address,
		Handler: mux,
	}

	s.log.Info("Admin server listening", "address", s.address)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.server.Shutdown(shutdownCtx)
	}()

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("admin server: %w", err)
	}
	return nil
}

// Close stops the admin server.
func (s *AdminServer) Close() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}
