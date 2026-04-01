//go:build !linux

package can

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/brutella/can"

	"github.com/open-ships/beacon/internal/admin"
)

// Reader is a stub for non-Linux platforms.
type Reader struct {
	iface     string
	bitrate   int
	autoUp    bool
	restartMS int
	log       *slog.Logger
	metrics   *admin.Metrics
}

// NewReader creates a new CAN Reader stub (not functional on non-Linux platforms).
func NewReader(iface string, bitrate int, autoUp bool, restartMS int, log *slog.Logger, metrics *admin.Metrics) *Reader {
	return &Reader{
		iface:     iface,
		bitrate:   bitrate,
		autoUp:    autoUp,
		restartMS: restartMS,
		log:       log,
		metrics:   metrics,
	}
}

// HealthCheck always returns an error on non-Linux platforms.
func (r *Reader) HealthCheck(_ context.Context) error {
	return fmt.Errorf("CAN reader not supported on this platform")
}

// Start always returns an error on non-Linux platforms.
func (r *Reader) Start(_ context.Context, _ chan<- *can.Frame) error {
	return fmt.Errorf("SocketCAN is only available on Linux")
}

// Close is a no-op on non-Linux platforms.
func (r *Reader) Close() error {
	return nil
}
