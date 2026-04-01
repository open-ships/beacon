package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	n2klib "github.com/open-ships/n2k"
	"github.com/spf13/cobra"

	"github.com/open-ships/beacon/internal/admin"
	"github.com/open-ships/beacon/internal/buffer"
	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/n2k"
	"github.com/open-ships/beacon/internal/sink"
	ssesink "github.com/open-ships/beacon/internal/sink/sse"
	tcpsink "github.com/open-ships/beacon/internal/sink/tcp"
)

var (
	cfgPath string
	version = "dev"
)

func main() {
	root := &cobra.Command{
		Use:     "beacon",
		Short:   "Bridge NMEA 2000 CAN bus data to TCP/SSE sinks",
		Version: version,
		RunE:    run,
	}
	root.PersistentFlags().StringVar(&cfgPath, "config", "config.toml", "Path to configuration file")
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := buildLogger(cfg.App.LogLevel)

	log.Info("beacon starting", "config", cfgPath)

	// Init OTel metrics
	metrics, _, err := admin.InitMetrics()
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}

	// Open SQLite buffer
	buf, err := buffer.Open(cfg.Buffer.Path, cfg.Buffer.MaxRows, metrics)
	if err != nil {
		return fmt.Errorf("open buffer: %w", err)
	}
	defer buf.Close()

	// Set up checkpoint flusher
	flusher := buffer.NewCheckpointFlusher(buf, time.Duration(cfg.Buffer.CheckpointMS)*time.Millisecond)

	// Root context with cancellation on signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	flusher.Start(ctx)

	// Build sinks
	checks := map[string]admin.HealthChecker{
		"buffer": buf,
	}

	var sinks []sink.Sink
	var readySinks []sink.ReadySink

	// SSE sink
	var sseSink *ssesink.SSESink
	var sseChain filter.Chain
	if cfg.Sinks.SSE.Enabled {
		sseSink = ssesink.NewSSESink(cfg.Sinks.SSE.Address, cfg.Sinks.SSE.Path, log,
			ssesink.WithMetrics(metrics),
			ssesink.WithBuffer(buf),
		)
		sseChain, err = filter.Parse(cfg.Sinks.SSE.Filters, log, metrics, sseSink.Name())
		if err != nil {
			return fmt.Errorf("parse SSE filter: %w", err)
		}
		sinks = append(sinks, sseSink)
		readySinks = append(readySinks, sseSink)
		checks["sink_sse"] = sseSink
	}

	// TCP sink
	var tcpSink *tcpsink.TCPSink
	var tcpChain filter.Chain
	if cfg.Sinks.TCP.Enabled {
		tcpSink = tcpsink.NewTCPSink(cfg.Sinks.TCP.Address, log, metrics)
		tcpChain, err = filter.Parse(cfg.Sinks.TCP.Filters, log, metrics, tcpSink.Name())
		if err != nil {
			return fmt.Errorf("parse TCP filter: %w", err)
		}
		sinks = append(sinks, tcpSink)
		readySinks = append(readySinks, tcpSink)
		checks["sink_tcp"] = tcpSink
	}

	// Auto-up SocketCAN interface if configured
	if cfg.CAN.Interface != "" && cfg.CAN.AutoUp {
		cmd := exec.Command("ip", "link", "set", cfg.CAN.Interface, "up", "type", "can", "bitrate", fmt.Sprintf("%d", cfg.CAN.Bitrate))
		if out, err := cmd.CombinedOutput(); err != nil {
			log.Warn("CAN auto_up failed", "iface", cfg.CAN.Interface, "err", err, "output", string(out))
		}
	}

	// Build n2k source options
	n2kOpts := []n2klib.Option{n2klib.IncludeUnknown(), n2klib.WithLogger(log)}
	if cfg.CAN.Interface != "" {
		n2kOpts = append(n2kOpts, n2klib.CAN(cfg.CAN.Interface))
	}
	for _, port := range cfg.CAN.USBPorts {
		n2kOpts = append(n2kOpts, n2klib.USB(port))
	}

	var wg sync.WaitGroup

	// Start sinks
	for _, s := range sinks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Start(ctx); err != nil {
				log.Error("sink error", "sink", s.Name(), "err", err)
			}
		}()
	}

	// Wait for all sinks to be ready (with timeout)
	readyTimeout := time.After(10 * time.Second)
	for _, rs := range readySinks {
		select {
		case <-rs.Ready():
		case <-readyTimeout:
			log.Warn("timeout waiting for sink readiness")
		}
	}

	// Start dispatchers
	if sseSink != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink.RunDispatcher(ctx, buf, sseSink, sseChain, log, metrics)
		}()
	}
	if tcpSink != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink.RunDispatcher(ctx, buf, tcpSink, tcpChain, log, metrics)
		}()
	}

	// Start admin server (non-blocking)
	adminSrv := admin.NewAdminServer(cfg.Admin.Address, checks, cfg, log)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := adminSrv.Start(ctx); err != nil {
			log.Error("admin server error", "err", err)
		}
	}()

	// Ingest loop: n2k.Receive reads CAN frames, decodes, and yields messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result, err := range n2klib.Receive(ctx, n2kOpts...) {
			if err != nil {
				log.Debug("n2k receive error", "err", err)
				continue
			}

			msg, err := n2k.FromDecoded(result)
			if err != nil {
				log.Debug("decode error", "err", err)
				continue
			}

			if metrics != nil {
				metrics.PGNMessagesTotal.Add(ctx, 1, admin.AttrPGN(msg.PGN))
				if string(msg.Payload) == "null" {
					metrics.PGNUnknownTotal.Add(ctx, 1)
				}
			}

			if err := buf.Insert(ctx, msg); err != nil {
				log.Error("buffer insert error", "err", err)
			}
		}
	}()

	log.Info("beacon ready")

	// Wait for signal
	<-sigCh
	log.Info("shutting down...")
	cancel()

	// Close sinks to unblock Start goroutines
	for _, s := range sinks {
		if err := s.Close(); err != nil {
			log.Error("sink close error", "sink", s.Name(), "err", err)
		}
	}

	// Flush checkpoints
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer flushCancel()
	if err := flusher.FlushAll(flushCtx); err != nil {
		log.Error("checkpoint flush error", "err", err)
	}
	flusher.Stop()

	// Wait for all goroutines to finish
	wg.Wait()

	log.Info("shutdown complete")
	return nil
}

func buildLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
