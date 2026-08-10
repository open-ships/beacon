package ui

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/filter"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/stats"
)

const (
	streamPreviewBuffer    = 8
	streamClientWriteLimit = 2 * time.Second
)

// writeStreamChunk gives each SSE write+flush a fresh bounded deadline. The
// admin server deliberately has no global WriteTimeout because streams are
// long-lived, so the handler itself must evict a client that stops reading.
func writeStreamChunk(controller *http.ResponseController, write func() error) error {
	if err := controller.SetWriteDeadline(time.Now().Add(streamClientWriteLimit)); err != nil {
		return err
	}
	if err := write(); err != nil {
		return err
	}
	return controller.Flush()
}

// handleComponentStream exposes a future-only, best-effort SSE tap used by
// source and sink overview pages. The tap is fed by stats.Registry at the
// source-received and sink-sent boundaries, so watching the UI neither
// consumes durable queue entries nor applies backpressure to routing.
func handleComponentStream(svc *config.Service, reg *stats.Registry, kind string, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var err error
		switch kind {
		case "source":
			_, err = svc.GetSource(r.Context(), id)
		case "sink":
			_, err = svc.GetSink(r.Context(), id)
		default:
			http.Error(w, "unsupported stream kind", http.StatusInternalServerError)
			return
		}
		if err != nil {
			if errors.Is(err, config.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			log.Error("ui: stream entity lookup failed", "kind", kind, "id", id, "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		filterExpression := strings.TrimSpace(r.URL.Query().Get("filter"))
		var streamFilter *filter.Chain
		if filterExpression != "" {
			streamFilter, err = filter.Compile([]string{filterExpression})
			if err != nil {
				http.Error(w, "invalid CEL filter: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		_, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		stream, unsubscribe := reg.SubscribeStream(kind, id, streamPreviewBuffer)
		defer unsubscribe()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Accel-Buffering", "no")
		controller := http.NewResponseController(w)
		if err := writeStreamChunk(controller, func() error {
			w.WriteHeader(http.StatusOK)
			_, err := io.WriteString(w, ": connected\n\n")
			return err
		}); err != nil {
			return
		}

		heartbeat := time.NewTicker(15 * time.Second)
		defer heartbeat.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case document, open := <-stream:
				if !open {
					return
				}
				if streamFilter != nil {
					var envelope msg.Envelope
					if err := json.Unmarshal(document, &envelope); err != nil {
						log.Debug("ui: skip malformed stream envelope", "kind", kind, "id", id, "err", err)
						continue
					}
					matches, err := streamFilter.Match(&envelope)
					if err != nil {
						log.Debug("ui: skip stream envelope after CEL evaluation error", "kind", kind, "id", id, "err", err)
						continue
					}
					if !matches {
						continue
					}
				}
				if err := writeStreamChunk(controller, func() error {
					_, err := fmt.Fprintf(w, "data: %s\n\n", document)
					return err
				}); err != nil {
					return
				}
			case <-heartbeat.C:
				if err := writeStreamChunk(controller, func() error {
					_, err := io.WriteString(w, ": keepalive\n\n")
					return err
				}); err != nil {
					return
				}
			}
		}
	}
}
