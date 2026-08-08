package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/open-ships/beacon/internal/metrics"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/queue"
)

const maxHTTPPostErrorBody = 4 << 10

// httpPostSink sends confirmed JSON-array batches to a remote HTTP(S)
// endpoint. The connector owns retry and checkpointing; this runtime returns
// success only for a 2xx response, making the response the route's confirmed
// delivery boundary.
type httpPostSink struct {
	id        string
	url       string
	headers   http.Header
	batchSize int
	timeout   time.Duration
	gzip      bool
	client    *http.Client
	met       *metrics.Set

	mu      sync.Mutex
	lastErr error
}

func newHTTPPostSink(cfg model.Sink, met *metrics.Set) (Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport
	if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = defaultTransport.Clone()
	}
	return &httpPostSink{
		id: cfg.ID, url: cfg.URL, headers: cloneHeaders(cfg.Headers),
		batchSize: cfg.EffectiveHTTPPostBatchSize(),
		timeout:   cfg.EffectiveHTTPPostRequestTimeout(),
		gzip:      cfg.Gzip,
		met:       met,
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func cloneHeaders(headers map[string]string) http.Header {
	out := make(http.Header, len(headers))
	for name, value := range headers {
		out.Set(name, value)
	}
	return out
}

func (s *httpPostSink) ID() string                   { return s.id }
func (s *httpPostSink) BatchSize() int               { return s.batchSize }
func (s *httpPostSink) DeliveryClass() DeliveryClass { return DeliveryConfirmed }

func (s *httpPostSink) State() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastErr != nil {
		return "degraded", s.lastErr
	}
	return "up", nil
}

func (s *httpPostSink) setError(err error) {
	s.mu.Lock()
	s.lastErr = err
	s.mu.Unlock()
}

func (s *httpPostSink) PushBatch(ctx context.Context, entries []queue.Entry) error {
	if len(entries) == 0 {
		return ctx.Err()
	}
	envelopes := make([]*msg.Envelope, len(entries))
	for i, entry := range entries {
		envelopes[i] = entry.Env
	}
	body, uncompressedBytes, err := encodeHTTPPostBatch(envelopes, s.gzip)
	if err != nil {
		s.setError(err)
		return fmt.Errorf("encode HTTP POST batch: %w", err)
	}

	requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		s.setError(err)
		return err
	}
	req.Header = s.headers.Clone()
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	encoding := "identity"
	if s.gzip {
		encoding = "gzip"
		req.Header.Set("Content-Encoding", "gzip")
	}
	req.Header.Set("Idempotency-Key", httpPostBatchID(s.id, entries))

	started := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		s.met.SinkHTTPRequest(context.WithoutCancel(ctx), s.id, "transport_error", encoding,
			int64(len(entries)), int64(len(body)), int64(uncompressedBytes), time.Since(started))
		s.setError(err)
		return fmt.Errorf("HTTP POST request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxHTTPPostErrorBody+1))
	status := strconv.Itoa(resp.StatusCode)
	metricCtx := context.WithoutCancel(ctx)
	s.met.SinkHTTPRequest(metricCtx, s.id, status, encoding,
		int64(len(entries)), int64(len(body)), int64(uncompressedBytes), time.Since(started))
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		s.setError(nil)
		return nil
	}
	detail := strings.Join(strings.Fields(string(responseBody)), " ")
	if len(responseBody) > maxHTTPPostErrorBody {
		detail = strings.TrimSpace(string(responseBody[:maxHTTPPostErrorBody])) + "…"
	}
	if readErr != nil {
		detail = "response body unavailable: " + readErr.Error()
	}
	if detail != "" {
		detail = ": " + detail
	}
	err = fmt.Errorf("HTTP POST endpoint returned %s%s", resp.Status, detail)
	if delay, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
		s.met.SinkHTTPRetryAfter(metricCtx, s.id, status, delay)
		err = &retryAfterError{err: err, delay: delay}
	}
	s.setError(err)
	return err
}

func encodeHTTPPostBatch(envelopes []*msg.Envelope, compress bool) ([]byte, int, error) {
	body, err := json.Marshal(envelopes)
	if err != nil || !compress {
		return body, len(body), err
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		_ = writer.Close()
		return nil, len(body), err
	}
	if err := writer.Close(); err != nil {
		return nil, len(body), err
	}
	return compressed.Bytes(), len(body), nil
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	digitsOnly := true
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			digitsOnly = false
			break
		}
	}
	if digitsOnly {
		seconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil || seconds > uint64(time.Duration(1<<63-1)/time.Second) {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := when.Sub(now)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}

func httpPostBatchID(sinkID string, entries []queue.Entry) string {
	first, last := entries[0], entries[len(entries)-1]
	connectorID := first.Env.ConnectorID
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%d", sinkID, connectorID, first.Seq, last.Seq)))
	return "beacon-" + hex.EncodeToString(sum[:16])
}

func (s *httpPostSink) Stop() {
	if closer, ok := s.client.Transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
