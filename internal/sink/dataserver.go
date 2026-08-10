package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	dataReadHeaderTimeout = 5 * time.Second
	dataReadTimeout       = 30 * time.Second
	dataIdleTimeout       = 90 * time.Second
	dataMaxHeaderBytes    = 64 << 10
)

var (
	dataRetryMin       = 250 * time.Millisecond
	dataRetryMax       = time.Minute
	dataStableServeAge = 30 * time.Second
)

type DataServerOption func(*DataServer)

// WithListenerWrapper applies wrapper to every initial or recovered listener.
// App uses this seam to share one accepted-connection budget with its admin
// server; standalone tests and embeddings can omit it.
func WithListenerWrapper(wrapper func(net.Listener) net.Listener) DataServerOption {
	return func(d *DataServer) { d.wrapListener = wrapper }
}

// DataServer hosts serve-mode sink endpoints with dynamically managed routes.
type DataServer struct {
	log          *slog.Logger
	srv          *http.Server
	runCtx       context.Context
	cancel       context.CancelFunc // cancels the base context, ending live streams
	wrapListener func(net.Listener) net.Listener
	wg           sync.WaitGroup
	stopOnce     sync.Once
	stopErr      error

	mu       sync.RWMutex
	routes   map[string]http.Handler
	ln       net.Listener
	bindAddr string
	serveErr error
	started  bool
	stopped  bool
}

func NewDataServer(addr string, log *slog.Logger, options ...DataServerOption) *DataServer {
	if log == nil {
		log = slog.Default()
	}
	d := &DataServer{log: log, routes: map[string]http.Handler{}}
	for _, option := range options {
		option(d)
	}
	// Request contexts derive from this base context, so cancelling it in
	// Stop unblocks every streaming handler (SSE select loop, WS CloseRead
	// pump). Without it Shutdown only closes idle connections and would
	// hang on active streams until its ctx expired.
	baseCtx, cancel := context.WithCancel(context.Background())
	d.runCtx = baseCtx
	d.cancel = cancel
	d.srv = &http.Server{
		Addr:              addr,
		Handler:           http.HandlerFunc(d.route),
		ReadHeaderTimeout: dataReadHeaderTimeout,
		ReadTimeout:       dataReadTimeout,
		IdleTimeout:       dataIdleTimeout,
		MaxHeaderBytes:    dataMaxHeaderBytes,
		// No WriteTimeout: SSE and WebSocket responses are intentionally
		// long-lived and end through request-context cancellation.
		BaseContext: func(net.Listener) context.Context { return baseCtx },
	}
	return d
}

func (d *DataServer) route(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	h, ok := d.routes[r.URL.Path]
	d.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	h.ServeHTTP(w, r)
}

func (d *DataServer) Start() error {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return errors.New("data server already started")
	}
	if d.stopped {
		d.mu.Unlock()
		return errors.New("data server is stopped")
	}
	d.started = true
	d.mu.Unlock()

	ln, err := d.listen(d.srv.Addr)
	if err != nil {
		d.mu.Lock()
		d.started = false
		d.mu.Unlock()
		return err
	}
	d.mu.Lock()
	if d.stopped {
		d.started = false
		d.mu.Unlock()
		_ = ln.Close()
		return errors.New("data server is stopped")
	}
	d.ln = ln
	d.bindAddr = ln.Addr().String()
	d.wg.Add(1)
	d.mu.Unlock()
	go d.serveLoop(ln)
	return nil
}

func (d *DataServer) listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return d.WrapListener(ln), nil
}

// WrapListener applies the process connection policy supplied by App. TCP
// sinks use it too, so their accepted clients share the same descriptor budget
// as admin and HTTP data traffic. A nil DataServer is the standalone/no-policy
// case used by tests and embeddings.
func (d *DataServer) WrapListener(ln net.Listener) net.Listener {
	if d == nil || d.wrapListener == nil {
		return ln
	}
	return d.wrapListener(ln)
}

func dataRetryDelay(delay time.Duration) time.Duration {
	if delay <= 1 {
		return delay
	}
	span := delay / 5
	return delay - span + time.Duration(rand.Int64N(int64(2*span+1))) // #nosec G404 -- scheduling jitter is not security-sensitive.
}

func (d *DataServer) serveLoop(ln net.Listener) {
	defer d.wg.Done()
	backoff := dataRetryMin
	for {
		servedAt := time.Now()
		err := d.srv.Serve(ln)
		if d.runCtx.Err() != nil || errors.Is(err, http.ErrServerClosed) {
			return
		}
		d.mu.Lock()
		d.serveErr = fmt.Errorf("serve data endpoints: %w", err)
		if d.ln == ln {
			d.ln = nil
		}
		bindAddr := d.bindAddr
		d.mu.Unlock()
		d.log.Error("data server stopped unexpectedly; rebinding", "err", err)
		if time.Since(servedAt) >= dataStableServeAge {
			backoff = dataRetryMin
		} else {
			backoff = min(backoff*2, dataRetryMax)
		}

		for {
			timer := time.NewTimer(dataRetryDelay(backoff))
			select {
			case <-d.runCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}

			next, listenErr := d.listen(bindAddr)
			if listenErr != nil {
				d.mu.Lock()
				d.serveErr = fmt.Errorf("rebind data endpoints: %w", listenErr)
				d.mu.Unlock()
				d.log.Debug("data server rebind failed", "err", listenErr, "retry_in", backoff)
				backoff = min(backoff*2, dataRetryMax)
				continue
			}
			if d.runCtx.Err() != nil {
				_ = next.Close()
				return
			}
			d.mu.Lock()
			d.ln = next
			d.serveErr = nil
			d.mu.Unlock()
			d.log.Info("data server rebound", "data", next.Addr())
			ln = next
			break
		}
	}
}

func (d *DataServer) Addr() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.ln != nil {
		return d.ln.Addr().String()
	}
	if d.bindAddr != "" {
		return d.bindAddr
	}
	return d.srv.Addr
}

// State reports whether serve-mode sink routes are currently reachable. A
// listener failure remains observable while the recovery loop is backing off.
func (d *DataServer) State() (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.stopped {
		return "error", errors.New("data server is stopped")
	}
	if d.serveErr != nil {
		return "error", d.serveErr
	}
	if !d.started || d.ln == nil {
		return "error", errors.New("data server is not listening")
	}
	return "up", nil
}

func (d *DataServer) SetRoute(path string, h http.Handler) {
	d.mu.Lock()
	d.routes[path] = h
	d.mu.Unlock()
}

func (d *DataServer) RemoveRoute(path string) {
	d.mu.Lock()
	delete(d.routes, path)
	d.mu.Unlock()
}

// Stop cancels all active request contexts (ending SSE/WS streams), then
// shuts the server down, waiting for handlers up to ctx's deadline.
func (d *DataServer) Stop(ctx context.Context) error {
	d.stopOnce.Do(func() {
		d.cancel()
		d.mu.Lock()
		d.stopped = true
		d.mu.Unlock()
		shutdownErr := d.srv.Shutdown(ctx)
		var closeErr error
		if shutdownErr != nil {
			// Shutdown can return when its context expires while active streams
			// are unwinding. Close guarantees the listener and connections no
			// longer consume descriptors before Stop returns.
			closeErr = d.srv.Close()
		}
		d.wg.Wait()
		d.stopErr = errors.Join(shutdownErr, closeErr)
	})
	return d.stopErr
}
