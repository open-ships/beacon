package sink

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// DataServer hosts serve-mode sink endpoints with dynamically managed routes.
type DataServer struct {
	log    *slog.Logger
	srv    *http.Server
	ln     net.Listener
	cancel context.CancelFunc // cancels the base context, ending live streams

	mu     sync.RWMutex
	routes map[string]http.Handler
}

func NewDataServer(addr string, log *slog.Logger) *DataServer {
	d := &DataServer{log: log, routes: map[string]http.Handler{}}
	// Request contexts derive from this base context, so cancelling it in
	// Stop unblocks every streaming handler (SSE select loop, WS CloseRead
	// pump). Without it Shutdown only closes idle connections and would
	// hang on active streams until its ctx expired.
	baseCtx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.srv = &http.Server{Addr: addr, Handler: http.HandlerFunc(d.route), ReadHeaderTimeout: 10 * time.Second,
		BaseContext: func(net.Listener) context.Context { return baseCtx }}
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
	ln, err := net.Listen("tcp", d.srv.Addr)
	if err != nil {
		return err
	}
	d.ln = ln
	go func() {
		if err := d.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			d.log.Error("data server error", "err", err)
		}
	}()
	return nil
}

func (d *DataServer) Addr() string {
	if d.ln == nil {
		return d.srv.Addr
	}
	return d.ln.Addr().String()
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
	d.cancel()
	return d.srv.Shutdown(ctx)
}
