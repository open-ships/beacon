package sink

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
)

// DataServer hosts serve-mode sink endpoints with dynamically managed routes.
type DataServer struct {
	log *slog.Logger
	srv *http.Server
	ln  net.Listener

	mu     sync.RWMutex
	routes map[string]http.Handler
}

func NewDataServer(addr string, log *slog.Logger) *DataServer {
	d := &DataServer{log: log, routes: map[string]http.Handler{}}
	d.srv = &http.Server{Addr: addr, Handler: http.HandlerFunc(d.route)}
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

func (d *DataServer) Stop(ctx context.Context) error { return d.srv.Shutdown(ctx) }
