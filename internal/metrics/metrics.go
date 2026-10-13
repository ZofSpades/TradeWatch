// Package metrics provides the Prometheus registry and the admin HTTP server
// that every TradeWatch service runs.
//
// The admin server exposes /metrics, /healthz (liveness) and /readyz
// (readiness), plus any service-specific routes such as marketsim's control
// API. Each binary also accepts a `healthcheck` subcommand that probes its
// own /healthz endpoint, because the distroless runtime images have no curl.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every TradeWatch metric name.
const Namespace = "tradewatch"

// NewRegistry returns a registry with the Go runtime and process collectors
// registered.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Server is a service's admin HTTP server.
type Server struct {
	addr  string
	mux   *http.ServeMux
	ready atomic.Bool
	log   *slog.Logger
}

// NewServer returns an admin server for reg listening on addr.
func NewServer(addr string, reg *prometheus.Registry, log *slog.Logger) *Server {
	s := &Server{addr: addr, mux: http.NewServeMux(), log: log}
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	s.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready\n"))
	})
	return s
}

// Handle registers an additional route.
func (s *Server) Handle(pattern string, h http.Handler) { s.mux.Handle(pattern, h) }

// HandleFunc registers an additional route.
func (s *Server) HandleFunc(pattern string, h http.HandlerFunc) { s.mux.HandleFunc(pattern, h) }

// SetReady sets the readiness reported by /readyz.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// Run serves until ctx is cancelled, then shuts down gracefully.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{Addr: s.addr, Handler: s.mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("admin server: %w", err)
	}
	s.log.Info("admin server listening", "addr", ln.Addr().String())
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// Probe checks the /healthz endpoint of a server listening on addr (for
// example ":8090") and returns a process exit code: 0 when healthy.
func Probe(addr string) int {
	host := addr
	if strings.HasPrefix(host, ":") {
		host = "127.0.0.1" + host
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + host + "/healthz")
	if err != nil {
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
