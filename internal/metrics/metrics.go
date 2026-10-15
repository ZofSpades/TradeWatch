// Package metrics provides the Prometheus registry and the admin HTTP server
// that every TradeWatch service runs.
//
// The admin server exposes /metrics, /healthz (liveness), /readyz
// (readiness) and the Go profiler under /debug/pprof/, plus any
// service-specific routes such as marketsim's control API. It is an
// operator interface: deployments keep the admin port on the internal
// network and never publish it. Each binary also accepts a `healthcheck`
// subcommand that probes its own /healthz endpoint, because the distroless
// runtime images have no curl.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every TradeWatch metric name.
const Namespace = "tradewatch"

// LatencyBuckets are the bucket bounds, in seconds, of the histograms that
// time an event's way through the pipeline. Neighbouring bounds are at most
// 1.5 times apart (doubling buckets would put a quantile anywhere within a
// factor of two), and they are round numbers, so dashboards stay readable.
var LatencyBuckets = []float64{
	0.0005, 0.001, 0.0015, 0.002, 0.003, 0.005, 0.0075,
	0.01, 0.015, 0.02, 0.03, 0.05, 0.075,
	0.1, 0.15, 0.2, 0.3, 0.5, 0.75,
	1, 1.5, 2, 3, 5, 7.5, 10, 15, 20, 30,
}

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
	// CPU profiles and execution traces stream for the requested duration;
	// the admin server sets no write timeout, so they are not cut short.
	s.mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	s.mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	s.mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	s.mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	s.mux.HandleFunc("POST /debug/pprof/symbol", pprof.Symbol)
	s.mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return s
}

// HandleFunc registers an additional route.
func (s *Server) HandleFunc(pattern string, h http.HandlerFunc) { s.mux.HandleFunc(pattern, h) }

// Handler returns the server's routes, for tests.
func (s *Server) Handler() http.Handler { return s.mux }

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
