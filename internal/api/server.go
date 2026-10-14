// Package api is TradeWatch's REST and server-sent-events API: alerts and
// their analyst workflow, the rules in force, a live feed, and an
// authenticated proxy to the simulator's control endpoints. The contract
// is openapi.yaml, which the tests check every response against.
package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/ZofSpades/tradewatch/internal/auth"
	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/metrics"
	"github.com/ZofSpades/tradewatch/internal/store"
)

//go:embed openapi.yaml
var spec []byte

// Spec returns the OpenAPI document.
func Spec() []byte { return spec }

// Config configures the API.
type Config struct {
	Addr            string
	HTTPAddr        string
	PostgresURL     string
	RedisAddr       string
	RedisPrefix     string
	SimURL          string
	PrometheusURL   string
	GrafanaURL      string
	TracesURL       string
	JWTSecret       string
	TokenTTL        time.Duration
	AnalystPassword string
	AdminPassword   string
}

// LoadConfig reads TW_API_*, TW_POSTGRES_URL and TW_REDIS_* variables. The
// signing secret and both passwords are required; `make env` generates
// them into .env.
func LoadConfig(env *config.Env) (Config, error) {
	cfg := Config{
		Addr:            env.String("TW_API_ADDR", ":8080"),
		HTTPAddr:        env.String("TW_HTTP_ADDR", ":8094"),
		PostgresURL:     env.Required("TW_POSTGRES_URL"),
		RedisAddr:       env.String("TW_REDIS_ADDR", "localhost:16379"),
		RedisPrefix:     env.String("TW_REDIS_PREFIX", "tw:"),
		SimURL:          env.String("TW_API_SIM_URL", "http://localhost:8090"),
		PrometheusURL:   env.String("TW_API_PROMETHEUS_URL", ""),
		GrafanaURL:      env.String("TW_API_GRAFANA_URL", ""),
		TracesURL:       env.String("TW_API_TRACES_URL", ""),
		JWTSecret:       env.Required("TW_API_JWT_SECRET"),
		TokenTTL:        env.Duration("TW_API_TOKEN_TTL", time.Hour),
		AnalystPassword: env.Required("TW_API_ANALYST_PASSWORD"),
		AdminPassword:   env.Required("TW_API_ADMIN_PASSWORD"),
	}
	if err := env.Err(); err != nil {
		return cfg, err
	}
	if _, err := url.Parse(cfg.SimURL); err != nil {
		return cfg, fmt.Errorf("TW_API_SIM_URL: %w", err)
	}
	return cfg, nil
}

// Users returns the two configured accounts: "analyst" and "admin".
func (c Config) Users() ([]auth.User, error) {
	analyst, err := auth.NewUser("analyst", auth.RoleAnalyst, c.AnalystPassword)
	if err != nil {
		return nil, err
	}
	admin, err := auth.NewUser("admin", auth.RoleAdmin, c.AdminPassword)
	if err != nil {
		return nil, err
	}
	return []auth.User{analyst, admin}, nil
}

type serverMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	logins   *prometheus.CounterVec
	streams  prometheus.Gauge
}

func newServerMetrics(reg prometheus.Registerer) *serverMetrics {
	ns, sub := metrics.Namespace, "api"
	m := &serverMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "requests_total",
			Help: "HTTP requests, by route, method and status code."}, []string{"route", "method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "request_duration_seconds",
			Help: "HTTP request latency, by route (streams excluded).", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)},
			[]string{"route"}),
		logins: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "logins_total",
			Help: "Login attempts, by result."}, []string{"result"}),
		streams: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "stream_clients",
			Help: "Connected server-sent-event clients."}),
	}
	reg.MustRegister(m.requests, m.duration, m.logins, m.streams)
	return m
}

// Server serves the API.
type Server struct {
	log   *slog.Logger
	store *store.Store
	views *live.Views
	auth  *auth.Authenticator
	sim   http.Handler
	hub   *hub
	m     *serverMetrics

	prom     *promClient // nil: GET /api/v1/system is unavailable
	links    Links
	sysCache systemCache
}

// NewServer wires the API. simURL is the simulator's admin base URL.
func NewServer(st *store.Store, views *live.Views, a *auth.Authenticator, simURL string, reg prometheus.Registerer, log *slog.Logger) (*Server, error) {
	target, err := url.Parse(simURL)
	if err != nil {
		return nil, err
	}
	s := &Server{log: log, store: st, views: views, auth: a, sim: newSimProxy(target, log), m: newServerMetrics(reg)}
	s.hub = newHub(views, s.m.streams, log)
	return s, nil
}

// route is one API endpoint.
type route struct {
	pattern string    // ServeMux pattern: "METHOD /path"
	role    auth.Role // empty for public endpoints
	handler http.HandlerFunc
}

func (s *Server) routes() []route {
	simulator := func(pattern string) route { return route{pattern, auth.RoleAdmin, s.proxySim} }
	return []route{
		{"POST /api/v1/auth/login", "", s.login},
		{"GET /api/v1/openapi.yaml", "", s.openapi},
		{"GET /api/v1/me", auth.RoleAnalyst, s.me},
		{"GET /api/v1/alerts", auth.RoleAnalyst, s.listAlerts},
		{"GET /api/v1/alerts/{key}", auth.RoleAnalyst, s.getAlert},
		{"GET /api/v1/alerts/{key}/audit", auth.RoleAnalyst, s.getAudit},
		{"POST /api/v1/alerts/{key}/transitions", auth.RoleAnalyst, s.transition},
		{"POST /api/v1/alerts/{key}/comments", auth.RoleAnalyst, s.comment},
		{"GET /api/v1/stats", auth.RoleAnalyst, s.stats},
		{"GET /api/v1/system", auth.RoleAnalyst, s.system},
		{"GET /api/v1/live", auth.RoleAnalyst, s.liveView},
		{"GET /api/v1/stream", auth.RoleAnalyst, s.stream},
		{"GET /api/v1/rules", auth.RoleAnalyst, s.rules},
		{"GET /api/v1/rules/{id}", auth.RoleAnalyst, s.ruleHistory},
		simulator("GET /api/v1/sim/status"),
		simulator("POST /api/v1/sim/start"),
		simulator("POST /api/v1/sim/stop"),
		simulator("PUT /api/v1/sim/pace"),
		simulator("GET /api/v1/sim/injection"),
		simulator("PUT /api/v1/sim/injection"),
		simulator("GET /api/v1/sim/scenarios"),
		simulator("POST /api/v1/sim/scenarios"),
	}
}

// Handler returns the API's HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, rt := range s.routes() {
		h := rt.handler
		if rt.role != "" {
			h = s.authorize(rt.role, h)
		}
		handler := s.instrument(rt.pattern, h)
		// Every request is traced, named by its route, except the event
		// stream, which lasts as long as the client stays connected. Each
		// streamed alert gets a span of its own in the hub instead.
		if rt.pattern != "GET /api/v1/stream" {
			handler = otelhttp.NewHandler(handler, rt.pattern)
		}
		mux.Handle(rt.pattern, handler)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		problem(w, http.StatusNotFound, "no such endpoint")
	})
	return mux
}

// Run serves the API on addr until ctx ends, then shuts down gracefully,
// closing open event streams first.
func (s *Server) Run(ctx context.Context, addr string) error {
	go s.hub.run(ctx)
	srv := &http.Server{
		Addr: addr, Handler: s.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
		// No write timeout: event streams are long-lived. Other handlers
		// bound their work with a request context deadline instead.
	}
	srv.RegisterOnShutdown(s.hub.closeAll)
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	s.log.Info("api listening", "addr", addr)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}

type claimsKey struct{}

func claimsOf(r *http.Request) *auth.Claims {
	c, _ := r.Context().Value(claimsKey{}).(*auth.Claims)
	return c
}

// authorize requires a valid bearer token whose role allows need.
func (s *Server) authorize(need auth.Role, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="tradewatch"`)
			problem(w, http.StatusUnauthorized, "a bearer token is required")
			return
		}
		claims, err := s.auth.Verify(token)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="tradewatch", error="invalid_token"`)
			problem(w, http.StatusUnauthorized, "the token is invalid or expired")
			return
		}
		if !claims.Role.Allows(need) {
			problem(w, http.StatusForbidden, fmt.Sprintf("this requires the %s role", need))
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims)))
	}
}

// statusRecorder captures the response status for metrics.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.code == 0 {
		r.code = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.code == 0 {
		r.code = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer, which
// event streams need for flushing.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// instrument sets the common response headers, records metrics, bounds
// non-streaming requests to 30 s, and turns panics into 500s.
func (s *Server) instrument(pattern string, next http.HandlerFunc) http.Handler {
	method, path, _ := strings.Cut(pattern, " ")
	streaming := path == "/api/v1/stream"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Responses carry alert data and tokens: never cache or sniff them.
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		rec := &statusRecorder{ResponseWriter: w}
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("handler panicked", "route", path, "panic", p)
				if rec.code == 0 {
					problem(rec, http.StatusInternalServerError, "internal error")
				}
			}
			code := rec.code
			if code == 0 {
				code = http.StatusOK
			}
			s.m.requests.WithLabelValues(path, method, strconv.Itoa(code)).Inc()
			if !streaming {
				s.m.duration.WithLabelValues(path).Observe(time.Since(start).Seconds())
			}
			if code >= 500 {
				s.log.Warn("request failed", "route", path, "method", method, "code", code)
			}
		}()
		if !streaming {
			ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
			defer cancel()
			r = r.WithContext(ctx)
		}
		next(rec, r)
	})
}

// problem writes an RFC 7807 problem response.
func problem(w http.ResponseWriter, code int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "about:blank", "title": http.StatusText(code), "status": code, "detail": detail,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// decode reads a JSON body of at most 16 KiB, rejecting unknown fields.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		problem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// internalError logs err and answers 500 without exposing it.
func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, context.Canceled) {
		return // the client went away
	}
	s.log.Error("request failed", "path", r.URL.Path, "err", err)
	problem(w, http.StatusInternalServerError, "internal error")
}

func (s *Server) proxySim(w http.ResponseWriter, r *http.Request) { s.sim.ServeHTTP(w, r) }

func (s *Server) openapi(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(spec)
}
