// Package tracing sets up OpenTelemetry tracing for a TradeWatch service and
// carries trace context across Kafka in record headers.
//
// Tracing is off unless TW_OTLP_ENDPOINT names an OTLP/HTTP endpoint (the
// compose stack points it at Jaeger). Two kinds of trace cross the
// pipeline:
//
//   - Market events. marketsim starts a trace for a sampled fraction of the
//     events it publishes (TW_TRACE_EVENT_SAMPLE_RATIO). The W3C trace
//     context travels in the records' headers through ingest to the
//     detectors, so a sampled event shows how long each hop took.
//   - Alerts. Every alert is traced: the detector raises it, the alert sink
//     stores it, and the API streams it to the dashboard. The alert carries
//     its trace ID. If the event that triggered the alert was sampled, the
//     alert's trace continues the event's.
//
// Tracing never changes what the pipeline computes: trace context lives
// only in record headers and in the alert's trace_id field, which the
// deterministic alert key does not cover.
package tracing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/ZofSpades/tradewatch/internal/config"
)

// Config configures tracing for one service.
type Config struct {
	// Endpoint is the OTLP/HTTP base URL, for example http://jaeger:4318.
	// Empty disables tracing.
	Endpoint string
	Service  string
	Instance string
	// EventSampleRatio is the fraction of market events marketsim traces.
	EventSampleRatio float64
}

// LoadConfig reads TW_OTLP_ENDPOINT and TW_TRACE_EVENT_SAMPLE_RATIO for the
// named service.
func LoadConfig(env *config.Env, service, instance string) (Config, error) {
	cfg := Config{
		Endpoint:         env.String("TW_OTLP_ENDPOINT", ""),
		Service:          service,
		Instance:         instance,
		EventSampleRatio: env.Float("TW_TRACE_EVENT_SAMPLE_RATIO", 0.001),
	}
	if err := env.Err(); err != nil {
		return cfg, err
	}
	if cfg.EventSampleRatio < 0 || cfg.EventSampleRatio > 1 {
		return cfg, fmt.Errorf("TW_TRACE_EVENT_SAMPLE_RATIO must be within [0, 1], got %v", cfg.EventSampleRatio)
	}
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return cfg, fmt.Errorf("TW_OTLP_ENDPOINT %q: want an http(s) URL", cfg.Endpoint)
		}
	}
	return cfg, nil
}

// Enabled reports whether spans are exported.
func (c Config) Enabled() bool { return c.Endpoint != "" }

// Setup installs the W3C trace-context propagator and, when an endpoint is
// configured, a tracer provider that batches spans to it. The returned
// function flushes and stops the provider; call it on shutdown. Without an
// endpoint the global no-op provider stays in place, so instrumented code
// costs almost nothing.
func Setup(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if !cfg.Enabled() {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(cfg.Endpoint+"/v1/traces"))
	if err != nil {
		return nil, fmt.Errorf("tracing: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName(cfg.Service), semconv.ServiceInstanceID(cfg.Instance), semconv.ServiceNamespace("tradewatch")))
	if err != nil {
		return nil, fmt.Errorf("tracing: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		// Code decides which traces to start (sampled events, every alert,
		// every API request); children follow their parent.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return errors.Join(tp.ForceFlush(ctx), tp.Shutdown(ctx))
	}, nil
}

// Init loads the tracing configuration of service from env, sets it up, and
// logs whether spans are exported. Call the returned function on shutdown
// to flush the spans still buffered.
func Init(ctx context.Context, env *config.Env, service string, log *slog.Logger) (Config, func(), error) {
	host, _ := os.Hostname()
	cfg, err := LoadConfig(env, service, host)
	if err != nil {
		return cfg, nil, err
	}
	shutdown, err := Setup(ctx, cfg)
	if err != nil {
		return cfg, nil, err
	}
	if cfg.Enabled() {
		log.Info("tracing enabled", "endpoint", cfg.Endpoint)
	}
	return cfg, func() {
		if err := shutdown(context.Background()); err != nil {
			log.Warn("flushing traces failed", "err", err)
		}
	}, nil
}
