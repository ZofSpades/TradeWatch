package tracing

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/ZofSpades/tradewatch/internal/config"
)

// recordSpans installs a tracer provider that records ended spans, for the
// duration of the test.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
	return rec
}

func TestLoadConfig(t *testing.T) {
	cfg, err := LoadConfig(config.FromMap(nil), "ingest", "host-1")
	if err != nil || cfg.Enabled() || cfg.EventSampleRatio != 0.001 || cfg.Service != "ingest" || cfg.Instance != "host-1" {
		t.Errorf("defaults: %+v, %v", cfg, err)
	}
	cfg, err = LoadConfig(config.FromMap(map[string]string{
		"TW_OTLP_ENDPOINT": "http://jaeger:4318", "TW_TRACE_EVENT_SAMPLE_RATIO": "0.25",
	}), "marketsim", "h")
	if err != nil || !cfg.Enabled() || cfg.EventSampleRatio != 0.25 {
		t.Errorf("configured: %+v, %v", cfg, err)
	}
	for _, bad := range []map[string]string{
		{"TW_TRACE_EVENT_SAMPLE_RATIO": "1.5"},
		{"TW_TRACE_EVENT_SAMPLE_RATIO": "-0.1"},
		{"TW_TRACE_EVENT_SAMPLE_RATIO": "often"},
		{"TW_OTLP_ENDPOINT": "jaeger:4318"},
		{"TW_OTLP_ENDPOINT": "ftp://jaeger:4318"},
	} {
		if _, err := LoadConfig(config.FromMap(bad), "x", "h"); err == nil {
			t.Errorf("%v was accepted", bad)
		}
	}
}

func TestSetupWithoutAnEndpointExportsNothing(t *testing.T) {
	shutdown, err := Setup(context.Background(), Config{Service: "x"})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	_, span := otel.Tracer("test").Start(context.Background(), "op")
	defer span.End()
	if span.SpanContext().IsValid() || span.IsRecording() {
		t.Error("a span was recorded without an endpoint")
	}
}

func TestRecordHeadersCarryTraceContext(t *testing.T) {
	recordSpans(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "send orders")
	defer span.End()

	r := &kgo.Record{Headers: []kgo.RecordHeader{{Key: "tw-schema", Value: []byte("tradewatch.v1.OrderEvent")}}}
	if Traced(r) {
		t.Fatal("a fresh record counts as traced")
	}
	Inject(ctx, r)
	Inject(ctx, r) // injecting again replaces the header rather than adding one
	if !Traced(r) || len(r.Headers) != 2 || r.Headers[0].Key != "tw-schema" {
		t.Fatalf("headers after inject: %v", r.Headers)
	}
	got := trace.SpanContextFromContext(Extract(context.Background(), r))
	if !got.IsRemote() || got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() || !got.IsSampled() {
		t.Errorf("extracted %v, want the span %v", got, span.SpanContext())
	}

	// The live channel carries the same context as a string.
	tp := Traceparent(ctx)
	if back := trace.SpanContextFromContext(FromTraceparent(context.Background(), tp)); back.TraceID() != span.SpanContext().TraceID() {
		t.Errorf("traceparent %q did not round-trip", tp)
	}
}

func TestUntracedContextsWriteNothing(t *testing.T) {
	recordSpans(t)
	r := &kgo.Record{}
	Inject(context.Background(), r)
	if Traced(r) || len(r.Headers) != 0 {
		t.Errorf("an untraced context wrote headers: %v", r.Headers)
	}
	if Traceparent(context.Background()) != "" {
		t.Error("an untraced context has a traceparent")
	}
	if ctx := FromTraceparent(context.Background(), ""); trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("an empty traceparent produced a span context")
	}
}

func TestEndSendRecordsWhereTheRecordLanded(t *testing.T) {
	rec := recordSpans(t)
	_, span := otel.Tracer("test").Start(context.Background(), "send alerts")
	EndSend(span, &kgo.Record{Topic: "alerts", Partition: 3, Offset: 42}, nil)
	_, failed := otel.Tracer("test").Start(context.Background(), "send alerts")
	EndSend(failed, &kgo.Record{Topic: "alerts"}, context.DeadlineExceeded)

	ended := rec.Ended()
	if len(ended) != 2 {
		t.Fatalf("%d spans ended", len(ended))
	}
	attrs := map[string]string{}
	for _, kv := range ended[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	if attrs["messaging.destination.partition.id"] != "3" || attrs["messaging.kafka.offset"] != "42" {
		t.Errorf("attributes = %v", attrs)
	}
	if ended[1].Status().Code.String() != "Error" || len(ended[1].Events()) == 0 {
		t.Errorf("a failed send ended with status %v and %d events", ended[1].Status(), len(ended[1].Events()))
	}
}
