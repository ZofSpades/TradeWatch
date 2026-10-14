package tracing

import (
	"context"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// headerTraceparent is the W3C trace-context header, as record headers carry
// it.
const headerTraceparent = "traceparent"

// recordCarrier adapts a record's headers to propagation.TextMapCarrier.
type recordCarrier struct{ r *kgo.Record }

func (c recordCarrier) Get(key string) string {
	for _, h := range c.r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c recordCarrier) Set(key, value string) {
	for i, h := range c.r.Headers {
		if h.Key == key {
			c.r.Headers[i].Value = []byte(value)
			return
		}
	}
	c.r.Headers = append(c.r.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c recordCarrier) Keys() []string {
	keys := make([]string, len(c.r.Headers))
	for i, h := range c.r.Headers {
		keys[i] = h.Key
	}
	return keys
}

// Inject writes the span context of ctx into r's headers. It writes
// nothing when ctx carries no sampled span, so untraced records stay
// unchanged.
func Inject(ctx context.Context, r *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, recordCarrier{r})
}

// Traced reports whether r carries trace context. It is a cheap check that
// lets consumers skip extraction for the untraced majority of records.
func Traced(r *kgo.Record) bool {
	for _, h := range r.Headers {
		if h.Key == headerTraceparent {
			return true
		}
	}
	return false
}

// Extract returns ctx with the remote span context carried by r's headers.
func Extract(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, recordCarrier{r})
}

// Traceparent returns the W3C traceparent of the span in ctx, or "" when
// there is none, for carriers other than Kafka records (the live channel).
func Traceparent(ctx context.Context) string {
	c := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, c)
	return c[headerTraceparent]
}

// FromTraceparent returns ctx with the remote span context of a traceparent.
func FromTraceparent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{headerTraceparent: traceparent})
}

// MessagingAttrs are the semantic-convention attributes of a Kafka
// operation on r. Offsets are only known once a record was produced or
// fetched; pass withOffset false for a record not yet produced.
func MessagingAttrs(op string, r *kgo.Record, withOffset bool) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingOperationName(op),
		semconv.MessagingDestinationName(r.Topic),
	}
	switch op {
	case "send":
		attrs = append(attrs, semconv.MessagingOperationTypeSend)
	case "process":
		attrs = append(attrs, semconv.MessagingOperationTypeProcess)
	}
	if len(r.Key) > 0 {
		attrs = append(attrs, semconv.MessagingKafkaMessageKey(string(r.Key)))
	}
	if withOffset {
		attrs = append(attrs, semconv.MessagingDestinationPartitionID(strconv.Itoa(int(r.Partition))),
			semconv.MessagingKafkaOffset(int(r.Offset)))
	}
	return attrs
}

// EndSend finishes a producer span from a produce callback: it records
// where the record landed, or the error.
func EndSend(span trace.Span, r *kgo.Record, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "not delivered")
	} else {
		span.SetAttributes(semconv.MessagingDestinationPartitionID(strconv.Itoa(int(r.Partition))),
			semconv.MessagingKafkaOffset(int(r.Offset)))
	}
	span.End()
}
