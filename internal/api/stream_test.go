package api

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"

	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/testutil"
	"github.com/ZofSpades/tradewatch/internal/tracing"
)

// Streaming a traced alert continues the alert's trace, with a span that
// counts the clients it reached; an untraced event adds no span.
func TestBroadcastContinuesATracedAlertsTrace(t *testing.T) {
	rec := testutil.RecordSpans(t)
	h := newHub(nil, prometheus.NewGauge(prometheus.GaugeOpts{Name: "clients"}), quiet)
	a, b := h.subscribe(), h.subscribe()

	ctx, stored := otel.Tracer("test").Start(context.Background(), "store alert")
	h.broadcast(live.Event{Type: live.EventRaised, Alert: live.Summary{Key: "k1"}, Trace: tracing.Traceparent(ctx)})
	h.broadcast(live.Event{Type: live.EventUpdated, Alert: live.Summary{Key: "k1"}})
	stored.End()
	for _, c := range []chan live.Event{a, b} {
		if first, second := <-c, <-c; first.Type != live.EventRaised || second.Type != live.EventUpdated {
			t.Errorf("client received %s then %s", first.Type, second.Type)
		}
	}

	var streamed int
	for _, sp := range rec.Ended() {
		if sp.Name() != "stream alert" {
			continue
		}
		streamed++
		if sp.Parent().SpanID() != stored.SpanContext().SpanID() || sp.SpanContext().TraceID() != stored.SpanContext().TraceID() {
			t.Errorf("stream span is not a child of the store span: parent %v", sp.Parent())
		}
		if attrs := testutil.SpanAttrs(sp); attrs["tradewatch.stream.clients"] != "2" || attrs["tradewatch.alert.key"] != "k1" {
			t.Errorf("attributes = %v", attrs)
		}
	}
	if streamed != 1 {
		t.Errorf("%d stream spans, want 1", streamed)
	}
}
