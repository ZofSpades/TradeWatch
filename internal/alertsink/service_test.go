package alertsink

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/store"
	"github.com/ZofSpades/tradewatch/internal/testutil"
	"github.com/ZofSpades/tradewatch/internal/tracing"
)

// Each traced delivery's span says whether it was stored or absorbed as a
// duplicate, and only newly stored alerts pass their trace to the views.
func TestMarkStoredTellsFirstDeliveriesFromDuplicates(t *testing.T) {
	rec := testutil.RecordSpans(t)
	traced := func(key string) pending {
		ctx, span := tracer().Start(context.Background(), "store alert")
		return pending{alert: &twpb.Alert{AlertKey: key}, ctx: ctx, span: span}
	}
	batch := []pending{
		traced("a"),                         // stored now
		traced("a"),                         // a second copy in the same batch
		traced("b"),                         // stored by an earlier delivery
		{alert: &twpb.Alert{AlertKey: "c"}}, // stored, but not traced
		traced("d"),                         // refused by the database
	}
	batch[4].rejected = true
	traces := markStored(batch, []*store.Alert{{Key: "a"}, {Key: "c"}})
	endSpans(batch, nil)

	if len(traces) != 1 || traces["a"] != tracing.Traceparent(batch[0].ctx) {
		t.Fatalf("traces = %v, want only a's", traces)
	}
	want := map[trace.SpanID]string{
		batch[0].span.SpanContext().SpanID(): "false",
		batch[1].span.SpanContext().SpanID(): "true",
		batch[2].span.SpanContext().SpanID(): "true",
		batch[4].span.SpanContext().SpanID(): "",
	}
	ended := rec.Ended()
	if len(ended) != len(want) {
		t.Fatalf("%d spans ended, want %d", len(ended), len(want))
	}
	for _, sp := range ended {
		if got := testutil.SpanAttrs(sp)["tradewatch.alert.duplicate"]; got != want[sp.SpanContext().SpanID()] {
			t.Errorf("span %s: duplicate = %q, want %q", sp.SpanContext().SpanID(), got, want[sp.SpanContext().SpanID()])
		}
	}
}
