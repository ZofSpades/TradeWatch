package store

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"

	"github.com/ZofSpades/tradewatch/internal/testutil"
)

// Queries made under a recording span get a child span each; others get
// none.
func TestQueriesUnderARecordingSpanGetChildSpans(t *testing.T) {
	rec := testutil.RecordSpans(t)
	var qt queryTracer
	run := func(ctx context.Context, sql string, err error) {
		qt.TraceQueryEnd(qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql}), nil, pgx.TraceQueryEndData{Err: err})
	}

	run(context.Background(), "SELECT 1", nil)
	if n := len(rec.Ended()); n != 0 {
		t.Fatalf("a query outside any span produced %d spans", n)
	}

	ctx, request := otel.Tracer("test").Start(context.Background(), "GET /api/v1/alerts")
	run(ctx, "\n  select alert_key FROM alerts WHERE status = $1", nil)
	run(ctx, "UPDATE alerts SET status = $1 WHERE alert_key = $2", errors.New("serialization failure"))
	run(ctx, "SELECT payload FROM alerts WHERE alert_key = $1", pgx.ErrNoRows)
	request.End()

	type want struct{ name, op, status string }
	var got []want
	for _, sp := range rec.Ended() {
		if sp.Name() == "GET /api/v1/alerts" {
			continue
		}
		if sp.Parent().SpanID() != request.SpanContext().SpanID() {
			t.Errorf("%s is not a child of the request", sp.Name())
		}
		attrs := testutil.SpanAttrs(sp)
		if attrs["db.system.name"] != "postgresql" || attrs["db.query.text"] == "" {
			t.Errorf("%s attributes = %v", sp.Name(), attrs)
		}
		got = append(got, want{sp.Name(), attrs["db.operation.name"], sp.Status().Code.String()})
	}
	expected := []want{
		{"postgres SELECT", "SELECT", "Unset"},
		{"postgres UPDATE", "UPDATE", "Error"},
		{"postgres SELECT", "SELECT", "Unset"}, // no rows is an answer, not a failure
	}
	if len(got) != len(expected) {
		t.Fatalf("query spans = %v", got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("span %d = %+v, want %+v", i, got[i], expected[i])
		}
	}
}
