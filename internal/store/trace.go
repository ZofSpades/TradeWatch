package store

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// tracer is looked up on use, so it follows whichever provider is installed:
// OpenTelemetry binds a tracer taken earlier to the first provider only.
func tracer() trace.Tracer { return otel.Tracer("github.com/ZofSpades/tradewatch/internal/store") }

// queryTracer gives each query made under a sampled span, such as an API
// request's, a child span. Queries made outside one, like the alert sink's
// batch writes, are not traced, so they cost nothing. The span records the
// statement, whose values are bind parameters and never part of its text.
type queryTracer struct{}

type querySpanKey struct{}

// operation is the statement's first keyword, such as SELECT.
func operation(sql string) string {
	sql = strings.TrimSpace(sql)
	if i := strings.IndexAny(sql, " \t\n"); i > 0 {
		sql = sql[:i]
	}
	return strings.ToUpper(sql)
}

func (queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	op := operation(data.SQL)
	ctx, span := tracer().Start(ctx, "postgres "+op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(semconv.DBSystemNamePostgreSQL, semconv.DBOperationName(op), semconv.DBQueryText(data.SQL)))
	return context.WithValue(ctx, querySpanKey{}, span)
}

func (queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span, ok := ctx.Value(querySpanKey{}).(trace.Span)
	if !ok {
		return
	}
	if data.Err != nil && !errors.Is(data.Err, pgx.ErrNoRows) {
		span.RecordError(data.Err)
		span.SetStatus(codes.Error, "query failed")
	}
	span.End()
}
