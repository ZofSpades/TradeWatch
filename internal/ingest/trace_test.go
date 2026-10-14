package ingest

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

// A sampled event's span waits until the sequencer releases the event, and
// the held spans stay bounded.
func TestSampledEventSpansWaitForRelease(t *testing.T) {
	rec := testutil.RecordSpans(t)
	s := NewService(Config{Group: "ingest"}, prometheus.NewRegistry(), slog.New(slog.DiscardHandler))
	start := func(name string) (context.Context, func() bool) {
		ctx, span := tracer().Start(context.Background(), name)
		s.holdTrace(ctx, eventID{"s1", "QZAP", uint64(len(s.traced) + 100)}, span)
		return ctx, span.IsRecording
	}

	ctx, span := tracer().Start(context.Background(), "process orders")
	s.holdTrace(ctx, eventID{"s1", "QZAP", 7}, span)
	if _, ok := s.releaseTrace(&twpb.EventHeader{SessionId: "s1", Symbol: "QZAP", Seq: 8}); ok {
		t.Error("released the span of another event")
	}
	te, ok := s.releaseTrace(&twpb.EventHeader{SessionId: "s1", Symbol: "QZAP", Seq: 7})
	if !ok || te.span != span {
		t.Fatal("the held span was not released")
	}
	if _, ok := s.releaseTrace(&twpb.EventHeader{SessionId: "s1", Symbol: "QZAP", Seq: 7}); ok {
		t.Error("a span was released twice")
	}
	te.span.End()

	// A span never released, such as a duplicate's, ends after the timeout.
	_, stale := tracer().Start(context.Background(), "process cancels")
	id := eventID{"s1", "QZAP", 9}
	s.holdTrace(ctx, id, stale)
	held := s.traced[id]
	held.since = time.Now().Add(-tracedTimeout - time.Second)
	s.traced[id] = held
	s.expireTraces()
	if len(s.traced) != 0 || stale.IsRecording() {
		t.Errorf("the stale span is still held (%d held, recording %v)", len(s.traced), stale.IsRecording())
	}

	// Beyond the bound, a sampled event's trace ends at ingest.
	for len(s.traced) < maxTraced {
		start("process trades")
	}
	if _, recording := start("process trades"); recording() {
		t.Error("a span beyond the bound is still recording")
	}
	if len(s.traced) != maxTraced {
		t.Errorf("%d spans held, want %d", len(s.traced), maxTraced)
	}

	var events []string
	for _, sp := range rec.Ended() {
		for _, e := range sp.Events() {
			events = append(events, e.Name)
		}
	}
	for _, want := range []string{
		"never released: a duplicate, or its session was abandoned",
		"not followed further: too many sampled events are waiting for release",
	} {
		found := false
		for _, e := range events {
			found = found || e == want
		}
		if !found {
			t.Errorf("no span records %q (events: %v)", want, events)
		}
	}
}
