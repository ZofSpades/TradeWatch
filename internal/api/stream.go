package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/tracing"
)

// tracer is looked up on use, so it follows whichever provider is installed:
// OpenTelemetry binds a tracer taken earlier to the first provider only.
func tracer() trace.Tracer { return otel.Tracer("github.com/ZofSpades/tradewatch/internal/api") }

// hub fans the live channel out to the event streams of this API instance:
// one Redis subscription per process, any number of clients.
type hub struct {
	views   *live.Views
	clients prometheus.Gauge
	log     *slog.Logger

	ready atomic.Bool
	mu    sync.Mutex
	subs  map[chan live.Event]struct{}
}

// clientBuffer is how many events a slow client may fall behind before it
// is disconnected; it can reconnect and catch up from the REST endpoints.
const clientBuffer = 64

func newHub(views *live.Views, clients prometheus.Gauge, log *slog.Logger) *hub {
	return &hub{views: views, clients: clients, log: log, subs: map[chan live.Event]struct{}{}}
}

// run subscribes to the live channel until ctx ends, resubscribing after
// failures.
func (h *hub) run(ctx context.Context) {
	delay := time.Second
	for ctx.Err() == nil {
		events, err := h.views.Subscribe(ctx)
		if err != nil {
			h.log.Warn("subscribing to live events failed; retrying", "err", err, "in", delay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			delay = min(2*delay, 30*time.Second)
			continue
		}
		delay = time.Second
		h.ready.Store(true)
		for ev := range events {
			h.broadcast(ev)
		}
		h.ready.Store(false)
	}
}

// broadcast hands an event to every connected client. A traced alert's
// trace continues here, with a span that records how many clients
// received it.
func (h *hub) broadcast(ev live.Event) {
	var span trace.Span
	if ev.Trace != "" {
		_, span = tracer().Start(tracing.FromTraceparent(context.Background(), ev.Trace), "stream alert",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attribute.String("tradewatch.alert.key", ev.Alert.Key), attribute.String("tradewatch.live.event", ev.Type)))
	}
	sent, dropped := 0, 0
	h.mu.Lock()
	for c := range h.subs {
		select {
		case c <- ev:
			sent++
		default: // too slow: drop the client rather than block everyone
			delete(h.subs, c)
			close(c)
			dropped++
		}
	}
	h.mu.Unlock()
	if span != nil {
		span.SetAttributes(attribute.Int("tradewatch.stream.clients", sent), attribute.Int("tradewatch.stream.dropped", dropped))
		span.End()
	}
}

func (h *hub) subscribe() chan live.Event {
	c := make(chan live.Event, clientBuffer)
	h.mu.Lock()
	h.subs[c] = struct{}{}
	h.mu.Unlock()
	h.clients.Inc()
	return c
}

func (h *hub) unsubscribe(c chan live.Event) {
	h.mu.Lock()
	if _, ok := h.subs[c]; ok {
		delete(h.subs, c)
		close(c)
	}
	h.mu.Unlock()
	h.clients.Dec()
}

// closeAll ends every stream, for a graceful shutdown.
func (h *hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.subs {
		delete(h.subs, c)
		close(c)
	}
}

// keepAlive is the interval of comment lines that keep idle streams open
// through proxies.
const keepAlive = 15 * time.Second

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	if !s.hub.ready.Load() {
		problem(w, http.StatusServiceUnavailable, "the live feed is unavailable")
		return
	}
	rc := http.NewResponseController(w)
	events := s.hub.subscribe()
	defer s.hub.unsubscribe(events)

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // let nginx pass events through unbuffered
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil || rc.Flush() != nil {
		return
	}
	tick := time.NewTicker(keepAlive)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		case ev, ok := <-events:
			if !ok {
				return // dropped as too slow, or shutting down
			}
			data, err := json.Marshal(ev.Alert)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data); err != nil || rc.Flush() != nil {
				return
			}
		}
	}
}
