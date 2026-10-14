package marketsim

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/recording"
	"github.com/ZofSpades/tradewatch/internal/sim"
	"github.com/ZofSpades/tradewatch/internal/tracing"
)

// tracer is looked up on use, so it follows whichever provider is installed:
// OpenTelemetry binds a tracer taken earlier to the first provider only.
func tracer() trace.Tracer { return otel.Tracer("github.com/ZofSpades/tradewatch/internal/marketsim") }

// pacer delays events so that simulated time advances at the configured
// pace. It only ever sleeps; it never changes event content, so pacing has
// no effect on what a session produces.
type pacer struct {
	mu          sync.Mutex
	mode        PaceMode
	speed, rate float64
	anchored    bool
	anchorWall  time.Time
	anchorEvent int64
	count       float64
}

func newPacer(mode PaceMode, speed, rate float64) *pacer {
	return &pacer{mode: mode, speed: speed, rate: rate}
}

// reanchor makes the next event the new pacing reference point, so a new
// session starts on time instead of catching up on the previous one.
func (p *pacer) reanchor() {
	p.mu.Lock()
	p.anchored = false
	p.mu.Unlock()
}

// set changes the pace and re-anchors it at the next event.
func (p *pacer) set(mode PaceMode, speed, rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode, p.speed, p.rate = mode, speed, rate
	p.anchored = false
}

func (p *pacer) settings() (PaceMode, float64, float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode, p.speed, p.rate
}

func (p *pacer) wait(ctx context.Context, eventTime int64) error {
	p.mu.Lock()
	if p.mode == PaceFast {
		p.mu.Unlock()
		return nil
	}
	now := time.Now()
	if !p.anchored {
		p.anchored, p.anchorWall, p.anchorEvent, p.count = true, now, eventTime, 0
	}
	var target time.Time
	if p.mode == PaceRealtime {
		target = p.anchorWall.Add(time.Duration(float64(eventTime-p.anchorEvent) / p.speed))
	} else {
		p.count++
		target = p.anchorWall.Add(time.Duration(p.count / p.rate * float64(time.Second)))
	}
	p.mu.Unlock()

	if d := target.Sub(now); d > time.Millisecond {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

// publishMetrics are the publisher's Prometheus series.
type publishMetrics struct {
	published *prometheus.CounterVec
	labels    *prometheus.CounterVec
	errors    prometheus.Counter
	simTime   prometheus.Gauge
}

func newPublishMetrics(reg prometheus.Registerer) *publishMetrics {
	m := &publishMetrics{
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "tradewatch", Subsystem: "marketsim", Name: "events_published_total",
			Help: "Market events handed to the Kafka producer, by feed.",
		}, []string{"feed"}),
		labels: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "tradewatch", Subsystem: "marketsim", Name: "labels_published_total",
			Help: "Ground-truth labels of injected abuse published, by scenario.",
		}, []string{"scenario"}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "tradewatch", Subsystem: "marketsim", Name: "produce_errors_total",
			Help: "Records the producer failed to deliver.",
		}),
		simTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "tradewatch", Subsystem: "marketsim", Name: "sim_time_seconds",
			Help: "Event time of the most recently published event, as a Unix timestamp.",
		}),
	}
	reg.MustRegister(m.published, m.labels, m.errors, m.simTime)
	return m
}

// publisher is the sim.Emitter that sends a session's events to Kafka and,
// optionally, to a recording.
type publisher struct {
	ctx     context.Context
	cl      *kgo.Client
	topics  kafka.Topics
	session string
	pace    *pacer
	rec     *recording.Writer
	metrics *publishMetrics
	onEvent func(eventTime int64)
	onLabel func()

	corrupt *corrupter
	// traceRatio is the fraction of events that start a trace.
	traceRatio float64
	keys       map[string][]byte
	counters   [5]prometheus.Counter
	lastFlush  time.Time

	mu  sync.Mutex
	err error
}

func newPublisher(ctx context.Context, cl *kgo.Client, topics kafka.Topics, session string, pace *pacer, rec *recording.Writer, m *publishMetrics, corrupt *corrupter, traceRatio float64, onEvent func(int64)) *publisher {
	p := &publisher{
		ctx: ctx, cl: cl, topics: topics, session: session, pace: pace, rec: rec, corrupt: corrupt, traceRatio: traceRatio,
		metrics: m, onEvent: onEvent, keys: map[string][]byte{},
	}
	for feed, name := range map[sim.Feed]string{sim.FeedOrders: "orders", sim.FeedCancels: "cancels", sim.FeedTrades: "trades", sim.FeedBook: "book_snapshots"} {
		p.counters[feed] = m.published.WithLabelValues(name)
	}
	return p
}

// recordingFlushInterval bounds how much of a recording a crash can lose.
const recordingFlushInterval = 2 * time.Second

var schemaOfFeed = [5]string{
	sim.FeedOrders:  events.SchemaOrder,
	sim.FeedCancels: events.SchemaCancel,
	sim.FeedTrades:  events.SchemaTrade,
	sim.FeedBook:    events.SchemaBookSnapshot,
}

func (p *publisher) topic(f sim.Feed) string {
	switch f {
	case sim.FeedOrders:
		return p.topics.Orders
	case sim.FeedCancels:
		return p.topics.Cancels
	case sim.FeedTrades:
		return p.topics.Trades
	default:
		return p.topics.BookSnapshots
	}
}

func (p *publisher) key(symbol string) []byte {
	k, ok := p.keys[symbol]
	if !ok {
		k = []byte(symbol)
		p.keys[symbol] = k
	}
	return k
}

// Emit implements sim.Emitter.
func (p *publisher) Emit(sym *sim.Symbol, ev *ob.Event) error {
	if err := p.failure(); err != nil {
		return err
	}
	if err := p.pace.wait(p.ctx, ev.Time); err != nil {
		return err
	}
	msg := sim.ToProto(p.session, sym, ev, 0)
	if p.rec != nil {
		wrapped, err := events.Wrap(msg)
		if err != nil {
			return err
		}
		if err := p.rec.WriteEvent(wrapped); err != nil {
			return err
		}
	}
	now := time.Now()
	if p.rec != nil && now.Sub(p.lastFlush) >= recordingFlushInterval {
		if err := p.rec.Flush(); err != nil {
			return err
		}
		p.lastFlush = now
	}
	// Recordings keep published_at at zero so they are reproducible; the
	// Kafka copy carries the wall-clock publish time for latency tracking.
	msg.(interface{ GetHeader() *twpb.EventHeader }).GetHeader().PublishedAtNs = now.UnixNano()
	value, err := events.Marshal(msg)
	if err != nil {
		return err
	}
	feed := sim.FeedOf(ev.Kind)
	meta := kafka.EventMeta{Schema: schemaOfFeed[feed], Session: p.session, Seq: ev.Seq, HasSeq: true}
	value, headers := p.corrupt.apply(msg, value, meta.Headers())
	rec := &kgo.Record{Topic: p.topic(feed), Key: p.key(sym.Symbol), Value: value, Headers: headers}
	// The client context, not the session context, is used for producing:
	// stopping a session must still deliver what was already handed over.
	if p.traceRatio > 0 && rand.Float64() < p.traceRatio {
		p.produceTraced(rec, sym.Symbol, ev.Seq)
	} else {
		p.cl.Produce(context.Background(), rec, p.delivered)
	}
	p.counters[feed].Inc()
	if p.onEvent != nil {
		p.onEvent(ev.Time)
	}
	return nil
}

// Label implements sim.LabelEmitter: ground truth goes to its own topic,
// which no detector reads, and into the recording.
func (p *publisher) Label(l *twpb.GroundTruthLabel) error {
	if err := p.failure(); err != nil {
		return err
	}
	if p.rec != nil {
		if err := p.rec.WriteLabel(l); err != nil {
			return err
		}
	}
	value, err := events.Marshal(l)
	if err != nil {
		return err
	}
	p.cl.Produce(context.Background(), &kgo.Record{
		Topic: p.topics.GroundTruth, Key: []byte(l.GetSessionId()), Value: value,
		Headers: kafka.EventMeta{Schema: events.SchemaGroundTruth, Session: l.GetSessionId()}.Headers(),
	}, p.delivered)
	p.metrics.labels.WithLabelValues(l.GetScenario()).Inc()
	if p.onLabel != nil {
		p.onLabel()
	}
	return nil
}

// produceTraced produces rec as the first hop of a new trace: a producer
// span that ends when the broker acknowledges the record, and whose
// context the record's headers carry on to ingest.
func (p *publisher) produceTraced(rec *kgo.Record, symbol string, seq uint64) {
	ctx, span := tracer().Start(context.Background(), "send "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(tracing.MessagingAttrs("send", rec, false)...),
		trace.WithAttributes(
			attribute.String("tradewatch.session", p.session),
			attribute.String("tradewatch.symbol", symbol),
			attribute.Int64("tradewatch.seq", int64(seq)),
		))
	tracing.Inject(ctx, rec)
	p.cl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		p.delivered(r, err)
		tracing.EndSend(span, r, err)
	})
}

func (p *publisher) delivered(_ *kgo.Record, err error) {
	if err == nil {
		return
	}
	p.metrics.errors.Inc()
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.mu.Unlock()
}

func (p *publisher) failure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}
