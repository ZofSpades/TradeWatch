package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/metrics"
)

// Config configures the ingest service.
type Config struct {
	Kafka kafka.Config
	Group string
	// CommitInterval is how often outputs are flushed and offsets committed.
	CommitInterval time.Duration
	// GapTimeout is how long a missing sequence number is waited for once
	// every feed is caught up.
	GapTimeout time.Duration
	// MaxHeld is the number of held records per partition above which feeds
	// that are ahead get paused.
	MaxHeld int
	// IdleAfter is how long a sequence may go without new records before it
	// is taken for abandoned (its session stopped without closing) and
	// dropped from the checkpoint. Zero keeps every sequence until its
	// session closes.
	IdleAfter time.Duration
	// RefDataTimeout bounds a reference-data catch-up for an unknown
	// account, symbol or session.
	RefDataTimeout time.Duration
	HTTPAddr       string
}

// LoadConfig reads TW_INGEST_* variables.
func LoadConfig(env *config.Env) (Config, error) {
	cfg := Config{
		Kafka:          kafka.LoadConfig(env, "ingest"),
		Group:          env.String("TW_INGEST_GROUP", "ingest"),
		CommitInterval: env.Duration("TW_INGEST_COMMIT_INTERVAL", time.Second),
		GapTimeout:     env.Duration("TW_INGEST_GAP_TIMEOUT", 5*time.Second),
		MaxHeld:        env.Int("TW_INGEST_MAX_HELD", 20_000),
		IdleAfter:      env.Duration("TW_INGEST_IDLE_AFTER", 10*time.Minute),
		RefDataTimeout: env.Duration("TW_INGEST_REFDATA_TIMEOUT", 2*time.Second),
		HTTPAddr:       env.String("TW_HTTP_ADDR", ":8091"),
	}
	if err := env.Err(); err != nil {
		return cfg, err
	}
	if cfg.CommitInterval <= 0 || cfg.GapTimeout <= 0 || cfg.MaxHeld <= 0 {
		return cfg, errors.New("ingest: commit interval, gap timeout and max held must be positive")
	}
	if cfg.IdleAfter < 0 {
		return cfg, errors.New("ingest: idle timeout must not be negative")
	}
	return cfg, cfg.Kafka.Validate()
}

type serviceMetrics struct {
	records    *prometheus.CounterVec
	dlq        *prometheus.CounterVec
	released   *prometheus.CounterVec
	gaps       *prometheus.CounterVec
	duplicates prometheus.Counter
	abandoned  prometheus.Counter
	held       prometheus.Gauge
	paused     prometheus.Gauge
	lag        prometheus.Gauge
	assigned   prometheus.Gauge
	commits    *prometheus.CounterVec
	commitTime prometheus.Histogram
	latency    prometheus.Histogram
}

func newServiceMetrics(reg prometheus.Registerer) *serviceMetrics {
	ns, sub := metrics.Namespace, "ingest"
	m := &serviceMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "records_total",
			Help: "Raw records consumed, by feed and outcome (accepted or rejected)."}, []string{"feed", "outcome"}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "dead_letters_total",
			Help: "Records sent to the dead-letter topic, by reason."}, []string{"reason"}),
		released: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "events_released_total",
			Help: "Events published to market_events in sequence order, by kind."}, []string{"kind"}),
		gaps: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "sequence_gaps_total",
			Help: "Sequence gaps filled with a SequenceGap marker, by reason."}, []string{"reason"}),
		duplicates: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "duplicates_total",
			Help: "Raw records dropped because their sequence number was already released or held."}),
		abandoned: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "abandoned_sequences_total",
			Help: "Sequences dropped after going idle without their session closing."}),
		held: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "held_records",
			Help: "Records waiting in the sequencer for an earlier sequence number."}),
		paused: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "paused_feeds",
			Help: "Feed partitions paused for backpressure."}),
		lag: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "consumer_lag_records",
			Help: "Records between the consume position and the high watermark, summed over owned partitions."}),
		assigned: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "assigned_partitions",
			Help: "Co-partitioned groups owned by this instance."}),
		commits: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "commits_total",
			Help: "Offset commits, by result."}, []string{"result"}),
		commitTime: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "commit_duration_seconds",
			Help: "Time to flush outputs and commit offsets.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 14)}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "event_latency_seconds",
			Help: "Wall-clock time from marketsim publishing an event to ingest releasing it.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)}),
	}
	reg.MustRegister(m.records, m.dlq, m.released, m.gaps, m.duplicates, m.abandoned, m.held, m.paused, m.lag, m.assigned,
		m.commits, m.commitTime, m.latency)
	return m
}

// group is the state of one co-partitioned partition number.
type group struct {
	seq    *Sequencer
	paused map[Feed]bool
	pos    [numFeeds]int64 // next offset to consume
	hwm    [numFeeds]int64
	prev   SequencerStats
}

// Service consumes the raw feeds and publishes market_events.
type Service struct {
	cfg      Config
	log      *slog.Logger
	cl       *kgo.Client
	adm      *kadm.Client
	ref      *RefDataTail
	topics   kafka.Topics
	feedOf   map[string]Feed
	topicIDs map[string]kadm.TopicID
	m        *serviceMetrics

	mu     sync.Mutex
	groups map[int32]*group

	produceMu  sync.Mutex
	produceErr error
	fatal      chan error
}

// ClientOptions returns the consumer-group options the service needs; the
// caller adds connection options and builds the client.
func (s *Service) ClientOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(s.cfg.Group),
		kgo.ConsumeTopics(s.topics.Raw()...),
		// Range assignment over co-partitioned topics gives one instance
		// partition p of every raw feed (ADR-0003).
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(s.onAssigned),
		kgo.OnPartitionsRevoked(s.onRevoked),
		kgo.OnPartitionsLost(s.onLost),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(10 * time.Second),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}
}

// NewService prepares the service. Call ClientOptions, build the group
// client and refdata client, then Attach before Run.
func NewService(cfg Config, reg prometheus.Registerer, log *slog.Logger) *Service {
	topics := cfg.Kafka.Topics()
	s := &Service{
		cfg: cfg, log: log, topics: topics, m: newServiceMetrics(reg),
		feedOf: map[string]Feed{
			topics.Orders: FeedOrders, topics.Cancels: FeedCancels,
			topics.Trades: FeedTrades, topics.BookSnapshots: FeedBook,
		},
		groups: map[int32]*group{},
		fatal:  make(chan error, 1),
	}
	return s
}

// Attach supplies the group client and the reference-data tail.
func (s *Service) Attach(cl *kgo.Client, ref *RefDataTail) {
	s.cl, s.adm, s.ref = cl, kadm.NewClient(cl), ref
}

func (s *Service) topicOf(f Feed) string { return s.topics.Raw()[f] }

func (s *Service) fail(err error) {
	select {
	case s.fatal <- err:
	default:
	}
}

// start creates the topology, resolves topic IDs and loads reference data.
func (s *Service) start(ctx context.Context) error {
	if err := kafka.EnsureTopics(ctx, s.adm, s.topics.Specs(s.cfg.Kafka.Partitions), s.cfg.Kafka.Replication); err != nil {
		return err
	}
	ids, err := kafka.TopicIDs(ctx, s.adm, s.topics.Raw()...)
	if err != nil {
		return err
	}
	s.topicIDs = ids
	go s.ref.Run(ctx)
	if err := s.ref.CatchUp(ctx, 30*time.Second); err != nil {
		return fmt.Errorf("initial reference data: %w", err)
	}
	return nil
}

// Run consumes until ctx is cancelled or an unrecoverable error occurs.
func (s *Service) Run(ctx context.Context) error {
	if err := s.start(ctx); err != nil {
		if ctx.Err() != nil {
			return nil // shut down while starting
		}
		return err
	}
	a, i, ses := s.ref.Data.Counts()
	s.log.Info("reference data loaded", "accounts", a, "instruments", i, "sessions", ses)

	lastCommit := time.Now()
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		fetches := s.cl.PollRecords(pctx, 10_000)
		cancel()
		select {
		case err := <-s.fatal:
			return err
		default:
		}
		s.mu.Lock()
		s.handle(ctx, fetches)
		s.checkGaps(ctx)
		s.backpressure()
		if time.Since(lastCommit) >= s.cfg.CommitInterval {
			s.forgetIdle()
			s.commitLocked(ctx)
			lastCommit = time.Now()
		}
		s.updateGauges()
		s.mu.Unlock()
		s.cl.AllowRebalance()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.commitLocked(shutdownCtx); err != nil {
		return err
	}
	s.log.Info("final commit done")
	select {
	case err := <-s.fatal:
		return err
	default:
		return nil
	}
}

func (s *Service) handle(ctx context.Context, fetches kgo.Fetches) {
	fetches.EachError(func(topic string, p int32, err error) {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return
		}
		s.log.Warn("fetch error", "topic", topic, "partition", p, "err", err)
	})
	fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
		feed, ok := s.feedOf[fp.Topic]
		g := s.groups[fp.Partition]
		if !ok || g == nil {
			return // revoked between fetch and processing
		}
		g.hwm[feed] = fp.HighWatermark
		for _, r := range fp.Records {
			s.handleRecord(ctx, g, feed, r)
		}
		if n := len(fp.Records); n > 0 {
			g.pos[feed] = fp.Records[n-1].Offset + 1
		}
	})
}

func isRefDataReason(reason string) bool {
	return reason == ReasonUnknownAccount || reason == ReasonUnknownSymbol || reason == ReasonUnknownSession
}

func (s *Service) handleRecord(ctx context.Context, g *group, feed Feed, r *kgo.Record) {
	meta := kafka.ParseEventMeta(r.Headers)
	raw := Raw{Feed: feed, Key: r.Key, Value: r.Value, Meta: meta}
	ev, rej := Parse(raw, s.ref.Data, time.Now().UnixNano())
	if rej != nil && isRefDataReason(rej.Reason) && !s.ref.SyncedWithin(time.Second) {
		// Reference data is published before the events that use it, so an
		// unknown identifier usually means the tail has not caught up yet.
		if err := s.ref.CatchUp(ctx, s.cfg.RefDataTimeout); err == nil {
			ev, rej = Parse(raw, s.ref.Data, time.Now().UnixNano())
		}
	}
	if rej != nil {
		s.m.records.WithLabelValues(feed.String(), "rejected").Inc()
		s.deadLetter(r, rej)
		if key, seq, ok := position(r, meta); ok {
			g.seq.Observe(feed, r.Offset, key, seq, nil, rej.Reason)
		} else {
			g.seq.ObserveUnsequenced(feed, r.Offset)
		}
		return
	}
	s.m.records.WithLabelValues(feed.String(), "accepted").Inc()
	h := events.HeaderOf(ev)
	g.seq.Observe(feed, r.Offset, SeqKey{Session: h.GetSessionId(), Symbol: h.GetSymbol()}, h.GetSeq(), ev, "")
}

// position recovers a rejected record's place in its sequence from the
// routing metadata, which is readable even when the value is not.
func position(r *kgo.Record, meta kafka.EventMeta) (SeqKey, uint64, bool) {
	sym := strings.ToUpper(strings.TrimSpace(string(r.Key)))
	if !meta.HasSeq || meta.Seq == 0 || meta.Session == "" || sym == "" {
		return SeqKey{}, 0, false
	}
	return SeqKey{Session: meta.Session, Symbol: sym}, meta.Seq, true
}

func (s *Service) emit(ev *twpb.MarketEvent) {
	h := events.HeaderOf(ev)
	value, err := events.Marshal(ev)
	if err != nil {
		s.fail(fmt.Errorf("marshal market event: %w", err))
		return
	}
	s.cl.Produce(context.Background(), &kgo.Record{
		Topic: s.topics.MarketEvents, Key: []byte(h.GetSymbol()), Value: value,
		Headers: kafka.EventMeta{Schema: events.SchemaMarketEvent, Session: h.GetSessionId(), Seq: h.GetSeq(), HasSeq: true}.Headers(),
	}, s.produced)
	s.m.released.WithLabelValues(events.KindOf(ev).String()).Inc()
	if pub := h.GetPublishedAtNs(); pub > 0 && ev.GetIngestedAtNs() > 0 {
		s.m.latency.Observe(float64(time.Now().UnixNano()-pub) / 1e9)
	}
}

func (s *Service) deadLetter(r *kgo.Record, rej *Rejection) {
	headers := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		headers[h.Key] = string(h.Value)
	}
	value, err := events.Marshal(&twpb.DeadLetter{
		SourceTopic: r.Topic, SourcePartition: r.Partition, SourceOffset: r.Offset,
		Key: r.Key, Value: r.Value, Headers: headers,
		Reason: rej.Reason, Detail: rej.Detail, Stage: "ingest", FailedAtNs: time.Now().UnixNano(),
	})
	if err != nil {
		s.fail(fmt.Errorf("marshal dead letter: %w", err))
		return
	}
	s.cl.Produce(context.Background(), &kgo.Record{
		Topic: s.topics.DeadLetter, Key: r.Key, Value: value,
		Headers: kafka.EventMeta{Schema: events.SchemaDeadLetter}.Headers(),
	}, s.produced)
	s.m.dlq.WithLabelValues(rej.Reason).Inc()
	s.log.Debug("dead-lettered record", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "reason", rej.Reason, "detail", rej.Detail)
}

func (s *Service) produced(_ *kgo.Record, err error) {
	if err == nil {
		return
	}
	s.produceMu.Lock()
	if s.produceErr == nil {
		s.produceErr = err
	}
	s.produceMu.Unlock()
}

// commitLocked flushes every output and commits the committable offsets of
// all owned partitions, with the sequencer checkpoint as metadata. If any
// output failed to be delivered nothing is committed and the service stops,
// so a restart reprocesses from the previous commit instead of losing data.
func (s *Service) commitLocked(ctx context.Context) error {
	if len(s.groups) == 0 {
		return nil
	}
	start := time.Now()
	if err := s.cl.Flush(ctx); err != nil {
		s.m.commits.WithLabelValues("error").Inc()
		return fmt.Errorf("flush: %w", err)
	}
	s.produceMu.Lock()
	perr := s.produceErr
	s.produceMu.Unlock()
	if perr != nil {
		s.m.commits.WithLabelValues("error").Inc()
		err := fmt.Errorf("an output was not delivered; refusing to commit: %w", perr)
		s.fail(err)
		return err
	}
	at := time.Now().UnixNano()
	offsets := map[string]map[int32]kafka.CommitOffset{}
	for p, g := range s.groups {
		md, err := g.seq.Checkpoint(at).Encode()
		if err != nil {
			s.fail(err)
			return err
		}
		for f := Feed(0); f < numFeeds; f++ {
			topic := s.topicOf(f)
			if offsets[topic] == nil {
				offsets[topic] = map[int32]kafka.CommitOffset{}
			}
			offsets[topic][p] = kafka.CommitOffset{Offset: g.seq.Committable(f), Metadata: md}
		}
	}
	if err := kafka.CommitWithMetadata(ctx, s.cl, s.cfg.Group, offsets, s.topicIDs); err != nil {
		s.m.commits.WithLabelValues("error").Inc()
		s.log.Warn("offset commit failed", "err", err)
		return err
	}
	s.m.commits.WithLabelValues("ok").Inc()
	s.m.commitTime.Observe(time.Since(start).Seconds())
	return nil
}

// forgetIdle drops the sequences of sessions that stopped without closing.
func (s *Service) forgetIdle() {
	for p, g := range s.groups {
		if n := g.seq.ForgetIdle(s.cfg.IdleAfter); n > 0 {
			s.log.Info("forgot sequences of abandoned sessions", "partition", p, "sequences", n)
		}
	}
}

func (s *Service) checkGaps(ctx context.Context) {
	for p, g := range s.groups {
		g.seq.CheckGaps(func() bool { return s.caughtUp(ctx, p, g) })
	}
}

// caughtUp asks the brokers for the current end offsets of partition p's
// feeds and reports whether consumption has reached all of them.
func (s *Service) caughtUp(ctx context.Context, p int32, g *group) bool {
	ends, err := s.adm.ListEndOffsets(ctx, s.topics.Raw()...)
	if err != nil {
		return false
	}
	for f := Feed(0); f < numFeeds; f++ {
		end, ok := ends.Lookup(s.topicOf(f), p)
		if !ok || end.Err != nil || g.pos[f] < end.Offset {
			return false
		}
	}
	return true
}

// backpressure pauses feeds while a partition holds too many records and
// resumes them as soon as the buffer drains or any blocked key could need
// them, so a pause can never starve the record a key is waiting for.
func (s *Service) backpressure() {
	for p, g := range s.groups {
		held := g.seq.Held()
		pausable := g.seq.PausableFeeds()
		for f := range g.paused {
			if !pausable[f] || held < s.cfg.MaxHeld/2 {
				s.cl.ResumeFetchPartitions(map[string][]int32{s.topicOf(f): {p}})
				delete(g.paused, f)
			}
		}
		if held <= s.cfg.MaxHeld {
			continue
		}
		for f := Feed(0); f < numFeeds; f++ {
			if pausable[f] && !g.paused[f] {
				s.cl.PauseFetchPartitions(map[string][]int32{s.topicOf(f): {p}})
				g.paused[f] = true
				s.log.Info("paused a feed that is ahead of every missing sequence number", "feed", f, "partition", p, "held", held)
			}
		}
	}
}

func (s *Service) resume(p int32, g *group) {
	for f := range g.paused {
		s.cl.ResumeFetchPartitions(map[string][]int32{s.topicOf(f): {p}})
	}
	g.paused = map[Feed]bool{}
}

func (s *Service) updateGauges() {
	var held, paused int
	var lag int64
	for _, g := range s.groups {
		held += g.seq.Held()
		paused += len(g.paused)
		for f := range g.pos {
			if d := g.hwm[f] - g.pos[f]; d > 0 {
				lag += d
			}
		}
		st := g.seq.Stats()
		s.m.duplicates.Add(float64(st.Duplicates - g.prev.Duplicates + st.StaleRecords - g.prev.StaleRecords))
		s.m.gaps.WithLabelValues(GapDeadLetter).Add(float64(st.DeadLetter - g.prev.DeadLetter))
		s.m.gaps.WithLabelValues(GapLost).Add(float64(st.LostGaps - g.prev.LostGaps))
		s.m.abandoned.Add(float64(st.Abandoned - g.prev.Abandoned))
		g.prev = st
	}
	s.m.held.Set(float64(held))
	s.m.paused.Set(float64(paused))
	s.m.lag.Set(float64(lag))
	s.m.assigned.Set(float64(len(s.groups)))
}

func (s *Service) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := map[int32]bool{}
	for _, ps := range assigned {
		for _, p := range ps {
			parts[p] = true
		}
	}
	var committed kadm.OffsetResponses
	var starts kadm.ListedOffsets
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if committed, err = s.adm.FetchOffsets(ctx, s.cfg.Group); err == nil {
			if starts, err = s.adm.ListStartOffsets(ctx, s.topics.Raw()...); err == nil {
				break
			}
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	if err != nil {
		s.fail(fmt.Errorf("restore assigned partitions: %w", err))
		return
	}
	for p := range parts {
		var cp Checkpoint
		restored := false
		for f := Feed(0); f < numFeeds; f++ {
			if o, ok := committed.Lookup(s.topicOf(f), p); ok && o.Err == nil {
				if c, ok := DecodeCheckpoint(o.Metadata); ok && (!restored || c.At > cp.At) {
					cp, restored = c, true
				}
			}
		}
		g := &group{paused: map[Feed]bool{}}
		g.seq = NewSequencer(cp, s.cfg.GapTimeout, time.Now, s.emit)
		for f := Feed(0); f < numFeeds; f++ {
			base := int64(0)
			if so, ok := starts.Lookup(s.topicOf(f), p); ok && so.Err == nil {
				base = so.Offset
			}
			if o, ok := committed.Lookup(s.topicOf(f), p); ok && o.Err == nil && o.At >= 0 {
				base = o.At
			}
			g.seq.SetBase(f, base)
			g.pos[f] = base
		}
		s.groups[p] = g
		s.log.Info("partition assigned", "partition", p, "restored_checkpoint", restored, "open_sequences", len(cp.Next))
	}
}

func (s *Service) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.commitLocked(ctx); err != nil {
		s.log.Warn("commit on revoke failed", "err", err)
	}
	s.drop(revoked)
}

func (s *Service) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.Warn("partitions lost; dropping state without committing")
	s.drop(lost)
}

func (s *Service) drop(parts map[string][]int32) {
	for _, ps := range parts {
		for _, p := range ps {
			if g, ok := s.groups[p]; ok {
				s.resume(p, g)
				delete(s.groups, p)
				s.log.Info("partition released", "partition", p)
			}
		}
	}
}
