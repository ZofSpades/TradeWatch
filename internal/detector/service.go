// Package detector runs the detection engine (internal/detect) as a Kafka
// consumer-group service: market_events in, alerts out.
package detector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/metrics"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// Config configures the detector service.
type Config struct {
	Kafka          kafka.Config
	Group          string
	RulesDir       string
	ReloadInterval time.Duration
	CommitInterval time.Duration
	Instance       string
	HTTPAddr       string
	Engine         detect.Config
}

// LoadConfig reads TW_DETECTOR_* variables.
func LoadConfig(env *config.Env) (Config, error) {
	host, _ := os.Hostname()
	cfg := Config{
		Kafka:          kafka.LoadConfig(env, "detector"),
		Group:          env.String("TW_DETECTOR_GROUP", "detector"),
		RulesDir:       env.String("TW_DETECTOR_RULES_DIR", "rules"),
		ReloadInterval: env.Duration("TW_DETECTOR_RELOAD_INTERVAL", 5*time.Second),
		CommitInterval: env.Duration("TW_DETECTOR_COMMIT_INTERVAL", time.Second),
		Instance:       env.String("TW_DETECTOR_INSTANCE", host),
		HTTPAddr:       env.String("TW_HTTP_ADDR", ":8092"),
		Engine: detect.Config{
			MaxOrders:    env.Int("TW_DETECTOR_MAX_ORDERS", 200_000),
			OrderHorizon: env.Duration("TW_DETECTOR_ORDER_HORIZON", 15*time.Minute),
			IdleAfter:    env.Duration("TW_DETECTOR_IDLE_AFTER", 10*time.Minute),
		},
	}
	cfg.Engine.Instance = cfg.Instance
	if err := env.Err(); err != nil {
		return cfg, err
	}
	if cfg.CommitInterval <= 0 || cfg.ReloadInterval <= 0 {
		return cfg, errors.New("detector: commit and reload intervals must be positive")
	}
	return cfg, cfg.Kafka.Validate()
}

// Checkpoint is the engine state stored in offset-commit metadata. The
// committed offset itself is the frontier: the next event to process.
type Checkpoint struct {
	Version int   `json:"v"`
	At      int64 `json:"at"`
	// LowWater is the offset reprocessing starts from to rebuild the state.
	LowWater int64 `json:"lw"`
	// Frontier is the committed offset. Alerts of events before it were
	// produced and acknowledged before the commit.
	Frontier int64             `json:"fr"`
	Next     map[string]uint64 `json:"next,omitempty"`
	Closed   []string          `json:"closed,omitempty"`
}

const maxCheckpointBytes = 3800

func (cp Checkpoint) encode() (string, error) {
	for {
		b, err := json.Marshal(cp)
		if err != nil {
			return "", err
		}
		if len(b) <= maxCheckpointBytes {
			return string(b), nil
		}
		if len(cp.Closed) == 0 {
			return "", fmt.Errorf("detector: checkpoint of %d bytes exceeds the metadata limit", len(b))
		}
		cp.Closed = cp.Closed[1:]
	}
}

func decodeCheckpoint(s string) (Checkpoint, bool) {
	var cp Checkpoint
	if s == "" || json.Unmarshal([]byte(s), &cp) != nil || cp.Version != 1 {
		return Checkpoint{}, false
	}
	return cp, true
}

type partition struct {
	eng *detect.Engine
	// next is the offset of the next event to process.
	next int64
	// replayUntil is the previous owner's frontier: alerts of events before
	// it were already produced, so reprocessing them only rebuilds state.
	replayUntil int64
	digest      string
	prev        detect.Stats
}

type serviceMetrics struct {
	events     prometheus.Counter
	alerts     *prometheus.CounterVec
	replayed   prometheus.Counter
	suppressed prometheus.Counter
	duplicates prometheus.Counter
	gaps       prometheus.Counter
	late       prometheus.Counter
	evicted    prometheus.Counter
	abandoned  prometheus.Counter
	decode     prometheus.Counter
	latency    prometheus.Histogram
	process    prometheus.Histogram
	lag        prometheus.Gauge
	assigned   prometheus.Gauge
	open       prometheus.Gauge
	commits    *prometheus.CounterVec
	commitTime prometheus.Histogram
	reloads    *prometheus.CounterVec
}

func newServiceMetrics(reg prometheus.Registerer) *serviceMetrics {
	ns, sub := metrics.Namespace, "detector"
	c := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: name, Help: help})
	}
	g := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: name, Help: help})
	}
	m := &serviceMetrics{
		events: c("events_total", "Market events processed."),
		alerts: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "alerts_total",
			Help: "Alerts produced, by abuse type and rule."}, []string{"abuse_type", "rule"}),
		replayed:   c("replayed_alerts_total", "Alerts regenerated while rebuilding state after a handover and not produced again."),
		suppressed: c("suppressed_alerts_total", "Repeat detections silenced by a rule's suppression period."),
		duplicates: c("duplicate_events_total", "Events dropped because their sequence number was already processed."),
		gaps:       c("sequence_gaps_total", "Sequence gaps seen (marked or not)."),
		late:       c("late_events_total", "Events older than their symbol's watermark."),
		evicted:    c("orders_evicted_total", "Orders forgotten because of the order memory bounds."),
		abandoned:  c("abandoned_symbols_total", "(session, symbol) states dropped after their session went idle without closing."),
		decode:     c("decode_errors_total", "market_events records that could not be decoded."),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "event_latency_seconds",
			Help: "Wall time from marketsim publishing an event to the detector processing it.", Buckets: metrics.LatencyBuckets}),
		process: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "process_seconds",
			Help: "Time to run one event through every detector.", Buckets: prometheus.ExponentialBuckets(1e-6, 2, 16)}),
		lag:      g("consumer_lag_records", "Records between the processing position and the high watermark, over owned partitions."),
		assigned: g("assigned_partitions", "market_events partitions owned by this instance."),
		open:     g("open_symbols", "Open (session, symbol) states across owned partitions."),
		commits: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "commits_total",
			Help: "Offset commits, by result."}, []string{"result"}),
		commitTime: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "commit_duration_seconds",
			Help: "Time to flush alerts and commit offsets.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 14)}),
		reloads: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "rule_reloads_total",
			Help: "Rule set reloads, by result."}, []string{"result"}),
	}
	reg.MustRegister(m.events, m.alerts, m.replayed, m.suppressed, m.duplicates, m.gaps, m.late, m.evicted, m.abandoned, m.decode,
		m.latency, m.process, m.lag, m.assigned, m.open, m.commits, m.commitTime, m.reloads)
	return m
}

// Service consumes market_events and produces alerts.
type Service struct {
	cfg      Config
	log      *slog.Logger
	cl       *kgo.Client
	adm      *kadm.Client
	topics   kafka.Topics
	topicIDs map[string]kadm.TopicID
	m        *serviceMetrics

	latest atomic.Pointer[rules.Set] // most recently loaded rule set

	mu    sync.Mutex
	parts map[int32]*partition
	seek  map[int32]int64 // low-water offsets to start newly assigned partitions from
	hwm   map[int32]int64

	produceMu  sync.Mutex
	produceErr error
	fatal      chan error
}

// NewService loads the rules and prepares the service.
func NewService(cfg Config, reg prometheus.Registerer, log *slog.Logger) (*Service, error) {
	set, err := rules.Load(cfg.RulesDir)
	if err != nil {
		return nil, err
	}
	if err := detect.Validate(set); err != nil {
		return nil, err
	}
	s := &Service{
		cfg: cfg, log: log, topics: cfg.Kafka.Topics(), m: newServiceMetrics(reg),
		parts: map[int32]*partition{}, seek: map[int32]int64{}, hwm: map[int32]int64{},
		fatal: make(chan error, 1),
	}
	s.latest.Store(set)
	return s, nil
}

// ClientOptions returns the consumer-group options the service needs.
func (s *Service) ClientOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(s.cfg.Group),
		kgo.ConsumeTopics(s.topics.MarketEvents),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(s.onAssigned),
		kgo.OnPartitionsRevoked(s.onRevoked),
		kgo.OnPartitionsLost(s.onLost),
		kgo.AdjustFetchOffsetsFn(s.adjustOffsets),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(10 * time.Second),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}
}

// Attach supplies the group client.
func (s *Service) Attach(cl *kgo.Client) { s.cl, s.adm = cl, kadm.NewClient(cl) }

func (s *Service) fail(err error) {
	select {
	case s.fatal <- err:
	default:
	}
}

// Run consumes until ctx is cancelled or an unrecoverable error occurs.
func (s *Service) Run(ctx context.Context) error {
	if err := s.start(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	watcher := &rules.Watcher{
		Dir: s.cfg.RulesDir, Interval: s.cfg.ReloadInterval, Validate: detect.Validate,
		OnLoad: func(set *rules.Set) {
			s.latest.Store(set)
			s.m.reloads.WithLabelValues("ok").Inc()
			s.log.Info("rules reloaded", "digest", set.Digest, "rules", len(set.Rules))
			s.publishRules(ctx, set)
		},
		OnError: func(err error) {
			s.m.reloads.WithLabelValues("error").Inc()
			s.log.Error("rule reload rejected; keeping the previous rules", "err", err)
		},
	}
	go watcher.Run(ctx)

	lastCommit := time.Now()
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		fetches := s.cl.PollRecords(pctx, 5_000)
		cancel()
		select {
		case err := <-s.fatal:
			return err
		default:
		}
		s.mu.Lock()
		s.applyRules()
		s.handle(fetches)
		if time.Since(lastCommit) >= s.cfg.CommitInterval {
			s.commitLocked(ctx)
			lastCommit = time.Now()
		}
		s.updateGauges()
		s.mu.Unlock()
		s.cl.AllowRebalance()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.commitLocked(shutdown); err != nil {
		return err
	}
	select {
	case err := <-s.fatal:
		return err
	default:
		return nil
	}
}

func (s *Service) start(ctx context.Context) error {
	if err := kafka.EnsureTopics(ctx, s.adm, s.topics.Specs(s.cfg.Kafka.Partitions), s.cfg.Kafka.Replication); err != nil {
		return err
	}
	ids, err := kafka.TopicIDs(ctx, s.adm, s.topics.MarketEvents)
	if err != nil {
		return err
	}
	s.topicIDs = ids
	set := s.latest.Load()
	s.log.Info("rules loaded", "digest", set.Digest, "rules", len(set.Rules), "enabled", len(set.Enabled()))
	s.publishRules(ctx, set)
	return nil
}

// publishRules records a loaded rule set on the rules_audit topic.
func (s *Service) publishRules(ctx context.Context, set *rules.Set) {
	value, err := events.Marshal(set.Snapshot(s.cfg.Instance))
	if err != nil {
		s.log.Error("encode rule set", "err", err)
		return
	}
	rec := &kgo.Record{Topic: s.topics.RulesAudit, Key: []byte(set.Digest), Value: value,
		Headers: kafka.EventMeta{Schema: events.SchemaRuleSet}.Headers()}
	if err := s.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		s.log.Warn("publish rule set", "err", err)
	}
}

// applyRules moves every engine to the most recent rule set. It runs
// between two batches, so a reload takes effect at a batch boundary.
func (s *Service) applyRules() {
	set := s.latest.Load()
	for p, part := range s.parts {
		if part.digest == set.Digest {
			continue
		}
		if err := part.eng.SetRules(set); err != nil {
			s.log.Error("cannot apply rules to partition", "partition", p, "err", err)
			continue
		}
		part.digest = set.Digest
	}
}

func (s *Service) handle(fetches kgo.Fetches) {
	fetches.EachError(func(topic string, p int32, err error) {
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			s.log.Warn("fetch error", "topic", topic, "partition", p, "err", err)
		}
	})
	fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
		part := s.parts[fp.Partition]
		if part == nil {
			return
		}
		s.hwm[fp.Partition] = fp.HighWatermark
		for _, r := range fp.Records {
			s.process(part, r)
		}
	})
}

func (s *Service) process(part *partition, r *kgo.Record) {
	part.next = r.Offset + 1
	var ev twpb.MarketEvent
	if err := events.Unmarshal(r.Value, &ev); err != nil {
		s.m.decode.Inc()
		return
	}
	start := time.Now()
	alerts := part.eng.Process(&ev, r.Offset)
	s.m.process.Observe(time.Since(start).Seconds())
	s.m.events.Inc()
	if h := events.HeaderOf(&ev); h.GetPublishedAtNs() > 0 {
		s.m.latency.Observe(float64(start.UnixNano()-h.GetPublishedAtNs()) / 1e9)
	}
	for _, a := range alerts {
		if r.Offset < part.replayUntil {
			s.m.replayed.Inc()
			continue
		}
		a.EmittedAtNs = time.Now().UnixNano()
		value, err := events.Marshal(a)
		if err != nil {
			s.fail(fmt.Errorf("encode alert: %w", err))
			return
		}
		s.cl.Produce(context.Background(), &kgo.Record{
			Topic: s.topics.Alerts, Key: []byte(a.GetAlertKey()), Value: value,
			Headers: kafka.EventMeta{Schema: events.SchemaAlert, Session: a.GetSessionId()}.Headers(),
		}, s.produced)
		s.m.alerts.WithLabelValues(a.GetAbuseType().String(), a.GetRuleId()).Inc()
	}
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

// commitLocked flushes alerts and commits the frontier of every owned
// partition with its checkpoint. A failed alert stops the service instead
// of being committed past. A partition that is still rebuilding its state
// keeps the previous owner's checkpoint: committing it before the replay
// reaches that frontier would move the frontier back, and would leave out
// restored sequences the replay has not reached yet.
func (s *Service) commitLocked(ctx context.Context) error {
	if len(s.parts) == 0 {
		return nil
	}
	start := time.Now()
	if err := s.cl.Flush(ctx); err != nil {
		s.m.commits.WithLabelValues("error").Inc()
		return err
	}
	s.produceMu.Lock()
	perr := s.produceErr
	s.produceMu.Unlock()
	if perr != nil {
		err := fmt.Errorf("an alert was not delivered; refusing to commit: %w", perr)
		s.m.commits.WithLabelValues("error").Inc()
		s.fail(err)
		return err
	}
	offsets := map[string]map[int32]kafka.CommitOffset{s.topics.MarketEvents: {}}
	for p, part := range s.parts {
		if part.next < part.replayUntil {
			continue
		}
		lw, next, closed, ok := part.eng.Checkpoint()
		if !ok {
			lw = part.next
		}
		md, err := Checkpoint{
			Version: 1, At: time.Now().UnixNano(), LowWater: min(lw, part.next), Frontier: part.next,
			Next: next, Closed: closed,
		}.encode()
		if err != nil {
			s.fail(err)
			return err
		}
		offsets[s.topics.MarketEvents][p] = kafka.CommitOffset{Offset: part.next, Metadata: md}
	}
	if len(offsets[s.topics.MarketEvents]) == 0 {
		return nil
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

func (s *Service) updateGauges() {
	var lag int64
	open := 0
	for p, part := range s.parts {
		if d := s.hwm[p] - part.next; d > 0 {
			lag += d
		}
		st := part.eng.Stats()
		open += st.OpenSymbols
		s.m.duplicates.Add(float64(st.Duplicates + st.Stale - part.prev.Duplicates - part.prev.Stale))
		s.m.gaps.Add(float64(st.Gaps - part.prev.Gaps))
		s.m.late.Add(float64(st.Late - part.prev.Late))
		s.m.suppressed.Add(float64(st.Suppressed - part.prev.Suppressed))
		s.m.evicted.Add(float64(st.OrdersEvicted - part.prev.OrdersEvicted))
		s.m.abandoned.Add(float64(st.Abandoned - part.prev.Abandoned))
		part.prev = st
	}
	s.m.lag.Set(float64(lag))
	s.m.assigned.Set(float64(len(s.parts)))
	s.m.open.Set(float64(open))
}

func (s *Service) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var committed kadm.OffsetResponses
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if committed, err = s.adm.FetchOffsets(ctx, s.cfg.Group); err == nil {
			break
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	if err != nil {
		s.fail(fmt.Errorf("fetch checkpoints: %w", err))
		return
	}
	set := s.latest.Load()
	for _, p := range assigned[s.topics.MarketEvents] {
		eng, err := detect.NewEngine(set, s.cfg.Engine)
		if err != nil {
			s.fail(err)
			return
		}
		part := &partition{eng: eng, digest: set.Digest}
		if o, ok := committed.Lookup(s.topics.MarketEvents, p); ok && o.Err == nil && o.At >= 0 {
			part.next = o.At
			if cp, ok := decodeCheckpoint(o.Metadata); ok {
				eng.Restore(cp.Next, cp.Closed)
				part.replayUntil = cp.Frontier
				if cp.LowWater < o.At {
					s.seek[p] = cp.LowWater
					part.next = cp.LowWater
				}
				s.log.Info("partition assigned", "partition", p, "frontier", cp.Frontier, "replay_from", cp.LowWater,
					"replay_events", cp.Frontier-cp.LowWater)
			}
		} else {
			s.log.Info("partition assigned without a checkpoint", "partition", p)
		}
		s.parts[p] = part
	}
}

// adjustOffsets starts restored partitions at their checkpoint's low-water
// offset instead of the committed frontier, so their state is rebuilt
// before new events are processed.
func (s *Service) adjustOffsets(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p, lw := range s.seek {
		if parts, ok := offsets[s.topics.MarketEvents]; ok {
			if _, ok := parts[p]; ok {
				parts[p] = kgo.NewOffset().At(lw).WithEpoch(-1)
			}
		}
		delete(s.seek, p)
	}
	return offsets, nil
}

func (s *Service) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.commitLocked(ctx); err != nil {
		s.log.Warn("commit on revoke failed", "err", err)
	}
	for _, p := range revoked[s.topics.MarketEvents] {
		delete(s.parts, p)
		delete(s.hwm, p)
	}
}

func (s *Service) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.Warn("partitions lost; dropping state without committing")
	for _, p := range lost[s.topics.MarketEvents] {
		delete(s.parts, p)
		delete(s.hwm, p)
	}
}
