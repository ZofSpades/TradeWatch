// Package alertsink stores the detectors' alerts in Postgres and keeps the
// live views in Redis up to date: alerts and rules_audit in, one row per
// alert key out.
//
// Delivery is at-least-once with an idempotent write. A batch of records
// is stored in one transaction before its offsets are committed, so a
// crash can only cause a redelivery, which the alert key absorbs (it
// increments the alert's delivery count). Redis is updated only for alerts
// stored for the first time, after the transaction committed; a sink that
// dies in between leaves the views behind, and the Rebuild every sink runs
// at start-up restores them.
package alertsink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/metrics"
	"github.com/ZofSpades/tradewatch/internal/store"
)

// Config configures the alert sink.
type Config struct {
	Kafka        kafka.Config
	Group        string
	PostgresURL  string
	RedisAddr    string
	RedisPrefix  string
	RecentAlerts int
	MaxBatch     int
	HTTPAddr     string
}

// LoadConfig reads TW_ALERTSINK_*, TW_POSTGRES_URL and TW_REDIS_* variables.
func LoadConfig(env *config.Env) (Config, error) {
	cfg := Config{
		Kafka:        kafka.LoadConfig(env, "alertsink"),
		Group:        env.String("TW_ALERTSINK_GROUP", "alertsink"),
		PostgresURL:  env.Required("TW_POSTGRES_URL"),
		RedisAddr:    env.String("TW_REDIS_ADDR", "localhost:16379"),
		RedisPrefix:  env.String("TW_REDIS_PREFIX", "tw:"),
		RecentAlerts: env.Int("TW_ALERTSINK_RECENT_ALERTS", 500),
		MaxBatch:     env.Int("TW_ALERTSINK_MAX_BATCH", 500),
		HTTPAddr:     env.String("TW_HTTP_ADDR", ":8093"),
	}
	if err := env.Err(); err != nil {
		return cfg, err
	}
	if cfg.RecentAlerts <= 0 || cfg.MaxBatch <= 0 {
		return cfg, errors.New("alertsink: recent alerts and max batch must be positive")
	}
	return cfg, cfg.Kafka.Validate()
}

type serviceMetrics struct {
	records    *prometheus.CounterVec
	raised     *prometheus.CounterVec
	duplicates prometheus.Counter
	ruleSets   prometheus.Counter
	deadLetter *prometheus.CounterVec
	retries    prometheus.Counter
	storeTime  prometheus.Histogram
	latency    prometheus.Histogram
	liveErrors prometheus.Counter
	rebuilds   prometheus.Counter
	commits    *prometheus.CounterVec
	lag        prometheus.Gauge
}

func newServiceMetrics(reg prometheus.Registerer) *serviceMetrics {
	ns, sub := metrics.Namespace, "alertsink"
	c := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: name, Help: help})
	}
	m := &serviceMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "records_total",
			Help: "Records consumed, by topic."}, []string{"topic"}),
		raised: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "alerts_raised_total",
			Help: "Alerts stored for the first time, by abuse type."}, []string{"abuse_type"}),
		duplicates: c("alert_duplicates_total", "Alert deliveries whose key was already stored."),
		ruleSets:   c("rule_sets_total", "Rule-set snapshots recorded."),
		deadLetter: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "dead_letters_total",
			Help: "Records sent to the dead-letter topic, by reason."}, []string{"reason"}),
		retries: c("store_retries_total", "Database writes retried after a transient error."),
		storeTime: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "store_seconds",
			Help: "Time to store one batch in Postgres.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "pipeline_latency_seconds",
			Help:    "Wall time from marketsim publishing the event that triggered an alert to the alert being stored.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16)}),
		liveErrors: c("live_errors_total", "Failed updates of the Redis live views."),
		rebuilds:   c("live_rebuilds_total", "Rebuilds of the Redis live views from Postgres."),
		commits: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "commits_total",
			Help: "Offset commits, by result."}, []string{"result"}),
		lag: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "consumer_lag_records",
			Help: "Records between the consume position and the high watermark, over owned partitions."}),
	}
	reg.MustRegister(m.records, m.raised, m.duplicates, m.ruleSets, m.deadLetter, m.retries, m.storeTime, m.latency,
		m.liveErrors, m.rebuilds, m.commits, m.lag)
	return m
}

// Service consumes alerts and rule-set snapshots.
type Service struct {
	cfg    Config
	log    *slog.Logger
	cl     *kgo.Client
	adm    *kadm.Client
	topics kafka.Topics
	store  *store.Store
	views  *live.Views
	m      *serviceMetrics

	// stale is set when the live views missed an update; they are rebuilt
	// from Postgres, at most every rebuildBackoff while that keeps failing.
	stale       bool
	nextRebuild time.Time
	lag         map[string]map[int32]int64
}

const rebuildBackoff = 5 * time.Second

// NewService prepares the sink. Call ClientOptions, build the group client
// and Attach it before Run.
func NewService(cfg Config, st *store.Store, views *live.Views, reg prometheus.Registerer, log *slog.Logger) *Service {
	return &Service{
		cfg: cfg, log: log, topics: cfg.Kafka.Topics(), store: st, views: views, m: newServiceMetrics(reg),
		lag: map[string]map[int32]int64{},
	}
}

// ClientOptions returns the consumer-group options the service needs.
func (s *Service) ClientOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(s.cfg.Group),
		kgo.ConsumeTopics(s.topics.Alerts, s.topics.RulesAudit),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(10 * time.Second),
	}
}

// Attach supplies the group client.
func (s *Service) Attach(cl *kgo.Client) { s.cl, s.adm = cl, kadm.NewClient(cl) }

// Run consumes until ctx is cancelled or the database stays unavailable.
func (s *Service) Run(ctx context.Context) error {
	if err := kafka.EnsureTopics(ctx, s.adm, s.topics.Specs(s.cfg.Kafka.Partitions), s.cfg.Kafka.Replication); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	s.rebuild(ctx)
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fetches := s.cl.PollRecords(pctx, s.cfg.MaxBatch)
		cancel()
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				s.log.Warn("fetch error", "topic", topic, "partition", p, "err", err)
			}
		})
		fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
			if n := len(fp.Records); n > 0 {
				if s.lag[fp.Topic] == nil {
					s.lag[fp.Topic] = map[int32]int64{}
				}
				s.lag[fp.Topic][fp.Partition] = fp.HighWatermark - fp.Records[n-1].Offset - 1
			}
		})
		if records := fetches.Records(); len(records) > 0 {
			if err := s.process(ctx, records); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if err := s.cl.CommitRecords(ctx, records...); err != nil {
				// The records were stored; a redelivery after a failed commit
				// is absorbed by the alert key.
				s.m.commits.WithLabelValues("error").Inc()
				s.log.Warn("offset commit failed", "err", err)
			} else {
				s.m.commits.WithLabelValues("ok").Inc()
			}
		} else if s.stale {
			s.rebuild(ctx)
		}
		var lag int64
		for _, parts := range s.lag {
			for _, l := range parts {
				lag += max(l, 0)
			}
		}
		s.m.lag.Set(float64(lag))
		s.cl.AllowRebalance()
	}
	return nil
}

// rebuild restores the live views from Postgres. A failure leaves them
// marked stale for another attempt; Postgres stays authoritative.
func (s *Service) rebuild(ctx context.Context) {
	if time.Now().Before(s.nextRebuild) {
		return
	}
	if err := s.views.Rebuild(ctx, s.store); err != nil {
		s.stale = true
		s.nextRebuild = time.Now().Add(rebuildBackoff)
		s.m.liveErrors.Inc()
		s.log.Warn("rebuilding the live views failed; will retry", "err", err)
		return
	}
	s.stale = false
	s.m.rebuilds.Inc()
	s.log.Info("live views rebuilt from Postgres")
}

// pending is a decoded alert and the record it came from.
type pending struct {
	rec   *kgo.Record
	alert *twpb.Alert
}

func (s *Service) process(ctx context.Context, records []*kgo.Record) error {
	var alerts []pending
	for _, r := range records {
		s.m.records.WithLabelValues(r.Topic).Inc()
		switch r.Topic {
		case s.topics.Alerts:
			var a twpb.Alert
			if err := events.Unmarshal(r.Value, &a); err != nil || a.GetAlertKey() == "" {
				if err == nil {
					err = errors.New("alert has no key")
				}
				if err := s.deadLetter(ctx, r, "decode_error", err); err != nil {
					return err
				}
				continue
			}
			alerts = append(alerts, pending{r, &a})
		case s.topics.RulesAudit:
			var snap twpb.RuleSetSnapshot
			if err := events.Unmarshal(r.Value, &snap); err != nil || snap.GetRulesetDigest() == "" {
				if err == nil {
					err = errors.New("rule set has no digest")
				}
				if err := s.deadLetter(ctx, r, "decode_error", err); err != nil {
					return err
				}
				continue
			}
			err := s.withRetry(ctx, func() error { return s.store.RecordRuleSet(ctx, &snap) })
			if err != nil && !transient(err) {
				if err := s.deadLetter(ctx, r, "store_rejected", err); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			s.m.ruleSets.Inc()
		}
	}
	raised, err := s.storeAlerts(ctx, alerts)
	if err != nil {
		return err
	}
	now := time.Now()
	published := make(map[string]int64, len(alerts))
	for _, p := range alerts {
		published[p.alert.GetAlertKey()] = p.alert.GetTriggerPublishedAtNs()
	}
	for _, a := range raised {
		s.m.raised.WithLabelValues(a.AbuseType).Inc()
		if pub := published[a.Key]; pub > 0 {
			s.m.latency.Observe(now.Sub(time.Unix(0, pub)).Seconds())
		}
	}
	if s.stale {
		s.rebuild(ctx) // the rebuild includes what was just stored
		return nil
	}
	if err := s.views.Raised(ctx, raised); err != nil {
		s.stale = true
		s.m.liveErrors.Inc()
		s.log.Warn("updating the live views failed; they will be rebuilt", "err", err)
	}
	return nil
}

// storeAlerts stores a batch in one transaction, retrying transient
// failures. If the database rejects the batch for good, the alerts are
// stored one by one so that only the offending alert is dead-lettered.
func (s *Service) storeAlerts(ctx context.Context, alerts []pending) ([]*store.Alert, error) {
	if len(alerts) == 0 {
		return nil, nil
	}
	batch := make([]*twpb.Alert, len(alerts))
	for i, p := range alerts {
		batch[i] = p.alert
	}
	start := time.Now()
	var raised []*store.Alert
	var dups int
	err := s.withRetry(ctx, func() error {
		var err error
		raised, dups, err = s.store.UpsertAlerts(ctx, batch)
		return err
	})
	s.m.storeTime.Observe(time.Since(start).Seconds())
	if err == nil {
		s.m.duplicates.Add(float64(dups))
		return raised, nil
	}
	if transient(err) {
		return nil, err
	}
	s.log.Warn("database rejected an alert batch; storing alerts one by one", "alerts", len(alerts), "err", err)
	raised = nil
	for _, p := range alerts {
		var one []*store.Alert
		var dup int
		err := s.withRetry(ctx, func() error {
			var err error
			one, dup, err = s.store.UpsertAlerts(ctx, []*twpb.Alert{p.alert})
			return err
		})
		switch {
		case err == nil:
			raised = append(raised, one...)
			s.m.duplicates.Add(float64(dup))
		case transient(err):
			return nil, err
		default:
			if err := s.deadLetter(ctx, p.rec, "store_rejected", err); err != nil {
				return nil, err
			}
		}
	}
	return raised, nil
}

// transient reports whether a database error may succeed on retry:
// connection problems, serialization failures and deadlocks, resource
// exhaustion and operator intervention. Other database errors reject the
// data itself.
func transient(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "08"), strings.HasPrefix(pgErr.Code, "40"),
			strings.HasPrefix(pgErr.Code, "53"), strings.HasPrefix(pgErr.Code, "57"):
			return true
		}
		return false
	}
	return !errors.Is(err, context.Canceled)
}

// withRetry runs op until it succeeds, fails with a non-transient error,
// or ten attempts with exponential backoff (up to 30 s apart) have failed.
func (s *Service) withRetry(ctx context.Context, op func() error) error {
	delay := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil || !transient(err) || attempt == 10 {
			return err
		}
		s.m.retries.Inc()
		s.log.Warn("database write failed; retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(2*delay, 30*time.Second)
	}
}

// deadLetter sends a record that cannot be stored to the dead-letter topic
// and waits for the broker's acknowledgement, so its offset can be
// committed.
func (s *Service) deadLetter(ctx context.Context, r *kgo.Record, reason string, cause error) error {
	headers := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		headers[h.Key] = string(h.Value)
	}
	value, err := events.Marshal(&twpb.DeadLetter{
		SourceTopic: r.Topic, SourcePartition: r.Partition, SourceOffset: r.Offset,
		Key: r.Key, Value: r.Value, Headers: headers,
		Reason: reason, Detail: cause.Error(), Stage: "alertsink", FailedAtNs: time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: s.topics.DeadLetter, Key: r.Key, Value: value,
		Headers: kafka.EventMeta{Schema: events.SchemaDeadLetter}.Headers()}
	if err := s.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("dead-letter %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
	}
	s.m.deadLetter.WithLabelValues(reason).Inc()
	s.log.Warn("dead-lettered a record", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset,
		"reason", reason, "err", cause)
	return nil
}
