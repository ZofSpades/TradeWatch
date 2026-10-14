//go:build integration

package alertsink

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/store"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

var quiet = slog.New(slog.DiscardHandler)

type env struct {
	kcfg  kafka.Config
	st    *store.Store
	views *live.Views
	prod  *kgo.Client
}

func setup(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	kcfg := kafka.Config{
		Brokers: testutil.KafkaBrokers(t), ClientID: "it-alertsink", Prefix: testutil.UniquePrefix(t),
		Partitions: 3, Replication: 1, LogLevel: kgo.LogLevelWarn,
	}
	prod, err := kafka.NewClient(kcfg, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var names []string
		for _, s := range kcfg.Topics().Specs(kcfg.Partitions) {
			names = append(names, s.Name)
		}
		_, _ = kadm.NewClient(prod).DeleteTopics(context.Background(), names...)
		prod.Close()
	})
	if err := kafka.EnsureTopics(ctx, kadm.NewClient(prod), kcfg.Topics().Specs(kcfg.Partitions), 1); err != nil {
		t.Fatal(err)
	}
	url := testutil.PostgresSchema(t)
	if _, err := store.Migrate(ctx, url, quiet); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	rdb := redis.NewClient(&redis.Options{Addr: testutil.RedisAddr(t)})
	t.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), kcfg.Prefix+"*").Result()
		if len(keys) > 0 {
			rdb.Del(context.Background(), keys...)
		}
		rdb.Close()
	})
	return &env{kcfg: kcfg, st: st, views: live.New(rdb, kcfg.Prefix, 100), prod: prod}
}

// sink is an alert sink running in the background.
type sink struct {
	cancel context.CancelFunc
	done   chan error
}

func (e *env) start(t *testing.T) *sink {
	t.Helper()
	cfg := Config{Kafka: e.kcfg, Group: e.kcfg.Prefix + "alertsink", RecentAlerts: 100, MaxBatch: 50}
	svc := NewService(cfg, e.st, e.views, prometheus.NewRegistry(), quiet)
	cl, err := kafka.NewClient(e.kcfg, quiet, svc.ClientOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	svc.Attach(cl)
	ctx, cancel := context.WithCancel(context.Background())
	s := &sink{cancel: cancel, done: make(chan error, 1)}
	go func() {
		err := svc.Run(ctx)
		cl.Close()
		s.done <- err
	}()
	return s
}

func (s *sink) stop(t *testing.T) {
	t.Helper()
	s.cancel()
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatalf("sink stopped with error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("sink did not stop")
	}
}

func testAlert(i int) *twpb.Alert {
	typ := twpb.AbuseType_ABUSE_TYPE_SPOOFING
	if i%2 == 0 {
		typ = twpb.AbuseType_ABUSE_TYPE_WASH_TRADING
	}
	return &twpb.Alert{
		AlertKey: fmt.Sprintf("key-%03d", i), SessionId: "S1", AbuseType: typ, Detector: "d", RuleId: "R",
		RuleVersion: 1, Severity: twpb.Severity_SEVERITY_HIGH, Score: 60, Symbol: "QZAP", AccountIds: []string{"A"},
		DetectedAtNs: int64(i) * int64(time.Second), Explanation: "e", DetectorInstance: "det-a",
		TriggerPublishedAtNs: time.Now().UnixNano(),
	}
}

func (e *env) produce(t *testing.T, topic string, values ...[]byte) {
	t.Helper()
	var recs []*kgo.Record
	for _, v := range values {
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte("k"), Value: v})
	}
	if err := e.prod.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func encode(t *testing.T, alerts ...*twpb.Alert) [][]byte {
	t.Helper()
	var out [][]byte
	for _, a := range alerts {
		b, err := events.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, b)
	}
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *env) total(t *testing.T) int64 {
	c, err := e.st.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return c.Total
}

func TestSinkStoresEachAlertOnceAcrossDuplicatesAndRestarts(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	topics := e.kcfg.Topics()

	var first []*twpb.Alert
	for i := range 20 {
		first = append(first, testAlert(i))
	}
	e.produce(t, topics.Alerts, encode(t, first...)...)
	e.produce(t, topics.Alerts, encode(t, first[3], first[7])...) // redelivered by a detector
	snap := &twpb.RuleSetSnapshot{RulesetDigest: "set-1", LoadedAtNs: time.Now().UnixNano(), LoadedBy: "det-a",
		Rules: []*twpb.RuleDocument{{RuleId: "R", Version: 1, Digest: "r1", Detector: "d", Name: "rule", Enabled: true}}}
	snapBytes, err := events.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	e.produce(t, topics.RulesAudit, snapBytes, snapBytes)

	s := e.start(t)
	waitFor(t, "20 alerts", func() bool { return e.total(t) == 20 })
	s.stop(t)

	// While the sink is down: new alerts, more duplicates, a record that is
	// not an alert, and an alert Postgres refuses (text cannot hold NUL).
	var second []*twpb.Alert
	for i := 20; i < 30; i++ {
		second = append(second, testAlert(i))
	}
	poison := testAlert(99)
	poison.Explanation = "bad\x00text"
	e.produce(t, topics.Alerts, encode(t, append(second, first[0], poison)...)...)
	e.produce(t, topics.Alerts, []byte("not a protobuf alert \xff\xff"))

	s = e.start(t)
	waitFor(t, "30 alerts", func() bool { return e.total(t) == 30 })
	waitFor(t, "live counts", func() bool {
		c, err := e.views.Counts(ctx)
		return err == nil && c.Total == 30
	})
	s.stop(t)

	counts, err := e.views.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Total != 30 || counts.ByType["wash_trading"] != 15 || counts.ByType["spoofing"] != 15 {
		t.Errorf("live counts %+v, want 30 alerts split 15/15", counts)
	}
	for key, want := range map[string]int{"key-000": 2, "key-003": 2, "key-007": 2, "key-001": 1, "key-025": 1} {
		a, _, err := e.st.Get(ctx, key)
		if err != nil || a.Deliveries != want {
			t.Errorf("%s: deliveries %v (err %v), want %d", key, a, err, want)
		}
	}
	if _, _, err := e.st.Get(ctx, poison.GetAlertKey()); err == nil {
		t.Error("the alert Postgres refused was stored")
	}
	if _, rules, err := e.st.CurrentRules(ctx); err != nil || len(rules) != 1 {
		t.Errorf("rule set: %v %v", rules, err)
	}

	// Both unusable records went to the dead-letter topic with a reason.
	dlq, err := kafka.NewClient(e.kcfg, quiet, kgo.ConsumeTopics(topics.DeadLetter), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer dlq.Close()
	reasons := map[string]int{}
	waitFor(t, "two dead letters", func() bool {
		fctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		dlq.PollFetches(fctx).EachRecord(func(r *kgo.Record) {
			var dl twpb.DeadLetter
			if events.Unmarshal(r.Value, &dl) == nil && dl.GetStage() == "alertsink" {
				reasons[dl.GetReason()]++
			}
		})
		return reasons["decode_error"]+reasons["store_rejected"] >= 2
	})
	if reasons["decode_error"] != 1 || reasons["store_rejected"] != 1 {
		t.Errorf("dead letters by reason: %v", reasons)
	}
}
