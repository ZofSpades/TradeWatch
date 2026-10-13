//go:build integration

package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/marketsim"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/sim"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

var quiet = slog.New(slog.DiscardHandler)

func itKafka(t *testing.T) kafka.Config {
	return kafka.Config{
		Brokers: testutil.KafkaBrokers(t), ClientID: "it-ingest", Prefix: testutil.UniquePrefix(t),
		Partitions: 6, Replication: 1, LogLevel: kgo.LogLevelWarn,
	}
}

func deleteTopics(t *testing.T, cfg kafka.Config) {
	t.Cleanup(func() {
		cl, err := kafka.NewClient(cfg, quiet)
		if err != nil {
			return
		}
		defer cl.Close()
		var names []string
		for _, s := range cfg.Topics().Specs(cfg.Partitions) {
			names = append(names, s.Name)
		}
		_, _ = kadm.NewClient(cl).DeleteTopics(context.Background(), names...)
	})
}

// instance is an ingest service running in-process.
type instance struct {
	cancel context.CancelFunc
	done   chan error
}

func startIngest(t *testing.T, kcfg kafka.Config, group string, gapTimeout time.Duration) *instance {
	t.Helper()
	cfg := Config{
		Kafka: kcfg, Group: group, CommitInterval: 200 * time.Millisecond, GapTimeout: gapTimeout,
		MaxHeld: 20_000, IdleAfter: 10 * time.Minute, RefDataTimeout: 2 * time.Second,
	}
	svc := NewService(cfg, prometheus.NewRegistry(), quiet)
	cl, err := kafka.NewClient(kcfg, quiet, svc.ClientOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	refCl, err := kafka.NewClient(kcfg, quiet, kgo.ConsumeTopics(kcfg.Topics().RefData), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	svc.Attach(cl, NewRefDataTail(refCl, kcfg.Topics().RefData, quiet))
	ctx, cancel := context.WithCancel(context.Background())
	inst := &instance{cancel: cancel, done: make(chan error, 1)}
	go func() {
		err := svc.Run(ctx)
		cl.Close()
		refCl.Close()
		inst.done <- err
	}()
	return inst
}

func (i *instance) stop(t *testing.T) {
	t.Helper()
	i.cancel()
	select {
	case err := <-i.done:
		if err != nil {
			t.Fatalf("ingest stopped with error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("ingest did not stop")
	}
}

// out is one consumed market_events record.
type out struct {
	partition int32
	offset    int64
	ev        *twpb.MarketEvent
}

// tail consumes a topic from the start in the background.
type tail struct {
	mu   sync.Mutex
	recs []out
	stop context.CancelFunc
}

func startTail(t *testing.T, kcfg kafka.Config, topic string) *tail {
	t.Helper()
	cl, err := kafka.NewClient(kcfg, quiet, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	tl := &tail{stop: cancel}
	go func() {
		defer cl.Close()
		for ctx.Err() == nil {
			fetches := cl.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				ev := &twpb.MarketEvent{}
				if topic == kcfg.Topics().MarketEvents {
					if err := events.Unmarshal(r.Value, ev); err != nil {
						t.Errorf("undecodable market event: %v", err)
						return
					}
				}
				tl.mu.Lock()
				tl.recs = append(tl.recs, out{r.Partition, r.Offset, ev})
				tl.mu.Unlock()
			})
		}
	}()
	t.Cleanup(cancel)
	return tl
}

func (tl *tail) waitFor(t *testing.T, cond func([]out) bool, timeout time.Duration) []out {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		tl.mu.Lock()
		snapshot := append([]out(nil), tl.recs...)
		tl.mu.Unlock()
		if cond(snapshot) {
			return snapshot
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d records", len(snapshot))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// simulateSession computes one session in-process and publishes the same
// session to the raw topics in the background at the given pace. It returns
// the expected events, keyed by (session, symbol) and seq with published_at
// cleared, and a channel that reports when publishing has finished.
func simulateSession(t *testing.T, kcfg kafka.Config, d time.Duration, pace marketsim.PaceMode, rate float64) (map[SeqKey]map[uint64][]byte, <-chan error) {
	t.Helper()
	mcfg := marketsim.Config{
		Kafka: kcfg, Seed: 21, Sessions: 1, Start: time.Date(2026, 2, 3, 14, 30, 0, 0, time.UTC),
		Duration: d, ClosingWindow: d / 10, SnapshotInterval: time.Second, Activity: 1,
		Pace: pace, Rate: rate, AutoStart: true,
	}
	prod, err := kafka.NewClient(kcfg, quiet)
	if err != nil {
		t.Fatal(err)
	}
	runner := marketsim.NewRunner(mcfg, prod, prometheus.NewRegistry(), quiet)
	scfg := runner.SessionConfig(0, time.Now())
	c := &collect{session: scfg.SessionID, events: map[SeqKey]map[uint64][]byte{}}
	s, err := sim.New(scfg, sim.DefaultUniverse(), c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		done <- runner.Run(ctx)
		prod.Close()
	}()
	return c.events, done
}

// expectedSession publishes one fast session and waits until it is in Kafka.
func expectedSession(t *testing.T, kcfg kafka.Config, d time.Duration) map[SeqKey]map[uint64][]byte {
	t.Helper()
	want, done := simulateSession(t, kcfg, d, marketsim.PaceFast, 0)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return want
}

type collect struct {
	session string
	events  map[SeqKey]map[uint64][]byte
}

func (c *collect) Emit(sym *sim.Symbol, ev *ob.Event) error {
	k := SeqKey{c.session, sym.Symbol}
	if c.events[k] == nil {
		c.events[k] = map[uint64][]byte{}
	}
	b, err := events.Marshal(sim.ToProto(c.session, sym, ev, 0))
	c.events[k][ev.Seq] = b
	return err
}

func count(m map[SeqKey]map[uint64][]byte) int {
	n := 0
	for _, v := range m {
		n += len(v)
	}
	return n
}

// uniqueKeys returns the number of distinct (key, seq) entries in recs.
func uniqueKeys(recs []out) int {
	seen := map[[2]any]bool{}
	for _, r := range recs {
		h := events.HeaderOf(r.ev)
		seen[[2]any{SeqKey{h.GetSessionId(), h.GetSymbol()}, h.GetSeq()}] = true
	}
	return len(seen)
}

// checkComplete verifies that recs contain every expected event, unaltered,
// with consistent enrichment and no gaps, and that within each partition
// every key's events appear in increasing sequence order (re-emissions
// after a restart may repeat a suffix, never reorder or skip).
func checkComplete(t *testing.T, recs []out, want map[SeqKey]map[uint64][]byte, uni *sim.Universe) {
	t.Helper()
	got := map[SeqKey]map[uint64]bool{}
	last := map[SeqKey]uint64{}
	for _, r := range recs {
		h := events.HeaderOf(r.ev)
		key := SeqKey{h.GetSessionId(), h.GetSymbol()}
		if g := r.ev.GetGap(); g != nil {
			t.Fatalf("unexpected gap %v seq %d-%d (%s)", key, h.GetSeq(), g.GetLastSeq(), g.GetReason())
		}
		if prev := last[key]; h.GetSeq() > prev+1 {
			t.Fatalf("%v: seq %d after %d at partition %d offset %d", key, h.GetSeq(), prev, r.partition, r.offset)
		}
		last[key] = h.GetSeq()
		if got[key] == nil {
			got[key] = map[uint64]bool{}
		}
		got[key][h.GetSeq()] = true

		// The wrapped payload must equal the raw event.
		inner := proto.Clone(events.Unwrap(r.ev))
		inner.(interface{ GetHeader() *twpb.EventHeader }).GetHeader().PublishedAtNs = 0
		b, _ := events.Marshal(inner)
		if exp := want[key][h.GetSeq()]; string(exp) != string(b) {
			t.Fatalf("%v seq %d payload differs from the raw event", key, h.GetSeq())
		}
		if r.ev.GetIngestedAtNs() == 0 || r.ev.GetEnrichment().GetTickSize() == 0 {
			t.Fatalf("%v seq %d is not enriched", key, h.GetSeq())
		}
		if tr := r.ev.GetTrade(); tr != nil {
			acct, _ := uni.Account(tr.GetBuyAccountId())
			if r.ev.GetEnrichment().GetBuyAccount().GetBeneficialOwnerId() != acct.BeneficialOwner {
				t.Fatalf("trade %d enriched with the wrong beneficial owner", tr.GetTradeId())
			}
		}
	}
	for key, seqs := range want {
		for seq := range seqs {
			if !got[key][seq] {
				t.Fatalf("%v seq %d missing from market_events", key, seq)
			}
		}
	}
}

func TestIngestSequencesASimulatedSession(t *testing.T) {
	kcfg := itKafka(t)
	deleteTopics(t, kcfg)
	want := expectedSession(t, kcfg, 4*time.Minute)

	inst := startIngest(t, kcfg, kcfg.Prefix+"ingest", 5*time.Second)
	events := startTail(t, kcfg, kcfg.Topics().MarketEvents)
	recs := events.waitFor(t, func(r []out) bool { return len(r) >= count(want) }, 2*time.Minute)
	inst.stop(t)

	checkComplete(t, recs, want, sim.DefaultUniverse())
	if len(recs) != count(want) {
		t.Errorf("%d records for %d events: duplicates without any restart", len(recs), count(want))
	}
	dlq := startTail(t, kcfg, kcfg.Topics().DeadLetter)
	time.Sleep(time.Second)
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if len(dlq.recs) != 0 {
		t.Errorf("%d records dead-lettered from a clean session", len(dlq.recs))
	}
	t.Logf("sequenced %d events across %d keys", len(recs), len(want))
}

func TestIngestResumesFromItsCheckpoint(t *testing.T) {
	kcfg := itKafka(t)
	deleteTopics(t, kcfg)
	// marketsim keeps publishing at a fixed rate while ingest instances stop,
	// start and rebalance, so every handover happens mid-stream.
	want, published := simulateSession(t, kcfg, 6*time.Minute, marketsim.PaceRate, 3_000)
	group := kcfg.Prefix + "ingest"
	tl := startTail(t, kcfg, kcfg.Topics().MarketEvents)

	// The first instance processes part of the session and stops.
	first := startIngest(t, kcfg, group, 5*time.Second)
	tl.waitFor(t, func(r []out) bool { return len(r) >= count(want)/4 }, time.Minute)
	first.stop(t)

	// A second instance joins, then a third instance joins and forces a
	// rebalance while the second is mid-stream.
	second := startIngest(t, kcfg, group, 5*time.Second)
	tl.waitFor(t, func(r []out) bool { return uniqueKeys(r) >= count(want)/2 }, time.Minute)
	third := startIngest(t, kcfg, group, 5*time.Second)
	recs := tl.waitFor(t, func(r []out) bool { return uniqueKeys(r) >= count(want) }, 2*time.Minute)
	second.stop(t)
	third.stop(t)
	if err := <-published; err != nil {
		t.Fatal(err)
	}

	// Re-emitted events are allowed (at-least-once) but nothing may be
	// missing, reordered, altered or replaced by a gap: a sequencer that
	// failed to restore its checkpoint would declare the already released
	// prefix lost.
	checkComplete(t, recs, want, sim.DefaultUniverse())
	t.Logf("%d records for %d events (%d re-emitted after handovers)", len(recs), count(want), len(recs)-count(want))
}

func produceRaw(t *testing.T, cl *kgo.Client, topic string, key string, value []byte, meta *kafka.EventMeta) {
	t.Helper()
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	if meta != nil {
		rec.Headers = meta.Headers()
	}
	if err := cl.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestIngestDeadLettersBadRecordsAndFillsGaps(t *testing.T) {
	kcfg := itKafka(t)
	deleteTopics(t, kcfg)
	topics := kcfg.Topics()
	cl, err := kafka.NewClient(kcfg, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := kafka.EnsureTopics(context.Background(), kadm.NewClient(cl), topics.Specs(6), 1); err != nil {
		t.Fatal(err)
	}
	for _, rd := range testRefRecords() {
		b, _ := events.Marshal(rd)
		produceRaw(t, cl, topics.RefData, "rd", b, nil)
	}

	mk := func(m proto.Message) []byte {
		b, err := events.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	meta := func(schema string, seq uint64) *kafka.EventMeta {
		return &kafka.EventMeta{Schema: schema, Session: "S1", Seq: seq, HasSeq: true}
	}
	o1 := validOrder()
	o1.Header.Seq = 1
	produceRaw(t, cl, topics.Orders, "QZAP", mk(o1), meta(events.SchemaOrder, 1))
	// seq 2: corrupt bytes, but headers intact.
	produceRaw(t, cl, topics.Trades, "QZAP", []byte{0xde, 0xad}, meta(events.SchemaTrade, 2))
	// seq 3: well-formed but for an account that does not exist.
	o3 := validOrder()
	o3.Header.Seq, o3.OrderId, o3.AccountId = 3, 3, "ACC-9999"
	produceRaw(t, cl, topics.Orders, "QZAP", mk(o3), meta(events.SchemaOrder, 3))
	// seq 4: valid cancel.
	c4 := validCancel()
	c4.Header.Seq = 4
	produceRaw(t, cl, topics.Cancels, "QZAP", mk(c4), meta(events.SchemaCancel, 4))
	// seq 5 is never published. A record without headers or a decodable
	// value carries no position at all.
	produceRaw(t, cl, topics.BookSnapshots, "QZAP", []byte("garbage"), nil)
	// seq 6 and 7: valid.
	b6 := validBook()
	b6.Header.Seq = 6
	produceRaw(t, cl, topics.BookSnapshots, "QZAP", mk(b6), meta(events.SchemaBookSnapshot, 6))
	t7 := validTrade()
	t7.Header.Seq = 7
	produceRaw(t, cl, topics.Trades, "QZAP", mk(t7), meta(events.SchemaTrade, 7))

	inst := startIngest(t, kcfg, kcfg.Prefix+"ingest", 500*time.Millisecond)
	defer inst.stop(t)
	tl := startTail(t, kcfg, topics.MarketEvents)
	recs := tl.waitFor(t, func(r []out) bool { return len(r) >= 7 }, time.Minute)
	var got []string
	for _, r := range recs {
		got = append(got, label(r.ev))
	}
	want := "[1:order 2-2:gap/decode_error 3-3:gap/unknown_account 4:cancel 5-5:gap/lost 6:book 7:trade]"
	if fmt.Sprint(got) != want {
		t.Fatalf("market_events = %v\nwant           %s", got, want)
	}

	reasons := map[string]int{}
	cl2, _ := kafka.NewClient(kcfg, quiet, kgo.ConsumeTopics(topics.DeadLetter), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cl2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for n := 0; n < 3; {
		cl2.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			var dl twpb.DeadLetter
			if err := events.Unmarshal(r.Value, &dl); err != nil {
				t.Fatal(err)
			}
			if dl.GetStage() != "ingest" || dl.GetSourceTopic() == "" || dl.GetFailedAtNs() == 0 {
				t.Errorf("incomplete dead letter %v", &dl)
			}
			reasons[dl.GetReason()]++
			n++
		})
		if ctx.Err() != nil {
			t.Fatal("timed out reading dead letters")
		}
	}
	if reasons[ReasonDecode] != 2 || reasons[ReasonUnknownAccount] != 1 {
		t.Errorf("dead-letter reasons = %v", reasons)
	}
}
