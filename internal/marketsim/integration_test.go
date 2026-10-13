//go:build integration

package marketsim

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/sim"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

type expectedKey struct {
	symbol string
	seq    uint64
}

// inProcess records what a session produces when simulated without Kafka,
// with published_at left at zero.
type inProcess struct {
	session string
	events  map[expectedKey][]byte
}

func (e *inProcess) Emit(sym *sim.Symbol, ev *ob.Event) error {
	b, err := events.Marshal(sim.ToProto(e.session, sym, ev, 0))
	e.events[expectedKey{sym.Symbol, ev.Seq}] = b
	return err
}

func decodeRaw(schema string, value []byte) (proto.Message, error) {
	var m proto.Message
	switch schema {
	case events.SchemaOrder:
		m = &twpb.OrderEvent{}
	case events.SchemaCancel:
		m = &twpb.CancelEvent{}
	case events.SchemaTrade:
		m = &twpb.TradeEvent{}
	case events.SchemaBookSnapshot:
		m = &twpb.BookSnapshot{}
	default:
		return nil, fmt.Errorf("unexpected schema %q", schema)
	}
	return m, events.Unmarshal(value, m)
}

// TestPublishedSessionMatchesInProcessSimulation runs a short session in
// fast mode through Kafka and checks that the four raw topics contain
// exactly the events an in-process simulation of the same configuration
// produces: every (symbol, seq) once, gap-free, with identical content.
func TestPublishedSessionMatchesInProcessSimulation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	log := slog.New(slog.DiscardHandler)

	cfg := Config{
		Kafka: kafka.Config{
			Brokers: testutil.KafkaBrokers(t), ClientID: "it-marketsim",
			Prefix: testutil.UniquePrefix(t), Partitions: 6, Replication: 1, LogLevel: kgo.LogLevelWarn,
		},
		Seed: 5, Sessions: 1,
		Start:            time.Date(2026, 2, 2, 14, 30, 0, 0, time.UTC),
		Duration:         5 * time.Minute,
		ClosingWindow:    30 * time.Second,
		SnapshotInterval: time.Second,
		Activity:         1,
		Pace:             PaceFast,
		AutoStart:        true,
	}
	topics := cfg.Kafka.Topics()
	prod, err := kafka.NewClient(cfg.Kafka, log)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	t.Cleanup(func() {
		names := []string{}
		for _, s := range topics.Specs(6) {
			names = append(names, s.Name)
		}
		_, _ = kadm.NewClient(prod).DeleteTopics(context.Background(), names...)
	})

	runner := NewRunner(cfg, prod, prometheus.NewRegistry(), log)
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if st := runner.Status(); st.SessionsCompleted != 1 || st.State != "finished" {
		t.Fatalf("status after run = %+v", st)
	}

	// The same session, simulated in-process.
	scfg := runner.SessionConfig(0, time.Now())
	want := &inProcess{session: scfg.SessionID, events: map[expectedKey][]byte{}}
	s, err := sim.New(scfg, sim.DefaultUniverse(), want)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	maxSeq := map[string]uint64{}
	for k := range want.events {
		maxSeq[k.symbol] = max(maxSeq[k.symbol], k.seq)
	}

	cons, err := kafka.NewClient(cfg.Kafka, log,
		kgo.ConsumeTopics(topics.Raw()...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()

	seen := map[expectedKey]bool{}
	for len(seen) < len(want.events) {
		fetches := cons.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out after %d of %d events", len(seen), len(want.events))
		}
		fetches.EachError(func(topic string, p int32, err error) { t.Fatalf("fetch %s/%d: %v", topic, p, err) })
		fetches.EachRecord(func(r *kgo.Record) {
			meta := kafka.ParseEventMeta(r.Headers)
			msg, err := decodeRaw(meta.Schema, r.Value)
			if err != nil {
				t.Fatalf("%s: %v", r.Topic, err)
			}
			h := msg.(interface{ GetHeader() *twpb.EventHeader }).GetHeader()
			if h.GetPublishedAtNs() == 0 {
				t.Errorf("%s/%d has no publish time", h.GetSymbol(), h.GetSeq())
			}
			if string(r.Key) != h.GetSymbol() || meta.Seq != h.GetSeq() || meta.Session != h.GetSessionId() || !meta.HasSeq {
				t.Errorf("key/headers %q %+v disagree with payload header %v", r.Key, meta, h)
			}
			h.PublishedAtNs = 0
			got, _ := events.Marshal(msg)
			k := expectedKey{h.GetSymbol(), h.GetSeq()}
			if seen[k] {
				t.Errorf("duplicate %v", k)
			}
			seen[k] = true
			if exp, ok := want.events[k]; !ok || string(exp) != string(got) {
				t.Errorf("%v differs from the in-process simulation", k)
			}
		})
	}
	for sym, n := range maxSeq {
		for seq := uint64(1); seq <= n; seq++ {
			if !seen[expectedKey{sym, seq}] {
				t.Fatalf("%s seq %d missing from the raw topics", sym, seq)
			}
		}
	}

	// Reference data: every account and instrument, plus the session.
	refc, err := kafka.NewClient(cfg.Kafka, log, kgo.ConsumeTopics(topics.RefData), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer refc.Close()
	wantRef := len(sim.DefaultUniverse().RefData()) + 1
	keys := map[string]bool{}
	for len(keys) < wantRef {
		fetches := refc.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("refdata: got %d of %d records", len(keys), wantRef)
		}
		fetches.EachRecord(func(r *kgo.Record) { keys[string(r.Key)] = true })
	}
	if !keys["session/"+scfg.SessionID] || !keys["instrument/QZAP"] {
		t.Errorf("refdata keys missing session or instrument: %d keys", len(keys))
	}
	t.Logf("verified %d events across %d symbols and %d refdata records", len(seen), len(maxSeq), len(keys))
}
