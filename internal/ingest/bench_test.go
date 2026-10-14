package ingest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

// benchSession is a seeded session's raw records, encoded and labelled
// with headers exactly as marketsim publishes them.
type benchSession struct {
	id   string
	raws []Raw
	ref  *ReferenceData
}

func (s *benchSession) Emit(sym *sim.Symbol, ev *ob.Event) error {
	msg := sim.ToProto(s.id, sym, ev, 0)
	value, err := events.Marshal(msg)
	if err != nil {
		return err
	}
	feed := Feed(sim.FeedOf(ev.Kind) - 1) // sim numbers its feeds from 1
	s.raws = append(s.raws, Raw{
		Feed: feed, Key: []byte(sym.Symbol), Value: value,
		Meta: kafka.EventMeta{Schema: feedSchemas[feed], Session: s.id, Seq: ev.Seq, HasSeq: true},
	})
	return nil
}

var rawWorkload = sync.OnceValues(func() (*benchSession, error) {
	cfg := sim.DefaultConfig()
	cfg.Seed, cfg.SessionID = 4242, "bench-4242"
	cfg.Duration, cfg.ClosingWindow = 20*time.Minute, 3*time.Minute
	cfg.Injection = sim.Injection{WashTrading: 4, Spoofing: 4, MomentumIgnition: 4, QuoteStuffing: 4, MarkingTheClose: 1}
	uni := sim.DefaultUniverse()
	bs := &benchSession{id: cfg.SessionID}
	s, err := sim.New(cfg, uni, bs)
	if err != nil {
		return nil, err
	}
	bs.ref = NewReferenceData(append(uni.RefData(),
		&twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: s.Session().Proto()}})...)
	return bs, s.Run(context.Background())
})

// BenchmarkParse decodes, validates and enriches one raw record, as ingest
// does for every record it consumes.
func BenchmarkParse(b *testing.B) {
	bs, err := rawWorkload()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, rej := Parse(bs.raws[i%len(bs.raws)], bs.ref, 1); rej != nil {
			b.Fatal(rej)
		}
	}
}

// BenchmarkSequencerInOrder releases a session's events through the
// sequencer as they arrive in order, the common case, and reports the cost
// per event.
func BenchmarkSequencerInOrder(b *testing.B) {
	bs, err := rawWorkload()
	if err != nil {
		b.Fatal(err)
	}
	evs := make([]*twpb.MarketEvent, len(bs.raws))
	for i, r := range bs.raws {
		ev, rej := Parse(r, bs.ref, 1)
		if rej != nil {
			b.Fatal(rej)
		}
		evs[i] = ev
	}
	released := 0
	emit := func(*twpb.MarketEvent) { released++ }
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		seq := NewSequencer(Checkpoint{}, 5*time.Second, time.Now, emit)
		var offsets [numFeeds]int64
		b.StartTimer()
		for i, r := range bs.raws {
			h := events.HeaderOf(evs[i])
			seq.Observe(r.Feed, offsets[r.Feed], SeqKey{Session: h.GetSessionId(), Symbol: h.GetSymbol()}, h.GetSeq(), evs[i], "")
			offsets[r.Feed]++
		}
	}
	if released != b.N*len(bs.raws) {
		b.Fatalf("released %d of %d events", released, b.N*len(bs.raws))
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(bs.raws)), "ns/event")
	b.ReportMetric(float64(len(bs.raws)), "events/op")
}
