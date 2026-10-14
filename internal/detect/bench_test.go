package detect_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/ingest"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/rules"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

// recorder keeps a session's events as ingest hands them to the detectors.
type recorder struct {
	id     string
	ref    *ingest.ReferenceData
	events []*twpb.MarketEvent
}

func (r *recorder) Emit(sym *sim.Symbol, ev *ob.Event) error {
	mev, rej := ingest.Normalize(sim.ToProto(r.id, sym, ev, 0), r.ref, ev.Time)
	if rej != nil {
		return rej
	}
	r.events = append(r.events, mev)
	return nil
}

// workload is a seeded 20-minute session (plus a 3-minute close) with
// every kind of abuse injected often, so each detector has work to do.
var workload = sync.OnceValues(func() ([]*twpb.MarketEvent, error) {
	cfg := sim.DefaultConfig()
	cfg.Seed, cfg.SessionID = 4242, "bench-4242"
	cfg.Duration, cfg.ClosingWindow = 20*time.Minute, 3*time.Minute
	cfg.Injection = sim.Injection{WashTrading: 4, Spoofing: 4, MomentumIgnition: 4, QuoteStuffing: 4, MarkingTheClose: 1}
	uni := sim.DefaultUniverse()
	r := &recorder{id: cfg.SessionID}
	s, err := sim.New(cfg, uni, r)
	if err != nil {
		return nil, err
	}
	r.ref = ingest.NewReferenceData(append(uni.RefData(),
		&twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: s.Session().Proto()}})...)
	return r.events, s.Run(context.Background())
})

// only returns a copy of set in which just the rule with the given
// detector is enabled.
func only(set *rules.Set, detector string) *rules.Set {
	one := *set
	one.Rules = nil
	for _, r := range set.Rules {
		c := *r
		c.Enabled = r.Enabled && r.Detector == detector
		one.Rules = append(one.Rules, &c)
	}
	return &one
}

// benchEngine runs the whole workload through a fresh engine per
// iteration and reports the cost per event.
func benchEngine(b *testing.B, set *rules.Set) {
	events, err := workload()
	if err != nil {
		b.Fatal(err)
	}
	alerts := 0
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		eng, err := detect.NewEngine(set, detect.Config{Instance: "bench"})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for i, ev := range events {
			alerts += len(eng.Process(ev, int64(i)))
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(events)), "ns/event")
	b.ReportMetric(float64(alerts)/float64(b.N), "alerts/op")
	b.ReportMetric(float64(len(events)), "events/op")
}

// BenchmarkEngine measures detection over a recorded session: all rules
// together, as the detector service runs them, and each detector alone.
func BenchmarkEngine(b *testing.B) {
	set, err := rules.Load("../../rules")
	if err != nil {
		b.Fatal(err)
	}
	b.Run("all", func(b *testing.B) { benchEngine(b, set) })
	for _, d := range []string{"wash_trading", "spoofing", "momentum_ignition", "quote_stuffing", "marking_the_close"} {
		b.Run(d, func(b *testing.B) { benchEngine(b, only(set, d)) })
	}
}
