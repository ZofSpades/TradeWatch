package detect

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/ingest"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

// tape is a simulated session after the ingest stage: enriched market
// events in sequence order, and the ground truth.
type tape struct {
	session string
	ref     *ingest.ReferenceData
	events  []*twpb.MarketEvent
	labels  []*twpb.GroundTruthLabel
}

func (tp *tape) Emit(sym *sim.Symbol, ev *ob.Event) error {
	mev, rej := ingest.Normalize(sim.ToProto(tp.session, sym, ev, 0), tp.ref, 1)
	if rej != nil {
		return fmt.Errorf("ingest rejected a simulated event: %v", rej)
	}
	tp.events = append(tp.events, mev)
	return nil
}

func (tp *tape) Label(l *twpb.GroundTruthLabel) error {
	tp.labels = append(tp.labels, l)
	return nil
}

func simulate(t testing.TB, seed uint64, d time.Duration, in sim.Injection) *tape {
	t.Helper()
	cfg := sim.DefaultConfig()
	cfg.Seed, cfg.SessionID, cfg.Duration, cfg.ClosingWindow, cfg.Injection = seed, fmt.Sprintf("T%d", seed), d, d/12, in
	uni := sim.DefaultUniverse()
	tp := &tape{session: cfg.SessionID}
	s, err := sim.New(cfg, uni, tp)
	if err != nil {
		t.Fatal(err)
	}
	tp.ref = ingest.NewReferenceData(append(uni.RefData(), &twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: s.Session().Proto()}})...)
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	return tp
}

var testInjection = sim.Injection{WashTrading: 2, Spoofing: 2, MomentumIgnition: 2, QuoteStuffing: 2, MarkingTheClose: 1}

type positioned struct {
	pos   int64
	alert *twpb.Alert
}

func run(t testing.TB, e *Engine, evs []*twpb.MarketEvent, from int) []positioned {
	t.Helper()
	var out []positioned
	for i := from; i < len(evs); i++ {
		for _, a := range e.Process(evs[i], int64(i)) {
			out = append(out, positioned{int64(i), a})
		}
	}
	return out
}

func TestEngineDropsDuplicatesAndStaleEvents(t *testing.T) {
	s := newStream(t, loadRules(t))
	s.continuous(px-100, px+100, 500)
	ev := &twpb.MarketEvent{Body: &twpb.MarketEvent_Book{Book: &twpb.BookSnapshot{
		Header: &twpb.EventHeader{SessionId: "S1", Symbol: "QZAP", Seq: 1, EventTimeNs: s.now},
		Phase:  twpb.TradingPhase_TRADING_PHASE_CONTINUOUS,
	}}}
	s.eng.Process(ev, 1) // seq 1 again
	gap := &twpb.MarketEvent{Body: &twpb.MarketEvent_Gap{Gap: &twpb.SequenceGap{
		Header: &twpb.EventHeader{SessionId: "S1", Symbol: "QZAP", Seq: 2, EventTimeNs: s.now}, LastSeq: 4, Reason: "lost",
	}}}
	s.eng.Process(gap, 2)
	s.seq = 4
	s.book(twpb.TradingPhase_TRADING_PHASE_CLOSED, nil, nil)
	s.continuous(px-100, px+100, 500) // after the close
	st := s.eng.Stats()
	if st.Duplicates != 1 || st.Gaps != 1 || st.LostEvents != 3 || st.Stale != 1 || st.OpenSymbols != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func snapshotAt(session string, seq uint64, eventTime, ingested time.Time) *twpb.MarketEvent {
	return &twpb.MarketEvent{
		Body: &twpb.MarketEvent_Book{Book: &twpb.BookSnapshot{
			Header: &twpb.EventHeader{SessionId: session, Symbol: "QZAP", Seq: seq, EventTimeNs: eventTime.UnixNano()},
			Phase:  twpb.TradingPhase_TRADING_PHASE_CONTINUOUS,
		}},
		IngestedAtNs: ingested.UnixNano(),
	}
}

// A marketsim restart abandons its session without a CLOSED snapshot. Once
// the stream has moved on, the engine must drop that session's state, or it
// would hold every checkpoint at the abandoned session's positions.
func TestEngineForgetsAbandonedSessions(t *testing.T) {
	eng, err := NewEngine(loadRules(t), Config{Instance: "x", IdleAfter: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	wall := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	// The abandoned session ran at a faster pace and is hours ahead of the
	// new one in event time, which therefore cannot reveal idleness.
	oldClock := wall.Add(6 * time.Hour)
	var pos int64
	feed := func(ev *twpb.MarketEvent) {
		eng.Process(ev, pos)
		pos++
	}
	for seq := uint64(1); seq <= 5; seq++ {
		at := time.Duration(seq) * time.Second
		feed(snapshotAt("OLD", seq, oldClock.Add(at), wall.Add(at)))
	}
	for seq := uint64(1); seq <= 11*60; seq++ { // one snapshot a second
		at := time.Duration(5+seq) * time.Second
		feed(snapshotAt("NEW", seq, wall.Add(at), wall.Add(at)))
		if seq == 9*60 {
			if lw, _, _, _ := eng.Checkpoint(); lw != 0 || eng.Stats().OpenSymbols != 2 {
				t.Fatalf("dropped a session before it was idle for 10m: low-water %d, stats %+v", lw, eng.Stats())
			}
		}
	}
	lw, next, _, ok := eng.Checkpoint()
	if st := eng.Stats(); st.Abandoned != 1 || st.OpenSymbols != 1 {
		t.Fatalf("stats = %+v, want the old session dropped", st)
	}
	if _, kept := next["OLD|QZAP"]; kept || !ok || lw != 5 {
		t.Fatalf("checkpoint at %d (ok=%v) with %v, want 5 without the old session", lw, ok, next)
	}

	// A dropped session that resumes starts over, reporting the gap.
	feed(snapshotAt("OLD", 6, oldClock.Add(time.Hour), wall.Add(12*time.Minute)))
	if st := eng.Stats(); st.OpenSymbols != 2 || st.Gaps != 1 || st.LostEvents != 5 {
		t.Fatalf("stats after resuming = %+v", st)
	}
}

// docs/detectors.md quotes the horizon of the default rule pack: marking
// the close looks back 30 minutes before a closing phase of up to 30
// minutes, plus a minute of chart context.
func TestEngineHorizonOfTheDefaultRules(t *testing.T) {
	eng, err := NewEngine(loadRules(t), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if h := eng.Horizon(); h != 61*time.Minute {
		t.Errorf("horizon = %v, want 61m", h)
	}
}

func TestEngineIsDeterministic(t *testing.T) {
	tp := simulate(t, 31, 40*time.Minute, testInjection)
	set := loadRules(t)
	a, _ := NewEngine(set, Config{Instance: "x"})
	b, _ := NewEngine(set, Config{Instance: "x"})
	ra, rb := run(t, a, tp.events, 0), run(t, b, tp.events, 0)
	if len(ra) == 0 {
		t.Fatal("no alerts; the test does not exercise detection")
	}
	if len(ra) != len(rb) {
		t.Fatalf("%d vs %d alerts", len(ra), len(rb))
	}
	for i := range ra {
		if !proto.Equal(ra[i].alert, rb[i].alert) {
			t.Fatalf("alert %d differs between identical runs", i)
		}
	}
	keys := map[string]bool{}
	for _, p := range ra {
		checkWellFormed(t, p.alert)
		if keys[p.alert.GetAlertKey()] {
			t.Fatalf("alert key %s produced twice", p.alert.GetAlertKey())
		}
		keys[p.alert.GetAlertKey()] = true
	}
}

// TestRestartFromCheckpointReproducesEveryAlert is the property the
// detector's delivery guarantees rest on. A run that crashes at position P,
// with a checkpoint taken at P, is resumed by a fresh engine that restores
// the checkpoint and reprocesses from its low-water position, emitting only
// alerts triggered at or after P. Together with the alerts emitted before
// the crash, that must be exactly the alerts of an uninterrupted run: none
// lost, none extra, none different.
func TestRestartFromCheckpointReproducesEveryAlert(t *testing.T) {
	tp := simulate(t, 32, 150*time.Minute, testInjection)
	set := loadRules(t)
	full, _ := NewEngine(set, Config{Instance: "x"})
	want := run(t, full, tp.events, 0)
	if len(want) < 10 {
		t.Fatalf("only %d alerts; the test does not exercise detection", len(want))
	}

	for _, frac := range []float64{0.25, 0.5, 0.75, 0.93} {
		crash := int(frac * float64(len(tp.events)))
		before, _ := NewEngine(set, Config{Instance: "x"})
		var got []positioned
		for i := 0; i < crash; i++ {
			for _, a := range before.Process(tp.events[i], int64(i)) {
				got = append(got, positioned{int64(i), proto.Clone(a).(*twpb.Alert)})
			}
		}
		lw, next, closed, ok := before.Checkpoint()
		if !ok || lw > int64(crash) {
			t.Fatalf("crash at %d: checkpoint %d ok=%v", crash, lw, ok)
		}
		after, _ := NewEngine(set, Config{Instance: "x"})
		after.Restore(next, closed)
		for _, p := range run(t, after, tp.events, int(lw)) {
			if p.pos >= int64(crash) {
				got = append(got, p)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("crash at %d (%.0f%%), replay from %d: %d alerts, want %d", crash, 100*frac, lw, len(got), len(want))
		}
		for i := range want {
			if want[i].pos != got[i].pos || !proto.Equal(want[i].alert, got[i].alert) {
				t.Fatalf("crash at %d: alert %d differs:\n got %v\nwant %v", crash, i, got[i].alert, want[i].alert)
			}
		}
		t.Logf("crash at %.0f%% (event %d): replayed %d events from %d, %d alerts identical", 100*frac, crash, crash-int(lw), lw, len(want))
	}
}

func TestSetRulesKeepsStateAndAppliesNewParameters(t *testing.T) {
	set := onlyRules(t, loadRules(t), "TW-WASH-001")
	s := newStream(t, set)
	s.owner("A1", "BO-X")
	s.owner("A2", "BO-X")
	s.continuous(px-200, px+200, 500)
	trade := func() {
		passive := s.limit("A1", sell, px+100, 1_000)
		s.advance(20 * time.Second)
		id := s.ioc("A2", buy, px+100, 250)
		s.fill(buy, "A2", id, "A1", passive, px+100, 250, 0, 750)
	}
	trade()
	trade()
	// Lower min_repeat from 3 to 2: the next same-owner trade is the third
	// in the window and the state of the first two must have been kept.
	stricter := onlyRules(t, loadRules(t, [3]string{"wash_trading.yaml", "min_repeat: 3", "min_repeat: 2"}), "TW-WASH-001")
	if err := s.eng.SetRules(stricter); err != nil {
		t.Fatal(err)
	}
	trade()
	if n := len(s.alertsOf("TW-WASH-001")); n != 1 {
		t.Fatalf("got %d alerts after the reload", n)
	}
	if f, _ := feature(s.alerts[0], "same_owner_trades"); f.GetValue() != 3 || f.GetThreshold() != 2 {
		t.Fatalf("feature = %v", f)
	}
	if err := s.eng.SetRules(onlyRules(t, loadRules(t, [3]string{"wash_trading.yaml", "min_ring_length: 3", "min_ring_length: 5"}), "TW-WASH-001")); err == nil {
		t.Fatal("a rule set with min_ring_length > max_ring_length was accepted")
	}
}

// TestDetectorsAgainstInjectedSessions is a regression guard: on seeded
// sessions with injected abuse, every detector must keep finding most of
// its episodes and raise few false alerts. The evaluator (cmd/evaluator)
// produces the published measurements; the floors here are deliberately
// below them so the test only fails on a real regression.
func TestDetectorsAgainstInjectedSessions(t *testing.T) {
	if testing.Short() {
		t.Skip("simulates several trading hours")
	}
	set := loadRules(t)
	floors := map[twpb.AbuseType][2]float64{ // recall, precision
		twpb.AbuseType_ABUSE_TYPE_WASH_TRADING:      {0.8, 0.9},
		twpb.AbuseType_ABUSE_TYPE_SPOOFING:          {0.6, 0.9},
		twpb.AbuseType_ABUSE_TYPE_MOMENTUM_IGNITION: {0.35, 0.9},
		twpb.AbuseType_ABUSE_TYPE_QUOTE_STUFFING:    {0.9, 0.9},
		twpb.AbuseType_ABUSE_TYPE_MARKING_THE_CLOSE: {0.35, 0.9},
	}
	labels, detected, alerts, matchedAlerts := map[twpb.AbuseType]int{}, map[twpb.AbuseType]int{}, map[twpb.AbuseType]int{}, map[twpb.AbuseType]int{}
	for seed := uint64(41); seed <= 43; seed++ {
		tp := simulate(t, seed, 2*time.Hour, sim.Injection{WashTrading: 1, Spoofing: 1, MomentumIgnition: 1, QuoteStuffing: 1, MarkingTheClose: 0.6})
		eng, _ := NewEngine(set, Config{Instance: "x"})
		found := map[int]bool{}
		for _, p := range run(t, eng, tp.events, 0) {
			a := p.alert
			alerts[a.GetAbuseType()]++
			hit := false
			for i, l := range tp.labels {
				if l.GetAbuseType() != a.GetAbuseType() || l.GetSymbol() != a.GetSymbol() {
					continue
				}
				shared := false
				for _, acct := range a.GetAccountIds() {
					shared = shared || slices.Contains(l.GetAccountIds(), acct)
				}
				if shared && a.GetDetectedAtNs() >= l.GetStartTimeNs() && a.GetWindowStartNs() <= l.GetEndTimeNs()+int64(time.Minute) {
					hit, found[i] = true, true
				}
			}
			if hit {
				matchedAlerts[a.GetAbuseType()]++
			}
		}
		for i, l := range tp.labels {
			labels[l.GetAbuseType()]++
			if found[i] {
				detected[l.GetAbuseType()]++
			}
		}
	}
	for typ, floor := range floors {
		if labels[typ] == 0 || alerts[typ] == 0 {
			t.Errorf("%v: %d labels, %d alerts", typ, labels[typ], alerts[typ])
			continue
		}
		recall := float64(detected[typ]) / float64(labels[typ])
		precision := float64(matchedAlerts[typ]) / float64(alerts[typ])
		t.Logf("%-30v labels=%3d recall=%.2f precision=%.2f", typ, labels[typ], recall, precision)
		if recall < floor[0] || precision < floor[1] {
			t.Errorf("%v: recall %.2f (floor %.2f), precision %.2f (floor %.2f)", typ, recall, floor[0], precision, floor[1])
		}
	}
}
