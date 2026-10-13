package sim

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
)

// tape records every event per symbol and every label.
type tape struct {
	events map[string][]ob.Event
	labels []*twpb.GroundTruthLabel
	onEmit func(*Sim, *ob.Event)
	sim    *Sim
}

func (t *tape) Emit(sym *Symbol, ev *ob.Event) error {
	if t.events == nil {
		t.events = map[string][]ob.Event{}
	}
	t.events[sym.Symbol] = append(t.events[sym.Symbol], *ev)
	if t.onEmit != nil {
		t.onEmit(t.sim, ev)
	}
	return nil
}

func (t *tape) Label(l *twpb.GroundTruthLabel) error {
	t.labels = append(t.labels, l)
	return nil
}

func (t *tape) byScenario(name string) []*twpb.GroundTruthLabel {
	var out []*twpb.GroundTruthLabel
	for _, l := range t.labels {
		if l.GetScenario() == name {
			out = append(out, l)
		}
	}
	return out
}

func runTape(t *testing.T, cfg Config, onEmit func(*Sim, *ob.Event)) (*Sim, *tape) {
	t.Helper()
	tp := &tape{onEmit: onEmit}
	s, err := New(cfg, DefaultUniverse(), tp)
	if err != nil {
		t.Fatal(err)
	}
	tp.sim = s
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, tp
}

func injected(seed uint64, d time.Duration, in Injection) Config {
	cfg := testConfig(seed, d)
	cfg.Injection = in
	return cfg
}

// eventIndex locates the orders and trades of one symbol's stream.
type eventIndex struct {
	accepted map[uint64]ob.Event // order ID -> acceptance
	trades   map[uint64]ob.Event
	cancels  map[uint64]ob.Event // order ID -> last cancel
	all      []ob.Event
}

func index(evs []ob.Event) eventIndex {
	ix := eventIndex{accepted: map[uint64]ob.Event{}, trades: map[uint64]ob.Event{}, cancels: map[uint64]ob.Event{}, all: evs}
	for _, e := range evs {
		switch e.Kind {
		case ob.EventAccepted:
			ix.accepted[e.OrderID] = e
		case ob.EventTrade:
			ix.trades[e.TradeID] = e
		case ob.EventCancelled:
			ix.cancels[e.OrderID] = e
		}
	}
	return ix
}

// checkLabelMatchesStream verifies a label against the event stream: every
// order and trade it lists exists, belongs to the label's accounts and lies
// inside the label's window.
func checkLabelMatchesStream(t *testing.T, l *twpb.GroundTruthLabel, ix eventIndex) {
	t.Helper()
	accounts := l.GetAccountIds()
	if l.GetLabelId() == "" || l.GetSessionId() == "" || len(accounts) == 0 || l.GetStartTimeNs() > l.GetEndTimeNs() {
		t.Fatalf("incomplete label %v", l)
	}
	for _, id := range l.GetOrderIds() {
		e, ok := ix.accepted[id]
		if !ok {
			t.Fatalf("%s: order %d is not in the stream", l.GetLabelId(), id)
		}
		if !slices.Contains(accounts, e.Account) {
			t.Fatalf("%s: order %d belongs to %s, not a labelled account", l.GetLabelId(), id, e.Account)
		}
		if e.Time < l.GetStartTimeNs() || e.Time > l.GetEndTimeNs() {
			t.Fatalf("%s: order %d at %d lies outside the label window", l.GetLabelId(), id, e.Time)
		}
	}
	for _, id := range l.GetTradeIds() {
		e, ok := ix.trades[id]
		if !ok {
			t.Fatalf("%s: trade %d is not in the stream", l.GetLabelId(), id)
		}
		if !slices.Contains(accounts, e.BuyAccount) && !slices.Contains(accounts, e.SellAccount) {
			t.Fatalf("%s: trade %d involves none of the labelled accounts", l.GetLabelId(), id)
		}
		if e.Time < l.GetStartTimeNs() || e.Time > l.GetEndTimeNs() {
			t.Fatalf("%s: trade %d lies outside the label window", l.GetLabelId(), id)
		}
	}
}

func TestNothingIsLabelledWithoutInjection(t *testing.T) {
	_, tp := runTape(t, testConfig(4, 30*time.Minute), nil)
	if len(tp.labels) != 0 {
		t.Fatalf("%d labels without any injection", len(tp.labels))
	}
}

func TestInjectedSessionsAreReproducible(t *testing.T) {
	in := Injection{WashTrading: 2, Spoofing: 2, MomentumIgnition: 2, QuoteStuffing: 2, MarkingTheClose: 0.5}
	_, a := runTape(t, injected(9, 40*time.Minute, in), nil)
	_, b := runTape(t, injected(9, 40*time.Minute, in), nil)
	if len(a.labels) == 0 || len(a.labels) != len(b.labels) {
		t.Fatalf("label counts %d and %d", len(a.labels), len(b.labels))
	}
	for i := range a.labels {
		if !proto.Equal(a.labels[i], b.labels[i]) {
			t.Fatalf("label %d differs between runs", i)
		}
	}
	for sym, evs := range a.events {
		if len(evs) != len(b.events[sym]) {
			t.Fatalf("%s: %d vs %d events", sym, len(evs), len(b.events[sym]))
		}
	}
}

func TestWashTradingEpisodes(t *testing.T) {
	s, tp := runTape(t, injected(12, time.Hour, Injection{WashTrading: 4}), nil)
	direct, ring := tp.byScenario(ScenarioWashDirect), tp.byScenario(ScenarioWashRing)
	if len(direct) == 0 || len(ring) == 0 {
		t.Fatalf("direct=%d ring=%d labels", len(direct), len(ring))
	}
	owner := func(id string) string {
		a, _ := s.Universe().Account(id)
		return a.BeneficialOwner
	}
	for _, l := range append(direct, ring...) {
		ix := index(tp.events[l.GetSymbol()])
		checkLabelMatchesStream(t, l, ix)
		if len(l.GetTradeIds()) == 0 {
			t.Fatalf("%s: wash label without trades", l.GetLabelId())
		}
		net := map[string]int64{}
		for _, id := range l.GetTradeIds() {
			tr := ix.trades[id]
			// Every labelled trade has an episode account on both sides.
			if !slices.Contains(l.GetAccountIds(), tr.BuyAccount) || !slices.Contains(l.GetAccountIds(), tr.SellAccount) {
				t.Fatalf("%s: trade %d is not between episode accounts", l.GetLabelId(), id)
			}
			net[tr.BuyAccount] += tr.Qty
			net[tr.SellAccount] -= tr.Qty
		}
		switch l.GetScenario() {
		case ScenarioWashDirect:
			if len(l.GetBeneficialOwnerIds()) != 1 || owner(l.GetAccountIds()[0]) != owner(l.GetAccountIds()[1]) {
				t.Fatalf("%s: direct wash accounts must share one owner", l.GetLabelId())
			}
		case ScenarioWashRing:
			if len(l.GetAccountIds()) != 3 || len(l.GetBeneficialOwnerIds()) != 3 {
				t.Fatalf("%s: a ring is three accounts with three different owners", l.GetLabelId())
			}
			// Only complete cycles are labelled: every member both bought
			// and sold. A leg can execute partially (another participant hit
			// the passive order first), so members are flat to within one
			// leg's quantity rather than exactly.
			bought, sold := map[string]bool{}, map[string]bool{}
			for _, id := range l.GetTradeIds() {
				bought[ix.trades[id].BuyAccount] = true
				sold[ix.trades[id].SellAccount] = true
			}
			legQty, _ := strconv.ParseInt(l.GetParams()["quantity"], 10, 64)
			for _, acct := range l.GetAccountIds() {
				if !bought[acct] || !sold[acct] {
					t.Fatalf("%s: ring member %s did not both buy and sell", l.GetLabelId(), acct)
				}
				if n := net[acct]; n > legQty || -n > legQty {
					t.Fatalf("%s: ring member %s ends %+d shares (leg %d)", l.GetLabelId(), acct, n, legQty)
				}
			}
		}
	}
}

func TestSpoofingEpisodes(t *testing.T) {
	_, tp := runTape(t, injected(13, time.Hour, Injection{Spoofing: 4}), nil)
	labels := tp.byScenario(ScenarioSpoofing)
	if len(labels) == 0 {
		t.Fatal("no spoofing labels")
	}
	executed := 0
	for _, l := range labels {
		ix := index(tp.events[l.GetSymbol()])
		checkLabelMatchesStream(t, l, ix)
		orders := l.GetOrderIds()
		genuine := ix.accepted[orders[0]]
		var layerQty int64
		for _, id := range orders[1:] {
			layer := ix.accepted[id]
			if layer.Side == genuine.Side {
				t.Fatalf("%s: layer %d is on the genuine order's side", l.GetLabelId(), id)
			}
			if layer.Qty <= genuine.Qty {
				t.Fatalf("%s: layer %d (%d) is not larger than the genuine order (%d)", l.GetLabelId(), id, layer.Qty, genuine.Qty)
			}
			layerQty += layer.Qty
			// A layer is pulled, unless the market filled it first (the risk a
			// spoofer runs).
			_, cancelled := ix.cancels[id]
			filled := false
			for _, tid := range l.GetTradeIds() {
				tr := ix.trades[tid]
				filled = filled || tr.BuyOrderID == id || tr.SellOrderID == id
			}
			if !cancelled && !filled && l.GetParams()["outcome"] != "session_closed" {
				t.Fatalf("%s: layer %d was neither cancelled nor filled", l.GetLabelId(), id)
			}
		}
		if l.GetParams()["outcome"] != "executed" {
			continue
		}
		executed++
		// The layers are pulled shortly after the genuine order executes.
		var firstFill int64
		for _, e := range ix.all {
			if e.Kind == ob.EventTrade && (e.BuyOrderID == genuine.OrderID || e.SellOrderID == genuine.OrderID) {
				firstFill = e.Time
				break
			}
		}
		for _, id := range orders[1:] {
			c, ok := ix.cancels[id]
			if !ok {
				continue
			}
			if lag := time.Duration(c.Time - firstFill); lag < 0 || lag > time.Second {
				t.Fatalf("%s: layer %d cancelled %v after the genuine fill", l.GetLabelId(), id, lag)
			}
		}
	}
	if executed*2 < len(labels) {
		t.Errorf("only %d of %d spoofing episodes got their genuine order executed", executed, len(labels))
	}
}

func TestMomentumIgnitionEpisodes(t *testing.T) {
	_, tp := runTape(t, injected(14, time.Hour, Injection{MomentumIgnition: 3}), nil)
	labels := tp.byScenario(ScenarioMomentum)
	if len(labels) == 0 {
		t.Fatal("no momentum labels")
	}
	moved := 0
	for _, l := range labels {
		ix := index(tp.events[l.GetSymbol()])
		checkLabelMatchesStream(t, l, ix)
		acct := l.GetAccountIds()[0]
		var bought, sold int64
		for _, id := range l.GetTradeIds() {
			tr := ix.trades[id]
			if tr.BuyAccount == acct {
				bought += tr.Qty
			}
			if tr.SellAccount == acct {
				sold += tr.Qty
			}
		}
		// Ignition in one direction, then trading out the other way.
		if bought == 0 || sold == 0 {
			t.Fatalf("%s: traded only one way (bought %d, sold %d)", l.GetLabelId(), bought, sold)
		}
		if bps, _ := strconv.Atoi(l.GetParams()["peak_move_bps"]); bps >= 30 {
			moved++
		}
	}
	if moved*2 < len(labels) {
		t.Errorf("only %d of %d ignitions moved the price by 30 bps or more", moved, len(labels))
	}
}

func TestQuoteStuffingEpisodes(t *testing.T) {
	_, tp := runTape(t, injected(15, time.Hour, Injection{QuoteStuffing: 4}), nil)
	labels := tp.byScenario(ScenarioQuoteStuffing)
	if len(labels) == 0 {
		t.Fatal("no quote stuffing labels")
	}
	for _, l := range labels {
		ix := index(tp.events[l.GetSymbol()])
		checkLabelMatchesStream(t, l, ix)
		acct := l.GetAccountIds()[0]
		var msgs int
		for _, e := range ix.all {
			if e.Time < l.GetStartTimeNs() || e.Time > l.GetEndTimeNs() || e.Account != acct {
				continue
			}
			if e.Kind == ob.EventAccepted || e.Kind == ob.EventCancelled {
				msgs++
			}
		}
		secs := float64(l.GetEndTimeNs()-l.GetStartTimeNs()) / 1e9
		if rate := float64(msgs) / secs; rate < 100 {
			t.Fatalf("%s: %.0f messages/s is not a burst", l.GetLabelId(), rate)
		}
		if len(l.GetTradeIds()) != 0 {
			t.Fatalf("%s: stuffing orders traded", l.GetLabelId())
		}
	}
}

func TestMarkingTheCloseEpisodes(t *testing.T) {
	s, tp := runTape(t, injected(16, time.Hour, Injection{MarkingTheClose: 1}), nil)
	labels := tp.byScenario(ScenarioMarkingTheClose)
	if len(labels) < len(s.Symbols())/2 {
		t.Fatalf("%d marking-the-close labels for %d symbols at probability 1", len(labels), len(s.Symbols()))
	}
	sess := s.Session()
	for _, l := range labels {
		ix := index(tp.events[l.GetSymbol()])
		checkLabelMatchesStream(t, l, ix)
		if l.GetStartTimeNs() < sess.ClosingStart || l.GetEndTimeNs() > sess.Close {
			t.Fatalf("%s: activity outside the closing phase", l.GetLabelId())
		}
		acct := l.GetAccountIds()[0]
		side := ""
		for _, id := range l.GetOrderIds() {
			o := ix.accepted[id]
			if side != "" && o.Side.String() != side {
				t.Fatalf("%s: marking the close trades one way only", l.GetLabelId())
			}
			side = o.Side.String()
			if o.TIF != ob.IOC || o.Account != acct {
				t.Fatalf("%s: orders must be aggressive and from the marker", l.GetLabelId())
			}
		}
	}
}

func TestContinuousAbuseIsNotPlannedInTheClosingPhase(t *testing.T) {
	s, tp := runTape(t, injected(17, time.Hour, Injection{WashTrading: 4, Spoofing: 4, MomentumIgnition: 4, QuoteStuffing: 4}), nil)
	for _, l := range tp.labels {
		// Episodes may run into the closing phase, but never start in it.
		if l.GetStartTimeNs() >= s.Session().ClosingStart {
			t.Fatalf("%s (%s) started inside the closing phase", l.GetLabelId(), l.GetScenario())
		}
	}
}

func TestTriggerRunsAScenarioOnDemand(t *testing.T) {
	cfg := testConfig(18, 20*time.Minute)
	var triggeredAt int64
	_, tp := runTape(t, cfg, func(s *Sim, ev *ob.Event) {
		if triggeredAt == 0 && ev.Time > s.Session().Open+int64(5*time.Minute) {
			triggeredAt = ev.Time
			if err := s.Do(func(s *Sim) {
				if err := s.Trigger(ScenarioQuoteStuffing, "QZAP"); err != nil {
					t.Error(err)
				}
			}); err != nil {
				t.Error(err)
			}
		}
	})
	labels := tp.byScenario(ScenarioQuoteStuffing)
	if len(labels) != 1 || labels[0].GetSymbol() != "QZAP" {
		t.Fatalf("triggered labels = %v", labels)
	}
	if lag := labels[0].GetStartTimeNs() - triggeredAt; lag < 0 || lag > int64(time.Second) {
		t.Errorf("scenario started %v after the trigger", time.Duration(lag))
	}
	s, _ := New(cfg, DefaultUniverse(), &tape{})
	if err := s.Trigger("pump.everything", ""); err == nil {
		t.Error("unknown scenario accepted")
	}
	if err := s.Trigger(ScenarioSpoofing, "NOPE"); err == nil {
		t.Error("unknown symbol accepted")
	}
}

func TestInjectionValidation(t *testing.T) {
	for _, in := range []Injection{{WashTrading: -1}, {Spoofing: -0.1}, {MarkingTheClose: 1.5}} {
		if err := in.Validate(); err == nil {
			t.Errorf("%+v accepted", in)
		}
	}
	if err := (Injection{WashTrading: 1, MarkingTheClose: 1}).Validate(); err != nil {
		t.Error(err)
	}
}
