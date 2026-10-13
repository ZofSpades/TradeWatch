package detect

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// repoRules is the rules directory shipped with the repository.
const repoRules = "../../rules"

// loadRules loads the repository rules, applying textual edits
// (file -> old -> new) to a temporary copy first.
func loadRules(t testing.TB, edits ...[3]string) *rules.Set {
	t.Helper()
	dir := repoRules
	if len(edits) > 0 {
		dir = t.TempDir()
		entries, err := os.ReadDir(repoRules)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			b, err := os.ReadFile(filepath.Join(repoRules, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			for _, ed := range edits {
				if ed[0] == e.Name() {
					if !strings.Contains(string(b), ed[1]) {
						t.Fatalf("%s does not contain %q", ed[0], ed[1])
					}
					b = []byte(strings.Replace(string(b), ed[1], ed[2], 1))
				}
			}
			if err := os.WriteFile(filepath.Join(dir, e.Name()), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	set, err := rules.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// onlyRules disables every rule except the given IDs.
func onlyRules(t testing.TB, set *rules.Set, ids ...string) *rules.Set {
	t.Helper()
	keep := map[string]bool{}
	for _, id := range ids {
		keep[id] = true
	}
	out := *set
	out.Rules = nil
	for _, r := range set.Rules {
		cp := *r
		cp.Enabled = keep[r.ID]
		out.Rules = append(out.Rules, &cp)
	}
	return &out
}

const (
	buy  = twpb.Side_SIDE_BUY
	sell = twpb.Side_SIDE_SELL
)

// stream builds a crafted event sequence for one symbol and feeds it to an
// engine.
type stream struct {
	t       *testing.T
	eng     *Engine
	session string
	symbol  string
	seq     uint64
	now     int64
	owners  map[string]string
	order   uint64
	trade   uint64
	alerts  []*twpb.Alert
}

func newStream(t *testing.T, set *rules.Set) *stream {
	t.Helper()
	eng, err := NewEngine(set, Config{Instance: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return &stream{
		t: t, eng: eng, session: "S1", symbol: "QZAP",
		now: time.Date(2026, 1, 5, 14, 30, 0, 0, time.UTC).UnixNano(), owners: map[string]string{},
	}
}

func (s *stream) owner(account, owner string) { s.owners[account] = owner }

func (s *stream) advance(d time.Duration) { s.now += int64(d) }

func (s *stream) header() *twpb.EventHeader {
	s.seq++
	return &twpb.EventHeader{SessionId: s.session, Symbol: s.symbol, Seq: s.seq, EventTimeNs: s.now}
}

func (s *stream) ref(account string) *twpb.AccountRef {
	o := s.owners[account]
	if o == "" {
		o = "BO-" + account
	}
	return &twpb.AccountRef{AccountId: account, BeneficialOwnerId: o}
}

func (s *stream) process(ev *twpb.MarketEvent) []*twpb.Alert {
	s.t.Helper()
	out := s.eng.Process(ev, int64(s.seq))
	got := make([]*twpb.Alert, len(out))
	copy(got, out)
	s.alerts = append(s.alerts, got...)
	return got
}

// levels builds price levels from (price, qty) pairs.
func levels(pq ...int64) []*twpb.PriceLevel {
	var out []*twpb.PriceLevel
	for i := 0; i+1 < len(pq); i += 2 {
		out = append(out, &twpb.PriceLevel{Price: pq[i], Quantity: pq[i+1], OrderCount: 1})
	}
	return out
}

func (s *stream) book(phase twpb.TradingPhase, bids, asks []*twpb.PriceLevel) {
	s.process(&twpb.MarketEvent{Body: &twpb.MarketEvent_Book{Book: &twpb.BookSnapshot{
		Header: s.header(), Phase: phase, Bids: bids, Asks: asks,
	}}})
}

// continuous publishes a snapshot with a simple two-sided book around mid.
func (s *stream) continuous(bid, ask, qty int64) {
	s.book(twpb.TradingPhase_TRADING_PHASE_CONTINUOUS,
		levels(bid, qty, bid-100, qty, bid-200, qty, bid-300, qty, bid-400, qty),
		levels(ask, qty, ask+100, qty, ask+200, qty, ask+300, qty, ask+400, qty))
}

func (s *stream) newOrder(account string, side twpb.Side, tif twpb.TimeInForce, price, qty int64) uint64 {
	s.order++
	typ := twpb.OrderType_ORDER_TYPE_LIMIT
	if price == 0 {
		typ = twpb.OrderType_ORDER_TYPE_MARKET
	}
	s.process(&twpb.MarketEvent{
		Body: &twpb.MarketEvent_Order{Order: &twpb.OrderEvent{
			Header: s.header(), Action: twpb.OrderAction_ORDER_ACTION_NEW, OrderId: s.order, AccountId: account,
			Side: side, Type: typ, TimeInForce: tif, Price: price, Quantity: qty,
		}},
		Enrichment: &twpb.Enrichment{Account: s.ref(account), TickSize: 100},
	})
	return s.order
}

func (s *stream) limit(account string, side twpb.Side, price, qty int64) uint64 {
	return s.newOrder(account, side, twpb.TimeInForce_TIME_IN_FORCE_DAY, price, qty)
}

func (s *stream) ioc(account string, side twpb.Side, price, qty int64) uint64 {
	return s.newOrder(account, side, twpb.TimeInForce_TIME_IN_FORCE_IOC, price, qty)
}

func (s *stream) cancel(account string, id uint64, side twpb.Side, price, qty, entry int64) {
	s.process(&twpb.MarketEvent{
		Body: &twpb.MarketEvent_Cancel{Cancel: &twpb.CancelEvent{
			Header: s.header(), OrderId: id, AccountId: account, Side: side, Price: price,
			CancelledQuantity: qty, Reason: twpb.CancelReason_CANCEL_REASON_USER, OrderEntryTimeNs: entry,
		}},
		Enrichment: &twpb.Enrichment{Account: s.ref(account), TickSize: 100},
	})
}

// fill reports a trade between a resting order and an aggressor.
func (s *stream) fill(aggressor twpb.Side, buyAcct string, buyID uint64, sellAcct string, sellID uint64, price, qty, buyLeaves, sellLeaves int64) uint64 {
	s.trade++
	s.process(&twpb.MarketEvent{
		Body: &twpb.MarketEvent_Trade{Trade: &twpb.TradeEvent{
			Header: s.header(), TradeId: s.trade, Price: price, Quantity: qty, AggressorSide: aggressor,
			BuyOrderId: buyID, SellOrderId: sellID, BuyAccountId: buyAcct, SellAccountId: sellAcct,
			BuyLeavesQuantity: buyLeaves, SellLeavesQuantity: sellLeaves,
		}},
		Enrichment: &twpb.Enrichment{BuyAccount: s.ref(buyAcct), SellAccount: s.ref(sellAcct), TickSize: 100},
	})
	return s.trade
}

// cross sends an aggressive IOC against a resting order and the resulting
// full fill.
func (s *stream) cross(account string, side twpb.Side, restingAcct string, restingID uint64, price, qty int64) uint64 {
	id := s.ioc(account, side, price, qty)
	if side == buy {
		return s.fill(buy, account, id, restingAcct, restingID, price, qty, 0, 0)
	}
	return s.fill(sell, restingAcct, restingID, account, id, price, qty, 0, 0)
}

func (s *stream) alertsOf(ruleID string) []*twpb.Alert {
	var out []*twpb.Alert
	for _, a := range s.alerts {
		if a.GetRuleId() == ruleID {
			out = append(out, a)
		}
	}
	return out
}

func feature(a *twpb.Alert, name string) (*twpb.Feature, bool) {
	for _, f := range a.GetEvidence().GetFeatures() {
		if f.GetName() == name {
			return f, true
		}
	}
	return nil, false
}

// Wrap is used by tests to check alerts are well formed.
func checkWellFormed(t *testing.T, a *twpb.Alert) {
	t.Helper()
	switch {
	case len(a.GetAlertKey()) != 32, a.GetRuleVersion() == 0, a.GetRuleDigest() == "", a.GetTaxonomyCode() == "":
		t.Fatalf("alert missing identity fields: %v", a)
	case a.GetExplanation() == "" || strings.Contains(a.GetExplanation(), "could not be rendered"):
		t.Fatalf("alert explanation: %q", a.GetExplanation())
	case len(a.GetAccountIds()) == 0 || a.GetScore() < 50 || a.GetScore() > 100:
		t.Fatalf("alert accounts/score: %v %v", a.GetAccountIds(), a.GetScore())
	case a.GetWindowStartNs() > a.GetWindowEndNs() || a.GetDetectedAtNs() < a.GetWindowStartNs():
		t.Fatalf("alert window: %d..%d detected %d", a.GetWindowStartNs(), a.GetWindowEndNs(), a.GetDetectedAtNs())
	}
	for _, f := range a.GetEvidence().GetFeatures() {
		if f.GetOp() != "" && !f.GetBreached() {
			t.Fatalf("alert reports feature %s=%v that did not breach %s %v", f.GetName(), f.GetValue(), f.GetOp(), f.GetThreshold())
		}
	}
	if events.SchemaName(a) != events.SchemaAlert {
		t.Fatal("not an alert")
	}
}
