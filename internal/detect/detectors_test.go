package detect

import (
	"strings"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

const px = 1_000_000 // 100.00 in price units

// --- Wash trading ------------------------------------------------------

func washStream(t *testing.T) *stream {
	s := newStream(t, onlyRules(t, loadRules(t), "TW-WASH-001"))
	s.owner("A1", "BO-X")
	s.owner("A2", "BO-X")
	// A spread of four ticks leaves room inside it.
	s.continuous(px-200, px+200, 500)
	return s
}

func TestWashDirectPrearrangedTrade(t *testing.T) {
	s := washStream(t)
	passive := s.limit("A1", sell, px, 300) // inside the spread
	s.advance(15 * time.Millisecond)
	s.cross("A2", buy, "A1", passive, px, 300)

	alerts := s.alertsOf("TW-WASH-001")
	if len(alerts) != 1 {
		t.Fatalf("got %d alerts, want 1", len(alerts))
	}
	a := alerts[0]
	checkWellFormed(t, a)
	if a.GetAbuseType() != twpb.AbuseType_ABUSE_TYPE_WASH_TRADING || len(a.GetAccountIds()) != 2 || a.GetBeneficialOwnerIds()[0] != "BO-X" {
		t.Fatalf("alert = %v", a)
	}
	if gap, _ := feature(a, "entry_gap_ms"); gap.GetValue() != 15 {
		t.Errorf("entry gap = %v ms, want 15", gap.GetValue())
	}
	if inside, _ := feature(a, "inside_spread"); inside.GetValue() != 1 {
		t.Error("the resting leg was inside the spread")
	}
}

func TestWashIncidentalSameOwnerCrossIsNotAlerted(t *testing.T) {
	s := washStream(t)
	// Two desks of one firm meet by accident: the resting order has been
	// in the book for half a minute and the sizes differ.
	passive := s.limit("A1", sell, px+100, 1_000)
	s.advance(30 * time.Second)
	id := s.ioc("A2", buy, px+100, 400)
	s.fill(buy, "A2", id, "A1", passive, px+100, 400, 0, 600)
	if n := len(s.alertsOf("TW-WASH-001")); n != 0 {
		t.Fatalf("incidental cross alerted %d times", n)
	}
}

func TestWashRepeatedSameOwnerTradesAreAlerted(t *testing.T) {
	s := washStream(t)
	for i := 0; i < 3; i++ {
		passive := s.limit("A1", sell, px+100, 1_000)
		s.advance(20 * time.Second)
		id := s.ioc("A2", buy, px+100, 250)
		s.fill(buy, "A2", id, "A1", passive, px+100, 250, 0, 750)
		s.advance(time.Minute)
	}
	alerts := s.alertsOf("TW-WASH-001")
	if len(alerts) != 1 {
		t.Fatalf("got %d alerts, want 1 after the third same-owner trade", len(alerts))
	}
	if f, _ := feature(alerts[0], "same_owner_trades"); f.GetValue() != 3 || f.GetThreshold() != 3 {
		t.Errorf("same_owner_trades = %v (threshold %v)", f.GetValue(), f.GetThreshold())
	}
}

func TestWashSuppressesRepeatsOfOneEpisode(t *testing.T) {
	s := washStream(t)
	for i := 0; i < 4; i++ {
		passive := s.limit("A1", sell, px, 300)
		s.advance(10 * time.Millisecond)
		s.cross("A2", buy, "A1", passive, px, 300)
		s.advance(20 * time.Second)
	}
	if n := len(s.alertsOf("TW-WASH-001")); n != 1 {
		t.Fatalf("one episode produced %d alerts", n)
	}
}

func ringLeg(s *stream, seller, buyer string, qty int64) {
	passive := s.limit(seller, sell, px, qty)
	s.advance(20 * time.Millisecond)
	s.cross(buyer, buy, seller, passive, px, qty)
	s.advance(8 * time.Second)
}

func TestWashRingAcrossDifferentOwners(t *testing.T) {
	s := washStream(t)
	s.owner("R1", "BO-1")
	s.owner("R2", "BO-2")
	s.owner("R3", "BO-3")
	ringLeg(s, "R1", "R2", 400)
	ringLeg(s, "R2", "R3", 400)
	if n := len(s.alertsOf("TW-WASH-001")); n != 0 {
		t.Fatalf("an open chain alerted %d times", n)
	}
	ringLeg(s, "R3", "R1", 400)
	alerts := s.alertsOf("TW-WASH-001")
	if len(alerts) != 1 {
		t.Fatalf("closing the ring produced %d alerts", len(alerts))
	}
	a := alerts[0]
	checkWellFormed(t, a)
	if len(a.GetAccountIds()) != 3 || len(a.GetBeneficialOwnerIds()) != 3 || len(a.GetEvidence().GetTradeIds()) != 3 {
		t.Fatalf("ring alert = %v", a)
	}
	if l, _ := feature(a, "ring_length"); l.GetValue() != 3 {
		t.Errorf("ring length %v", l.GetValue())
	}
}

func TestWashUnbalancedRingIsNotAlerted(t *testing.T) {
	s := washStream(t)
	s.owner("R1", "BO-1")
	s.owner("R2", "BO-2")
	s.owner("R3", "BO-3")
	ringLeg(s, "R1", "R2", 1_000)
	ringLeg(s, "R2", "R3", 1_000)
	ringLeg(s, "R3", "R1", 200) // a fifth of the other legs
	if n := len(s.alertsOf("TW-WASH-001")); n != 0 {
		t.Fatalf("unbalanced cycle alerted %d times", n)
	}
}

// --- Spoofing ----------------------------------------------------------

func spoofStream(t *testing.T) *stream {
	s := newStream(t, onlyRules(t, loadRules(t), "TW-SPOOF-001"))
	s.continuous(px-100, px, 300)
	return s
}

// layer places n buy orders of qty each behind the bid and returns them.
func layer(s *stream, account string, n int, qty int64) []uint64 {
	var ids []uint64
	for i := 1; i <= n; i++ {
		ids = append(ids, s.limit(account, buy, px-100-int64(i)*100, qty))
	}
	return ids
}

func pull(s *stream, account string, ids []uint64, qty, entry int64) {
	for i, id := range ids {
		s.cancel(account, id, buy, px-100-int64(i+1)*100, qty, entry)
	}
}

func TestSpoofingLayersPulledAfterOppositeFill(t *testing.T) {
	s := spoofStream(t)
	genuine := s.limit("SPF", sell, px, 200)
	s.advance(200 * time.Millisecond)
	entry := s.now
	ids := layer(s, "SPF", 3, 3_000) // ten times the displayed level size
	s.advance(4 * time.Second)
	s.cross("NOISE", buy, "SPF", genuine, px, 200)
	s.advance(250 * time.Millisecond)
	pull(s, "SPF", ids, 3_000, entry)
	s.advance(time.Second)
	s.continuous(px-100, px, 300) // the group is evaluated once its window passes

	alerts := s.alertsOf("TW-SPOOF-001")
	if len(alerts) != 1 {
		t.Fatalf("got %d spoofing alerts, want 1", len(alerts))
	}
	a := alerts[0]
	checkWellFormed(t, a)
	if f, _ := feature(a, "layers"); f.GetValue() != 3 {
		t.Errorf("layers = %v", f.GetValue())
	}
	if f, _ := feature(a, "cancel_after_fill_ms"); f.GetValue() != 250 {
		t.Errorf("cancel after fill = %v ms", f.GetValue())
	}
	if f, _ := feature(a, "size_vs_depth"); f.GetValue() < 9.9 {
		t.Errorf("size vs depth = %v", f.GetValue())
	}
	if len(a.GetEvidence().GetTradeIds()) != 1 || len(a.GetEvidence().GetOrderIds()) != 4 {
		t.Errorf("evidence: trades %v orders %v", a.GetEvidence().GetTradeIds(), a.GetEvidence().GetOrderIds())
	}
}

func TestSpoofingNeedsAnOppositeFill(t *testing.T) {
	s := spoofStream(t)
	entry := s.now
	ids := layer(s, "BIG", 3, 3_000)
	s.advance(10 * time.Second)
	pull(s, "BIG", ids, 3_000, entry) // a large order pulled with no execution on the other side
	s.advance(time.Second)
	s.continuous(px-100, px, 300)
	if n := len(s.alertsOf("TW-SPOOF-001")); n != 0 {
		t.Fatalf("cancels without an opposite fill alerted %d times", n)
	}
}

func TestSpoofingIgnoresSlowCancels(t *testing.T) {
	s := spoofStream(t)
	genuine := s.limit("SPF", sell, px, 200)
	entry := s.now
	ids := layer(s, "SPF", 3, 3_000)
	s.cross("NOISE", buy, "SPF", genuine, px, 200)
	s.advance(5 * time.Second) // well past max_cancel_after_fill
	pull(s, "SPF", ids, 3_000, entry)
	s.advance(time.Second)
	s.continuous(px-100, px, 300)
	if n := len(s.alertsOf("TW-SPOOF-001")); n != 0 {
		t.Fatalf("slow cancels alerted %d times", n)
	}
}

func TestSpoofingIgnoresOrdersSizedLikeTheBook(t *testing.T) {
	s := spoofStream(t)
	genuine := s.limit("MM", sell, px, 200)
	entry := s.now
	ids := layer(s, "MM", 3, 300) // the size of the displayed levels
	s.cross("NOISE", buy, "MM", genuine, px, 200)
	s.advance(100 * time.Millisecond)
	pull(s, "MM", ids, 300, entry)
	s.advance(time.Second)
	s.continuous(px-100, px, 300)
	if n := len(s.alertsOf("TW-SPOOF-001")); n != 0 {
		t.Fatalf("market-maker-sized cancels alerted %d times", n)
	}
}

// --- Momentum ignition -------------------------------------------------

// quietHistory publishes snapshots for long enough to estimate volatility,
// with the mid oscillating by one tick.
func quietHistory(s *stream, d time.Duration) {
	for i := 0; i < int(d/time.Second); i++ {
		bump := int64(i%2) * 100
		s.continuous(px-100+bump, px+100+bump, 500)
		s.advance(time.Second)
	}
}

func TestMomentumIgnitionIsAlerted(t *testing.T) {
	s := newStream(t, onlyRules(t, loadRules(t), "TW-MOMO-001"))
	quietHistory(s, 10*time.Minute)
	seed := s.limit("MM", sell, px+100, 500)
	s.cross("N1", buy, "MM", seed, px+100, 100) // establishes the last price
	// Five aggressive buys, each 25 bps higher.
	for i := int64(1); i <= 5; i++ {
		p := px + 100 + i*2_500
		r := s.limit("MM", sell, p, 1_000)
		s.cross("IGN", buy, "MM", r, p, 1_000)
		s.advance(time.Second)
	}
	if n := len(s.alertsOf("TW-MOMO-001")); n != 0 {
		t.Fatal("alerted before any reversal")
	}
	s.advance(20 * time.Second)
	for i := int64(0); i < 3; i++ {
		p := px + 10_000 - i*2_500
		r := s.limit("MM", buy, p, 1_000)
		s.cross("IGN", sell, "MM", r, p, 1_000)
		s.advance(time.Second)
	}
	alerts := s.alertsOf("TW-MOMO-001")
	if len(alerts) != 1 {
		t.Fatalf("got %d momentum alerts, want 1", len(alerts))
	}
	a := alerts[0]
	checkWellFormed(t, a)
	if f, _ := feature(a, "move_bps"); f.GetValue() < 100 {
		t.Errorf("move = %v bps", f.GetValue())
	}
	// The seed trade that set the last price is the only other aggressive buy.
	if f, _ := feature(a, "aggressive_share"); f.GetValue() < 0.95 {
		t.Errorf("share = %v", f.GetValue())
	}
}

func TestMomentumFollowerIsNotAlerted(t *testing.T) {
	s := newStream(t, onlyRules(t, loadRules(t), "TW-MOMO-001"))
	quietHistory(s, 10*time.Minute)
	// The price jumps on news: market makers re-quote 100 bps higher and
	// trade among themselves.
	r := s.limit("MM1", sell, px+10_000, 500)
	s.cross("MM2", buy, "MM1", r, px+10_000, 100)
	s.continuous(px+9_900, px+10_100, 500)
	// A momentum trader then buys aggressively and exits later.
	for i := int64(0); i < 4; i++ {
		p := px + 10_100 + i*100
		r := s.limit("MM1", sell, p, 500)
		s.cross("FOLLOW", buy, "MM1", r, p, 500)
		s.advance(time.Second)
	}
	s.advance(time.Minute)
	for i := int64(0); i < 4; i++ {
		r := s.limit("MM1", buy, px+10_000, 500)
		s.cross("FOLLOW", sell, "MM1", r, px+10_000, 500)
	}
	if n := len(s.alertsOf("TW-MOMO-001")); n != 0 {
		t.Fatalf("a trader following the move alerted %d times", n)
	}
}

// --- Quote stuffing ----------------------------------------------------

// stuffingBurst runs ten minutes of normal flow followed by a burst of 150
// orders cancelled 3 ms after entry, and returns the stuffing alerts.
func stuffingBurst(t *testing.T, set *rules.Set) []*twpb.Alert {
	s := newStream(t, onlyRules(t, set, "TW-STUFF-001"))
	s.continuous(px-100, px+100, 500)
	// Ten minutes of normal flow: three accounts sending a few messages a
	// second.
	for i := 0; i < 600; i++ {
		for _, acct := range []string{"MM1", "MM2", "MM3"} {
			id := s.limit(acct, buy, px-200, 100)
			s.advance(50 * time.Millisecond)
			s.cancel(acct, id, buy, px-200, 100, s.now-int64(50*time.Millisecond))
		}
		s.advance(800 * time.Millisecond)
	}
	// A burst of 150 orders, each cancelled 3 ms later, inside one second.
	for i := 0; i < 150; i++ {
		id := s.limit("STUFF", buy, px-500, 100)
		s.advance(3 * time.Millisecond)
		s.cancel("STUFF", id, buy, px-500, 100, s.now-int64(3*time.Millisecond))
		s.advance(3 * time.Millisecond)
	}
	alerts := s.alertsOf("TW-STUFF-001")
	if len(alerts) != 1 || alerts[0].GetAccountIds()[0] != "STUFF" {
		t.Fatalf("alerts = %v", alerts)
	}
	checkWellFormed(t, alerts[0])
	return alerts
}

func TestQuoteStuffingBurstIsAlerted(t *testing.T) {
	a := stuffingBurst(t, loadRules(t))[0]
	if f, _ := feature(a, "median_lifetime_ms"); f.GetValue() != 3 {
		t.Errorf("median lifetime = %v ms", f.GetValue())
	}
	for _, want := range []string{"within 1 s,", "over the last 10 min ("} {
		if !strings.Contains(a.GetExplanation(), want) {
			t.Errorf("explanation %q does not contain %q", a.GetExplanation(), want)
		}
	}
}

// The rate multiple compares a per-second rate with the per-second
// baseline whatever the burst window, so the same messages counted over a
// window twice as long make half the multiple.
func TestQuoteStuffingRateIsPerSecond(t *testing.T) {
	one := stuffingBurst(t, loadRules(t))[0]
	two := stuffingBurst(t, loadRules(t, [3]string{"quote_stuffing.yaml", "burst_window: 1s", "burst_window: 2s"}))[0]
	m1, _ := feature(one, "rate_multiple")
	m2, _ := feature(two, "rate_multiple")
	n1, _ := feature(one, "messages")
	n2, _ := feature(two, "messages")
	if n1.GetValue() != n2.GetValue() || m2.GetValue() != m1.GetValue()/2 {
		t.Errorf("1 s window: %v messages, multiple %v; 2 s window: %v messages, multiple %v",
			n1.GetValue(), m1.GetValue(), n2.GetValue(), m2.GetValue())
	}
	if !strings.Contains(two.GetExplanation(), "within 2 s,") {
		t.Errorf("explanation %q does not name the 2 s window", two.GetExplanation())
	}
}

func TestHumanDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		500 * time.Millisecond: "500 ms", time.Second: "1 s", 90 * time.Second: "90 s",
		1500 * time.Millisecond: "1.5 s", 10 * time.Minute: "10 min", 90 * time.Minute: "90 min", 2 * time.Hour: "2 h",
	} {
		if got := humanDuration(d); got != want {
			t.Errorf("humanDuration(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestQuoteStuffingIgnoresBusyAccountsThatTrade(t *testing.T) {
	s := newStream(t, onlyRules(t, loadRules(t), "TW-STUFF-001"))
	s.continuous(px-100, px+100, 500)
	// An aggressive participant sending 120 orders in a second, every one
	// of which trades.
	for i := 0; i < 120; i++ {
		r := s.limit("MM", sell, px+100, 100)
		s.cross("HFT", buy, "MM", r, px+100, 100)
		s.advance(8 * time.Millisecond)
	}
	if n := len(s.alertsOf("TW-STUFF-001")); n != 0 {
		t.Fatalf("an account whose orders trade alerted %d times", n)
	}
}

// --- Marking the close -------------------------------------------------

func closingStream(t *testing.T) *stream {
	s := newStream(t, onlyRules(t, loadRules(t), "TW-CLOSE-001"))
	s.continuous(px-100, px+100, 500)
	return s
}

func TestMarkingTheCloseIsAlerted(t *testing.T) {
	s := closingStream(t)
	s.advance(time.Minute)
	s.book(twpb.TradingPhase_TRADING_PHASE_CLOSING, levels(px-100, 500), levels(px+100, 500))
	// Normal closing flow.
	r := s.limit("MM", sell, px+100, 1_000)
	s.cross("N1", buy, "MM", r, px+100, 200)
	s.advance(30 * time.Second)
	// Five aggressive buys from an account inactive all day, each 10 bps up.
	for i := int64(1); i <= 5; i++ {
		p := px + 100 + i*1_000
		r := s.limit("MM", sell, p, 600)
		s.cross("CLOSER", buy, "MM", r, p, 600)
		s.advance(5 * time.Second)
	}
	alerts := s.alertsOf("TW-CLOSE-001")
	if len(alerts) != 1 || alerts[0].GetAccountIds()[0] != "CLOSER" {
		t.Fatalf("alerts = %v", alerts)
	}
	checkWellFormed(t, alerts[0])
	if f, _ := feature(alerts[0], "concentration"); f.GetValue() != 1 {
		t.Errorf("concentration = %v", f.GetValue())
	}
	if !strings.Contains(alerts[0].GetExplanation(), "the 30 min before it") {
		t.Errorf("explanation %q does not name the lookback", alerts[0].GetExplanation())
	}
}

func TestExecutionAlgorithmFinishingAtTheCloseIsNotAlerted(t *testing.T) {
	s := closingStream(t)
	// An institutional desk traded steadily for the last hour.
	for i := 0; i < 60; i++ {
		r := s.limit("MM", sell, px+100, 1_000)
		s.cross("VWAP", buy, "MM", r, px+100, 500)
		s.advance(time.Minute)
	}
	s.book(twpb.TradingPhase_TRADING_PHASE_CLOSING, levels(px-100, 500), levels(px+100, 500))
	for i := int64(1); i <= 5; i++ {
		p := px + 100 + i*1_000
		r := s.limit("MM", sell, p, 600)
		s.cross("VWAP", buy, "MM", r, p, 600)
		s.advance(5 * time.Second)
	}
	s.book(twpb.TradingPhase_TRADING_PHASE_CLOSED, nil, nil)
	if n := len(s.alertsOf("TW-CLOSE-001")); n != 0 {
		t.Fatalf("an execution spread over the hour alerted %d times", n)
	}
}
