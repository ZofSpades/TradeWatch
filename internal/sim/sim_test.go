package sim

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
)

// collector records every emitted event and hashes its protobuf encoding.
type collector struct {
	session string
	hash    [32]byte
	n       int
	bySym   map[string]*symStats
	last    map[string]ob.Event
}

type symStats struct {
	events, orders, replaces, cancels, trades, snapshots int
	volume                                               int64
	spreads                                              []int64 // ticks, from snapshots
	crossed                                              int
	lastSeq                                              uint64
	gaps                                                 int
	accounts                                             map[string]bool
}

func newCollector(session string) *collector {
	return &collector{session: session, bySym: map[string]*symStats{}, last: map[string]ob.Event{}}
}

func (c *collector) Emit(sym *Symbol, ev *ob.Event) error {
	b, err := events.Marshal(ToProto(c.session, sym, ev, 0))
	if err != nil {
		return err
	}
	h := sha256.New()
	h.Write(c.hash[:])
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(len(b)))
	h.Write(n[:])
	h.Write(b)
	copy(c.hash[:], h.Sum(nil))
	c.n++

	st, ok := c.bySym[sym.Symbol]
	if !ok {
		st = &symStats{accounts: map[string]bool{}}
		c.bySym[sym.Symbol] = st
	}
	st.events++
	if ev.Seq != st.lastSeq+1 {
		st.gaps++
	}
	st.lastSeq = ev.Seq
	switch ev.Kind {
	case ob.EventAccepted:
		st.orders++
		st.accounts[ev.Account] = true
	case ob.EventReplaced:
		st.replaces++
	case ob.EventCancelled:
		st.cancels++
	case ob.EventTrade:
		st.trades++
		st.volume += ev.Qty
	case ob.EventSnapshot:
		st.snapshots++
		if len(ev.Bids) > 0 && len(ev.Asks) > 0 {
			spread := ev.Asks[0].Price - ev.Bids[0].Price
			if spread <= 0 {
				st.crossed++
			}
			st.spreads = append(st.spreads, spread/sym.TickSize)
		}
	}
	c.last[sym.Symbol] = *ev
	return nil
}

func runSession(t *testing.T, cfg Config) (*Sim, *collector) {
	t.Helper()
	c := newCollector(cfg.SessionID)
	s, err := New(cfg, DefaultUniverse(), c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, c
}

func testConfig(seed uint64, d time.Duration) Config {
	cfg := DefaultConfig()
	cfg.Seed = seed
	cfg.SessionID = "test"
	cfg.Duration = d
	cfg.ClosingWindow = d / 10
	return cfg
}

func median(xs []int64) int64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]int64(nil), xs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

func TestSessionsAreReproducible(t *testing.T) {
	_, a := runSession(t, testConfig(7, 20*time.Minute))
	_, b := runSession(t, testConfig(7, 20*time.Minute))
	if a.hash != b.hash || a.n != b.n {
		t.Fatalf("same seed produced different streams: %d vs %d events", a.n, b.n)
	}
	_, c := runSession(t, testConfig(8, 20*time.Minute))
	if a.hash == c.hash {
		t.Fatal("different seeds produced identical streams")
	}
}

func TestSessionShape(t *testing.T) {
	s, c := runSession(t, testConfig(3, time.Hour))
	if len(c.bySym) != len(s.Symbols()) {
		t.Fatalf("events for %d of %d symbols", len(c.bySym), len(s.Symbols()))
	}
	for _, sym := range s.Symbols() {
		st := c.bySym[sym.Symbol]
		t.Logf("%s %-6v events=%6d orders=%5d replaces=%5d cancels=%5d trades=%5d volume=%8d accounts=%3d medianSpread=%d ticks",
			sym.Symbol, sym.Tier, st.events, st.orders, st.replaces, st.cancels, st.trades, st.volume, len(st.accounts), median(st.spreads))
		if st.gaps != 0 {
			t.Errorf("%s: %d sequence gaps", sym.Symbol, st.gaps)
		}
		if st.crossed != 0 {
			t.Errorf("%s: %d crossed snapshots", sym.Symbol, st.crossed)
		}
		if st.trades < 200 {
			t.Errorf("%s: only %d trades in an hour", sym.Symbol, st.trades)
		}
		if sp := median(st.spreads); sp < 1 || sp > 12 {
			t.Errorf("%s: median spread %d ticks is implausible", sym.Symbol, sp)
		}
		last := c.last[sym.Symbol]
		if last.Kind != ob.EventSnapshot || sym.Phase != twpb.TradingPhase_TRADING_PHASE_CLOSED {
			t.Errorf("%s: session must end with a CLOSED snapshot, got %v in phase %v", sym.Symbol, last.Kind, sym.Phase)
		}
		if sym.Book.OpenOrders() != 0 {
			t.Errorf("%s: %d orders still resting after the close", sym.Symbol, sym.Book.OpenOrders())
		}
	}
	// More liquid tiers must be busier.
	hi, lo := c.bySym["QZAP"], c.bySym["TALX"]
	if hi.trades <= lo.trades || hi.events <= lo.events {
		t.Errorf("high-liquidity symbol is not busier than a low-liquidity one: %d/%d trades", hi.trades, lo.trades)
	}
	st := s.Stats()
	t.Logf("session: %d events, %d orders (%d rejected), %d trades, %d replaces, %d cancels, %d snapshots",
		st.Events, st.Orders, st.Rejected, st.Trades, st.Replaces, st.Cancels, st.Snapshots)
	if st.Rejected > st.Orders/100 {
		t.Errorf("%d of %d orders rejected; participants send invalid orders", st.Rejected, st.Orders)
	}
}

func TestParticipantsBehave(t *testing.T) {
	s, _ := runSession(t, testConfig(11, 2*time.Hour))
	var parents, completed int
	for _, a := range s.agents {
		switch ag := a.(type) {
		case *marketMaker:
			if abs64(ag.inventory) > 2*ag.maxInv {
				t.Errorf("market maker %s on %s ran inventory to %d (limit %d)", ag.acct.ID, ag.sym.Symbol, ag.inventory, ag.maxInv)
			}
		case *parentOrder:
			if ag.end > s.session.Close {
				t.Errorf("parent order ends after the close")
			}
			parents++
			if float64(ag.filled) >= 0.9*float64(ag.total) {
				completed++
			}
			if ag.filled > ag.total {
				t.Errorf("parent order overfilled: %d of %d", ag.filled, ag.total)
			}
		}
	}
	if parents == 0 || float64(completed) < 0.8*float64(parents) {
		t.Errorf("only %d of %d institutional parents reached 90%% of their size", completed, parents)
	}
}

func TestConfigValidation(t *testing.T) {
	bad := []func(*Config){
		func(c *Config) { c.SessionID = "" },
		func(c *Config) { c.Duration = 0 },
		func(c *Config) { c.ClosingWindow = c.Duration },
		func(c *Config) { c.SnapshotInterval = 0 },
		func(c *Config) { c.SnapshotDepth = 0 },
		func(c *Config) { c.Activity = 0 },
		func(c *Config) { c.Symbols = []string{"NOPE"} },
	}
	for i, mutate := range bad {
		cfg := DefaultConfig()
		mutate(&cfg)
		if _, err := New(cfg, DefaultUniverse(), newCollector("x")); err == nil {
			t.Errorf("case %d: invalid config accepted", i)
		}
	}
}

func TestUniverseIsStable(t *testing.T) {
	a, b := DefaultUniverse(), DefaultUniverse()
	if len(a.Accounts) != len(b.Accounts) {
		t.Fatal("universe size differs between builds")
	}
	ids := map[string]bool{}
	for i := range a.Accounts {
		if *a.Accounts[i] != *b.Accounts[i] {
			t.Fatalf("account %d differs between builds", i)
		}
		if ids[a.Accounts[i].ID] {
			t.Fatalf("duplicate account ID %s", a.Accounts[i].ID)
		}
		ids[a.Accounts[i].ID] = true
	}
	// Desks of one institutional firm share a beneficial owner.
	insts := a.ByRole(RoleInstitutional)
	if insts[0].BeneficialOwner != insts[1].BeneficialOwner || insts[0].ID == insts[1].ID {
		t.Error("institutional desks must share a beneficial owner")
	}
	if got := len(a.RefData()); got != len(a.Accounts)+len(a.Instruments) {
		t.Errorf("refdata has %d records", got)
	}
}

func TestVolumeCurve(t *testing.T) {
	if volumeCurve(0) != 0 || volumeCurve(1) < 0.999999 || volumeCurve(1) > 1.000001 {
		t.Fatalf("curve endpoints: %v %v", volumeCurve(0), volumeCurve(1))
	}
	// U-shape: the first and last tenths trade more than the middle tenth.
	edge := volumeCurve(0.1) - volumeCurve(0)
	mid := volumeCurve(0.55) - volumeCurve(0.45)
	if edge <= mid {
		t.Errorf("volume curve is not U-shaped: edge %.3f mid %.3f", edge, mid)
	}
}
