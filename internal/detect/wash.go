package detect

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// washParams are the parameters of the wash_trading detector.
type washParams struct {
	Window         rules.Duration `yaml:"window"`
	MaxEntryGap    rules.Duration `yaml:"max_entry_gap"`
	MinSizeMatch   float64        `yaml:"min_size_match"`
	MinRepeat      int            `yaml:"min_repeat"`
	MinRingLength  int            `yaml:"min_ring_length"`
	MaxRingLength  int            `yaml:"max_ring_length"`
	MinRingBalance float64        `yaml:"min_ring_balance"`
}

func (p *washParams) Validate() error {
	if p.MinRingLength > p.MaxRingLength {
		return fmt.Errorf("min_ring_length %d exceeds max_ring_length %d", p.MinRingLength, p.MaxRingLength)
	}
	if p.MaxEntryGap.D() >= p.Window.D() {
		return errors.New("max_entry_gap must be shorter than window")
	}
	return nil
}

func (p *washParams) Horizon() time.Duration { return p.Window.D() }

// washEdge is a trade between different beneficial owners that looks
// pre-arranged: both orders were entered within max_entry_gap of each
// other at matching sizes, and the resting order was priced inside the
// spread.
type washEdge struct {
	t              int64
	seq            uint64
	trade          *twpb.TradeEvent
	sellerOwner    string
	buyerOwner     string
	passive, aggro *OrderInfo
}

// washDetector finds trades that change no beneficial ownership.
//
// Direct: both sides of a trade have the same beneficial owner, and the
// trade looks pre-arranged or the owner has traded with itself min_repeat
// times within the window. Incidental crosses between two desks of one
// firm, which happen in normal flow, are neither pre-arranged nor frequent.
//
// Ring: pre-arranged-looking trades between different owners are kept as
// directed edges (seller to buyer) of a small counterparty graph. When a new
// edge closes a cycle of min..max_ring_length owners, with every edge later
// than the previous one and quantities balanced to min_ring_balance, the
// shares have gone round in a circle and the ring is reported.
type washDetector struct {
	sameOwner map[string][]int64 // owner -> times of same-owner trades
	edges     []washEdge
}

func newWashDetector() Detector { return &washDetector{sameOwner: map[string][]int64{}} }

func init() {
	Register(Kind{
		Name:      "wash_trading",
		NewParams: func() Params { return &washParams{} },
		New:       newWashDetector,
		Samples: []ExplanationData{
			{Symbol: "QZAP", Accounts: []string{"ACC-0001", "ACC-0002"}, Owners: []string{"BO-0001"},
				F:     map[string]float64{"same_owner_trades": 2, "entry_gap_ms": 12, "size_match": 1, "inside_spread": 1},
				Facts: map[string]string{"pattern": "direct"}},
			// An account trading with itself.
			{Symbol: "QZAP", Accounts: []string{"ACC-0001"}, Owners: []string{"BO-0001"},
				F:     map[string]float64{"same_owner_trades": 3, "entry_gap_ms": 8000, "size_match": 0.4, "inside_spread": 0},
				Facts: map[string]string{"pattern": "direct"}},
			{Symbol: "QZAP", Accounts: []string{"ACC-0001", "ACC-0002", "ACC-0003"}, Owners: []string{"BO-0001", "BO-0002", "BO-0003"},
				F:     map[string]float64{"ring_length": 3, "ring_balance": 1, "ring_duration_s": 42},
				Facts: map[string]string{"pattern": "ring"}},
		},
	})
}

func sizeMatch(a, b int64) float64 {
	if a <= 0 || b <= 0 {
		return 0
	}
	return float64(min(a, b)) / float64(max(a, b))
}

func (d *washDetector) expire(cutoff int64) {
	for owner, ts := range d.sameOwner {
		i := 0
		for i < len(ts) && ts[i] < cutoff {
			i++
		}
		if i == len(ts) {
			delete(d.sameOwner, owner)
		} else if i > 0 {
			d.sameOwner[owner] = ts[i:]
		}
	}
	i := 0
	for i < len(d.edges) && d.edges[i].t < cutoff {
		i++
	}
	if i > 0 {
		d.edges = append(d.edges[:0], d.edges[i:]...)
	}
}

func (d *washDetector) OnEvent(c *Context) {
	tr := c.Event().GetTrade()
	if tr == nil {
		return
	}
	p := c.Params().(*washParams)
	now := c.Now()
	d.expire(now - p.Window.Ns())

	enr := c.Event().GetEnrichment()
	buyerOwner, sellerOwner := enr.GetBuyAccount().GetBeneficialOwnerId(), enr.GetSellAccount().GetBeneficialOwnerId()
	passiveID, aggroID := tr.GetSellOrderId(), tr.GetBuyOrderId()
	if tr.GetAggressorSide() == twpb.Side_SIDE_SELL {
		passiveID, aggroID = aggroID, passiveID
	}
	m := c.Market()
	passive, okp := m.Order(passiveID)
	aggro, oka := m.Order(aggroID)
	var gap, match float64
	prearranged := false
	if okp && oka {
		gap = float64(aggro.Entry-passive.Entry) / float64(time.Millisecond)
		match = sizeMatch(passive.Qty, aggro.Qty)
		prearranged = aggro.Entry-passive.Entry <= p.MaxEntryGap.Ns() && match >= p.MinSizeMatch
	}

	if buyerOwner != "" && buyerOwner == sellerOwner {
		times := append(d.sameOwner[buyerOwner], now)
		d.sameOwner[buyerOwner] = times
		if !prearranged && len(times) < p.MinRepeat {
			return
		}
		// A pre-arranged trade alerts on its own; otherwise repetition does,
		// and the entry gap and size match are reported for context only.
		var features []Feature
		if prearranged {
			features = []Feature{
				AtLeast("same_owner_trades", float64(len(times)), 1, "trades"),
				AtMost("entry_gap_ms", gap, float64(p.MaxEntryGap.D()/time.Millisecond), "ms"),
				AtLeast("size_match", match, p.MinSizeMatch, "ratio"),
			}
		} else {
			features = []Feature{
				AtLeast("same_owner_trades", float64(len(times)), float64(p.MinRepeat), "trades"),
				Info("entry_gap_ms", gap, "ms"),
				Info("size_match", match, "ratio"),
			}
		}
		inside := 0.0
		if okp && passive.InsideSpread {
			inside = 1
		}
		features = append(features, Info("inside_spread", inside, "bool"))
		d.emitDirect(c, tr, buyerOwner, passive, aggro, features)
		return
	}

	if !prearranged || !passive.InsideSpread || buyerOwner == "" || sellerOwner == "" {
		return
	}
	e := washEdge{t: now, seq: c.Seq(), trade: tr, sellerOwner: sellerOwner, buyerOwner: buyerOwner, passive: passive, aggro: aggro}
	if cycle := d.findCycle(e, p); cycle != nil {
		d.emitRing(c, append(cycle, e), p)
	}
	d.edges = append(d.edges, e)
}

// findCycle looks for earlier edges forming a path, in increasing time,
// from the new edge's buyer back to its seller, so that together with the
// new edge they close a cycle of distinct owners.
func (d *washDetector) findCycle(e washEdge, p *washParams) []washEdge {
	var path []washEdge
	seen := map[string]bool{e.buyerOwner: true}
	var walk func(owner string, after int64) bool
	walk = func(owner string, after int64) bool {
		length := len(path) + 1 // edges in the cycle including the new one
		if owner == e.sellerOwner {
			return length >= p.MinRingLength
		}
		if length >= p.MaxRingLength {
			return false
		}
		for _, f := range d.edges {
			if f.t <= after || f.sellerOwner != owner || seen[f.buyerOwner] && f.buyerOwner != e.sellerOwner {
				continue
			}
			seen[f.buyerOwner] = true
			path = append(path, f)
			if walk(f.buyerOwner, f.t) {
				return true
			}
			path = path[:len(path)-1]
			delete(seen, f.buyerOwner)
		}
		return false
	}
	if walk(e.buyerOwner, -1) {
		return path
	}
	return nil
}

func (d *washDetector) emitDirect(c *Context, tr *twpb.TradeEvent, owner string, passive, aggro *OrderInfo, features []Feature) {
	start := c.Now()
	var orders []uint64
	var timeline []*twpb.EvidenceEvent
	for _, o := range []*OrderInfo{passive, aggro} {
		if o != nil {
			start = min(start, o.Entry)
			orders = append(orders, o.ID)
			timeline = append(timeline, evidenceOrder(o, "wash_leg"))
		}
	}
	timeline = append(timeline, evidenceTrade(c.Now(), c.Seq(), tr, tr.GetSellAccountId(), "wash_trade"))
	c.Emit(Draft{
		Accounts:    []string{tr.GetBuyAccountId(), tr.GetSellAccountId()},
		Anchor:      fmt.Sprintf("trade:%d", tr.GetTradeId()),
		SuppressKey: "owner:" + owner,
		WindowStart: start, WindowEnd: c.Now(),
		Features: features,
		Facts:    map[string]string{"pattern": "direct"},
		Orders:   orders, Trades: []uint64{tr.GetTradeId()}, Timeline: timeline,
	})
}

func (d *washDetector) emitRing(c *Context, cycle []washEdge, p *washParams) {
	minQty, maxQty := cycle[0].trade.GetQuantity(), cycle[0].trade.GetQuantity()
	var accounts, owners []string
	var orders, trades []uint64
	var timeline []*twpb.EvidenceEvent
	start := cycle[0].passive.Entry
	for _, e := range cycle {
		q := e.trade.GetQuantity()
		minQty, maxQty = min(minQty, q), max(maxQty, q)
		accounts = append(accounts, e.trade.GetBuyAccountId(), e.trade.GetSellAccountId())
		owners = append(owners, e.buyerOwner, e.sellerOwner)
		orders = append(orders, e.passive.ID, e.aggro.ID)
		trades = append(trades, e.trade.GetTradeId())
		start = min(start, e.passive.Entry)
		timeline = append(timeline, evidenceTrade(e.t, e.seq, e.trade, e.trade.GetSellAccountId(), "ring_leg"))
	}
	balance := float64(minQty) / float64(maxQty)
	if balance < p.MinRingBalance {
		return
	}
	owners = sortedUnique(owners)
	c.Emit(Draft{
		Accounts:    accounts,
		Anchor:      fmt.Sprintf("ring:%d", cycle[len(cycle)-1].trade.GetTradeId()),
		SuppressKey: "ring:" + strings.Join(owners, ","),
		WindowStart: start, WindowEnd: c.Now(),
		Features: []Feature{
			AtLeast("ring_length", float64(len(cycle)), float64(p.MinRingLength), "legs"),
			AtLeast("ring_balance", balance, p.MinRingBalance, "ratio"),
			Info("ring_duration_s", float64(cycle[len(cycle)-1].t-cycle[0].t)/1e9, "s"),
		},
		Facts:  map[string]string{"pattern": "ring"},
		Orders: orders, Trades: trades, Timeline: timeline,
	})
}
