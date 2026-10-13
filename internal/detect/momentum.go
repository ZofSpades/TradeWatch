package detect

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// momoParams are the parameters of the momentum_ignition detector.
type momoParams struct {
	ImpulseWindow      rules.Duration `yaml:"impulse_window"`
	MinImpulseOrders   int            `yaml:"min_impulse_orders"`
	MinMoveBps         float64        `yaml:"min_move_bps"`
	MinMoveSigma       float64        `yaml:"min_move_sigma"`
	VolatilityLookback rules.Duration `yaml:"volatility_lookback"`
	MinAggressiveShare float64        `yaml:"min_aggressive_share"`
	ReversalWindow     rules.Duration `yaml:"reversal_window"`
	MinUnwindRatio     float64        `yaml:"min_unwind_ratio"`
}

func (p *momoParams) Validate() error {
	if p.VolatilityLookback.D() < 30*time.Second {
		return errors.New("volatility_lookback must be at least 30s to estimate volatility")
	}
	return nil
}

func (p *momoParams) Horizon() time.Duration {
	return max(p.VolatilityLookback.D(), p.ReversalWindow.D()) + p.ImpulseWindow.D()
}

type aggrTrade struct {
	t       int64
	seq     uint64
	account string
	side    twpb.Side
	qty     int64
	order   uint64
	pre     int64 // last trade price before this trade
	trade   *twpb.TradeEvent
}

type momoUnwind struct {
	t     int64
	seq   uint64
	trade *twpb.TradeEvent
}

type impulse struct {
	dir        twpb.Side
	start, end int64
	qty        int64
	orders     int
	trades     []aggrTrade
	move       float64 // bps
	sigma      float64
	share      float64
	unwound    int64
	unwinds    []momoUnwind
}

// momoDetector finds momentum ignition: an account trades aggressively in
// one direction, moving the price far beyond its recent volatility, and
// then trades a large part of the position back.
//
// The move is measured from the price just before the account's own first
// aggressive trade of the burst, and must be at least min_move_bps and
// min_move_sigma times the volatility expected over the burst's duration
// (the mid's volatility per square-root second, estimated from thirty-second
// returns over the volatility_lookback before the burst, scaled by the
// square root of the burst's length). The account must account for
// min_aggressive_share of all aggressive volume in that direction since its
// first trade. A trader who
// merely follows a move (momentum traders, reactions to news) joins after
// the price has already moved, so the move measured from its own entry is
// small. Once such an impulse exists, the account's trades in the other
// direction within reversal_window count as unwinding; reaching
// min_unwind_ratio of the impulse volume completes the pattern.
type momoDetector struct {
	window []aggrTrade // aggressive trades within impulse_window, oldest first
	cands  map[string]*impulse
}

func newMomoDetector() Detector { return &momoDetector{cands: map[string]*impulse{}} }

func init() {
	Register(Kind{
		Name:      "momentum_ignition",
		NewParams: func() Params { return &momoParams{} },
		New:       newMomoDetector,
		Samples: []ExplanationData{{
			Symbol: "QZAP", Accounts: []string{"ACC-0001"}, Owners: []string{"BO-0001"},
			F: map[string]float64{"impulse_orders": 8, "impulse_s": 12, "aggressive_share": 0.82, "move_bps": 74,
				"move_sigma": 9.1, "unwind_ratio": 0.9, "impulse_qty": 9200},
			Facts: map[string]string{"direction": "buy"},
		}},
	})
}

func (d *momoDetector) OnEvent(c *Context) {
	tr := c.Event().GetTrade()
	if tr == nil {
		return
	}
	p := c.Params().(*momoParams)
	now := c.Now()
	m := c.Market()

	i := 0
	for i < len(d.window) && d.window[i].t < now-p.ImpulseWindow.Ns() {
		i++
	}
	d.window = d.window[i:]
	for acct, im := range d.cands {
		if now-im.end > p.ReversalWindow.Ns() {
			delete(d.cands, acct)
		}
	}

	aggrSide := tr.GetAggressorSide()
	aggrAcct, passAcct := tr.GetBuyAccountId(), tr.GetSellAccountId()
	aggrOrder := tr.GetBuyOrderId()
	if aggrSide == twpb.Side_SIDE_SELL {
		aggrAcct, passAcct = passAcct, aggrAcct
		aggrOrder = tr.GetSellOrderId()
	}

	// Unwinding by accounts with an impulse waiting for its reversal.
	for _, leg := range []struct {
		account string
		side    twpb.Side
	}{{aggrAcct, aggrSide}, {passAcct, opposite(aggrSide)}} {
		im := d.cands[leg.account]
		if im == nil || leg.side == im.dir || now <= im.end {
			continue
		}
		im.unwound += tr.GetQuantity()
		im.unwinds = append(im.unwinds, momoUnwind{t: now, seq: c.Seq(), trade: tr})
		if float64(im.unwound) >= p.MinUnwindRatio*float64(im.qty) {
			d.emit(c, p, leg.account, im)
			delete(d.cands, leg.account)
		}
	}

	d.window = append(d.window, aggrTrade{
		t: now, seq: c.Seq(), account: aggrAcct, side: aggrSide, qty: tr.GetQuantity(),
		order: aggrOrder, pre: m.PrevLastPrice, trade: tr,
	})
	d.detectImpulse(c, p, aggrAcct, aggrSide)
}

func (d *momoDetector) detectImpulse(c *Context, p *momoParams, account string, dir twpb.Side) {
	now := c.Now()
	var mine []aggrTrade
	orders := map[uint64]bool{}
	for _, t := range d.window {
		if t.account == account && t.side == dir {
			mine = append(mine, t)
			orders[t.order] = true
		}
	}
	if len(orders) < p.MinImpulseOrders {
		return
	}
	first := mine[0]
	pre := first.pre
	if pre <= 0 {
		return
	}
	sign := 1.0
	if dir == twpb.Side_SIDE_SELL {
		sign = -1
	}
	m := c.Market()
	move := sign * float64(m.LastPrice-pre) / float64(pre) * 1e4
	if move < p.MinMoveBps {
		return
	}
	var mineQty, totalQty int64
	for _, t := range d.window {
		if t.side == dir && t.t >= first.t {
			totalQty += t.qty
			if t.account == account {
				mineQty += t.qty
			}
		}
	}
	share := float64(mineQty) / float64(totalQty)
	if share < p.MinAggressiveShare {
		return
	}
	vol, ok := m.Volatility(first.t-p.VolatilityLookback.Ns(), first.t)
	if !ok {
		return
	}
	dt := math.Max(1, float64(now-first.t)/1e9)
	sigma := (move / 1e4) / (math.Max(vol, 1e-6) * math.Sqrt(dt))
	if sigma < p.MinMoveSigma {
		return
	}
	im := d.cands[account]
	if im != nil && im.dir != dir {
		// Aggressive trading against a pending impulse is its reversal, not
		// a new impulse: it is counted as unwinding above.
		return
	}
	if im == nil {
		im = &impulse{dir: dir}
		d.cands[account] = im
	}
	im.start, im.end, im.qty, im.orders = first.t, now, mineQty, len(orders)
	im.trades, im.move, im.sigma, im.share = mine, move, sigma, share
}

func (d *momoDetector) emit(c *Context, p *momoParams, account string, im *impulse) {
	var orders, trades []uint64
	var timeline []*twpb.EvidenceEvent
	for _, t := range im.trades {
		orders = append(orders, t.order)
		trades = append(trades, t.trade.GetTradeId())
		timeline = append(timeline, evidenceTrade(t.t, t.seq, t.trade, account, "impulse"))
	}
	for _, u := range im.unwinds {
		trades = append(trades, u.trade.GetTradeId())
		timeline = append(timeline, evidenceTrade(u.t, u.seq, u.trade, account, "unwind"))
	}
	c.Emit(Draft{
		Accounts:    []string{account},
		Anchor:      fmt.Sprintf("impulse:%d", im.trades[0].trade.GetTradeId()),
		WindowStart: im.start, WindowEnd: c.Now(),
		Features: []Feature{
			AtLeast("impulse_orders", float64(im.orders), float64(p.MinImpulseOrders), "orders"),
			AtLeast("move_bps", im.move, p.MinMoveBps, "bps"),
			AtLeast("move_sigma", im.sigma, p.MinMoveSigma, "sigma"),
			AtLeast("aggressive_share", im.share, p.MinAggressiveShare, "ratio"),
			AtLeast("unwind_ratio", float64(im.unwound)/float64(im.qty), p.MinUnwindRatio, "ratio"),
			Info("impulse_qty", float64(im.qty), "shares"),
			Info("impulse_s", float64(im.end-im.start)/1e9, "s"),
		},
		Facts:  map[string]string{"direction": sideName(im.dir)},
		Orders: orders, Trades: trades, Timeline: timeline,
	})
}
