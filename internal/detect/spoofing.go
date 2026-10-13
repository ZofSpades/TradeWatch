package detect

import (
	"errors"
	"fmt"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// spoofParams are the parameters of the spoofing detector.
type spoofParams struct {
	Window             rules.Duration `yaml:"window"`
	MinSizeVsDepth     float64        `yaml:"min_size_vs_depth"`
	MaxOrderLifetime   rules.Duration `yaml:"max_order_lifetime"`
	MaxCancelAfterFill rules.Duration `yaml:"max_cancel_after_fill"`
	MinLayers          int            `yaml:"min_layers"`
	CancelGroupWindow  rules.Duration `yaml:"cancel_group_window"`
	MinCancelRatio     float64        `yaml:"min_cancel_ratio"`
}

func (p *spoofParams) Validate() error {
	if p.CancelGroupWindow.D() >= p.Window.D() || p.MaxCancelAfterFill.D() >= p.Window.D() {
		return errors.New("cancel_group_window and max_cancel_after_fill must be shorter than window")
	}
	return nil
}

func (p *spoofParams) Horizon() time.Duration {
	return max(p.Window.D(), p.MaxOrderLifetime.D()) + p.CancelGroupWindow.D()
}

type timedQty struct {
	t   int64
	qty int64
}

type spoofFill struct {
	t     int64
	seq   uint64
	side  twpb.Side
	order uint64
	trade *twpb.TradeEvent
}

type spoofCancel struct {
	t        int64
	seq      uint64
	order    OrderInfo
	lifetime int64
}

type spoofGroup struct {
	side        twpb.Side
	first, last int64
	cancels     []spoofCancel
}

type spoofAccount struct {
	placed    map[twpb.Side][]timedQty
	cancelled map[twpb.Side][]timedQty
	fills     []spoofFill
	group     *spoofGroup
}

// spoofDetector finds layering: an account cancels several large orders on
// one side shortly after one of its orders on the other side executed.
//
// A cancelled order counts as a layer when it was at least
// min_size_vs_depth times the mean displayed level size when it arrived and
// lived at most max_order_lifetime. Layers cancelled within
// cancel_group_window of each other form a group. The group is evaluated
// once the window has passed, so that every layer pulled together is part
// of one alert. It is reported when it has min_layers layers, the account
// had an opposite-side fill while they rested and no more than
// max_cancel_after_fill before the first cancel, and the account cancelled
// at least min_cancel_ratio of what it placed on the layer side in the
// window.
//
// Market makers cancel and replace constantly but their orders are no
// larger than the depth they make up; institutional orders can be large
// and get cancelled, but are not followed by fills on the other side.
type spoofDetector struct {
	accounts map[string]*spoofAccount
	pending  []string // accounts with an open group, in order of first cancel
}

func newSpoofDetector() Detector { return &spoofDetector{accounts: map[string]*spoofAccount{}} }

func init() {
	Register(Kind{
		Name:      "spoofing",
		NewParams: func() Params { return &spoofParams{} },
		New:       newSpoofDetector,
		Samples: []ExplanationData{{
			Symbol: "QZAP", Accounts: []string{"ACC-0001"}, Owners: []string{"BO-0001"},
			F: map[string]float64{"layers": 4, "size_vs_depth": 6.5, "cancel_after_fill_ms": 180, "max_lifetime_s": 21,
				"cancel_ratio": 0.96, "opposite_fill_qty": 400},
			Facts: map[string]string{"layer_side": "buy", "fill_side": "sell"},
		}},
	})
}

func opposite(s twpb.Side) twpb.Side {
	if s == twpb.Side_SIDE_BUY {
		return twpb.Side_SIDE_SELL
	}
	return twpb.Side_SIDE_BUY
}

func sideName(s twpb.Side) string {
	if s == twpb.Side_SIDE_BUY {
		return "buy"
	}
	return "sell"
}

func (d *spoofDetector) account(id string) *spoofAccount {
	a, ok := d.accounts[id]
	if !ok {
		a = &spoofAccount{placed: map[twpb.Side][]timedQty{}, cancelled: map[twpb.Side][]timedQty{}}
		d.accounts[id] = a
	}
	return a
}

func trimTimed(xs []timedQty, cutoff int64) []timedQty {
	i := 0
	for i < len(xs) && xs[i].t < cutoff {
		i++
	}
	return xs[i:]
}

func sumTimed(xs []timedQty) int64 {
	var s int64
	for _, x := range xs {
		s += x.qty
	}
	return s
}

func (d *spoofDetector) OnEvent(c *Context) {
	p := c.Params().(*spoofParams)
	now := c.Now()
	d.evaluateDue(c, p, now)

	ev := c.Event()
	switch c.Kind() {
	case events.KindOrder:
		o := ev.GetOrder()
		a := d.account(o.GetAccountId())
		added := o.GetQuantity()
		if o.GetAction() == twpb.OrderAction_ORDER_ACTION_REPLACE {
			added = o.GetQuantity() - o.GetPreviousQuantity()
		}
		if added > 0 {
			a.placed[o.GetSide()] = append(trimTimed(a.placed[o.GetSide()], now-p.Window.Ns()), timedQty{now, added})
		}
	case events.KindTrade:
		tr := ev.GetTrade()
		for _, leg := range []struct {
			account string
			side    twpb.Side
			order   uint64
		}{{tr.GetBuyAccountId(), twpb.Side_SIDE_BUY, tr.GetBuyOrderId()}, {tr.GetSellAccountId(), twpb.Side_SIDE_SELL, tr.GetSellOrderId()}} {
			a := d.account(leg.account)
			i := 0
			for i < len(a.fills) && a.fills[i].t < now-p.Window.Ns() {
				i++
			}
			a.fills = append(a.fills[i:], spoofFill{t: now, seq: c.Seq(), side: leg.side, order: leg.order, trade: tr})
		}
	case events.KindCancel:
		cn := ev.GetCancel()
		if cn.GetReason() != twpb.CancelReason_CANCEL_REASON_USER {
			return
		}
		side := cn.GetSide()
		a := d.account(cn.GetAccountId())
		a.cancelled[side] = append(trimTimed(a.cancelled[side], now-p.Window.Ns()), timedQty{now, cn.GetCancelledQuantity()})
		info, ok := c.Market().Order(cn.GetOrderId())
		lifetime := now - cn.GetOrderEntryTimeNs()
		if !ok || info.DepthRatio < p.MinSizeVsDepth || lifetime > p.MaxOrderLifetime.Ns() {
			return
		}
		if a.group != nil && (a.group.side != side || now-a.group.first > p.CancelGroupWindow.Ns()) {
			d.evaluate(c, p, cn.GetAccountId(), a)
		}
		if a.group == nil {
			a.group = &spoofGroup{side: side, first: now}
			d.pending = append(d.pending, cn.GetAccountId())
		}
		a.group.cancels = append(a.group.cancels, spoofCancel{t: now, seq: c.Seq(), order: *info, lifetime: lifetime})
		a.group.last = now
	}
}

func (d *spoofDetector) evaluateDue(c *Context, p *spoofParams, now int64) {
	for len(d.pending) > 0 {
		id := d.pending[0]
		a := d.accounts[id]
		if a == nil || a.group == nil {
			d.pending = d.pending[1:]
			continue
		}
		if now-a.group.first < p.CancelGroupWindow.Ns() {
			return
		}
		d.evaluate(c, p, id, a)
	}
}

// evaluate closes an account's cancel group and reports it if it is a
// layering pattern.
func (d *spoofDetector) evaluate(c *Context, p *spoofParams, account string, a *spoofAccount) {
	g := a.group
	a.group = nil
	for i, id := range d.pending {
		if id == account {
			d.pending = append(d.pending[:i], d.pending[i+1:]...)
			break
		}
	}
	if len(g.cancels) < p.MinLayers {
		return
	}
	entryMin := g.cancels[0].order.Entry
	var maxRatio float64
	var maxLife int64
	for _, cn := range g.cancels {
		entryMin = min(entryMin, cn.order.Entry)
		maxRatio = max(maxRatio, cn.order.DepthRatio)
		maxLife = max(maxLife, cn.lifetime)
	}
	fillSide := opposite(g.side)
	var fills []spoofFill
	var fillQty int64
	for _, f := range a.fills {
		if f.side == fillSide && f.t >= entryMin && f.t <= g.first {
			fills = append(fills, f)
			fillQty += f.trade.GetQuantity()
		}
	}
	if len(fills) == 0 {
		return
	}
	lag := g.first - fills[len(fills)-1].t
	if lag > p.MaxCancelAfterFill.Ns() {
		return
	}
	placed := sumTimed(a.placed[g.side])
	ratio := 1.0
	if placed > 0 {
		ratio = min(1, float64(sumTimed(a.cancelled[g.side]))/float64(placed))
	}
	if ratio < p.MinCancelRatio {
		return
	}

	var orders, trades []uint64
	var timeline []*twpb.EvidenceEvent
	for _, cn := range g.cancels {
		o := cn.order
		orders = append(orders, o.ID)
		timeline = append(timeline, evidenceOrder(&o, "layer"), &twpb.EvidenceEvent{
			EventTimeNs: cn.t, Seq: cn.seq, Kind: twpb.EvidenceKind_EVIDENCE_KIND_CANCEL, OrderId: o.ID,
			AccountId: account, Side: o.Side, Price: o.Price, Quantity: o.Open, Role: "layer_cancel",
		})
	}
	for _, f := range fills {
		orders = append(orders, f.order)
		trades = append(trades, f.trade.GetTradeId())
		timeline = append(timeline, evidenceTrade(f.t, f.seq, f.trade, account, "genuine_fill"))
	}
	c.Emit(Draft{
		Accounts:    []string{account},
		Anchor:      fmt.Sprintf("layers:%d", g.cancels[0].order.ID),
		WindowStart: entryMin, WindowEnd: g.last,
		Features: []Feature{
			AtLeast("layers", float64(len(g.cancels)), float64(p.MinLayers), "orders"),
			AtLeast("size_vs_depth", maxRatio, p.MinSizeVsDepth, "x"),
			AtMost("cancel_after_fill_ms", float64(lag)/1e6, float64(p.MaxCancelAfterFill.D()/time.Millisecond), "ms"),
			AtMost("max_lifetime_s", float64(maxLife)/1e9, p.MaxOrderLifetime.D().Seconds(), "s"),
			AtLeast("cancel_ratio", ratio, p.MinCancelRatio, "ratio"),
			Info("opposite_fill_qty", float64(fillQty), "shares"),
		},
		Facts:  map[string]string{"layer_side": sideName(g.side), "fill_side": sideName(fillSide)},
		Orders: orders, Trades: trades, Timeline: timeline,
	})
}
