package detect

import (
	"fmt"
	"slices"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// The baseline detectors are deliberately naive: each looks at the single
// most obvious signal of its abuse type, with the same thresholds as the
// corresponding production detector where the signals overlap. They exist
// only for the evaluation, to measure what the production detectors' extra
// evidence buys; the live rule pack does not use them.

// --- Wash trading: every trade between accounts of one beneficial owner.

type selfTradeParams struct{}

func (p *selfTradeParams) Validate() error        { return nil }
func (p *selfTradeParams) Horizon() time.Duration { return 0 }

type selfTradeDetector struct{}

func init() {
	Register(Kind{
		Name:      "baseline_self_trade",
		NewParams: func() Params { return &selfTradeParams{} },
		New:       func() Detector { return selfTradeDetector{} },
		Samples: []ExplanationData{{Symbol: "QZAP", Accounts: []string{"ACC-0001", "ACC-0002"}, Owners: []string{"BO-0001"},
			F: map[string]float64{"quantity": 300}}},
	})
}

func (selfTradeDetector) OnEvent(c *Context) {
	tr := c.Event().GetTrade()
	if tr == nil {
		return
	}
	enr := c.Event().GetEnrichment()
	owner := enr.GetBuyAccount().GetBeneficialOwnerId()
	if owner == "" || owner != enr.GetSellAccount().GetBeneficialOwnerId() {
		return
	}
	c.Emit(Draft{
		Accounts: []string{tr.GetBuyAccountId(), tr.GetSellAccountId()}, Anchor: fmt.Sprintf("trade:%d", tr.GetTradeId()),
		SuppressKey: "owner:" + owner, WindowStart: c.Now(), WindowEnd: c.Now(),
		Features: []Feature{Info("quantity", float64(tr.GetQuantity()), "shares")},
		Trades:   []uint64{tr.GetTradeId()},
		Timeline: []*twpb.EvidenceEvent{evidenceTrade(c.Now(), c.Seq(), tr, tr.GetSellAccountId(), "self_trade")},
	})
}

// --- Spoofing: an account that cancels many orders, and most of what it
// placed, on one side.

type cancelRatioParams struct {
	Window         rules.Duration `yaml:"window"`
	MinCancels     int            `yaml:"min_cancels"`
	MinCancelRatio float64        `yaml:"min_cancel_ratio"`
}

func (p *cancelRatioParams) Validate() error        { return nil }
func (p *cancelRatioParams) Horizon() time.Duration { return p.Window.D() }

type sideActivity struct {
	placed    []timedQty
	cancelled []timedQty
}

type cancelRatioDetector struct {
	accounts map[string]map[twpb.Side]*sideActivity
}

func init() {
	Register(Kind{
		Name:      "baseline_cancel_ratio",
		NewParams: func() Params { return &cancelRatioParams{} },
		New:       func() Detector { return &cancelRatioDetector{accounts: map[string]map[twpb.Side]*sideActivity{}} },
		Samples: []ExplanationData{{Symbol: "QZAP", Accounts: []string{"ACC-0001"}, Owners: []string{"BO-0001"},
			F: map[string]float64{"cancels": 6, "cancel_ratio": 0.95}, Facts: map[string]string{"side": "buy"}}},
	})
}

func (d *cancelRatioDetector) activity(account string, side twpb.Side) *sideActivity {
	sides := d.accounts[account]
	if sides == nil {
		sides = map[twpb.Side]*sideActivity{}
		d.accounts[account] = sides
	}
	a := sides[side]
	if a == nil {
		a = &sideActivity{}
		sides[side] = a
	}
	return a
}

func (d *cancelRatioDetector) OnEvent(c *Context) {
	p := c.Params().(*cancelRatioParams)
	now, cut := c.Now(), c.Now()-p.Window.Ns()
	switch c.Kind() {
	case events.KindOrder:
		o := c.Event().GetOrder()
		if o.GetAction() == twpb.OrderAction_ORDER_ACTION_NEW {
			a := d.activity(o.GetAccountId(), o.GetSide())
			a.placed = append(trimTimed(a.placed, cut), timedQty{now, o.GetQuantity()})
		}
	case events.KindCancel:
		cn := c.Event().GetCancel()
		if cn.GetReason() != twpb.CancelReason_CANCEL_REASON_USER {
			return
		}
		a := d.activity(cn.GetAccountId(), cn.GetSide())
		a.placed = trimTimed(a.placed, cut)
		a.cancelled = append(trimTimed(a.cancelled, cut), timedQty{now, cn.GetCancelledQuantity()})
		placed := sumTimed(a.placed)
		if len(a.cancelled) < p.MinCancels || placed == 0 {
			return
		}
		ratio := min(1, float64(sumTimed(a.cancelled))/float64(placed))
		if ratio < p.MinCancelRatio {
			return
		}
		c.Emit(Draft{
			Accounts: []string{cn.GetAccountId()}, Anchor: fmt.Sprintf("cancel:%d", cn.GetOrderId()),
			WindowStart: a.cancelled[0].t, WindowEnd: now,
			Features: []Feature{
				AtLeast("cancels", float64(len(a.cancelled)), float64(p.MinCancels), "orders"),
				AtLeast("cancel_ratio", ratio, p.MinCancelRatio, "ratio"),
			},
			Facts:  map[string]string{"side": sideName(cn.GetSide())},
			Orders: []uint64{cn.GetOrderId()},
		})
	}
}

// --- Momentum ignition: an account trading aggressively in one direction
// while the price moves that way.

type priceMoveParams struct {
	Window     rules.Duration `yaml:"window"`
	MinOrders  int            `yaml:"min_orders"`
	MinMoveBps float64        `yaml:"min_move_bps"`
}

func (p *priceMoveParams) Validate() error        { return nil }
func (p *priceMoveParams) Horizon() time.Duration { return p.Window.D() }

type priceMoveDetector struct {
	window []aggrTrade
}

func init() {
	Register(Kind{
		Name:      "baseline_price_move",
		NewParams: func() Params { return &priceMoveParams{} },
		New:       func() Detector { return &priceMoveDetector{} },
		Samples: []ExplanationData{{Symbol: "QZAP", Accounts: []string{"ACC-0001"}, Owners: []string{"BO-0001"},
			F: map[string]float64{"aggressive_orders": 4, "move_bps": 41}, Facts: map[string]string{"direction": "buy"}}},
	})
}

func (d *priceMoveDetector) OnEvent(c *Context) {
	tr := c.Event().GetTrade()
	if tr == nil {
		return
	}
	p := c.Params().(*priceMoveParams)
	now := c.Now()
	i := 0
	for i < len(d.window) && d.window[i].t < now-p.Window.Ns() {
		i++
	}
	d.window = d.window[i:]
	side, account, order := tr.GetAggressorSide(), tr.GetBuyAccountId(), tr.GetBuyOrderId()
	if side == twpb.Side_SIDE_SELL {
		account, order = tr.GetSellAccountId(), tr.GetSellOrderId()
	}
	d.window = append(d.window, aggrTrade{t: now, seq: c.Seq(), account: account, side: side, qty: tr.GetQuantity(),
		order: order, pre: c.Market().PrevLastPrice, trade: tr})
	start := d.window[0].pre
	if start <= 0 {
		return
	}
	move := float64(c.Market().LastPrice-start) / float64(start) * 1e4
	if side == twpb.Side_SIDE_SELL {
		move = -move
	}
	orders := map[uint64]bool{}
	var trades []uint64
	for _, t := range d.window {
		if t.account == account && t.side == side {
			orders[t.order] = true
			trades = append(trades, t.trade.GetTradeId())
		}
	}
	if len(orders) < p.MinOrders || move < p.MinMoveBps {
		return
	}
	ids := make([]uint64, 0, len(orders))
	for o := range orders {
		ids = append(ids, o)
	}
	slices.Sort(ids)
	c.Emit(Draft{
		Accounts: []string{account}, Anchor: fmt.Sprintf("move:%d", tr.GetTradeId()),
		WindowStart: d.window[0].t, WindowEnd: now,
		Features: []Feature{
			AtLeast("aggressive_orders", float64(len(orders)), float64(p.MinOrders), "orders"),
			AtLeast("move_bps", move, p.MinMoveBps, "bps"),
		},
		Facts:  map[string]string{"direction": sideName(side)},
		Orders: ids, Trades: trades,
	})
}

// --- Quote stuffing: an account sending many messages in a short window.

type messageRateParams struct {
	Window      rules.Duration `yaml:"window"`
	MinMessages int            `yaml:"min_messages"`
}

func (p *messageRateParams) Validate() error        { return nil }
func (p *messageRateParams) Horizon() time.Duration { return p.Window.D() }

type messageRateDetector struct {
	msgs map[string][]int64
}

func init() {
	Register(Kind{
		Name:      "baseline_message_rate",
		NewParams: func() Params { return &messageRateParams{} },
		New:       func() Detector { return &messageRateDetector{msgs: map[string][]int64{}} },
		Samples: []ExplanationData{{Symbol: "QZAP", Accounts: []string{"ACC-0001"}, Owners: []string{"BO-0001"},
			F: map[string]float64{"messages": 140}, Facts: map[string]string{"window": "1 s"}}},
	})
}

func (d *messageRateDetector) OnEvent(c *Context) {
	var account string
	switch c.Kind() {
	case events.KindOrder:
		account = c.Event().GetOrder().GetAccountId()
	case events.KindCancel:
		if c.Event().GetCancel().GetReason() != twpb.CancelReason_CANCEL_REASON_USER {
			return
		}
		account = c.Event().GetCancel().GetAccountId()
	default:
		return
	}
	p := c.Params().(*messageRateParams)
	now := c.Now()
	msgs := append(trimTimes(d.msgs[account], now-p.Window.Ns()), now)
	d.msgs[account] = msgs
	if len(msgs) < p.MinMessages {
		return
	}
	c.Emit(Draft{
		Accounts: []string{account}, Anchor: fmt.Sprintf("burst:%d", msgs[0]),
		WindowStart: msgs[0], WindowEnd: now,
		Features: []Feature{AtLeast("messages", float64(len(msgs)), float64(p.MinMessages), "messages")},
		Facts:    map[string]string{"window": humanDuration(p.Window.D())},
	})
}

// --- Marking the close: an account with a large share of the volume
// traded in the closing phase.

type closeVolumeParams struct {
	MinVolumeShare float64 `yaml:"min_volume_share"`
}

func (p *closeVolumeParams) Validate() error        { return nil }
func (p *closeVolumeParams) Horizon() time.Duration { return maxClosingPhase }

type closeVolumeDetector struct {
	closing bool
	start   int64
	total   int64
	volume  map[string]int64
}

func init() {
	Register(Kind{
		Name:      "baseline_close_volume",
		NewParams: func() Params { return &closeVolumeParams{} },
		New:       func() Detector { return &closeVolumeDetector{volume: map[string]int64{}} },
		Samples: []ExplanationData{{Symbol: "QZAP", Accounts: []string{"ACC-0001"}, Owners: []string{"BO-0001"},
			F: map[string]float64{"volume_share": 0.21}}},
	})
}

func (d *closeVolumeDetector) OnEvent(c *Context) {
	switch c.Kind() {
	case events.KindBook:
		switch c.Event().GetBook().GetPhase() {
		case twpb.TradingPhase_TRADING_PHASE_CLOSING:
			if !d.closing {
				d.closing, d.start = true, c.Now()
			}
		case twpb.TradingPhase_TRADING_PHASE_CLOSED:
			if !d.closing || d.total == 0 {
				return
			}
			p := c.Params().(*closeVolumeParams)
			ids := make([]string, 0, len(d.volume))
			for id := range d.volume {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			for _, id := range ids {
				share := float64(d.volume[id]) / float64(d.total)
				if share < p.MinVolumeShare {
					continue
				}
				c.Emit(Draft{
					Accounts: []string{id}, Anchor: fmt.Sprintf("close:%d", d.start),
					WindowStart: d.start, WindowEnd: c.Now(),
					Features: []Feature{AtLeast("volume_share", share, p.MinVolumeShare, "ratio")},
				})
			}
		}
	case events.KindTrade:
		if !d.closing {
			return
		}
		tr := c.Event().GetTrade()
		d.total += tr.GetQuantity()
		d.volume[tr.GetBuyAccountId()] += tr.GetQuantity()
		d.volume[tr.GetSellAccountId()] += tr.GetQuantity()
	}
}
