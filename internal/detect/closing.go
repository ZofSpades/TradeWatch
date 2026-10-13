package detect

import (
	"fmt"
	"slices"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// closeParams are the parameters of the marking_the_close detector.
type closeParams struct {
	LookbackBeforeClose   rules.Duration `yaml:"lookback_before_close"`
	MinVolumeShare        float64        `yaml:"min_volume_share"`
	MinAggressiveFraction float64        `yaml:"min_aggressive_fraction"`
	MinConcentration      float64        `yaml:"min_concentration"`
	MinMoveBps            float64        `yaml:"min_move_bps"`
	MinOrders             int            `yaml:"min_orders"`
}

func (p *closeParams) Validate() error { return nil }

// maxClosingPhase is the longest closing phase the detector's horizon
// allows for; the simulator's default is ten minutes.
const maxClosingPhase = 30 * time.Minute

func (p *closeParams) Horizon() time.Duration { return p.LookbackBeforeClose.D() + maxClosingPhase }

type minuteQty struct {
	minute int64
	qty    int64
}

type closeTrade struct {
	t     int64
	seq   uint64
	trade *twpb.TradeEvent
}

type closeAccount struct {
	minutes []minuteQty // traded quantity per minute of event time, before the close
	pre     int64       // quantity traded in the lookback before the closing phase
	closing int64       // quantity traded in the closing phase
	// since is the symbol's closing-phase volume before the account's first
	// closing-phase trade; its share is measured against what traded after.
	since  int64
	aggr   map[twpb.Side]int64
	orders map[uint64]bool // aggressive orders in the closing phase
	trades []closeTrade
}

// closeDetector finds marking the close: during the closing phase an
// account trades aggressively in one direction, takes a large share of
// the closing volume, the price moves its way, and its trading is
// concentrated in the closing phase rather than spread over the
// lookback_before_close before it, as an execution algorithm finishing at
// the close would be.
type closeDetector struct {
	accounts map[string]*closeAccount
	closing  bool
	start    int64
	startMid float64
	total    int64
}

func newCloseDetector() Detector { return &closeDetector{accounts: map[string]*closeAccount{}} }

func init() {
	Register(Kind{
		Name:      "marking_the_close",
		NewParams: func() Params { return &closeParams{} },
		New:       newCloseDetector,
		Samples: []ExplanationData{{
			Symbol: "QZAP", Accounts: []string{"ACC-0001"}, Owners: []string{"BO-0001"},
			F:     map[string]float64{"orders": 9, "volume_share": 0.34, "aggressive_fraction": 1, "move_bps": 22, "concentration": 1},
			Facts: map[string]string{"direction": "buy", "lookback": "30 min"},
		}},
	})
}

func (d *closeDetector) account(id string) *closeAccount {
	a, ok := d.accounts[id]
	if !ok {
		a = &closeAccount{aggr: map[twpb.Side]int64{}, orders: map[uint64]bool{}}
		d.accounts[id] = a
	}
	return a
}

func (d *closeDetector) OnEvent(c *Context) {
	p := c.Params().(*closeParams)
	now := c.Now()
	ev := c.Event()
	switch c.Kind() {
	case events.KindBook:
		switch ev.GetBook().GetPhase() {
		case twpb.TradingPhase_TRADING_PHASE_CLOSING:
			if d.closing {
				return
			}
			d.closing, d.start, d.startMid = true, now, c.Market().Mid()
			from := (now - p.LookbackBeforeClose.Ns()) / int64(time.Minute)
			for _, a := range d.accounts {
				for _, mq := range a.minutes {
					if mq.minute >= from {
						a.pre += mq.qty
					}
				}
			}
		case twpb.TradingPhase_TRADING_PHASE_CLOSED:
			ids := make([]string, 0, len(d.accounts))
			for id := range d.accounts {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			for _, id := range ids {
				d.evaluate(c, p, id, d.accounts[id])
			}
		}
	case events.KindTrade:
		tr := ev.GetTrade()
		qty := tr.GetQuantity()
		aggrSide := tr.GetAggressorSide()
		aggrAcct, aggrOrder := tr.GetBuyAccountId(), tr.GetBuyOrderId()
		if aggrSide == twpb.Side_SIDE_SELL {
			aggrAcct, aggrOrder = tr.GetSellAccountId(), tr.GetSellOrderId()
		}
		minute := now / int64(time.Minute)
		for _, id := range []string{tr.GetBuyAccountId(), tr.GetSellAccountId()} {
			a := d.account(id)
			if d.closing {
				if a.closing == 0 {
					a.since = d.total
				}
				a.closing += qty
				continue
			}
			if n := len(a.minutes); n > 0 && a.minutes[n-1].minute == minute {
				a.minutes[n-1].qty += qty
			} else {
				a.minutes = append(a.minutes, minuteQty{minute, qty})
			}
			from := (now - p.LookbackBeforeClose.Ns()) / int64(time.Minute)
			i := 0
			for i < len(a.minutes) && a.minutes[i].minute < from {
				i++
			}
			a.minutes = a.minutes[i:]
		}
		if !d.closing {
			return
		}
		d.total += qty
		a := d.account(aggrAcct)
		a.aggr[aggrSide] += qty
		a.orders[aggrOrder] = true
		a.trades = append(a.trades, closeTrade{t: now, seq: c.Seq(), trade: tr})
		d.evaluate(c, p, aggrAcct, a)
	}
}

func (d *closeDetector) evaluate(c *Context, p *closeParams, id string, a *closeAccount) {
	if !d.closing || len(a.orders) < p.MinOrders || a.closing == 0 || d.total == 0 || d.startMid <= 0 {
		return
	}
	dir := twpb.Side_SIDE_BUY
	if a.aggr[twpb.Side_SIDE_SELL] > a.aggr[twpb.Side_SIDE_BUY] {
		dir = twpb.Side_SIDE_SELL
	}
	sign := 1.0
	if dir == twpb.Side_SIDE_SELL {
		sign = -1
	}
	share := float64(a.closing) / float64(d.total-a.since)
	aggrFrac := float64(a.aggr[dir]) / float64(a.closing)
	conc := float64(a.closing) / float64(a.closing+a.pre)
	move := sign * (float64(c.Market().LastPrice) - d.startMid) / d.startMid * 1e4
	if share < p.MinVolumeShare || aggrFrac < p.MinAggressiveFraction || conc < p.MinConcentration || move < p.MinMoveBps {
		return
	}
	var orders, trades []uint64
	var timeline []*twpb.EvidenceEvent
	for o := range a.orders {
		orders = append(orders, o)
	}
	for _, t := range a.trades {
		trades = append(trades, t.trade.GetTradeId())
		timeline = append(timeline, evidenceTrade(t.t, t.seq, t.trade, id, "closing_trade"))
	}
	c.Emit(Draft{
		Accounts:    []string{id},
		Anchor:      fmt.Sprintf("close:%d", d.start),
		WindowStart: a.trades[0].t, WindowEnd: c.Now(),
		Features: []Feature{
			AtLeast("orders", float64(len(a.orders)), float64(p.MinOrders), "orders"),
			AtLeast("volume_share", share, p.MinVolumeShare, "ratio"),
			AtLeast("aggressive_fraction", aggrFrac, p.MinAggressiveFraction, "ratio"),
			AtLeast("concentration", conc, p.MinConcentration, "ratio"),
			AtLeast("move_bps", move, p.MinMoveBps, "bps"),
		},
		Facts:  map[string]string{"direction": sideName(dir), "lookback": humanDuration(p.LookbackBeforeClose.D())},
		Orders: orders, Trades: trades, Timeline: timeline,
	})
}
