package detect

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// stuffParams are the parameters of the quote_stuffing detector.
type stuffParams struct {
	BurstWindow       rules.Duration `yaml:"burst_window"`
	MinMessages       int            `yaml:"min_messages"`
	MinRateMultiple   float64        `yaml:"min_rate_multiple"`
	BaselineWindow    rules.Duration `yaml:"baseline_window"`
	MaxFillRatio      float64        `yaml:"max_fill_ratio"`
	MaxMedianLifetime rules.Duration `yaml:"max_median_lifetime"`
}

func (p *stuffParams) Validate() error {
	if p.BurstWindow.D() >= p.BaselineWindow.D() {
		return errors.New("burst_window must be shorter than baseline_window")
	}
	return nil
}

func (p *stuffParams) Horizon() time.Duration { return p.BaselineWindow.D() + p.BurstWindow.D() }

// timedID is an order ID with the time it was sent.
type timedID struct {
	t  int64
	id uint64
}

// timedVal is a measurement with the time it was made.
type timedVal struct {
	t int64
	v int64
}

type stuffAccount struct {
	msgs      []int64    // message times (new, replace, cancel) in the burst window
	orders    []timedID  // new orders in the burst window
	fills     []int64    // fill times in the burst window
	lifetimes []timedVal // lifetimes of orders cancelled in the burst window
	// Per-second message counts within the baseline window.
	second      int64
	secondCount int64
	peaks       []timedVal
}

// stuffDetector finds quote stuffing: an account sends far more order and
// cancel messages in a short window than any normal participant, with
// orders that live only milliseconds and almost never trade.
//
// The baseline is the 95th percentile, over every account active in the
// last baseline_window, of each account's busiest second. A percentile
// rather than the maximum keeps a single bursting account from raising its
// own bar.
type stuffDetector struct {
	accounts map[string]*stuffAccount
	// baseline is cached per second of event time.
	baselineSecond int64
	baseline       float64
}

func newStuffDetector() Detector { return &stuffDetector{accounts: map[string]*stuffAccount{}} }

func init() {
	Register(Kind{
		Name:      "quote_stuffing",
		NewParams: func() Params { return &stuffParams{} },
		New:       newStuffDetector,
		Samples: []ExplanationData{{
			Symbol: "QZAP", Accounts: []string{"ACC-0001"}, Owners: []string{"BO-0001"},
			F:     map[string]float64{"messages": 310, "rate_multiple": 44, "baseline_rate": 7, "median_lifetime_ms": 5, "fill_ratio": 0},
			Facts: map[string]string{"burst_window": "1 s", "baseline_window": "10 min"},
		}},
	})
}

func (d *stuffDetector) account(id string) *stuffAccount {
	a, ok := d.accounts[id]
	if !ok {
		a = &stuffAccount{}
		d.accounts[id] = a
	}
	return a
}

func (d *stuffDetector) OnEvent(c *Context) {
	p := c.Params().(*stuffParams)
	now := c.Now()
	ev := c.Event()
	var acct string
	switch c.Kind() {
	case events.KindOrder:
		o := ev.GetOrder()
		acct = o.GetAccountId()
		if o.GetAction() == twpb.OrderAction_ORDER_ACTION_NEW {
			a := d.account(acct)
			a.orders = append(a.orders, timedID{now, o.GetOrderId()})
		}
	case events.KindCancel:
		cn := ev.GetCancel()
		if cn.GetReason() != twpb.CancelReason_CANCEL_REASON_USER {
			return
		}
		acct = cn.GetAccountId()
		a := d.account(acct)
		a.lifetimes = append(a.lifetimes, timedVal{now, now - cn.GetOrderEntryTimeNs()})
	case events.KindTrade:
		tr := ev.GetTrade()
		for _, id := range []string{tr.GetBuyAccountId(), tr.GetSellAccountId()} {
			if a, ok := d.accounts[id]; ok {
				a.fills = append(a.fills, now)
			}
		}
		return
	default:
		return
	}

	a := d.account(acct)
	cut := now - p.BurstWindow.Ns()
	a.msgs = trimTimes(append(a.msgs, now), cut)
	a.orders = trimIDs(a.orders, cut)
	a.fills = trimTimes(a.fills, cut)
	a.lifetimes = trimVals(a.lifetimes, cut)
	sec := now / int64(time.Second)
	if sec != a.second {
		if a.secondCount > 0 {
			a.peaks = append(a.peaks, timedVal{a.second * int64(time.Second), a.secondCount})
		}
		a.second, a.secondCount = sec, 0
	}
	a.secondCount++
	a.peaks = trimVals(a.peaks, now-p.BaselineWindow.Ns())

	count := len(a.msgs)
	if count < p.MinMessages {
		return
	}
	baseline := d.baselineRate(now, p)
	multiple := float64(count) / p.BurstWindow.D().Seconds() / baseline
	var fillRatio float64
	if len(a.orders) > 0 {
		fillRatio = float64(len(a.fills)) / float64(len(a.orders))
	}
	lives := make([]int64, len(a.lifetimes))
	for i, l := range a.lifetimes {
		lives[i] = l.v
	}
	if multiple < p.MinRateMultiple || fillRatio > p.MaxFillRatio || len(lives) == 0 {
		return
	}
	slices.Sort(lives)
	median := float64(lives[len(lives)/2]) / 1e6
	if median > float64(p.MaxMedianLifetime.D()/time.Millisecond) {
		return
	}

	orders := make([]uint64, len(a.orders))
	for i, o := range a.orders {
		orders[i] = o.id
	}
	var timeline []*twpb.EvidenceEvent
	m := c.Market()
	for i, id := range orders {
		// Bound the evidence: the first and last twenty orders of the burst.
		if i >= 20 && i < len(orders)-20 {
			continue
		}
		if info, ok := m.Order(id); ok {
			timeline = append(timeline, evidenceOrder(info, "burst_order"))
		}
	}
	start := a.msgs[0]
	anchor := uint64(0)
	if len(orders) > 0 {
		anchor = orders[0]
	}
	c.Emit(Draft{
		Accounts:    []string{acct},
		Anchor:      fmt.Sprintf("burst:%d", anchor),
		WindowStart: start, WindowEnd: now,
		Features: []Feature{
			AtLeast("messages", float64(count), float64(p.MinMessages), "messages"),
			AtLeast("rate_multiple", multiple, p.MinRateMultiple, "x"),
			AtMost("fill_ratio", fillRatio, p.MaxFillRatio, "ratio"),
			AtMost("median_lifetime_ms", median, float64(p.MaxMedianLifetime.D()/time.Millisecond), "ms"),
			Info("baseline_rate", baseline, "messages/s"),
		},
		Facts:  map[string]string{"burst_window": humanDuration(p.BurstWindow.D()), "baseline_window": humanDuration(p.BaselineWindow.D())},
		Orders: orders, Timeline: timeline,
	})
}

// baselineRate is the 95th percentile of the accounts' busiest seconds in
// the baseline window, at least one message per second.
func (d *stuffDetector) baselineRate(now int64, p *stuffParams) float64 {
	sec := now / int64(time.Second)
	if sec == d.baselineSecond && d.baseline > 0 {
		return d.baseline
	}
	cut := now - p.BaselineWindow.Ns()
	var peaks []int64
	for id, a := range d.accounts {
		if len(a.msgs) == 0 && (len(a.peaks) == 0 || a.peaks[len(a.peaks)-1].t < cut) && a.second*int64(time.Second) < cut {
			delete(d.accounts, id) // idle for the whole baseline window
			continue
		}
		var peak int64
		for _, pk := range a.peaks {
			if pk.t >= cut {
				peak = max(peak, pk.v)
			}
		}
		if peak > 0 {
			peaks = append(peaks, peak)
		}
	}
	baseline := 1.0
	if len(peaks) > 0 {
		slices.Sort(peaks)
		baseline = max(1, float64(peaks[(len(peaks)*95)/100]))
	}
	d.baselineSecond, d.baseline = sec, baseline
	return baseline
}

func trimTimes(xs []int64, cutoff int64) []int64 {
	i := 0
	for i < len(xs) && xs[i] < cutoff {
		i++
	}
	return xs[i:]
}

func trimIDs(xs []timedID, cutoff int64) []timedID {
	i := 0
	for i < len(xs) && xs[i].t < cutoff {
		i++
	}
	return xs[i:]
}

func trimVals(xs []timedVal, cutoff int64) []timedVal {
	i := 0
	for i < len(xs) && xs[i].t < cutoff {
		i++
	}
	return xs[i:]
}
