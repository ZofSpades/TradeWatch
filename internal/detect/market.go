package detect

import (
	"math"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// OrderInfo is what the engine remembers about a recent order.
type OrderInfo struct {
	ID      uint64
	Account string
	Owner   string
	Side    twpb.Side
	Type    twpb.OrderType
	TIF     twpb.TimeInForce
	Price   int64
	// Qty is the order's original quantity (for a replace, the new open
	// quantity); Open is what is still open.
	Qty  int64
	Open int64
	// Entry is the event time the order was accepted.
	Entry int64
	// DepthRatio is Qty divided by the mean displayed quantity per level of
	// the top five levels on the order's side, from the snapshot in force
	// when the order arrived (0 if that side was empty).
	DepthRatio float64
	// InsideSpread reports whether a limit order's price was strictly
	// between the best bid and best ask of that snapshot.
	InsideSpread bool
}

// sample is one second's view of the market, taken from book snapshots.
type sample struct {
	t        int64
	mid      float64
	last     int64
	bid, ask int64
	bidSize  int64
	askSize  int64
}

// Market is the shared, read-only view of one symbol's market that
// detectors see. Depth and the best bid and offer come from the latest book
// snapshot, which is authoritative and arrives every second, so the view
// after a restart is identical to the view without one. Order details are
// kept for a bounded age only; older orders are unknown to every detector,
// again identically with or without a restart.
type Market struct {
	Phase        twpb.TradingPhase
	ClosingStart int64 // event time the closing phase began, or 0
	Bids, Asks   []*twpb.PriceLevel
	SnapshotTime int64

	LastPrice     int64
	LastTradeTime int64
	// PrevLastPrice is the last trade price before the current event (equal
	// to LastPrice except while a trade is being processed).
	PrevLastPrice int64

	samples []sample // one per second, oldest first

	orders     map[uint64]*OrderInfo
	orderQueue []uint64 // order IDs in entry order, for age-based eviction
	queueHead  int
}

func newMarket() *Market {
	return &Market{orders: map[uint64]*OrderInfo{}}
}

// Order returns a remembered order.
func (m *Market) Order(id uint64) (*OrderInfo, bool) {
	o, ok := m.orders[id]
	return o, ok
}

// BestBid returns the best bid of the latest snapshot.
func (m *Market) BestBid() (int64, bool) {
	if len(m.Bids) == 0 {
		return 0, false
	}
	return m.Bids[0].GetPrice(), true
}

// BestAsk returns the best ask of the latest snapshot.
func (m *Market) BestAsk() (int64, bool) {
	if len(m.Asks) == 0 {
		return 0, false
	}
	return m.Asks[0].GetPrice(), true
}

// Mid returns the mid of the latest snapshot, or the last trade price.
func (m *Market) Mid() float64 {
	b, okb := m.BestBid()
	a, oka := m.BestAsk()
	if okb && oka {
		return float64(a+b) / 2
	}
	return float64(m.LastPrice)
}

// MeanLevelQty is the mean quantity of the top n levels on one side.
func (m *Market) MeanLevelQty(side twpb.Side, n int) float64 {
	levels := m.Bids
	if side == twpb.Side_SIDE_SELL {
		levels = m.Asks
	}
	if len(levels) > n {
		levels = levels[:n]
	}
	if len(levels) == 0 {
		return 0
	}
	var sum int64
	for _, l := range levels {
		sum += l.GetQuantity()
	}
	return float64(sum) / float64(len(levels))
}

// volatilityInterval is the return interval used to estimate volatility.
// One-second returns of a mid are dominated by quotes flickering around the
// spread, which mean-reverts and so overstates volatility over longer
// horizons; thirty-second returns mostly average that out.
const volatilityInterval = 30 * int64(1e9)

// Volatility estimates the mid's volatility per square-root second over
// [from, to): the standard deviation of thirty-second log returns, divided
// by the square root of thirty. It reports false with fewer than ten
// returns.
func (m *Market) Volatility(from, to int64) (float64, bool) {
	var n int
	var sum, sumSq float64
	prev, prevT := 0.0, int64(0)
	for _, s := range m.samples {
		if s.t < from || s.t >= to || s.mid <= 0 {
			continue
		}
		if prev == 0 {
			prev, prevT = s.mid, s.t
			continue
		}
		if s.t-prevT < volatilityInterval {
			continue
		}
		r := math.Log(s.mid / prev)
		sum += r
		sumSq += r * r
		n++
		prev, prevT = s.mid, s.t
	}
	if n < 10 {
		return 0, false
	}
	mean := sum / float64(n)
	return math.Sqrt(math.Max(0, sumSq/float64(n)-mean*mean)) / math.Sqrt(30), true
}

// context returns the price samples within [from, to], at most max of them
// (evenly thinned), for an alert's chart.
func (m *Market) context(from, to int64, max int) []*twpb.PricePoint {
	var in []sample
	for _, s := range m.samples {
		if s.t >= from && s.t <= to {
			in = append(in, s)
		}
	}
	step := 1
	if len(in) > max {
		step = (len(in) + max - 1) / max
	}
	out := make([]*twpb.PricePoint, 0, len(in)/step+1)
	for i := 0; i < len(in); i += step {
		s := in[i]
		out = append(out, &twpb.PricePoint{
			EventTimeNs: s.t, LastPrice: s.last, BestBid: s.bid, BestAsk: s.ask,
			BidSize: s.bidSize, AskSize: s.askSize,
		})
	}
	return out
}

// applySnapshot replaces the depth view and records a one-second sample.
func (m *Market) applySnapshot(b *twpb.BookSnapshot, now int64) {
	m.Bids, m.Asks, m.SnapshotTime = b.GetBids(), b.GetAsks(), now
	if b.GetPhase() == twpb.TradingPhase_TRADING_PHASE_CLOSING && m.Phase != twpb.TradingPhase_TRADING_PHASE_CLOSING {
		m.ClosingStart = now
	}
	m.Phase = b.GetPhase()
	if n := len(m.samples); n > 0 && now-m.samples[n-1].t < int64(1e9) {
		return
	}
	s := sample{t: now, mid: m.Mid(), last: m.LastPrice}
	if len(m.Bids) > 0 {
		s.bid, s.bidSize = m.Bids[0].GetPrice(), m.Bids[0].GetQuantity()
	}
	if len(m.Asks) > 0 {
		s.ask, s.askSize = m.Asks[0].GetPrice(), m.Asks[0].GetQuantity()
	}
	m.samples = append(m.samples, s)
}

// remember records a new order with its depth context.
func (m *Market) remember(o *OrderInfo, maxOrders int) {
	if mean := m.MeanLevelQty(o.Side, 5); mean > 0 {
		o.DepthRatio = float64(o.Qty) / mean
	}
	if o.Type == twpb.OrderType_ORDER_TYPE_LIMIT {
		b, okb := m.BestBid()
		a, oka := m.BestAsk()
		o.InsideSpread = okb && oka && o.Price > b && o.Price < a
	}
	if _, exists := m.orders[o.ID]; !exists {
		m.orderQueue = append(m.orderQueue, o.ID)
	}
	m.orders[o.ID] = o
	for len(m.orders) > maxOrders {
		m.evictOldest()
	}
}

func (m *Market) evictOldest() {
	for m.queueHead < len(m.orderQueue) {
		id := m.orderQueue[m.queueHead]
		m.queueHead++
		if _, ok := m.orders[id]; ok {
			delete(m.orders, id)
			break
		}
	}
	m.compactQueue()
}

func (m *Market) compactQueue() {
	if m.queueHead > 1024 && m.queueHead*2 > len(m.orderQueue) {
		m.orderQueue = append([]uint64(nil), m.orderQueue[m.queueHead:]...)
		m.queueHead = 0
	}
}

// expire forgets orders entered before cutoff and samples older than
// sampleCutoff. It returns the number of orders evicted.
func (m *Market) expire(cutoff, sampleCutoff int64) int {
	n := 0
	for m.queueHead < len(m.orderQueue) {
		id := m.orderQueue[m.queueHead]
		o, ok := m.orders[id]
		if ok && o.Entry >= cutoff {
			break
		}
		if ok {
			delete(m.orders, id)
			n++
		}
		m.queueHead++
	}
	m.compactQueue()
	i := 0
	for i < len(m.samples) && m.samples[i].t < sampleCutoff {
		i++
	}
	if i > 0 {
		m.samples = append(m.samples[:0], m.samples[i:]...)
	}
	return n
}
