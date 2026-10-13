package sim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
)

// bookImbalance returns (bidDepth - askDepth) / (bidDepth + askDepth) over
// the top levels, in [-1, 1].
func bookImbalance(b *ob.Book, levels int) float64 {
	var bid, ask int64
	for _, l := range b.Depth(ob.Buy, levels) {
		bid += l.Qty
	}
	for _, l := range b.Depth(ob.Sell, levels) {
		ask += l.Qty
	}
	if bid+ask == 0 {
		return 0
	}
	return float64(bid-ask) / float64(bid+ask)
}

// passivePrice clamps a limit price so that it does not cross the opposite
// side of the book.
func passivePrice(b *ob.Book, side ob.Side, price, tick int64) int64 {
	if side == ob.Buy {
		if a, ok := b.BestAsk(); ok && price >= a.Price {
			price = a.Price - tick
		}
	} else if bb, ok := b.BestBid(); ok && price <= bb.Price {
		price = bb.Price + tick
	}
	return price
}

// marketableLimit returns a limit price that crosses the spread by up to
// `through` ticks, or false when the opposite side is empty.
func marketableLimit(b *ob.Book, side ob.Side, through, tick int64) (int64, bool) {
	if side == ob.Buy {
		a, ok := b.BestAsk()
		return a.Price + through*tick, ok
	}
	bb, ok := b.BestBid()
	price := bb.Price - through*tick
	if price < tick {
		price = tick
	}
	return price, ok
}

func randomSide(r *rand.Rand) ob.Side {
	if r.IntN(2) == 0 {
		return ob.Buy
	}
	return ob.Sell
}

// marketMaker continuously quotes both sides of one symbol around its fair
// value. Quotes are shaded towards book imbalance (so displayed depth moves
// prices, which is what spoofers exploit) and skewed against inventory.
// Market makers re-quote often, so their cancel and replace counts are high.
// That is a legitimate behaviour the detectors must not flag.
type marketMaker struct {
	acct       *Account
	sym        *Symbol
	rng        *rand.Rand
	wake       int64
	halfSpread float64
	size       float64
	maxInv     int64
	inventory  int64
	bid, ask   uint64
}

func newMarketMaker(s *Sim, sym *Symbol, acct *Account, p tierParams) *marketMaker {
	return &marketMaker{
		acct: acct, sym: sym,
		rng:        streamRNG(s.cfg.Seed, "mm/"+acct.ID+"/"+sym.Symbol),
		wake:       int64(float64(p.mmWake) / s.cfg.Activity),
		halfSpread: p.mmHalfSpread, size: p.mmSize, maxInv: p.mmMaxInventory,
	}
}

func (m *marketMaker) Start(s *Sim) {
	s.Schedule(s.session.Open+int64(m.rng.Float64()*float64(time.Second)), m)
}

func (m *marketMaker) Wake(s *Sim) int64 {
	if s.closed {
		return -1
	}
	tick := m.sym.TickSize
	imb := bookImbalance(m.sym.Book, 5)
	inv := float64(m.inventory) / float64(m.maxInv)
	center := m.sym.fair + (1.5*imb-2.0*inv)*float64(tick)
	half := m.halfSpread * float64(tick) * (0.8 + 0.4*m.rng.Float64())
	bidPx := floorTick(center-half, tick)
	askPx := ceilTick(center+half, tick)
	if askPx-bidPx < tick {
		askPx = bidPx + tick
	}
	bidQty := roundQty(lognormal(m.rng, m.size, 0.3), 100)
	askQty := roundQty(lognormal(m.rng, m.size, 0.3), 100)
	canBid, canAsk := m.inventory < m.maxInv, m.inventory > -m.maxInv

	// Move the side that moves away from the spread first, so a re-quote
	// never runs into this maker's own opposite quote.
	current := int64(0)
	if o, ok := m.sym.Book.Order(m.bid); ok {
		current = o.Price
	}
	if bidPx > current {
		m.ask = m.requote(s, ob.Sell, m.ask, askPx, askQty, canAsk)
		m.bid = m.requote(s, ob.Buy, m.bid, bidPx, bidQty, canBid)
	} else {
		m.bid = m.requote(s, ob.Buy, m.bid, bidPx, bidQty, canBid)
		m.ask = m.requote(s, ob.Sell, m.ask, askPx, askQty, canAsk)
	}
	return s.now + expNs(m.rng, m.wake)
}

func (m *marketMaker) requote(s *Sim, side ob.Side, id uint64, price, qty int64, enabled bool) uint64 {
	book := m.sym.Book
	o, resting := book.Order(id)
	if !enabled {
		if resting {
			_ = s.Cancel(m, m.sym, m.acct.ID, id)
		}
		return 0
	}
	price = passivePrice(book, side, price, m.sym.TickSize)
	if price <= 0 {
		return id
	}
	if resting {
		if o.Price == price && 2*o.Qty >= qty {
			return id
		}
		if err := s.Replace(m, m.sym, m.acct.ID, id, price, qty); err != nil {
			return id
		}
		if _, still := book.Order(id); still {
			return id
		}
		return 0
	}
	nid, err := s.Submit(m, m.sym, ob.Request{Account: m.acct.ID, Side: side, Type: ob.Limit, TIF: ob.Day, Price: price, Qty: qty})
	if err != nil {
		return 0
	}
	if _, ok := book.Order(nid); ok {
		return nid
	}
	return 0
}

func (m *marketMaker) OnFill(_ *Sim, f Fill) {
	if f.Side == ob.Buy {
		m.inventory += f.Qty
	} else {
		m.inventory -= f.Qty
	}
}

// noiseTrader models uninformed retail flow on one symbol: Poisson arrivals
// from a large pool of accounts. A bit under half the orders are marketable;
// the rest are passive limit orders that are cancelled after a random
// lifetime if they have not filled.
type noiseTrader struct {
	sym        *Symbol
	accounts   []*Account
	rng        *rand.Rand
	arrival    int64
	medianSize float64
	resting    []noiseOrder
}

type noiseOrder struct {
	id      uint64
	account string
	expires int64
}

func newNoiseTrader(s *Sim, sym *Symbol, accounts []*Account, p tierParams) *noiseTrader {
	return &noiseTrader{
		sym: sym, accounts: accounts,
		rng:        streamRNG(s.cfg.Seed, "noise/"+sym.Symbol),
		arrival:    int64(float64(p.noiseArrival) / s.cfg.Activity),
		medianSize: p.noiseMedianSize,
	}
}

func (n *noiseTrader) Start(s *Sim) {
	s.Schedule(s.session.Open+int64(5*time.Second)+expNs(n.rng, n.arrival), n)
}

func (n *noiseTrader) Wake(s *Sim) int64 {
	if s.closed {
		return -1
	}
	n.cancelExpired(s)
	book, tick := n.sym.Book, n.sym.TickSize
	acct := n.accounts[n.rng.IntN(len(n.accounts))]
	side := randomSide(n.rng)
	qty := roundQty(lognormal(n.rng, n.medianSize, 0.9), 10)

	if n.rng.Float64() < 0.45 {
		if n.rng.Float64() < 0.3 {
			_, _ = s.Submit(n, n.sym, ob.Request{Account: acct.ID, Side: side, Type: ob.Market, TIF: ob.IOC, Qty: qty})
		} else if price, ok := marketableLimit(book, side, int64(n.rng.IntN(2)), tick); ok {
			_, _ = s.Submit(n, n.sym, ob.Request{Account: acct.ID, Side: side, Type: ob.Limit, TIF: ob.IOC, Price: price, Qty: qty})
		}
		return s.now + expNs(n.rng, n.arrival)
	}

	// Passive: join, improve by a tick or sit a few ticks behind the touch.
	var ref int64
	if side == ob.Buy {
		if l, ok := book.BestBid(); ok {
			ref = l.Price
		} else {
			ref = floorTick(n.sym.fair-float64(2*tick), tick)
		}
	} else if l, ok := book.BestAsk(); ok {
		ref = l.Price
	} else {
		ref = ceilTick(n.sym.fair+float64(2*tick), tick)
	}
	offset := int64(n.rng.IntN(5)-1) * tick // -1..3 ticks behind
	price := ref - offset
	if side == ob.Sell {
		price = ref + offset
	}
	price = passivePrice(book, side, price, tick)
	if price > 0 {
		id, err := s.Submit(n, n.sym, ob.Request{Account: acct.ID, Side: side, Type: ob.Limit, TIF: ob.Day, Price: price, Qty: qty})
		if _, ok := book.Order(id); err == nil && ok {
			n.resting = append(n.resting, noiseOrder{id: id, account: acct.ID, expires: s.now + expNs(n.rng, int64(45*time.Second))})
		}
	}
	return s.now + expNs(n.rng, n.arrival)
}

func (n *noiseTrader) cancelExpired(s *Sim) {
	kept := n.resting[:0]
	for _, o := range n.resting {
		if _, ok := n.sym.Book.Order(o.id); !ok {
			continue
		}
		if s.now >= o.expires {
			_ = s.Cancel(n, n.sym, o.account, o.id)
			continue
		}
		kept = append(kept, o)
	}
	n.resting = kept
}

// parentOrder is an institutional order worked over a time window with a
// TWAP or VWAP schedule. It posts passive child orders at the touch and
// crosses the spread when it falls behind schedule or a child has rested too
// long. A quarter of parents finish at the close, which produces legitimate
// aggressive flow in the closing window.
type parentOrder struct {
	sym          *Symbol
	acct         *Account
	rng          *rand.Rand
	side         ob.Side
	total        int64
	start, end   int64
	vwap         bool
	interval     int64
	childSize    int64
	patience     int64
	filled       int64
	working      uint64
	workingSince int64
	done         bool
	openTime     int64
	length       int64
}

func newParentOrder(s *Sim, sym *Symbol, acct *Account, k int, pick *rand.Rand, tp tierParams) *parentOrder {
	sess := s.session
	length := sess.Close - sess.Open
	dur := int64((0.15 + 0.45*pick.Float64()) * float64(length))
	start := sess.Open + int64(pick.Float64()*float64(length-dur))
	if pick.Float64() < 0.25 {
		start = sess.Close - dur
	}
	total := roundQty(lognormal(pick, tp.parentMedianSize, 0.5), 100)
	interval := int64(time.Duration(20+pick.IntN(40)) * time.Second)
	po := &parentOrder{
		sym: sym, acct: acct,
		rng:      streamRNG(s.cfg.Seed, fmt.Sprintf("parent/%s/%d", sym.Symbol, k)),
		side:     randomSide(pick),
		total:    total,
		start:    start,
		end:      start + dur,
		vwap:     pick.IntN(2) == 0,
		interval: interval,
		patience: interval / 2,
		openTime: sess.Open,
		length:   length,
	}
	po.childSize = max(100, roundQty(float64(total)*float64(interval)/float64(dur), 100))
	return po
}

func (p *parentOrder) Start(s *Sim) { s.Schedule(p.start, p) }

// volumeCurve is the cumulative intraday volume profile for a U-shaped
// density f(x) = 1 + 8(x-0.5)^2 on [0, 1], normalized to F(1) = 1.
func volumeCurve(x float64) float64 {
	x = math.Max(0, math.Min(1, x))
	return (x + (8.0/3.0)*(math.Pow(x-0.5, 3)+0.125)) / (5.0 / 3.0)
}

// due returns the quantity the schedule says should be done by now.
func (p *parentOrder) due(now int64) int64 {
	var progress float64
	if p.vwap {
		at := func(t int64) float64 { return volumeCurve(float64(t-p.openTime) / float64(p.length)) }
		progress = (at(now) - at(p.start)) / (at(p.end) - at(p.start))
	} else {
		progress = float64(now-p.start) / float64(p.end-p.start)
	}
	progress = math.Max(0, math.Min(1, progress))
	return int64(progress * float64(p.total))
}

func (p *parentOrder) Wake(s *Sim) int64 {
	if s.closed || p.done {
		return -1
	}
	if p.working != 0 {
		if _, ok := p.sym.Book.Order(p.working); !ok {
			p.working = 0
		}
	}
	remaining := p.total - p.filled
	if remaining <= 0 {
		p.finish(s)
		return -1
	}
	behind := p.due(s.now) - p.filled
	switch {
	case s.now >= p.end:
		p.cancelWorking(s)
		p.take(s, remaining)
		if s.now-p.end > int64(5*time.Minute) {
			p.finish(s)
			return -1
		}
		return s.now + int64(5*time.Second)
	case behind > 3*p.childSize || (p.working != 0 && s.now-p.workingSince > p.patience && behind > 0):
		p.cancelWorking(s)
		p.take(s, min(behind, 2*p.childSize, remaining))
	case p.working == 0 && behind > -p.childSize:
		p.post(s, min(max(behind, 0)+p.childSize, remaining))
	}
	return s.now + jitterNs(p.rng, p.interval, 0.3)
}

func (p *parentOrder) post(s *Sim, qty int64) {
	book, tick := p.sym.Book, p.sym.TickSize
	var price int64
	if p.side == ob.Buy {
		if l, ok := book.BestBid(); ok {
			price = l.Price
		} else {
			price = floorTick(p.sym.fair-float64(tick), tick)
		}
	} else if l, ok := book.BestAsk(); ok {
		price = l.Price
	} else {
		price = ceilTick(p.sym.fair+float64(tick), tick)
	}
	price = passivePrice(book, p.side, price, tick)
	if price <= 0 {
		return
	}
	id, err := s.Submit(p, p.sym, ob.Request{Account: p.acct.ID, Side: p.side, Type: ob.Limit, TIF: ob.Day, Price: price, Qty: qty})
	if _, ok := book.Order(id); err == nil && ok {
		p.working, p.workingSince = id, s.now
	}
}

func (p *parentOrder) take(s *Sim, qty int64) {
	if qty <= 0 {
		return
	}
	if price, ok := marketableLimit(p.sym.Book, p.side, 2, p.sym.TickSize); ok {
		_, _ = s.Submit(p, p.sym, ob.Request{Account: p.acct.ID, Side: p.side, Type: ob.Limit, TIF: ob.IOC, Price: price, Qty: qty})
	}
}

func (p *parentOrder) cancelWorking(s *Sim) {
	if p.working != 0 {
		_ = s.Cancel(p, p.sym, p.acct.ID, p.working)
		p.working = 0
	}
}

func (p *parentOrder) finish(s *Sim) {
	p.cancelWorking(s)
	p.done = true
}

func (p *parentOrder) OnFill(_ *Sim, f Fill) {
	p.filled += f.Qty
	if f.Leaves == 0 && f.OrderID == p.working {
		p.working = 0
	}
}

func (p *parentOrder) OnCancel(_ *Sim, c Cancel) {
	if c.OrderID == p.working {
		p.working = 0
	}
}

// momentumTrader is a proprietary trend follower. When the mid price has
// moved by more than a volatility-scaled threshold over its lookback, it
// joins the move with an aggressive order and exits after a holding period.
// It reacts to moves rather than causing them, so it is a legitimate
// lookalike of momentum ignition: one account trading aggressively in one
// direction and then the other.
type momentumTrader struct {
	sym       *Symbol
	acct      *Account
	rng       *rand.Rand
	threshold float64
	size      int64
	hold      int64
	mids      []int64 // one sample per second, oldest first
	position  int64
	exitAt    int64
	cooldown  int64
}

const momentumLookback = 30 // seconds

func newMomentumTrader(s *Sim, sym *Symbol, acct *Account, p tierParams) *momentumTrader {
	sigma := sym.DailyVol * math.Sqrt(momentumLookback/float64(tradingSecondsPerDay))
	minMove := 3 * float64(sym.TickSize) / float64(sym.RefPrice)
	return &momentumTrader{
		sym: sym, acct: acct,
		rng:       streamRNG(s.cfg.Seed, "momentum/"+acct.ID+"/"+sym.Symbol),
		threshold: math.Max(5*sigma, minMove),
		size:      p.momentumSize,
		hold:      int64(2 * time.Minute),
	}
}

func (m *momentumTrader) Start(s *Sim) {
	s.Schedule(s.session.Open+int64(time.Second)+int64(m.rng.Float64()*float64(time.Second)), m)
}

func (m *momentumTrader) Wake(s *Sim) int64 {
	if s.closed {
		return -1
	}
	book := m.sym.Book
	b, okb := book.BestBid()
	a, oka := book.BestAsk()
	if okb && oka {
		m.mids = append(m.mids, (b.Price+a.Price)/2)
		if len(m.mids) > momentumLookback+1 {
			m.mids = m.mids[1:]
		}
	}
	switch {
	case m.position != 0 && s.now >= m.exitAt:
		side := ob.Sell
		if m.position < 0 {
			side = ob.Buy
		}
		m.take(s, side, abs64(m.position))
		m.cooldown = s.now + int64(10*time.Minute)
	case m.position == 0 && s.now >= m.cooldown && len(m.mids) > momentumLookback:
		ret := float64(m.mids[len(m.mids)-1])/float64(m.mids[0]) - 1
		if math.Abs(ret) > m.threshold {
			side := ob.Buy
			if ret < 0 {
				side = ob.Sell
			}
			m.take(s, side, m.size)
			m.exitAt = s.now + expNs(m.rng, m.hold)
		}
	}
	return s.now + int64(time.Second)
}

func (m *momentumTrader) take(s *Sim, side ob.Side, qty int64) {
	if price, ok := marketableLimit(m.sym.Book, side, 3, m.sym.TickSize); ok {
		_, _ = s.Submit(m, m.sym, ob.Request{Account: m.acct.ID, Side: side, Type: ob.Limit, TIF: ob.IOC, Price: price, Qty: qty})
	}
}

func (m *momentumTrader) OnFill(_ *Sim, f Fill) {
	if f.Side == ob.Buy {
		m.position += f.Qty
	} else {
		m.position -= f.Qty
	}
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
