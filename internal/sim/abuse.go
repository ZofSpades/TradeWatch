package sim

import (
	"math"
	"strconv"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
)

// abuseSizes are the order sizes manipulators use on a symbol of a tier.
func abuseSizes(tier twpb.LiquidityTier) (trade float64, sweepCap int64) {
	switch tier {
	case twpb.LiquidityTier_LIQUIDITY_TIER_HIGH:
		return 800, 3_000
	case twpb.LiquidityTier_LIQUIDITY_TIER_MEDIUM:
		return 500, 2_000
	default:
		return 300, 1_000
	}
}

// --- Wash trading ------------------------------------------------------

// washEpisode trades a quantity back and forth between accounts that act
// for the same interest, so trades print with no change in beneficial
// ownership.
//
// Direct wash: the two accounts of one owner alternate as buyer and seller.
// Ring: three colluding accounts with *different* owners trade in a
// circle (A sells to B, B to C, C to A), which no single-owner check can
// see.
//
// Each leg posts a passive order at a price strictly inside the spread, so
// no other order rests at that level, and the other leg hits it a few
// milliseconds later with an IOC. Only trades with an episode account on
// both sides are labelled as wash trades.
type washEpisode struct {
	episode
	ring     bool
	legs     [][2]*Account // (seller, buyer)
	leg      int
	qty      int64
	gap      int64
	retries  int
	waiting  bool // the passive leg is resting, the aggressive leg is due
	passive  uint64
	post     *Account
	hit      *Account
	postSide ob.Side
	price    int64
	// legWash holds, per executed leg, the trades with an episode account on
	// both sides.
	legWash [][]uint64
	legMark int // len(trades) when the current leg started
}

func (w *washEpisode) Start(s *Sim) {}

func (w *washEpisode) begin(s *Sim) bool {
	var accounts []*Account
	if w.ring {
		for _, ring := range s.uni.Rings() {
			free := true
			for _, a := range ring {
				free = free && !s.busy[a.ID]
			}
			if free {
				accounts = ring
				break
			}
		}
		for c, cycles := 0, 2+w.rng.IntN(3); c < cycles; c++ {
			for i := range accounts {
				w.legs = append(w.legs, [2]*Account{accounts[i], accounts[(i+1)%len(accounts)]})
			}
		}
	} else {
		washers := s.uni.ByRole(RoleWasher)
		// A washer firm owns two consecutive accounts.
		var pairs [][2]*Account
		for i := 0; i+1 < len(washers); i += 2 {
			if !s.busy[washers[i].ID] && !s.busy[washers[i+1].ID] {
				pairs = append(pairs, [2]*Account{washers[i], washers[i+1]})
			}
		}
		if len(pairs) > 0 {
			p := pairs[w.rng.IntN(len(pairs))]
			accounts = p[:]
			for r, rounds := 0, 3+w.rng.IntN(6); r < rounds; r++ {
				if r%2 == 0 {
					w.legs = append(w.legs, [2]*Account{p[0], p[1]})
				} else {
					w.legs = append(w.legs, [2]*Account{p[1], p[0]})
				}
			}
		}
	}
	if !w.episode.begin(s, accounts) {
		return false
	}
	base, _ := abuseSizes(w.sym.Tier)
	w.qty = roundQty(lognormal(w.rng, base, 0.4), 100)
	w.gap = int64(4*time.Second) + w.rng.Int64N(int64(16*time.Second))
	w.params["legs"] = strconv.Itoa(len(w.legs))
	w.params["quantity"] = strconv.FormatInt(w.qty, 10)
	return true
}

func (w *washEpisode) Wake(s *Sim) int64 {
	if !w.started && !w.begin(s) {
		w.end(s)
		return -1
	}
	if s.closed || w.done {
		w.finish(s)
		return -1
	}
	book, tick := w.sym.Book, w.sym.TickSize
	if w.waiting {
		w.waiting = false
		w.legMark = len(w.trades)
		w.submit(s, w, w.hit, ob.Request{Side: w.postSide.Opposite(), Type: ob.Limit, TIF: ob.IOC, Price: w.price, Qty: w.qty})
		w.cancel(s, w, w.post, w.passive) // anything the IOC did not take
		var legTrades []uint64
		for _, id := range w.trades[w.legMark:] {
			if w.fills[id] == 2 {
				legTrades = append(legTrades, id)
			}
		}
		w.legWash = append(w.legWash, legTrades)
		w.leg++
		if w.leg == len(w.legs) {
			w.finish(s)
			return -1
		}
		return s.now + jitterNs(w.rng, w.gap, 0.4)
	}
	bid, okb := book.BestBid()
	ask, oka := book.BestAsk()
	if !okb || !oka || ask.Price-bid.Price < 2*tick {
		// No price strictly inside the spread: wait for it to widen.
		if w.retries++; w.retries > 60 {
			w.finish(s)
			return -1
		}
		return s.now + int64(2*time.Second)
	}
	inside := (ask.Price-bid.Price)/tick - 1
	w.price = bid.Price + tick*(1+w.rng.Int64N(inside))
	seller, buyer := w.legs[w.leg][0], w.legs[w.leg][1]
	w.post, w.hit, w.postSide = seller, buyer, ob.Sell
	if w.rng.IntN(2) == 0 {
		w.post, w.hit, w.postSide = buyer, seller, ob.Buy
	}
	id, ok := w.submit(s, w, w.post, ob.Request{Side: w.postSide, Type: ob.Limit, TIF: ob.Day, Price: w.price, Qty: w.qty})
	if !ok {
		w.finish(s)
		return -1
	}
	w.passive, w.waiting = id, true
	return s.now + int64(time.Millisecond) + w.rng.Int64N(int64(150*time.Millisecond))
}

func (w *washEpisode) OnFill(s *Sim, f Fill) { w.recordFill(s, f) }

func (w *washEpisode) finish(s *Sim) {
	if w.done {
		return
	}
	// A ring is only circular trading once a full cycle has executed, so
	// only complete cycles are labelled; each direct-wash leg is complete
	// on its own.
	cycle := 1
	if w.ring {
		cycle = len(w.accounts)
	}
	var wash []uint64
	for c := 0; c+cycle <= len(w.legWash); c += cycle {
		complete := true
		for _, lt := range w.legWash[c : c+cycle] {
			complete = complete && len(lt) > 0
		}
		if complete {
			for _, lt := range w.legWash[c : c+cycle] {
				wash = append(wash, lt...)
			}
		}
	}
	if len(wash) > 0 {
		w.params["wash_trades"] = strconv.Itoa(len(wash))
		w.label(s, wash)
	}
	w.end(s)
}

// --- Spoofing / layering ----------------------------------------------

// spoofEpisode places a genuine order on one side, then several large
// orders just behind the touch on the other side. Market makers shade their
// quotes towards displayed depth, so the layers make the genuine order more
// likely to execute at a good price. As soon as it executes, the layers are
// cancelled within a few hundred milliseconds.
type spoofEpisode struct {
	episode
	acct     *Account
	genuine  uint64
	side     ob.Side // side of the genuine order
	layers   []uint64
	stage    int
	placedAt int64
	filledAt int64
	reaction int64
	maxWait  int64
}

func (sp *spoofEpisode) Start(s *Sim) {}

func (sp *spoofEpisode) Wake(s *Sim) int64 {
	if !sp.started {
		sp.acct = sp.pick(s, RoleSpoofer)
		if sp.acct == nil || !sp.begin(s, []*Account{sp.acct}) {
			sp.end(s)
			return -1
		}
		sp.side = randomSide(sp.rng)
		sp.reaction = int64(50*time.Millisecond) + sp.rng.Int64N(int64(350*time.Millisecond))
		sp.maxWait = int64(120 * time.Second)
	}
	if s.closed || sp.done {
		sp.finish(s, "session_closed")
		return -1
	}
	book, tick := sp.sym.Book, sp.sym.TickSize
	switch sp.stage {
	case 0: // genuine order, joining the touch on its side
		touch, ok := book.BestAsk()
		if sp.side == ob.Buy {
			touch, ok = book.BestBid()
		}
		if !ok {
			sp.finish(s, "no_market")
			return -1
		}
		base, _ := abuseSizes(sp.sym.Tier)
		qty := roundQty(lognormal(sp.rng, base/2, 0.3), 100)
		id, ok := sp.submit(s, sp, sp.acct, ob.Request{Side: sp.side, Type: ob.Limit, TIF: ob.Day, Price: touch.Price, Qty: qty})
		if !ok {
			sp.finish(s, "rejected")
			return -1
		}
		sp.genuine = id
		sp.params["genuine_side"] = sp.side.String()
		sp.params["genuine_quantity"] = strconv.FormatInt(qty, 10)
		sp.stage = 1
		return s.now + int64(100*time.Millisecond) + sp.rng.Int64N(int64(200*time.Millisecond))
	case 1: // layers on the other side, one to five ticks behind its touch
		layerSide := sp.side.Opposite()
		touch, ok := book.BestBid()
		if layerSide == ob.Sell {
			touch, ok = book.BestAsk()
		}
		if !ok {
			sp.finish(s, "no_market")
			return -1
		}
		n := 3 + sp.rng.IntN(3)
		multiple := 5 + 7*sp.rng.Float64()
		size := max(1_000, roundQty(multiple*float64(typicalLevelQty(book, layerSide, 5, 300)), 100))
		for i := 1; i <= n; i++ {
			price := touch.Price - int64(i)*tick
			if layerSide == ob.Sell {
				price = touch.Price + int64(i)*tick
			}
			if price <= 0 {
				break
			}
			if id, ok := sp.submit(s, sp, sp.acct, ob.Request{Side: layerSide, Type: ob.Limit, TIF: ob.Day, Price: price, Qty: size}); ok {
				sp.layers = append(sp.layers, id)
			}
		}
		sp.params["layers"] = strconv.Itoa(len(sp.layers))
		sp.params["layer_quantity"] = strconv.FormatInt(size, 10)
		sp.params["size_multiple"] = strconv.FormatFloat(math.Round(multiple*10)/10, 'f', 1, 64)
		sp.placedAt = s.now
		sp.stage = 2
		return s.now + int64(200*time.Millisecond)
	case 2: // wait for the genuine order to execute
		switch {
		case sp.filledAt > 0:
			sp.stage = 3
			return max(s.now, sp.filledAt+sp.reaction)
		case s.now-sp.placedAt > sp.maxWait:
			sp.stage = 3
			return s.now
		}
		return s.now + int64(200*time.Millisecond)
	default: // pull everything
		for _, id := range sp.layers {
			sp.cancel(s, sp, sp.acct, id)
		}
		sp.cancel(s, sp, sp.acct, sp.genuine)
		outcome := "not_executed"
		if sp.filledAt > 0 {
			outcome = "executed"
			sp.params["cancel_latency_ms"] = ms(s.now - sp.filledAt)
		}
		sp.finish(s, outcome)
		return -1
	}
}

func (sp *spoofEpisode) OnFill(s *Sim, f Fill) {
	sp.recordFill(s, f)
	if f.OrderID == sp.genuine && sp.filledAt == 0 {
		sp.filledAt = s.now
	}
}

func (sp *spoofEpisode) finish(s *Sim, outcome string) {
	if sp.done {
		return
	}
	if len(sp.layers) > 0 {
		sp.params["outcome"] = outcome
		sp.label(s, sp.trades)
	}
	sp.end(s)
}

// --- Momentum ignition / pump and dump ---------------------------------

// momentumEpisode sweeps the book aggressively in one direction to start a
// price move (which momentum traders then follow), holds briefly, then
// trades out of the position in the other direction.
type momentumEpisode struct {
	episode
	acct      *Account
	dir       ob.Side
	stage     int
	orders    int
	maxOrders int
	interval  int64
	target    float64
	startMid  float64
	peakMove  float64
	position  int64
	holdUntil int64
	dumpStart int64
}

func (m *momentumEpisode) Start(s *Sim) {}

func (m *momentumEpisode) Wake(s *Sim) int64 {
	if !m.started {
		m.acct = m.pick(s, RoleMomentumIgniter)
		mid, ok := midPrice(m.sym.Book)
		if m.acct == nil || !ok || !m.begin(s, []*Account{m.acct}) {
			m.end(s)
			return -1
		}
		m.dir = randomSide(m.rng)
		m.startMid = mid
		m.maxOrders = 6 + m.rng.IntN(9)
		m.interval = int64(500*time.Millisecond) + m.rng.Int64N(int64(2*time.Second))
		// Target at least six ticks, and 0.4%-1.2% of the price.
		m.target = math.Max(6*float64(m.sym.TickSize)/mid, 0.004+0.008*m.rng.Float64())
		m.params["direction"] = m.dir.String()
		m.params["target_move_bps"] = strconv.Itoa(int(m.target * 1e4))
	}
	if s.closed || m.done {
		m.finish(s)
		return -1
	}
	book, tick := m.sym.Book, m.sym.TickSize
	_, sweepCap := abuseSizes(m.sym.Tier)
	if mid, ok := midPrice(book); ok {
		move := (mid/m.startMid - 1)
		if m.dir == ob.Sell {
			move = -move
		}
		m.peakMove = math.Max(m.peakMove, move)
	}
	switch m.stage {
	case 0: // ignition: take the best two levels repeatedly
		opp := m.dir.Opposite()
		var qty int64
		for _, l := range book.Depth(opp, 2) {
			qty += l.Qty
		}
		if price, ok := marketableLimit(book, m.dir, 2, tick); ok && qty > 0 {
			m.submit(s, m, m.acct, ob.Request{Side: m.dir, Type: ob.Limit, TIF: ob.IOC, Price: price, Qty: min(qty, sweepCap)})
		}
		m.orders++
		if m.peakMove >= m.target || m.orders >= m.maxOrders {
			m.stage = 1
			m.holdUntil = s.now + int64(10*time.Second) + m.rng.Int64N(int64(35*time.Second))
			return m.holdUntil
		}
		return s.now + jitterNs(m.rng, m.interval, 0.3)
	case 1: // hold while followers pile in
		m.stage = 2
		m.dumpStart = s.now
		fallthrough
	default: // dump: trade out of the position
		if m.position == 0 || s.now-m.dumpStart > int64(3*time.Minute) {
			m.finish(s)
			return -1
		}
		side := ob.Sell
		if m.position < 0 {
			side = ob.Buy
		}
		if price, ok := marketableLimit(book, side, 2, tick); ok {
			m.submit(s, m, m.acct, ob.Request{Side: side, Type: ob.Limit, TIF: ob.IOC, Price: price, Qty: min(abs64(m.position), sweepCap)})
		}
		return s.now + int64(time.Second) + m.rng.Int64N(int64(3*time.Second))
	}
}

func (m *momentumEpisode) OnFill(s *Sim, f Fill) {
	m.recordFill(s, f)
	if f.Side == ob.Buy {
		m.position += f.Qty
	} else {
		m.position -= f.Qty
	}
}

func (m *momentumEpisode) finish(s *Sim) {
	if m.done {
		return
	}
	if len(m.trades) > 0 {
		m.params["peak_move_bps"] = strconv.Itoa(int(m.peakMove * 1e4))
		m.params["ignition_orders"] = strconv.Itoa(m.orders)
		m.label(s, m.trades)
	}
	m.end(s)
}

// --- Quote stuffing ----------------------------------------------------

// stuffingEpisode floods the book with hundreds of orders per second, each
// cancelled milliseconds later, priced away from the touch so they never
// trade. The purpose is to congest the venue and other participants' feeds.
type stuffingEpisode struct {
	episode
	acct     *Account
	until    int64
	interval int64
	last     uint64
	sent     int
}

func (q *stuffingEpisode) Start(s *Sim) {}

func (q *stuffingEpisode) Wake(s *Sim) int64 {
	if !q.started {
		q.acct = q.pick(s, RoleQuoteStuffer)
		if q.acct == nil || !q.begin(s, []*Account{q.acct}) {
			q.end(s)
			return -1
		}
		duration := int64(2*time.Second) + q.rng.Int64N(int64(4*time.Second))
		rate := 150 + q.rng.Float64()*250 // messages per second
		q.until = s.now + duration
		// Each wake sends one order and one cancel: two messages.
		q.interval = int64(2 / rate * float64(time.Second))
		q.params["target_rate_per_s"] = strconv.Itoa(int(rate))
		q.params["duration_ms"] = ms(duration)
	}
	if s.closed || q.done || s.now >= q.until {
		q.cancel(s, q, q.acct, q.last)
		q.finish(s)
		return -1
	}
	q.cancel(s, q, q.acct, q.last)
	side := randomSide(q.rng)
	book, tick := q.sym.Book, q.sym.TickSize
	// Price a few ticks behind the touch; if that side of the book is empty,
	// behind the fair value instead, so the burst keeps its rate.
	ref := floorTick(q.sym.fair, tick)
	if side == ob.Buy {
		if l, ok := book.BestBid(); ok {
			ref = l.Price
		}
	} else if l, ok := book.BestAsk(); ok {
		ref = l.Price
	}
	k := int64(3 + q.rng.IntN(6))
	price := ref - k*tick
	if side == ob.Sell {
		price = ref + k*tick
	}
	if price = passivePrice(book, side, price, tick); price > 0 {
		q.last, _ = q.submit(s, q, q.acct, ob.Request{Side: side, Type: ob.Limit, TIF: ob.Day, Price: price, Qty: roundQty(100+400*q.rng.Float64(), 100)})
		q.sent++
	}
	return s.now + jitterNs(q.rng, q.interval, 0.2)
}

func (q *stuffingEpisode) OnFill(s *Sim, f Fill) { q.recordFill(s, f) }

func (q *stuffingEpisode) finish(s *Sim) {
	if q.done {
		return
	}
	if q.sent > 0 {
		q.params["orders"] = strconv.Itoa(q.sent)
		q.label(s, q.trades)
	}
	q.end(s)
}

// --- Marking the close -------------------------------------------------

// closeEpisode trades aggressively in one direction during the last minutes
// before the close to move the closing price, and does not unwind.
type closeEpisode struct {
	episode
	acct     *Account
	dir      ob.Side
	orders   int
	n        int
	interval int64
	startMid float64
}

func (c *closeEpisode) Start(s *Sim) {}

func (c *closeEpisode) Wake(s *Sim) int64 {
	if !c.started {
		c.acct = c.pick(s, RoleCloseMarker)
		mid, ok := midPrice(c.sym.Book)
		if c.acct == nil || !ok || !c.begin(s, []*Account{c.acct}) {
			c.end(s)
			return -1
		}
		c.dir = ob.Buy
		if c.rng.Float64() < 0.4 {
			c.dir = ob.Sell
		}
		c.startMid = mid
		c.n = 6 + c.rng.IntN(10)
		c.interval = max(int64(time.Second), (s.session.Close-s.now-int64(3*time.Second))/int64(c.n))
		c.params["direction"] = c.dir.String()
	}
	if s.closed || c.done || s.now >= s.session.Close-int64(2*time.Second) {
		c.finish(s)
		return -1
	}
	book, tick := c.sym.Book, c.sym.TickSize
	_, sweepCap := abuseSizes(c.sym.Tier)
	level, ok := book.BestAsk()
	if c.dir == ob.Sell {
		level, ok = book.BestBid()
	}
	if price, mok := marketableLimit(book, c.dir, 1, tick); ok && mok {
		c.submit(s, c, c.acct, ob.Request{Side: c.dir, Type: ob.Limit, TIF: ob.IOC, Price: price, Qty: min(max(level.Qty, 100), sweepCap)})
	}
	c.orders++
	if c.orders >= c.n {
		c.finish(s)
		return -1
	}
	return s.now + jitterNs(c.rng, c.interval, 0.3)
}

func (c *closeEpisode) OnFill(s *Sim, f Fill) { c.recordFill(s, f) }

func (c *closeEpisode) finish(s *Sim) {
	if c.done {
		return
	}
	if len(c.trades) > 0 {
		if mid, ok := midPrice(c.sym.Book); ok {
			move := mid/c.startMid - 1
			if c.dir == ob.Sell {
				move = -move
			}
			c.params["move_bps"] = strconv.Itoa(int(move * 1e4))
		}
		c.params["orders"] = strconv.Itoa(c.orders)
		c.label(s, c.trades)
	}
	c.end(s)
}
