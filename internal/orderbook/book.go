package orderbook

import "fmt"

// Config holds the per-symbol trading parameters of a Book.
type Config struct {
	Symbol string
	// TickSize is the minimum price increment, in price units. Must be > 0.
	TickSize int64
	// LotSize is the minimum quantity increment, in shares. Must be > 0.
	LotSize int64
	// MaxQty bounds the quantity of a single order. Zero means 1e9.
	MaxQty int64
	// MaxPrice bounds limit prices. Zero means 1e12 price units.
	MaxPrice int64
}

const (
	defaultMaxQty   = 1_000_000_000
	defaultMaxPrice = 1_000_000_000_000
)

// Book is a single-symbol limit order book.
type Book struct {
	cfg    Config
	bids   bookSide
	asks   bookSide
	orders map[uint64]*order

	seq         uint64
	lastOrderID uint64
	lastTradeID uint64
	priority    uint64

	lastPrice int64
	volume    int64
}

// New returns an empty book.
func New(cfg Config) (*Book, error) {
	if cfg.TickSize <= 0 {
		return nil, fmt.Errorf("orderbook: tick size must be positive, got %d", cfg.TickSize)
	}
	if cfg.LotSize <= 0 {
		return nil, fmt.Errorf("orderbook: lot size must be positive, got %d", cfg.LotSize)
	}
	if cfg.MaxQty == 0 {
		cfg.MaxQty = defaultMaxQty
	}
	if cfg.MaxPrice == 0 {
		cfg.MaxPrice = defaultMaxPrice
	}
	return &Book{
		cfg:    cfg,
		bids:   newBookSide(true),
		asks:   newBookSide(false),
		orders: make(map[uint64]*order),
	}, nil
}

// Config returns the book's configuration.
func (b *Book) Config() Config { return b.cfg }

// Seq returns the sequence number of the most recent event (0 if none).
func (b *Book) Seq() uint64 { return b.seq }

// LastPrice returns the price of the most recent trade, or 0.
func (b *Book) LastPrice() int64 { return b.lastPrice }

// Volume returns the total traded quantity.
func (b *Book) Volume() int64 { return b.volume }

// OpenOrders returns the number of resting orders.
func (b *Book) OpenOrders() int { return len(b.orders) }

// BestBid returns the best bid level.
func (b *Book) BestBid() (Level, bool) { return bestOf(&b.bids) }

// BestAsk returns the best ask level.
func (b *Book) BestAsk() (Level, bool) { return bestOf(&b.asks) }

func bestOf(s *bookSide) (Level, bool) {
	l := s.best()
	if l == nil {
		return Level{}, false
	}
	return Level{Price: l.price, Qty: l.qty, Count: l.count}, true
}

// Depth returns up to n aggregated levels of the given side, best first.
func (b *Book) Depth(side Side, n int) []Level {
	return b.sideOf(side).top(n)
}

// Order returns a resting order by ID.
func (b *Book) Order(id uint64) (OrderView, bool) {
	o, ok := b.orders[id]
	if !ok {
		return OrderView{}, false
	}
	return OrderView{ID: o.id, Account: o.account, Side: o.side, Price: o.price, Qty: o.qty, EntryTime: o.entryTime}, true
}

func (b *Book) sideOf(s Side) *bookSide {
	if s == Buy {
		return &b.bids
	}
	return &b.asks
}

func (b *Book) nextSeq() uint64 {
	b.seq++
	return b.seq
}

func (b *Book) validatePrice(p int64) error {
	switch {
	case p <= 0 || p > b.cfg.MaxPrice:
		return ErrInvalidPrice
	case p%b.cfg.TickSize != 0:
		return ErrTickSize
	}
	return nil
}

func (b *Book) validateQty(q int64) error {
	switch {
	case q <= 0 || q > b.cfg.MaxQty:
		return ErrInvalidQuantity
	case q%b.cfg.LotSize != 0:
		return ErrLotSize
	}
	return nil
}

func (b *Book) validate(r Request) error {
	if r.Side != Buy && r.Side != Sell {
		return ErrInvalidSide
	}
	switch r.Type {
	case Limit:
		if r.TIF != Day && r.TIF != IOC && r.TIF != FOK {
			return ErrInvalidTIF
		}
		if err := b.validatePrice(r.Price); err != nil {
			return err
		}
	case Market:
		// Market orders never rest, so DAY is meaningless for them.
		if r.TIF != IOC && r.TIF != FOK {
			return ErrInvalidTIF
		}
		if r.Price != 0 {
			return ErrInvalidPrice
		}
	default:
		return ErrInvalidType
	}
	return b.validateQty(r.Qty)
}

// Submit processes a new order at event time t. Resulting events are
// appended to dst, which is returned together with the assigned order ID.
func (b *Book) Submit(dst []Event, t int64, r Request) ([]Event, uint64, error) {
	if err := b.validate(r); err != nil {
		return dst, 0, err
	}
	b.lastOrderID++
	id := b.lastOrderID
	dst = append(dst, Event{
		Kind: EventAccepted, Seq: b.nextSeq(), Time: t,
		OrderID: id, Account: r.Account, Side: r.Side, Type: r.Type, TIF: r.TIF,
		Price: r.Price, Qty: r.Qty,
	})

	if r.TIF == FOK && !b.canFill(r.Side, r.Type, r.Price, r.Qty) {
		return b.appendCancel(dst, t, id, r.Account, r.Side, r.Price, r.Qty, ReasonFOKUnfilled, t), id, nil
	}

	var remaining int64
	dst, remaining = b.match(dst, t, id, r.Account, r.Side, r.Type, r.Price, r.Qty)
	if remaining == 0 {
		return dst, id, nil
	}
	switch {
	case r.Type == Market:
		dst = b.appendCancel(dst, t, id, r.Account, r.Side, r.Price, remaining, ReasonNoLiquidity, t)
	case r.TIF == IOC:
		dst = b.appendCancel(dst, t, id, r.Account, r.Side, r.Price, remaining, ReasonIOCRemainder, t)
	case r.TIF == FOK:
		// canFill guarantees a full fill; reaching this is a matching bug.
		panic("orderbook: FOK order partially filled")
	default:
		b.rest(id, r.Account, r.Side, r.Price, remaining, t)
	}
	return dst, id, nil
}

// Cancel removes a resting order on behalf of account.
func (b *Book) Cancel(dst []Event, t int64, account string, id uint64) ([]Event, error) {
	o, ok := b.orders[id]
	if !ok {
		return dst, ErrUnknownOrder
	}
	if o.account != account {
		return dst, ErrNotOwner
	}
	dst = b.appendCancel(dst, t, o.id, o.account, o.side, o.price, o.qty, ReasonUser, o.entryTime)
	b.unrest(o)
	return dst, nil
}

// Replace changes the price and/or open quantity of a resting order on behalf
// of account. newQty is the new open quantity.
func (b *Book) Replace(dst []Event, t int64, account string, id uint64, newPrice, newQty int64) ([]Event, error) {
	o, ok := b.orders[id]
	if !ok {
		return dst, ErrUnknownOrder
	}
	if o.account != account {
		return dst, ErrNotOwner
	}
	if err := b.validatePrice(newPrice); err != nil {
		return dst, err
	}
	if err := b.validateQty(newQty); err != nil {
		return dst, err
	}
	if newPrice == o.price && newQty == o.qty {
		return dst, ErrNoChange
	}

	dst = append(dst, Event{
		Kind: EventReplaced, Seq: b.nextSeq(), Time: t,
		OrderID: o.id, Account: o.account, Side: o.side, Type: Limit, TIF: Day,
		Price: newPrice, Qty: newQty, PrevPrice: o.price, PrevQty: o.qty,
	})

	if newPrice == o.price && newQty < o.qty {
		// Reducing size at the same price keeps time priority.
		o.level.qty -= o.qty - newQty
		o.qty = newQty
		return dst, nil
	}

	// Any other change loses priority: the order re-enters as if new, and
	// may trade if the new price crosses the book.
	account, side, entry := o.account, o.side, o.entryTime
	b.unrest(o)
	var remaining int64
	dst, remaining = b.match(dst, t, id, account, side, Limit, newPrice, newQty)
	if remaining > 0 {
		b.rest(id, account, side, newPrice, remaining, entry)
	}
	return dst, nil
}

// CloseSession cancels every resting order with ReasonSessionEnd. Bids are
// cancelled before asks, each side from the best price outward and each
// level in queue order.
func (b *Book) CloseSession(dst []Event, t int64) []Event {
	for _, s := range []*bookSide{&b.bids, &b.asks} {
		for i := len(s.levels) - 1; i >= 0; i-- {
			for o := s.levels[i].head; o != nil; o = o.next {
				dst = b.appendCancel(dst, t, o.id, o.account, o.side, o.price, o.qty, ReasonSessionEnd, o.entryTime)
				delete(b.orders, o.id)
			}
		}
		*s = newBookSide(s.bid)
	}
	return dst
}

// Snapshot appends an EventSnapshot with up to depth levels per side.
func (b *Book) Snapshot(dst []Event, t int64, depth int) []Event {
	return append(dst, Event{
		Kind: EventSnapshot, Seq: b.nextSeq(), Time: t,
		Bids: b.bids.top(depth), Asks: b.asks.top(depth),
		LastPrice: b.lastPrice, Volume: b.volume,
	})
}

func (b *Book) appendCancel(dst []Event, t int64, id uint64, account string, side Side, price, qty int64, reason CancelReason, entry int64) []Event {
	return append(dst, Event{
		Kind: EventCancelled, Seq: b.nextSeq(), Time: t,
		OrderID: id, Account: account, Side: side, Price: price, Qty: qty,
		Reason: reason, EntryTime: entry,
	})
}

// crosses reports whether an incoming order on side with the given limit
// can trade against a resting price.
func crosses(side Side, limit, resting int64) bool {
	if side == Buy {
		return limit >= resting
	}
	return limit <= resting
}

// canFill reports whether qty can be filled immediately within the limit.
func (b *Book) canFill(side Side, typ OrderType, limit, qty int64) bool {
	opp := b.sideOf(side.Opposite())
	var available int64
	for i := len(opp.levels) - 1; i >= 0; i-- {
		l := opp.levels[i]
		if typ == Limit && !crosses(side, limit, l.price) {
			break
		}
		available += l.qty
		if available >= qty {
			return true
		}
	}
	return false
}

// match executes an incoming order against the opposite side and returns
// the unfilled quantity.
func (b *Book) match(dst []Event, t int64, id uint64, account string, side Side, typ OrderType, limit, qty int64) ([]Event, int64) {
	opp := b.sideOf(side.Opposite())
	for qty > 0 {
		lvl := opp.best()
		if lvl == nil || (typ == Limit && !crosses(side, limit, lvl.price)) {
			break
		}
		for qty > 0 && lvl.head != nil {
			r := lvl.head
			fill := min(qty, r.qty)
			qty -= fill
			r.qty -= fill
			lvl.qty -= fill
			b.lastTradeID++
			b.lastPrice = lvl.price
			b.volume += fill

			ev := Event{
				Kind: EventTrade, Seq: b.nextSeq(), Time: t,
				TradeID: b.lastTradeID, Price: lvl.price, Qty: fill, Aggressor: side,
			}
			if side == Buy {
				ev.BuyOrderID, ev.BuyAccount, ev.BuyLeaves = id, account, qty
				ev.SellOrderID, ev.SellAccount, ev.SellLeaves = r.id, r.account, r.qty
			} else {
				ev.SellOrderID, ev.SellAccount, ev.SellLeaves = id, account, qty
				ev.BuyOrderID, ev.BuyAccount, ev.BuyLeaves = r.id, r.account, r.qty
			}
			dst = append(dst, ev)

			if r.qty == 0 {
				lvl.remove(r)
				delete(b.orders, r.id)
			}
		}
		if lvl.head == nil {
			opp.removeLevel(lvl)
		}
	}
	return dst, qty
}

func (b *Book) rest(id uint64, account string, side Side, price, qty, entry int64) {
	b.priority++
	o := &order{
		id: id, account: account, side: side, price: price, qty: qty,
		entryTime: entry, priority: b.priority,
	}
	b.sideOf(side).levelAt(price).push(o)
	b.orders[id] = o
}

func (b *Book) unrest(o *order) {
	l := o.level
	l.remove(o)
	if l.head == nil {
		b.sideOf(o.side).removeLevel(l)
	}
	delete(b.orders, o.id)
}
