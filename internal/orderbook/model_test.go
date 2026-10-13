package orderbook

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"

	"pgregory.net/rapid"
)

// refBook is a deliberately naive order book used as a test oracle. It keeps
// resting orders in an unsorted slice and finds the next order to trade
// against by scanning every order, so its behaviour can be checked by
// reading it. It implements the same rules as Book independently. The
// property tests require both to produce identical event streams.
type refBook struct {
	cfg     Config
	resting []*refOrder

	seq, lastOrderID, lastTradeID, prio uint64
	lastPrice, volume                   int64
}

type refOrder struct {
	id         uint64
	account    string
	side       Side
	price, qty int64
	entry      int64
	prio       uint64
}

func newRefBook(cfg Config) *refBook {
	if cfg.MaxQty == 0 {
		cfg.MaxQty = defaultMaxQty
	}
	if cfg.MaxPrice == 0 {
		cfg.MaxPrice = defaultMaxPrice
	}
	return &refBook{cfg: cfg}
}

func (r *refBook) next() uint64 { r.seq++; return r.seq }

func (r *refBook) checkPrice(p int64) error {
	if p <= 0 || p > r.cfg.MaxPrice {
		return ErrInvalidPrice
	}
	if p%r.cfg.TickSize != 0 {
		return ErrTickSize
	}
	return nil
}

func (r *refBook) checkQty(q int64) error {
	if q <= 0 || q > r.cfg.MaxQty {
		return ErrInvalidQuantity
	}
	if q%r.cfg.LotSize != 0 {
		return ErrLotSize
	}
	return nil
}

// Validation order is part of the contract: side, type, time in force,
// price, quantity.
func (r *refBook) checkRequest(q Request) error {
	if q.Side != Buy && q.Side != Sell {
		return ErrInvalidSide
	}
	if q.Type == Limit {
		if q.TIF != Day && q.TIF != IOC && q.TIF != FOK {
			return ErrInvalidTIF
		}
		if err := r.checkPrice(q.Price); err != nil {
			return err
		}
	} else if q.Type == Market {
		if q.TIF != IOC && q.TIF != FOK {
			return ErrInvalidTIF
		}
		if q.Price != 0 {
			return ErrInvalidPrice
		}
	} else {
		return ErrInvalidType
	}
	return r.checkQty(q.Qty)
}

// acceptable reports whether an incoming order on side with the given limit
// may trade at price.
func acceptable(side Side, limit, price int64) bool {
	if side == Buy {
		return price <= limit
	}
	return price >= limit
}

// firstToTrade returns the resting order an incoming order on side trades
// against first: the best price, then the earliest entry.
func (r *refBook) firstToTrade(side Side) *refOrder {
	var best *refOrder
	for _, o := range r.resting {
		if o.side == side {
			continue
		}
		if best == nil {
			best = o
			continue
		}
		betterPrice := (o.side == Sell && o.price < best.price) || (o.side == Buy && o.price > best.price)
		if betterPrice || (o.price == best.price && o.prio < best.prio) {
			best = o
		}
	}
	return best
}

func (r *refBook) find(id uint64) *refOrder {
	for _, o := range r.resting {
		if o.id == id {
			return o
		}
	}
	return nil
}

func (r *refBook) drop(id uint64) {
	for i, o := range r.resting {
		if o.id == id {
			r.resting = append(r.resting[:i], r.resting[i+1:]...)
			return
		}
	}
}

func (r *refBook) cancelEvent(t int64, id uint64, account string, side Side, price, qty int64, reason CancelReason, entry int64) Event {
	return Event{
		Kind: EventCancelled, Seq: r.next(), Time: t, OrderID: id, Account: account,
		Side: side, Price: price, Qty: qty, Reason: reason, EntryTime: entry,
	}
}

func (r *refBook) trade(evs []Event, t int64, id uint64, account string, side Side, typ OrderType, limit, qty int64) ([]Event, int64) {
	for qty > 0 {
		o := r.firstToTrade(side)
		if o == nil || (typ == Limit && !acceptable(side, limit, o.price)) {
			break
		}
		fill := min(qty, o.qty)
		qty -= fill
		o.qty -= fill
		r.lastTradeID++
		r.lastPrice = o.price
		r.volume += fill
		e := Event{Kind: EventTrade, Seq: r.next(), Time: t, TradeID: r.lastTradeID, Price: o.price, Qty: fill, Aggressor: side}
		if side == Buy {
			e.BuyOrderID, e.BuyAccount, e.BuyLeaves = id, account, qty
			e.SellOrderID, e.SellAccount, e.SellLeaves = o.id, o.account, o.qty
		} else {
			e.SellOrderID, e.SellAccount, e.SellLeaves = id, account, qty
			e.BuyOrderID, e.BuyAccount, e.BuyLeaves = o.id, o.account, o.qty
		}
		evs = append(evs, e)
		if o.qty == 0 {
			r.drop(o.id)
		}
	}
	return evs, qty
}

func (r *refBook) Submit(t int64, q Request) ([]Event, uint64, error) {
	if err := r.checkRequest(q); err != nil {
		return nil, 0, err
	}
	r.lastOrderID++
	id := r.lastOrderID
	evs := []Event{{
		Kind: EventAccepted, Seq: r.next(), Time: t, OrderID: id, Account: q.Account,
		Side: q.Side, Type: q.Type, TIF: q.TIF, Price: q.Price, Qty: q.Qty,
	}}
	if q.TIF == FOK {
		var available int64
		for _, o := range r.resting {
			if o.side != q.Side && (q.Type == Market || acceptable(q.Side, q.Price, o.price)) {
				available += o.qty
			}
		}
		if available < q.Qty {
			return append(evs, r.cancelEvent(t, id, q.Account, q.Side, q.Price, q.Qty, ReasonFOKUnfilled, t)), id, nil
		}
	}
	evs, rest := r.trade(evs, t, id, q.Account, q.Side, q.Type, q.Price, q.Qty)
	switch {
	case rest == 0:
	case q.Type == Market:
		evs = append(evs, r.cancelEvent(t, id, q.Account, q.Side, q.Price, rest, ReasonNoLiquidity, t))
	case q.TIF == IOC:
		evs = append(evs, r.cancelEvent(t, id, q.Account, q.Side, q.Price, rest, ReasonIOCRemainder, t))
	default:
		r.prio++
		r.resting = append(r.resting, &refOrder{id: id, account: q.Account, side: q.Side, price: q.Price, qty: rest, entry: t, prio: r.prio})
	}
	return evs, id, nil
}

func (r *refBook) Cancel(t int64, account string, id uint64) ([]Event, error) {
	o := r.find(id)
	if o == nil {
		return nil, ErrUnknownOrder
	}
	if o.account != account {
		return nil, ErrNotOwner
	}
	r.drop(id)
	return []Event{r.cancelEvent(t, o.id, o.account, o.side, o.price, o.qty, ReasonUser, o.entry)}, nil
}

func (r *refBook) Replace(t int64, account string, id uint64, price, qty int64) ([]Event, error) {
	o := r.find(id)
	if o == nil {
		return nil, ErrUnknownOrder
	}
	if o.account != account {
		return nil, ErrNotOwner
	}
	if err := r.checkPrice(price); err != nil {
		return nil, err
	}
	if err := r.checkQty(qty); err != nil {
		return nil, err
	}
	if price == o.price && qty == o.qty {
		return nil, ErrNoChange
	}
	evs := []Event{{
		Kind: EventReplaced, Seq: r.next(), Time: t, OrderID: id, Account: o.account,
		Side: o.side, Type: Limit, TIF: Day, Price: price, Qty: qty, PrevPrice: o.price, PrevQty: o.qty,
	}}
	if price == o.price && qty < o.qty {
		o.qty = qty
		return evs, nil
	}
	r.drop(id)
	evs, rest := r.trade(evs, t, id, o.account, o.side, Limit, price, qty)
	if rest > 0 {
		r.prio++
		r.resting = append(r.resting, &refOrder{id: id, account: o.account, side: o.side, price: price, qty: rest, entry: o.entry, prio: r.prio})
	}
	return evs, nil
}

func (r *refBook) CloseSession(t int64) []Event {
	orders := append([]*refOrder(nil), r.resting...)
	sort.Slice(orders, func(i, j int) bool {
		a, b := orders[i], orders[j]
		if a.side != b.side {
			return a.side == Buy
		}
		if a.price != b.price {
			if a.side == Buy {
				return a.price > b.price
			}
			return a.price < b.price
		}
		return a.prio < b.prio
	})
	var evs []Event
	for _, o := range orders {
		evs = append(evs, r.cancelEvent(t, o.id, o.account, o.side, o.price, o.qty, ReasonSessionEnd, o.entry))
	}
	r.resting = nil
	return evs
}

func (r *refBook) levels(side Side, depth int) []Level {
	agg := map[int64]*Level{}
	for _, o := range r.resting {
		if o.side != side {
			continue
		}
		l, ok := agg[o.price]
		if !ok {
			l = &Level{Price: o.price}
			agg[o.price] = l
		}
		l.Qty += o.qty
		l.Count++
	}
	out := make([]Level, 0, len(agg))
	for _, l := range agg {
		out = append(out, *l)
	}
	sort.Slice(out, func(i, j int) bool {
		if side == Buy {
			return out[i].Price > out[j].Price
		}
		return out[i].Price < out[j].Price
	})
	if len(out) > depth {
		out = out[:depth]
	}
	return out
}

func (r *refBook) Snapshot(t int64, depth int) []Event {
	return []Event{{
		Kind: EventSnapshot, Seq: r.next(), Time: t,
		Bids: r.levels(Buy, depth), Asks: r.levels(Sell, depth),
		LastPrice: r.lastPrice, Volume: r.volume,
	}}
}

// fataler is the subset of testing.TB and *rapid.T used by the checkers.
type fataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

// checkInvariants verifies the internal consistency of a Book.
func checkInvariants(t fataler, b *Book) {
	t.Helper()
	inBook := 0
	for _, s := range []*bookSide{&b.bids, &b.asks} {
		side := Sell
		if s.bid {
			side = Buy
		}
		if len(s.levels) != len(s.byPrice) {
			t.Fatalf("%v: %d levels but %d indexed", side, len(s.levels), len(s.byPrice))
		}
		for i, l := range s.levels {
			if s.byPrice[l.price] != l {
				t.Fatalf("%v: level %d not indexed by price", side, l.price)
			}
			if i > 0 && !s.better(l.price, s.levels[i-1].price) {
				t.Fatalf("%v: levels not sorted worst-to-best at index %d", side, i)
			}
			if l.head == nil || l.tail == nil || l.head.prev != nil || l.tail.next != nil {
				t.Fatalf("%v: level %d has broken ends", side, l.price)
			}
			var qty int64
			var count int
			var lastPrio uint64
			for o := l.head; o != nil; o = o.next {
				switch {
				case o.level != l || o.price != l.price || o.side != side:
					t.Fatalf("order %d is linked into the wrong level", o.id)
				case o.qty <= 0:
					t.Fatalf("order %d rests with non-positive quantity %d", o.id, o.qty)
				case count > 0 && o.priority <= lastPrio:
					t.Fatalf("level %d is not in time-priority order", l.price)
				case o.next != nil && o.next.prev != o:
					t.Fatalf("level %d has a broken back link", l.price)
				case b.orders[o.id] != o:
					t.Fatalf("order %d is not indexed", o.id)
				}
				qty += o.qty
				count++
				lastPrio = o.priority
			}
			if qty != l.qty || count != l.count {
				t.Fatalf("level %d aggregates qty=%d count=%d, orders sum to qty=%d count=%d", l.price, l.qty, l.count, qty, count)
			}
			inBook += count
		}
	}
	if inBook != len(b.orders) {
		t.Fatalf("%d orders indexed but %d linked into levels", len(b.orders), inBook)
	}
	bid, okb := b.BestBid()
	ask, oka := b.BestAsk()
	if okb && oka && bid.Price >= ask.Price {
		t.Fatalf("book is crossed: bid %d >= ask %d", bid.Price, ask.Price)
	}
}

// ledger replays a Book's event stream and checks it is self-consistent: it
// conserves quantity per order, reports correct leaves, never trades through
// a limit and executes at the resting order's price.
type ledger struct {
	open   map[uint64]int64
	limit  map[uint64]int64
	typ    map[uint64]OrderType
	tif    map[uint64]TimeInForce
	qty    map[uint64]int64
	filled map[uint64]int64
}

func newLedger() *ledger {
	return &ledger{
		open: map[uint64]int64{}, limit: map[uint64]int64{}, typ: map[uint64]OrderType{},
		tif: map[uint64]TimeInForce{}, qty: map[uint64]int64{}, filled: map[uint64]int64{},
	}
}

func (l *ledger) apply(t fataler, evs []Event) {
	t.Helper()
	for _, e := range evs {
		switch e.Kind {
		case EventAccepted:
			if _, dup := l.open[e.OrderID]; dup {
				t.Fatalf("order %d accepted twice", e.OrderID)
			}
			l.open[e.OrderID], l.qty[e.OrderID] = e.Qty, e.Qty
			l.limit[e.OrderID], l.typ[e.OrderID], l.tif[e.OrderID] = e.Price, e.Type, e.TIF
		case EventReplaced:
			if l.open[e.OrderID] != e.PrevQty {
				t.Fatalf("replace of %d reports prev qty %d, ledger has %d", e.OrderID, e.PrevQty, l.open[e.OrderID])
			}
			l.open[e.OrderID], l.limit[e.OrderID] = e.Qty, e.Price
		case EventTrade:
			if e.Qty <= 0 {
				t.Fatalf("trade %d has non-positive quantity", e.TradeID)
			}
			aggressor, passive := e.BuyOrderID, e.SellOrderID
			if e.Aggressor == Sell {
				aggressor, passive = passive, aggressor
			}
			if l.limit[passive] != e.Price {
				t.Fatalf("trade %d at %d, resting order %d rests at %d", e.TradeID, e.Price, passive, l.limit[passive])
			}
			if l.typ[aggressor] == Limit && !acceptable(e.Aggressor, l.limit[aggressor], e.Price) {
				t.Fatalf("trade %d at %d trades through the aggressor's limit %d", e.TradeID, e.Price, l.limit[aggressor])
			}
			for _, leg := range []struct {
				id     uint64
				leaves int64
			}{{e.BuyOrderID, e.BuyLeaves}, {e.SellOrderID, e.SellLeaves}} {
				l.open[leg.id] -= e.Qty
				l.filled[leg.id] += e.Qty
				if l.open[leg.id] != leg.leaves || leg.leaves < 0 {
					t.Fatalf("order %d: trade reports leaves %d, ledger has %d", leg.id, leg.leaves, l.open[leg.id])
				}
			}
		case EventCancelled:
			if l.open[e.OrderID] != e.Qty {
				t.Fatalf("cancel of %d removes %d, but %d is open", e.OrderID, e.Qty, l.open[e.OrderID])
			}
			l.open[e.OrderID] = 0
		}
	}
}

// checkSubmitted verifies the time-in-force rules for an order right after
// the Submit that created it.
func (l *ledger) checkSubmitted(t fataler, id uint64) {
	t.Helper()
	if (l.typ[id] == Market || l.tif[id] != Day) && l.open[id] != 0 {
		t.Fatalf("order %d (type %d, tif %d) left %d open", id, l.typ[id], l.tif[id], l.open[id])
	}
	if l.tif[id] == FOK && l.filled[id] != 0 && l.filled[id] != l.qty[id] {
		t.Fatalf("FOK order %d partially filled: %d of %d", id, l.filled[id], l.qty[id])
	}
}

// checkBook verifies that the ledger's open quantities match the book.
func (l *ledger) checkBook(t fataler, b *Book) {
	t.Helper()
	for id, open := range l.open {
		o, resting := b.Order(id)
		if open > 0 && (!resting || o.Qty != open) {
			t.Fatalf("order %d: ledger has %d open, book has %+v (resting=%v)", id, open, o, resting)
		}
		if open == 0 && resting {
			t.Fatalf("order %d: fully closed in the ledger but resting in the book", id)
		}
		if open < 0 {
			t.Fatalf("order %d: negative open quantity %d", id, open)
		}
	}
	if len(b.orders) > len(l.open) {
		t.Fatalf("book holds orders the event stream never accepted")
	}
}

const testMid = 1_000_000

func drawRequest(t *rapid.T) Request {
	r := Request{
		Account: rapid.SampledFrom([]string{"A", "B", "C"}).Draw(t, "account"),
		Side:    rapid.SampledFrom([]Side{Buy, Sell}).Draw(t, "side"),
		Type:    Limit,
		TIF:     rapid.SampledFrom([]TimeInForce{Day, Day, Day, Day, IOC, FOK}).Draw(t, "tif"),
		Price:   testMid + 100*int64(rapid.IntRange(-8, 8).Draw(t, "ticks")),
		Qty:     10 * int64(rapid.IntRange(1, 30).Draw(t, "lots")),
	}
	switch rapid.IntRange(0, 19).Draw(t, "variant") {
	case 0, 1:
		r.Type, r.Price = Market, 0
		r.TIF = rapid.SampledFrom([]TimeInForce{IOC, IOC, FOK}).Draw(t, "marketTIF")
	case 2:
		// Deliberately invalid requests exercise validation parity.
		switch rapid.IntRange(0, 6).Draw(t, "invalid") {
		case 0:
			r.Price += 50
		case 1:
			r.Qty = 0
		case 2:
			r.Qty += 5
		case 3:
			r.Type, r.TIF, r.Price = Market, Day, 0
		case 4:
			r.Side = 0
		case 5:
			r.Qty = 10_010
		case 6:
			r.Price = -r.Price
		}
	}
	return r
}

func TestBookMatchesReferenceModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		cfg := Config{Symbol: "PBT", TickSize: 100, LotSize: 10, MaxQty: 10_000}
		book, err := New(cfg)
		if err != nil {
			rt.Fatal(err)
		}
		model := newRefBook(cfg)
		led := newLedger()
		owner := map[uint64]string{}
		var ids []uint64
		var now int64

		same := func(rt *rapid.T, op string, got, want []Event, gotErr, wantErr error) {
			rt.Helper()
			if !errors.Is(gotErr, wantErr) || (gotErr == nil) != (wantErr == nil) {
				rt.Fatalf("%s: book error %v, model error %v", op, gotErr, wantErr)
			}
			if !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
				rt.Fatalf("%s: event streams differ\n book: %+v\nmodel: %+v", op, got, want)
			}
			led.apply(rt, got)
		}
		pick := func(rt *rapid.T) (uint64, string) {
			if len(ids) == 0 || rapid.IntRange(0, 19).Draw(rt, "unknownOrder") == 0 {
				return 999_999, "A"
			}
			id := rapid.SampledFrom(ids).Draw(rt, "order")
			account := owner[id]
			if rapid.IntRange(0, 9).Draw(rt, "wrongOwner") == 0 {
				account = "X"
			}
			return id, account
		}

		rt.Repeat(map[string]func(*rapid.T){
			"submit": func(rt *rapid.T) {
				now += rapid.Int64Range(0, 1_000_000).Draw(rt, "dt")
				req := drawRequest(rt)
				got, gotID, gotErr := book.Submit(nil, now, req)
				want, wantID, wantErr := model.Submit(now, req)
				same(rt, fmt.Sprintf("submit %+v", req), got, want, gotErr, wantErr)
				if gotID != wantID {
					rt.Fatalf("order id %d, model %d", gotID, wantID)
				}
				if gotErr == nil {
					ids = append(ids, gotID)
					owner[gotID] = req.Account
					led.checkSubmitted(rt, gotID)
				}
			},
			"cancel": func(rt *rapid.T) {
				id, account := pick(rt)
				got, gotErr := book.Cancel(nil, now, account, id)
				want, wantErr := model.Cancel(now, account, id)
				same(rt, fmt.Sprintf("cancel %d by %s", id, account), got, want, gotErr, wantErr)
			},
			"replace": func(rt *rapid.T) {
				id, account := pick(rt)
				price := testMid + 100*int64(rapid.IntRange(-8, 8).Draw(rt, "newTicks"))
				qty := 10 * int64(rapid.IntRange(1, 30).Draw(rt, "newLots"))
				if o, ok := book.Order(id); ok && rapid.Bool().Draw(rt, "samePrice") {
					price = o.Price
				}
				got, gotErr := book.Replace(nil, now, account, id, price, qty)
				want, wantErr := model.Replace(now, account, id, price, qty)
				same(rt, fmt.Sprintf("replace %d by %s to %d@%d", id, account, qty, price), got, want, gotErr, wantErr)
			},
			"snapshot": func(rt *rapid.T) {
				depth := rapid.IntRange(1, 5).Draw(rt, "depth")
				same(rt, "snapshot", book.Snapshot(nil, now, depth), model.Snapshot(now, depth), nil, nil)
			},
			"close": func(rt *rapid.T) {
				if rapid.IntRange(0, 9).Draw(rt, "closeNow") != 0 {
					rt.Skip("close is rare")
				}
				same(rt, "close", book.CloseSession(nil, now), model.CloseSession(now), nil, nil)
			},
			"": func(rt *rapid.T) {
				checkInvariants(rt, book)
				led.checkBook(rt, book)
				if book.Seq() != model.seq {
					rt.Fatalf("sequence %d, model %d", book.Seq(), model.seq)
				}
			},
		})
	})
}

// randomWorkload drives a book with a seeded pseudo-random mix of operations
// resembling simulator traffic and returns every event it produced.
func randomWorkload(t fataler, seed uint64, n int, check func(*Book, []Event)) []Event {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	b, err := New(Config{Symbol: "RND", TickSize: 100, LotSize: 10})
	if err != nil {
		t.Fatalf("new book: %v", err)
	}
	accounts := []string{"MM1", "MM2", "N1", "N2", "N3", "INST"}
	var ids []uint64
	var all []Event
	var evs []Event
	now := int64(0)
	for i := 0; i < n; i++ {
		now += int64(rng.IntN(5_000_000))
		evs = evs[:0]
		account := accounts[rng.IntN(len(accounts))]
		side := Side(1 + rng.IntN(2))
		switch op := rng.IntN(100); {
		case op < 55:
			tif := []TimeInForce{Day, Day, Day, IOC, FOK}[rng.IntN(5)]
			price := int64(testMid + 100*(rng.IntN(21)-10))
			var id uint64
			evs, id, _ = b.Submit(evs, now, Request{Account: account, Side: side, Type: Limit, TIF: tif, Price: price, Qty: int64(10 * (1 + rng.IntN(50)))})
			ids = append(ids, id)
		case op < 60:
			evs, _, _ = b.Submit(evs, now, Request{Account: account, Side: side, Type: Market, TIF: IOC, Qty: int64(10 * (1 + rng.IntN(30)))})
		case op < 82:
			if len(ids) > 0 {
				id := ids[rng.IntN(len(ids))]
				if o, ok := b.Order(id); ok {
					evs, _ = b.Cancel(evs, now, o.Account, id)
				}
			}
		case op < 97:
			if len(ids) > 0 {
				id := ids[rng.IntN(len(ids))]
				if o, ok := b.Order(id); ok {
					evs, _ = b.Replace(evs, now, o.Account, id, int64(testMid+100*(rng.IntN(21)-10)), int64(10*(1+rng.IntN(50))))
				}
			}
		default:
			evs = b.Snapshot(evs, now, 5)
		}
		all = append(all, evs...)
		if check != nil {
			check(b, evs)
		}
	}
	return all
}

func TestDeterministicUnderSeed(t *testing.T) {
	a := randomWorkload(t, 42, 20_000, nil)
	b := randomWorkload(t, 42, 20_000, nil)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed produced different event streams")
	}
	c := randomWorkload(t, 43, 20_000, nil)
	if reflect.DeepEqual(a, c) {
		t.Fatal("different seeds produced identical event streams")
	}
	if len(trades(a)) == 0 {
		t.Fatal("workload produced no trades; it is not exercising matching")
	}
}

func TestRandomizedSoak(t *testing.T) {
	n := 200_000
	if testing.Short() {
		n = 20_000
	}
	led := newLedger()
	step := 0
	randomWorkload(t, 7, n, func(b *Book, evs []Event) {
		led.apply(t, evs)
		step++
		if step%997 == 0 {
			checkInvariants(t, b)
			led.checkBook(t, b)
		}
	})
}
