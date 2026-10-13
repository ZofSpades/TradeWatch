package orderbook

import (
	"errors"
	"testing"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

func newTestBook(t testing.TB) *Book {
	t.Helper()
	b, err := New(Config{Symbol: "TEST", TickSize: 1, LotSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func limit(account string, side Side, price, qty int64) Request {
	return Request{Account: account, Side: side, Type: Limit, TIF: Day, Price: price, Qty: qty}
}

func mustSubmit(t *testing.T, b *Book, r Request) ([]Event, uint64) {
	t.Helper()
	evs, id, err := b.Submit(nil, 0, r)
	if err != nil {
		t.Fatalf("submit %+v: %v", r, err)
	}
	return evs, id
}

func trades(evs []Event) []Event {
	var out []Event
	for _, e := range evs {
		if e.Kind == EventTrade {
			out = append(out, e)
		}
	}
	return out
}

func kinds(evs []Event) []EventKind {
	out := make([]EventKind, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}
	return out
}

func equalKinds(a []EventKind, b ...EventKind) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRestingOrdersSetBestPrices(t *testing.T) {
	b := newTestBook(t)
	mustSubmit(t, b, limit("A", Buy, 100, 10))
	mustSubmit(t, b, limit("A", Buy, 101, 20))
	mustSubmit(t, b, limit("A", Buy, 99, 30))
	mustSubmit(t, b, limit("B", Sell, 103, 40))
	mustSubmit(t, b, limit("B", Sell, 102, 50))
	mustSubmit(t, b, limit("B", Sell, 102, 5))

	if bid, _ := b.BestBid(); bid.Price != 101 || bid.Qty != 20 {
		t.Errorf("best bid = %+v", bid)
	}
	if ask, _ := b.BestAsk(); ask.Price != 102 || ask.Qty != 55 || ask.Count != 2 {
		t.Errorf("best ask = %+v", ask)
	}
	bids := b.Depth(Buy, 10)
	if len(bids) != 3 || bids[0].Price != 101 || bids[1].Price != 100 || bids[2].Price != 99 {
		t.Errorf("bid depth = %+v", bids)
	}
	asks := b.Depth(Sell, 1)
	if len(asks) != 1 || asks[0].Price != 102 {
		t.Errorf("ask depth = %+v", asks)
	}
	if b.OpenOrders() != 6 {
		t.Errorf("open orders = %d", b.OpenOrders())
	}
}

func TestPricePriorityAndTradeAtRestingPrice(t *testing.T) {
	b := newTestBook(t)
	_, a := mustSubmit(t, b, limit("A", Sell, 102, 100))
	_, c := mustSubmit(t, b, limit("C", Sell, 101, 100))

	evs, buy := mustSubmit(t, b, limit("B", Buy, 103, 150))
	tr := trades(evs)
	if len(tr) != 2 {
		t.Fatalf("got %d trades, want 2: %+v", len(tr), evs)
	}
	if tr[0].SellOrderID != c || tr[0].Price != 101 || tr[0].Qty != 100 {
		t.Errorf("first trade = %+v, want 100 @ 101 against the better-priced order", tr[0])
	}
	if tr[1].SellOrderID != a || tr[1].Price != 102 || tr[1].Qty != 50 {
		t.Errorf("second trade = %+v, want 50 @ 102", tr[1])
	}
	for _, e := range tr {
		if e.BuyOrderID != buy || e.Aggressor != Buy || e.BuyAccount != "B" {
			t.Errorf("trade attribution wrong: %+v", e)
		}
	}
	if tr[1].BuyLeaves != 0 || tr[1].SellLeaves != 50 {
		t.Errorf("leaves = buy %d sell %d", tr[1].BuyLeaves, tr[1].SellLeaves)
	}
	if b.LastPrice() != 102 || b.Volume() != 150 {
		t.Errorf("last=%d volume=%d", b.LastPrice(), b.Volume())
	}
}

func TestTimePriorityWithinLevel(t *testing.T) {
	b := newTestBook(t)
	_, first := mustSubmit(t, b, limit("A", Sell, 101, 100))
	_, second := mustSubmit(t, b, limit("B", Sell, 101, 100))
	_, third := mustSubmit(t, b, limit("C", Sell, 101, 100))

	evs, _ := mustSubmit(t, b, limit("D", Buy, 101, 150))
	tr := trades(evs)
	if len(tr) != 2 || tr[0].SellOrderID != first || tr[1].SellOrderID != second || tr[1].Qty != 50 {
		t.Fatalf("trades = %+v", tr)
	}
	if o, ok := b.Order(second); !ok || o.Qty != 50 {
		t.Errorf("second order should rest with 50, got %+v %v", o, ok)
	}
	if o, ok := b.Order(third); !ok || o.Qty != 100 {
		t.Errorf("third order should be untouched, got %+v %v", o, ok)
	}
	if _, ok := b.Order(first); ok {
		t.Error("fully filled order must leave the book")
	}
}

func TestLimitOrderNeverTradesThroughItsLimit(t *testing.T) {
	b := newTestBook(t)
	mustSubmit(t, b, limit("A", Sell, 101, 100))
	_, far := mustSubmit(t, b, limit("A", Sell, 102, 100))

	evs, buy := mustSubmit(t, b, limit("B", Buy, 101, 150))
	if tr := trades(evs); len(tr) != 1 || tr[0].Price != 101 {
		t.Fatalf("trades = %+v", tr)
	}
	if o, ok := b.Order(buy); !ok || o.Qty != 50 || o.Price != 101 {
		t.Errorf("remainder should rest at the limit, got %+v %v", o, ok)
	}
	if o, _ := b.Order(far); o.Qty != 100 {
		t.Errorf("order beyond the limit was touched: %+v", o)
	}
	if bid, _ := b.BestBid(); bid.Price != 101 {
		t.Errorf("best bid = %+v", bid)
	}
	if ask, _ := b.BestAsk(); ask.Price != 102 {
		t.Errorf("best ask = %+v", ask)
	}
}

func TestMarketOrderSweepsAndCancelsRemainder(t *testing.T) {
	b := newTestBook(t)
	mustSubmit(t, b, limit("A", Sell, 101, 100))
	mustSubmit(t, b, limit("A", Sell, 102, 100))

	evs, id := mustSubmit(t, b, Request{Account: "B", Side: Buy, Type: Market, TIF: IOC, Qty: 250})
	if !equalKinds(kinds(evs), EventAccepted, EventTrade, EventTrade, EventCancelled) {
		t.Fatalf("events = %v", kinds(evs))
	}
	c := evs[3]
	if c.OrderID != id || c.Qty != 50 || c.Reason != ReasonNoLiquidity {
		t.Errorf("cancel = %+v", c)
	}
	if _, ok := b.BestAsk(); ok {
		t.Error("ask side should be empty")
	}
	if _, ok := b.Order(id); ok {
		t.Error("market orders must never rest")
	}
}

func TestMarketOrderOnEmptyBook(t *testing.T) {
	b := newTestBook(t)
	evs, _ := mustSubmit(t, b, Request{Account: "B", Side: Sell, Type: Market, TIF: IOC, Qty: 10})
	if !equalKinds(kinds(evs), EventAccepted, EventCancelled) || evs[1].Qty != 10 || evs[1].Reason != ReasonNoLiquidity {
		t.Fatalf("events = %+v", evs)
	}
	if evs[0].Price != 0 {
		t.Errorf("market order accepted with price %d", evs[0].Price)
	}
}

func TestIOCRemainderIsCancelled(t *testing.T) {
	b := newTestBook(t)
	mustSubmit(t, b, limit("A", Sell, 101, 100))

	evs, id := mustSubmit(t, b, Request{Account: "B", Side: Buy, Type: Limit, TIF: IOC, Price: 101, Qty: 150})
	if !equalKinds(kinds(evs), EventAccepted, EventTrade, EventCancelled) {
		t.Fatalf("events = %v", kinds(evs))
	}
	if evs[2].Qty != 50 || evs[2].Reason != ReasonIOCRemainder {
		t.Errorf("cancel = %+v", evs[2])
	}
	if _, ok := b.Order(id); ok {
		t.Error("IOC order must not rest")
	}
	if _, ok := b.BestBid(); ok {
		t.Error("bid side should be empty")
	}
}

func TestIOCThatDoesNotCross(t *testing.T) {
	b := newTestBook(t)
	mustSubmit(t, b, limit("A", Sell, 101, 100))
	evs, _ := mustSubmit(t, b, Request{Account: "B", Side: Buy, Type: Limit, TIF: IOC, Price: 100, Qty: 100})
	if !equalKinds(kinds(evs), EventAccepted, EventCancelled) || evs[1].Qty != 100 {
		t.Fatalf("events = %+v", evs)
	}
}

func TestFOKFillsInFull(t *testing.T) {
	b := newTestBook(t)
	mustSubmit(t, b, limit("A", Sell, 101, 100))
	mustSubmit(t, b, limit("A", Sell, 102, 100))

	evs, _ := mustSubmit(t, b, Request{Account: "B", Side: Buy, Type: Limit, TIF: FOK, Price: 102, Qty: 200})
	if !equalKinds(kinds(evs), EventAccepted, EventTrade, EventTrade) {
		t.Fatalf("events = %v", kinds(evs))
	}
}

func TestFOKKilledWhenLiquidityInsufficient(t *testing.T) {
	b := newTestBook(t)
	mustSubmit(t, b, limit("A", Sell, 101, 100))
	mustSubmit(t, b, limit("A", Sell, 103, 200))

	// 300 shares are offered in total, but only 100 within the limit of 102.
	evs, _ := mustSubmit(t, b, Request{Account: "B", Side: Buy, Type: Limit, TIF: FOK, Price: 102, Qty: 200})
	if !equalKinds(kinds(evs), EventAccepted, EventCancelled) {
		t.Fatalf("events = %v", kinds(evs))
	}
	if evs[1].Qty != 200 || evs[1].Reason != ReasonFOKUnfilled {
		t.Errorf("cancel = %+v", evs[1])
	}
	if ask, _ := b.BestAsk(); ask.Price != 101 || ask.Qty != 100 {
		t.Errorf("book changed by a killed FOK: %+v", ask)
	}

	evs, _ = mustSubmit(t, b, Request{Account: "B", Side: Buy, Type: Market, TIF: FOK, Qty: 301})
	if !equalKinds(kinds(evs), EventAccepted, EventCancelled) {
		t.Fatalf("market FOK beyond total depth: %v", kinds(evs))
	}
	evs, _ = mustSubmit(t, b, Request{Account: "B", Side: Buy, Type: Market, TIF: FOK, Qty: 300})
	if len(trades(evs)) != 2 {
		t.Fatalf("market FOK of exactly the total depth should fill: %v", kinds(evs))
	}
}

func TestCancel(t *testing.T) {
	b := newTestBook(t)
	evs, id, err := b.Submit(nil, 1_000, limit("A", Buy, 100, 70))
	if err != nil {
		t.Fatal(err)
	}
	seq := evs[0].Seq

	if _, err := b.Cancel(nil, 2_000, "B", id); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("cancel by another account: %v", err)
	}
	evs, err = b.Cancel(nil, 2_000, "A", id)
	if err != nil {
		t.Fatal(err)
	}
	c := evs[0]
	if c.Kind != EventCancelled || c.Qty != 70 || c.Reason != ReasonUser || c.EntryTime != 1_000 || c.Time != 2_000 || c.Seq != seq+1 {
		t.Errorf("cancel = %+v", c)
	}
	if _, ok := b.BestBid(); ok || b.OpenOrders() != 0 {
		t.Error("book should be empty")
	}
	if _, err := b.Cancel(nil, 3_000, "A", id); !errors.Is(err, ErrUnknownOrder) {
		t.Errorf("second cancel: %v", err)
	}
}

func TestReplaceReducingQuantityKeepsPriority(t *testing.T) {
	b := newTestBook(t)
	_, a := mustSubmit(t, b, limit("A", Buy, 100, 100))
	mustSubmit(t, b, limit("B", Buy, 100, 100))

	evs, err := b.Replace(nil, 0, "A", a, 100, 40)
	if err != nil {
		t.Fatal(err)
	}
	r := evs[0]
	if r.Kind != EventReplaced || r.Qty != 40 || r.PrevQty != 100 || r.PrevPrice != 100 {
		t.Errorf("replace = %+v", r)
	}
	if bid, _ := b.BestBid(); bid.Qty != 140 {
		t.Errorf("level quantity = %d, want 140", bid.Qty)
	}
	evs, _ = mustSubmit(t, b, limit("C", Sell, 100, 40))
	if tr := trades(evs); len(tr) != 1 || tr[0].BuyOrderID != a {
		t.Errorf("reduced order should keep its place at the front: %+v", tr)
	}
}

func TestReplaceIncreasingQuantityLosesPriority(t *testing.T) {
	b := newTestBook(t)
	_, a := mustSubmit(t, b, limit("A", Buy, 100, 100))
	_, bo := mustSubmit(t, b, limit("B", Buy, 100, 100))

	if _, err := b.Replace(nil, 0, "A", a, 100, 150); err != nil {
		t.Fatal(err)
	}
	evs, _ := mustSubmit(t, b, limit("C", Sell, 100, 100))
	if tr := trades(evs); len(tr) != 1 || tr[0].BuyOrderID != bo {
		t.Errorf("increased order should have moved behind B: %+v", tr)
	}
	if o, _ := b.Order(a); o.Qty != 150 {
		t.Errorf("replaced order = %+v", o)
	}
}

func TestReplaceToCrossingPriceTrades(t *testing.T) {
	b := newTestBook(t)
	_, bid := mustSubmit(t, b, limit("A", Buy, 100, 100))
	_, ask := mustSubmit(t, b, limit("B", Sell, 102, 60))

	evs, err := b.Replace(nil, 5, "A", bid, 102, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !equalKinds(kinds(evs), EventReplaced, EventTrade) {
		t.Fatalf("events = %v", kinds(evs))
	}
	tr := evs[1]
	if tr.BuyOrderID != bid || tr.SellOrderID != ask || tr.Price != 102 || tr.Qty != 60 || tr.Aggressor != Buy || tr.BuyLeaves != 40 {
		t.Errorf("trade = %+v", tr)
	}
	if o, ok := b.Order(bid); !ok || o.Price != 102 || o.Qty != 40 {
		t.Errorf("remainder = %+v %v", o, ok)
	}
}

func TestReplaceKeepsEntryTimeAndReportsOpenQuantity(t *testing.T) {
	b := newTestBook(t)
	_, id, _ := b.Submit(nil, 10, limit("A", Buy, 100, 100))
	mustSubmit(t, b, limit("B", Sell, 100, 30))

	evs, err := b.Replace(nil, 20, "A", id, 99, 50)
	if err != nil {
		t.Fatal(err)
	}
	if evs[0].PrevQty != 70 {
		t.Errorf("PrevQty = %d, want the open quantity 70", evs[0].PrevQty)
	}
	evs, _ = b.Cancel(nil, 30, "A", id)
	if evs[0].EntryTime != 10 {
		t.Errorf("entry time = %d, want the original 10", evs[0].EntryTime)
	}
}

func TestReplaceRejections(t *testing.T) {
	b, _ := New(Config{Symbol: "TEST", TickSize: 100, LotSize: 10})
	_, id, _ := b.Submit(nil, 0, limit("A", Buy, 10_000, 100))
	seq := b.Seq()

	cases := []struct {
		name    string
		account string
		id      uint64
		price   int64
		qty     int64
		want    error
	}{
		{"unknown order", "A", 999, 10_000, 100, ErrUnknownOrder},
		{"not owner", "B", id, 10_000, 50, ErrNotOwner},
		{"no change", "A", id, 10_000, 100, ErrNoChange},
		{"zero quantity", "A", id, 10_000, 0, ErrInvalidQuantity},
		{"off lot", "A", id, 10_000, 55, ErrLotSize},
		{"off tick", "A", id, 10_050, 100, ErrTickSize},
		{"zero price", "A", id, 0, 100, ErrInvalidPrice},
	}
	for _, tc := range cases {
		evs, err := b.Replace(nil, 0, tc.account, tc.id, tc.price, tc.qty)
		if !errors.Is(err, tc.want) || len(evs) != 0 {
			t.Errorf("%s: err=%v events=%d, want %v", tc.name, err, len(evs), tc.want)
		}
	}
	if b.Seq() != seq {
		t.Errorf("rejected replaces consumed sequence numbers: %d -> %d", seq, b.Seq())
	}
}

func TestSubmitValidation(t *testing.T) {
	b, err := New(Config{Symbol: "TEST", TickSize: 100, LotSize: 10, MaxQty: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		req  Request
		want error
	}{
		{"bad side", Request{Side: 0, Type: Limit, TIF: Day, Price: 100, Qty: 10}, ErrInvalidSide},
		{"bad type", Request{Side: Buy, Type: 7, TIF: Day, Price: 100, Qty: 10}, ErrInvalidType},
		{"bad tif", Request{Side: Buy, Type: Limit, TIF: 9, Price: 100, Qty: 10}, ErrInvalidTIF},
		{"market day", Request{Side: Buy, Type: Market, TIF: Day, Qty: 10}, ErrInvalidTIF},
		{"market with price", Request{Side: Buy, Type: Market, TIF: IOC, Price: 100, Qty: 10}, ErrInvalidPrice},
		{"zero price", Request{Side: Buy, Type: Limit, TIF: Day, Price: 0, Qty: 10}, ErrInvalidPrice},
		{"negative price", Request{Side: Buy, Type: Limit, TIF: Day, Price: -100, Qty: 10}, ErrInvalidPrice},
		{"off tick", Request{Side: Buy, Type: Limit, TIF: Day, Price: 150, Qty: 10}, ErrTickSize},
		{"zero qty", Request{Side: Buy, Type: Limit, TIF: Day, Price: 100, Qty: 0}, ErrInvalidQuantity},
		{"negative qty", Request{Side: Sell, Type: Limit, TIF: Day, Price: 100, Qty: -10}, ErrInvalidQuantity},
		{"off lot", Request{Side: Sell, Type: Limit, TIF: Day, Price: 100, Qty: 15}, ErrLotSize},
		{"too large", Request{Side: Sell, Type: Limit, TIF: Day, Price: 100, Qty: 1_010}, ErrInvalidQuantity},
	}
	for _, tc := range cases {
		evs, id, err := b.Submit(nil, 0, tc.req)
		if !errors.Is(err, tc.want) || len(evs) != 0 || id != 0 {
			t.Errorf("%s: err=%v events=%d id=%d, want %v", tc.name, err, len(evs), id, tc.want)
		}
	}
	if b.Seq() != 0 || b.OpenOrders() != 0 {
		t.Errorf("rejected orders changed the book: seq=%d open=%d", b.Seq(), b.OpenOrders())
	}
	if _, err := New(Config{TickSize: 0, LotSize: 1}); err == nil {
		t.Error("zero tick size accepted")
	}
	if _, err := New(Config{TickSize: 1, LotSize: 0}); err == nil {
		t.Error("zero lot size accepted")
	}
}

func TestCloseSessionCancelsEverythingInDeterministicOrder(t *testing.T) {
	b := newTestBook(t)
	_, b1 := mustSubmit(t, b, limit("A", Buy, 99, 10))
	_, b2 := mustSubmit(t, b, limit("A", Buy, 100, 10))
	_, b3 := mustSubmit(t, b, limit("B", Buy, 100, 10))
	_, a1 := mustSubmit(t, b, limit("A", Sell, 102, 10))
	_, a2 := mustSubmit(t, b, limit("B", Sell, 101, 10))

	evs := b.CloseSession(nil, 99)
	want := []uint64{b2, b3, b1, a2, a1}
	if len(evs) != len(want) {
		t.Fatalf("got %d cancels, want %d", len(evs), len(want))
	}
	for i, e := range evs {
		if e.OrderID != want[i] || e.Reason != ReasonSessionEnd || e.Kind != EventCancelled {
			t.Errorf("cancel %d = %+v, want order %d", i, e, want[i])
		}
	}
	if b.OpenOrders() != 0 {
		t.Error("orders left after close")
	}
	if _, ok := b.BestBid(); ok {
		t.Error("bids left after close")
	}
	// The book keeps working after a close.
	if _, _, err := b.Submit(nil, 100, limit("A", Buy, 100, 10)); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotAggregatesLevelsAndConsumesSequence(t *testing.T) {
	b := newTestBook(t)
	mustSubmit(t, b, limit("A", Buy, 100, 10))
	mustSubmit(t, b, limit("B", Buy, 100, 15))
	mustSubmit(t, b, limit("A", Buy, 98, 5))
	mustSubmit(t, b, limit("A", Sell, 103, 7))
	before := b.Seq()

	evs := b.Snapshot(nil, 5, 1)
	s := evs[0]
	if s.Kind != EventSnapshot || s.Seq != before+1 {
		t.Fatalf("snapshot = %+v", s)
	}
	if len(s.Bids) != 1 || s.Bids[0] != (Level{Price: 100, Qty: 25, Count: 2}) {
		t.Errorf("bids = %+v", s.Bids)
	}
	if len(s.Asks) != 1 || s.Asks[0] != (Level{Price: 103, Qty: 7, Count: 1}) {
		t.Errorf("asks = %+v", s.Asks)
	}
}

func TestSequenceNumbersAreGapFree(t *testing.T) {
	b := newTestBook(t)
	var all []Event
	add := func(evs []Event) { all = append(all, evs...) }

	evs, id := mustSubmit(t, b, limit("A", Sell, 101, 100))
	add(evs)
	evs, _ = mustSubmit(t, b, limit("B", Buy, 101, 40))
	add(evs)
	evs, _ = b.Replace(nil, 0, "A", id, 102, 30)
	add(evs)
	_, _, _ = b.Submit(nil, 0, Request{Side: 0}) // rejected: consumes nothing
	evs, _ = mustSubmit(t, b, Request{Account: "C", Side: Buy, Type: Market, TIF: IOC, Qty: 100})
	add(evs)
	add(b.Snapshot(nil, 0, 5))
	add(b.CloseSession(nil, 0))

	for i, e := range all {
		if e.Seq != uint64(i+1) {
			t.Fatalf("event %d (%v) has seq %d", i, e.Kind, e.Seq)
		}
	}
}

func TestSelfTradeIsAllowed(t *testing.T) {
	b := newTestBook(t)
	mustSubmit(t, b, limit("A", Sell, 101, 10))
	evs, _ := mustSubmit(t, b, limit("A", Buy, 101, 10))
	tr := trades(evs)
	if len(tr) != 1 || tr[0].BuyAccount != "A" || tr[0].SellAccount != "A" {
		t.Fatalf("self-trade = %+v", tr)
	}
}

// The engine's enums are converted to protobuf by casting, so their numeric
// values must stay identical.
func TestEnumValuesMatchProtobuf(t *testing.T) {
	pairs := []struct {
		name      string
		got, want int32
	}{
		{"buy", int32(Buy), int32(twpb.Side_SIDE_BUY)},
		{"sell", int32(Sell), int32(twpb.Side_SIDE_SELL)},
		{"limit", int32(Limit), int32(twpb.OrderType_ORDER_TYPE_LIMIT)},
		{"market", int32(Market), int32(twpb.OrderType_ORDER_TYPE_MARKET)},
		{"day", int32(Day), int32(twpb.TimeInForce_TIME_IN_FORCE_DAY)},
		{"ioc", int32(IOC), int32(twpb.TimeInForce_TIME_IN_FORCE_IOC)},
		{"fok", int32(FOK), int32(twpb.TimeInForce_TIME_IN_FORCE_FOK)},
		{"user", int32(ReasonUser), int32(twpb.CancelReason_CANCEL_REASON_USER)},
		{"ioc remainder", int32(ReasonIOCRemainder), int32(twpb.CancelReason_CANCEL_REASON_IOC_REMAINDER)},
		{"fok unfilled", int32(ReasonFOKUnfilled), int32(twpb.CancelReason_CANCEL_REASON_FOK_UNFILLED)},
		{"no liquidity", int32(ReasonNoLiquidity), int32(twpb.CancelReason_CANCEL_REASON_NO_LIQUIDITY)},
		{"session end", int32(ReasonSessionEnd), int32(twpb.CancelReason_CANCEL_REASON_SESSION_END)},
	}
	for _, p := range pairs {
		if p.got != p.want {
			t.Errorf("%s: engine %d, protobuf %d", p.name, p.got, p.want)
		}
	}
}
