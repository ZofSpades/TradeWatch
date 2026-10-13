package ingest

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
)

func testRef() *ReferenceData { return NewReferenceData(testRefRecords()...) }

func testRefRecords() []*twpb.RefDataRecord {
	return []*twpb.RefDataRecord{
		&twpb.RefDataRecord{Record: &twpb.RefDataRecord_Instrument{Instrument: &twpb.Instrument{
			Symbol: "QZAP", TickSize: 100, LotSize: 1, LiquidityTier: twpb.LiquidityTier_LIQUIDITY_TIER_HIGH}}},
		&twpb.RefDataRecord{Record: &twpb.RefDataRecord_Instrument{Instrument: &twpb.Instrument{
			Symbol: "LOTS", TickSize: 100, LotSize: 100}}},
		&twpb.RefDataRecord{Record: &twpb.RefDataRecord_Account{Account: &twpb.Account{
			AccountId: "ACC-0001", BeneficialOwnerId: "BO-0001", EntityId: "ENT-0001", Kind: twpb.AccountKind_ACCOUNT_KIND_RETAIL}}},
		&twpb.RefDataRecord{Record: &twpb.RefDataRecord_Account{Account: &twpb.Account{
			AccountId: "ACC-0002", BeneficialOwnerId: "BO-0001", EntityId: "ENT-0001", Kind: twpb.AccountKind_ACCOUNT_KIND_RETAIL}}},
		&twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: &twpb.TradingSession{SessionId: "S1"}}},
	}
}

func hdr(seq uint64) *twpb.EventHeader {
	return &twpb.EventHeader{SessionId: "S1", Symbol: "QZAP", Seq: seq, EventTimeNs: 1_000}
}

func validOrder() *twpb.OrderEvent {
	return &twpb.OrderEvent{
		Header: hdr(1), Action: twpb.OrderAction_ORDER_ACTION_NEW, OrderId: 1, AccountId: "ACC-0001",
		Side: twpb.Side_SIDE_BUY, Type: twpb.OrderType_ORDER_TYPE_LIMIT, TimeInForce: twpb.TimeInForce_TIME_IN_FORCE_DAY,
		Price: 1_000_000, Quantity: 100,
	}
}

func validCancel() *twpb.CancelEvent {
	return &twpb.CancelEvent{
		Header: hdr(2), OrderId: 1, AccountId: "ACC-0001", Side: twpb.Side_SIDE_BUY, Price: 1_000_000,
		CancelledQuantity: 100, Reason: twpb.CancelReason_CANCEL_REASON_USER, OrderEntryTimeNs: 900,
	}
}

func validTrade() *twpb.TradeEvent {
	return &twpb.TradeEvent{
		Header: hdr(3), TradeId: 1, Price: 1_000_000, Quantity: 50, AggressorSide: twpb.Side_SIDE_SELL,
		BuyOrderId: 1, SellOrderId: 2, BuyAccountId: "ACC-0001", SellAccountId: "ACC-0002", BuyLeavesQuantity: 50,
	}
}

func validBook() *twpb.BookSnapshot {
	return &twpb.BookSnapshot{
		Header: hdr(4), Phase: twpb.TradingPhase_TRADING_PHASE_CONTINUOUS,
		Bids: []*twpb.PriceLevel{{Price: 999_900, Quantity: 100, OrderCount: 1}, {Price: 999_800, Quantity: 50, OrderCount: 2}},
		Asks: []*twpb.PriceLevel{{Price: 1_000_100, Quantity: 70, OrderCount: 1}},
	}
}

func raw(t *testing.T, f Feed, m proto.Message) Raw {
	t.Helper()
	b, err := events.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	h := m.(interface{ GetHeader() *twpb.EventHeader }).GetHeader()
	return Raw{
		Feed: f, Key: []byte(h.GetSymbol()), Value: b,
		Meta: kafka.EventMeta{Schema: feedSchemas[f], Session: h.GetSessionId(), Seq: h.GetSeq(), HasSeq: true},
	}
}

func TestParseAcceptsAndEnriches(t *testing.T) {
	ref := testRef()
	ev, rej := Parse(raw(t, FeedTrades, validTrade()), ref, 42)
	if rej != nil {
		t.Fatal(rej)
	}
	if ev.GetTrade().GetTradeId() != 1 || ev.GetIngestedAtNs() != 42 {
		t.Errorf("event = %v", ev)
	}
	e := ev.GetEnrichment()
	if e.GetBuyAccount().GetBeneficialOwnerId() != "BO-0001" || e.GetSellAccount().GetAccountId() != "ACC-0002" {
		t.Errorf("trade enrichment = %v", e)
	}
	if e.GetTickSize() != 100 || e.GetLiquidityTier() != twpb.LiquidityTier_LIQUIDITY_TIER_HIGH {
		t.Errorf("instrument enrichment = %v", e)
	}

	ev, rej = Parse(raw(t, FeedOrders, validOrder()), ref, 0)
	if rej != nil || ev.GetEnrichment().GetAccount().GetKind() != twpb.AccountKind_ACCOUNT_KIND_RETAIL {
		t.Fatalf("order: %v %v", ev, rej)
	}
	if _, rej := Parse(raw(t, FeedCancels, validCancel()), ref, 0); rej != nil {
		t.Fatal(rej)
	}
	if _, rej := Parse(raw(t, FeedBook, validBook()), ref, 0); rej != nil {
		t.Fatal(rej)
	}
}

func TestParseNormalizesSymbolAndIdentifiers(t *testing.T) {
	o := validOrder()
	o.Header.Symbol = " qzap "
	o.AccountId = " ACC-0001 "
	r := raw(t, FeedOrders, o)
	r.Key = []byte("qzap")
	ev, rej := Parse(r, testRef(), 0)
	if rej != nil {
		t.Fatal(rej)
	}
	if ev.GetOrder().GetHeader().GetSymbol() != "QZAP" || ev.GetOrder().GetAccountId() != "ACC-0001" {
		t.Errorf("not normalized: %v", ev)
	}
}

func TestParseRejections(t *testing.T) {
	ref := testRef()
	type tc struct {
		name   string
		raw    func(t *testing.T) Raw
		reason string
	}
	mutated := func(f Feed, mk func() proto.Message, mutate func(proto.Message)) func(t *testing.T) Raw {
		return func(t *testing.T) Raw {
			m := mk()
			mutate(m)
			return raw(t, f, m)
		}
	}
	order := func() proto.Message { return validOrder() }
	cancel := func() proto.Message { return validCancel() }
	trade := func() proto.Message { return validTrade() }
	book := func() proto.Message { return validBook() }
	o := func(f func(*twpb.OrderEvent)) func(proto.Message) {
		return func(m proto.Message) { f(m.(*twpb.OrderEvent)) }
	}
	c := func(f func(*twpb.CancelEvent)) func(proto.Message) {
		return func(m proto.Message) { f(m.(*twpb.CancelEvent)) }
	}
	tr := func(f func(*twpb.TradeEvent)) func(proto.Message) {
		return func(m proto.Message) { f(m.(*twpb.TradeEvent)) }
	}
	b := func(f func(*twpb.BookSnapshot)) func(proto.Message) {
		return func(m proto.Message) { f(m.(*twpb.BookSnapshot)) }
	}

	cases := []tc{
		{"garbage bytes", func(t *testing.T) Raw {
			r := raw(t, FeedOrders, validOrder())
			r.Value = []byte{0xff, 0xff, 0xff}
			return r
		}, ReasonDecode},
		{"schema header for another feed", func(t *testing.T) Raw {
			r := raw(t, FeedOrders, validOrder())
			r.Meta.Schema = events.SchemaTrade
			return r
		}, ReasonSchemaMismatch},
		{"key differs from symbol", func(t *testing.T) Raw {
			r := raw(t, FeedOrders, validOrder())
			r.Key = []byte("VNTR")
			return r
		}, ReasonKeyMismatch},
		{"seq header differs", func(t *testing.T) Raw {
			r := raw(t, FeedOrders, validOrder())
			r.Meta.Seq = 99
			return r
		}, ReasonHeaderMismatch},
		{"session header differs", func(t *testing.T) Raw {
			r := raw(t, FeedOrders, validOrder())
			r.Meta.Session = "S2"
			return r
		}, ReasonHeaderMismatch},
		{"no header", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Header = nil })), ReasonMissingField},
		{"no session", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Header.SessionId = "" })), ReasonMissingField},
		{"zero seq", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Header.Seq = 0 })), ReasonMissingField},
		{"no event time", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Header.EventTimeNs = 0 })), ReasonInvalidField},
		{"unknown symbol", func(t *testing.T) Raw {
			m := validOrder()
			m.Header.Symbol = "NOPE"
			return raw(t, FeedOrders, m)
		}, ReasonUnknownSymbol},
		{"unknown session", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Header.SessionId = "S9" })), ReasonUnknownSession},
		{"unknown account", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.AccountId = "ACC-9999" })), ReasonUnknownAccount},
		{"negative quantity", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Quantity = -100 })), ReasonInvalidField},
		{"off-tick price", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Price = 1_000_050 })), ReasonOffTick},
		{"bad side", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Side = 7 })), ReasonInvalidField},
		{"bad type", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Type = 0 })), ReasonInvalidField},
		{"market with price", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) {
			m.Type, m.TimeInForce = twpb.OrderType_ORDER_TYPE_MARKET, twpb.TimeInForce_TIME_IN_FORCE_IOC
		})), ReasonInvalidField},
		{"replace without previous", mutated(FeedOrders, order, o(func(m *twpb.OrderEvent) { m.Action = twpb.OrderAction_ORDER_ACTION_REPLACE })), ReasonInvalidField},
		{"cancel entered after itself", mutated(FeedCancels, cancel, c(func(m *twpb.CancelEvent) { m.OrderEntryTimeNs = 5_000 })), ReasonInvalidField},
		{"cancel zero qty", mutated(FeedCancels, cancel, c(func(m *twpb.CancelEvent) { m.CancelledQuantity = 0 })), ReasonInvalidField},
		{"cancel bad reason", mutated(FeedCancels, cancel, c(func(m *twpb.CancelEvent) { m.Reason = 42 })), ReasonInvalidField},
		{"self-referencing trade", mutated(FeedTrades, trade, tr(func(m *twpb.TradeEvent) { m.SellOrderId = m.BuyOrderId })), ReasonInvalidField},
		{"trade unknown seller", mutated(FeedTrades, trade, tr(func(m *twpb.TradeEvent) { m.SellAccountId = "X" })), ReasonUnknownAccount},
		{"trade negative leaves", mutated(FeedTrades, trade, tr(func(m *twpb.TradeEvent) { m.BuyLeavesQuantity = -1 })), ReasonInvalidField},
		{"crossed book", mutated(FeedBook, book, b(func(m *twpb.BookSnapshot) { m.Asks[0].Price = 999_900 })), ReasonInvalidField},
		{"unsorted bids", mutated(FeedBook, book, b(func(m *twpb.BookSnapshot) { m.Bids[1].Price = 1_000_000 })), ReasonInvalidField},
		{"empty level", mutated(FeedBook, book, b(func(m *twpb.BookSnapshot) { m.Bids[0].OrderCount = 0 })), ReasonInvalidField},
		{"bad phase", mutated(FeedBook, book, b(func(m *twpb.BookSnapshot) { m.Phase = 0 })), ReasonInvalidField},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, rej := Parse(tc.raw(t), ref, 0)
			if rej == nil {
				t.Fatalf("accepted: %v", ev)
			}
			if rej.Reason != tc.reason {
				t.Errorf("reason %q (%s), want %q", rej.Reason, rej.Detail, tc.reason)
			}
		})
	}
}

func TestParseRejectsOffLotQuantity(t *testing.T) {
	m := validOrder()
	m.Header.Symbol = "LOTS"
	m.Quantity = 150
	if _, rej := Parse(raw(t, FeedOrders, m), testRef(), 0); rej == nil || rej.Reason != ReasonOffLot {
		t.Fatalf("got %v", rej)
	}
}

// FuzzParse feeds arbitrary bytes to the parser. It must never panic, and
// anything it accepts must be internally consistent: the event's header
// matches the record key and passes validation again.
func FuzzParse(f *testing.F) {
	ref := testRef()
	for _, m := range []proto.Message{validOrder(), validCancel(), validTrade(), validBook()} {
		b, _ := events.Marshal(m)
		f.Add(uint8(0), []byte("QZAP"), b)
		f.Add(uint8(1), []byte("QZAP"), b)
		f.Add(uint8(2), []byte("QZAP"), b)
		f.Add(uint8(3), []byte("QZAP"), b)
	}
	f.Add(uint8(0), []byte(""), []byte{})
	f.Fuzz(func(t *testing.T, feed uint8, key, value []byte) {
		r := Raw{Feed: Feed(feed % uint8(numFeeds)), Key: key, Value: value}
		ev, rej := Parse(r, ref, 1)
		if rej != nil {
			if rej.Reason == "" {
				t.Fatal("rejection without a reason")
			}
			return
		}
		h := events.HeaderOf(ev)
		if h == nil || h.GetSymbol() == "" || h.GetSeq() == 0 || ev.GetEnrichment() == nil {
			t.Fatalf("accepted an incomplete event: %v", ev)
		}
		// Re-encoding the accepted event's payload must parse identically.
		b, err := events.Marshal(events.Unwrap(ev))
		if err != nil {
			t.Fatal(err)
		}
		again, rej := Parse(Raw{Feed: r.Feed, Key: []byte(h.GetSymbol()), Value: b}, ref, 1)
		if rej != nil {
			t.Fatalf("accepted event fails re-validation: %v", rej)
		}
		if !proto.Equal(ev, again) {
			t.Fatalf("parse is not stable:\n%v\n%v", ev, again)
		}
	})
}
