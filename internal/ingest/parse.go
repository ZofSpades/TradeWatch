// Package ingest validates, normalizes and enriches the raw exchange feeds
// and re-sequences them into the per-symbol ordered market_events stream.
package ingest

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
)

// Feed is the index of a raw feed within a co-partitioned group.
type Feed int

const (
	FeedOrders Feed = iota
	FeedCancels
	FeedTrades
	FeedBook
	numFeeds
)

func (f Feed) String() string {
	switch f {
	case FeedOrders:
		return "orders"
	case FeedCancels:
		return "cancels"
	case FeedTrades:
		return "trades"
	case FeedBook:
		return "book_snapshots"
	default:
		return "unknown"
	}
}

var feedSchemas = [numFeeds]string{
	FeedOrders:  events.SchemaOrder,
	FeedCancels: events.SchemaCancel,
	FeedTrades:  events.SchemaTrade,
	FeedBook:    events.SchemaBookSnapshot,
}

// Reason codes for rejected records. They are stable, machine-readable
// identifiers stored in DeadLetter.reason.
const (
	ReasonDecode         = "decode_error"
	ReasonSchemaMismatch = "schema_mismatch"
	ReasonMissingField   = "missing_field"
	ReasonInvalidField   = "invalid_field"
	ReasonKeyMismatch    = "key_mismatch"
	ReasonHeaderMismatch = "header_mismatch"
	ReasonUnknownSymbol  = "unknown_symbol"
	ReasonUnknownAccount = "unknown_account"
	ReasonUnknownSession = "unknown_session"
	ReasonOffTick        = "price_off_tick"
	ReasonOffLot         = "quantity_off_lot"
)

// Rejection explains why a record cannot be ingested.
type Rejection struct {
	Reason string
	Detail string
}

func (r *Rejection) Error() string { return r.Reason + ": " + r.Detail }

func reject(reason, format string, args ...any) *Rejection {
	return &Rejection{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// Reference is the reference data ingest validates and enriches against.
type Reference interface {
	Account(id string) (*twpb.Account, bool)
	Instrument(symbol string) (*twpb.Instrument, bool)
	Session(id string) (*twpb.TradingSession, bool)
}

// Raw is a record read from one of the raw feeds.
type Raw struct {
	Feed  Feed
	Key   []byte
	Value []byte
	Meta  kafka.EventMeta
}

// Parse turns a raw record into an enriched MarketEvent, or explains why it
// must be dead-lettered. It never panics on arbitrary input.
//
// Normalization is limited to canonical forms that do not change meaning:
// symbols are upper-cased and identifiers trimmed. Anything else that
// deviates from the schema's rules is rejected rather than repaired.
// Repairing data silently would hide upstream defects from surveillance.
func Parse(r Raw, ref Reference, ingestedAt int64) (*twpb.MarketEvent, *Rejection) {
	if r.Feed < 0 || r.Feed >= numFeeds {
		return nil, reject(ReasonSchemaMismatch, "unknown feed %d", r.Feed)
	}
	if r.Meta.Schema != "" && r.Meta.Schema != feedSchemas[r.Feed] {
		return nil, reject(ReasonSchemaMismatch, "%s carries %q", r.Feed, r.Meta.Schema)
	}
	var msg proto.Message
	switch r.Feed {
	case FeedOrders:
		msg = &twpb.OrderEvent{}
	case FeedCancels:
		msg = &twpb.CancelEvent{}
	case FeedTrades:
		msg = &twpb.TradeEvent{}
	default:
		msg = &twpb.BookSnapshot{}
	}
	if err := events.Unmarshal(r.Value, msg); err != nil {
		return nil, reject(ReasonDecode, "%v", err)
	}
	ev, err := events.Wrap(msg)
	if err != nil {
		return nil, reject(ReasonSchemaMismatch, "%v", err)
	}
	h := events.HeaderOf(ev)
	if h == nil {
		return nil, reject(ReasonMissingField, "header")
	}
	h.Symbol = strings.ToUpper(strings.TrimSpace(h.Symbol))
	h.SessionId = strings.TrimSpace(h.SessionId)
	switch {
	case h.SessionId == "":
		return nil, reject(ReasonMissingField, "header.session_id")
	case h.Symbol == "":
		return nil, reject(ReasonMissingField, "header.symbol")
	case h.Seq == 0:
		return nil, reject(ReasonMissingField, "header.seq")
	case h.EventTimeNs <= 0:
		return nil, reject(ReasonInvalidField, "header.event_time_ns %d", h.EventTimeNs)
	}
	if key := strings.ToUpper(strings.TrimSpace(string(r.Key))); key != h.Symbol {
		return nil, reject(ReasonKeyMismatch, "record key %q, payload symbol %q", r.Key, h.Symbol)
	}
	if r.Meta.HasSeq && r.Meta.Seq != h.Seq {
		return nil, reject(ReasonHeaderMismatch, "tw-seq %d, payload seq %d", r.Meta.Seq, h.Seq)
	}
	if r.Meta.Session != "" && r.Meta.Session != h.SessionId {
		return nil, reject(ReasonHeaderMismatch, "tw-session %q, payload session %q", r.Meta.Session, h.SessionId)
	}
	inst, ok := ref.Instrument(h.Symbol)
	if !ok {
		return nil, reject(ReasonUnknownSymbol, "%q", h.Symbol)
	}
	if _, ok := ref.Session(h.SessionId); !ok {
		return nil, reject(ReasonUnknownSession, "%q", h.SessionId)
	}

	enr := &twpb.Enrichment{LiquidityTier: inst.GetLiquidityTier(), TickSize: inst.GetTickSize()}
	var rej *Rejection
	switch m := msg.(type) {
	case *twpb.OrderEvent:
		rej = checkOrder(m, inst, ref, enr)
	case *twpb.CancelEvent:
		rej = checkCancel(m, inst, ref, enr)
	case *twpb.TradeEvent:
		rej = checkTrade(m, inst, ref, enr)
	case *twpb.BookSnapshot:
		rej = checkBook(m, inst)
	}
	if rej != nil {
		return nil, rej
	}
	ev.Enrichment = enr
	ev.IngestedAtNs = ingestedAt
	return ev, nil
}

func accountRef(ref Reference, id string) (*twpb.AccountRef, bool) {
	a, ok := ref.Account(id)
	if !ok {
		return nil, false
	}
	return &twpb.AccountRef{
		AccountId: a.GetAccountId(), BeneficialOwnerId: a.GetBeneficialOwnerId(),
		EntityId: a.GetEntityId(), Kind: a.GetKind(),
	}, true
}

func validSide(s twpb.Side) bool { return s == twpb.Side_SIDE_BUY || s == twpb.Side_SIDE_SELL }

func checkPrice(field string, p int64, inst *twpb.Instrument) *Rejection {
	if p <= 0 {
		return reject(ReasonInvalidField, "%s %d must be positive", field, p)
	}
	if tick := inst.GetTickSize(); tick > 0 && p%tick != 0 {
		return reject(ReasonOffTick, "%s %d is not a multiple of tick %d", field, p, tick)
	}
	return nil
}

func checkQty(field string, q int64, inst *twpb.Instrument) *Rejection {
	if q <= 0 {
		return reject(ReasonInvalidField, "%s %d must be positive", field, q)
	}
	if lot := inst.GetLotSize(); lot > 0 && q%lot != 0 {
		return reject(ReasonOffLot, "%s %d is not a multiple of lot %d", field, q, lot)
	}
	return nil
}

func checkOrder(m *twpb.OrderEvent, inst *twpb.Instrument, ref Reference, enr *twpb.Enrichment) *Rejection {
	m.AccountId = strings.TrimSpace(m.AccountId)
	switch {
	case m.Action != twpb.OrderAction_ORDER_ACTION_NEW && m.Action != twpb.OrderAction_ORDER_ACTION_REPLACE:
		return reject(ReasonInvalidField, "action %d", m.Action)
	case m.OrderId == 0:
		return reject(ReasonMissingField, "order_id")
	case m.AccountId == "":
		return reject(ReasonMissingField, "account_id")
	case !validSide(m.Side):
		return reject(ReasonInvalidField, "side %d", m.Side)
	}
	switch m.Type {
	case twpb.OrderType_ORDER_TYPE_LIMIT:
		if m.TimeInForce < twpb.TimeInForce_TIME_IN_FORCE_DAY || m.TimeInForce > twpb.TimeInForce_TIME_IN_FORCE_FOK {
			return reject(ReasonInvalidField, "time_in_force %d", m.TimeInForce)
		}
		if r := checkPrice("price", m.Price, inst); r != nil {
			return r
		}
	case twpb.OrderType_ORDER_TYPE_MARKET:
		if m.TimeInForce != twpb.TimeInForce_TIME_IN_FORCE_IOC && m.TimeInForce != twpb.TimeInForce_TIME_IN_FORCE_FOK {
			return reject(ReasonInvalidField, "market order with time_in_force %d", m.TimeInForce)
		}
		if m.Price != 0 {
			return reject(ReasonInvalidField, "market order with price %d", m.Price)
		}
	default:
		return reject(ReasonInvalidField, "type %d", m.Type)
	}
	if r := checkQty("quantity", m.Quantity, inst); r != nil {
		return r
	}
	if m.Action == twpb.OrderAction_ORDER_ACTION_REPLACE {
		if m.Type != twpb.OrderType_ORDER_TYPE_LIMIT || m.TimeInForce != twpb.TimeInForce_TIME_IN_FORCE_DAY {
			return reject(ReasonInvalidField, "replace of a non-resting order type")
		}
		if r := checkPrice("previous_price", m.PreviousPrice, inst); r != nil {
			return r
		}
		if r := checkQty("previous_quantity", m.PreviousQuantity, inst); r != nil {
			return r
		}
	}
	acct, ok := accountRef(ref, m.AccountId)
	if !ok {
		return reject(ReasonUnknownAccount, "%q", m.AccountId)
	}
	enr.Account = acct
	return nil
}

func checkCancel(m *twpb.CancelEvent, inst *twpb.Instrument, ref Reference, enr *twpb.Enrichment) *Rejection {
	m.AccountId = strings.TrimSpace(m.AccountId)
	switch {
	case m.OrderId == 0:
		return reject(ReasonMissingField, "order_id")
	case m.AccountId == "":
		return reject(ReasonMissingField, "account_id")
	case !validSide(m.Side):
		return reject(ReasonInvalidField, "side %d", m.Side)
	case m.Reason < twpb.CancelReason_CANCEL_REASON_USER || m.Reason > twpb.CancelReason_CANCEL_REASON_SESSION_END:
		return reject(ReasonInvalidField, "reason %d", m.Reason)
	case m.OrderEntryTimeNs <= 0 || m.OrderEntryTimeNs > m.Header.GetEventTimeNs():
		return reject(ReasonInvalidField, "order_entry_time_ns %d", m.OrderEntryTimeNs)
	}
	// Remainders of market orders are cancelled at price zero.
	if m.Price != 0 {
		if r := checkPrice("price", m.Price, inst); r != nil {
			return r
		}
	}
	if r := checkQty("cancelled_quantity", m.CancelledQuantity, inst); r != nil {
		return r
	}
	acct, ok := accountRef(ref, m.AccountId)
	if !ok {
		return reject(ReasonUnknownAccount, "%q", m.AccountId)
	}
	enr.Account = acct
	return nil
}

func checkTrade(m *twpb.TradeEvent, inst *twpb.Instrument, ref Reference, enr *twpb.Enrichment) *Rejection {
	m.BuyAccountId = strings.TrimSpace(m.BuyAccountId)
	m.SellAccountId = strings.TrimSpace(m.SellAccountId)
	switch {
	case m.TradeId == 0:
		return reject(ReasonMissingField, "trade_id")
	case m.BuyOrderId == 0 || m.SellOrderId == 0:
		return reject(ReasonMissingField, "buy_order_id or sell_order_id")
	case m.BuyOrderId == m.SellOrderId:
		return reject(ReasonInvalidField, "order %d trades with itself", m.BuyOrderId)
	case !validSide(m.AggressorSide):
		return reject(ReasonInvalidField, "aggressor_side %d", m.AggressorSide)
	case m.BuyLeavesQuantity < 0 || m.SellLeavesQuantity < 0:
		return reject(ReasonInvalidField, "negative leaves quantity")
	}
	if r := checkPrice("price", m.Price, inst); r != nil {
		return r
	}
	if r := checkQty("quantity", m.Quantity, inst); r != nil {
		return r
	}
	buy, ok := accountRef(ref, m.BuyAccountId)
	if !ok {
		return reject(ReasonUnknownAccount, "buy account %q", m.BuyAccountId)
	}
	sell, ok := accountRef(ref, m.SellAccountId)
	if !ok {
		return reject(ReasonUnknownAccount, "sell account %q", m.SellAccountId)
	}
	enr.BuyAccount, enr.SellAccount = buy, sell
	return nil
}

func checkBook(m *twpb.BookSnapshot, inst *twpb.Instrument) *Rejection {
	if m.Phase < twpb.TradingPhase_TRADING_PHASE_CONTINUOUS || m.Phase > twpb.TradingPhase_TRADING_PHASE_CLOSED {
		return reject(ReasonInvalidField, "phase %d", m.Phase)
	}
	check := func(side string, levels []*twpb.PriceLevel, better func(a, b int64) bool) *Rejection {
		for i, l := range levels {
			if r := checkPrice(side+" price", l.GetPrice(), inst); r != nil {
				return r
			}
			if l.GetQuantity() <= 0 || l.GetOrderCount() == 0 {
				return reject(ReasonInvalidField, "%s level %d is empty", side, i)
			}
			if i > 0 && !better(levels[i-1].GetPrice(), l.GetPrice()) {
				return reject(ReasonInvalidField, "%s levels are not sorted best first", side)
			}
		}
		return nil
	}
	if r := check("bid", m.Bids, func(a, b int64) bool { return a > b }); r != nil {
		return r
	}
	if r := check("ask", m.Asks, func(a, b int64) bool { return a < b }); r != nil {
		return r
	}
	if len(m.Bids) > 0 && len(m.Asks) > 0 && m.Bids[0].GetPrice() >= m.Asks[0].GetPrice() {
		return reject(ReasonInvalidField, "crossed book: bid %d >= ask %d", m.Bids[0].GetPrice(), m.Asks[0].GetPrice())
	}
	if m.SessionVolume < 0 || m.LastTradePrice < 0 {
		return reject(ReasonInvalidField, "negative volume or last price")
	}
	return nil
}
