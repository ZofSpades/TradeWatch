// Package orderbook implements a single-symbol limit order book with
// price-time-priority matching.
//
// Supported order types are limit and market orders. Time-in-force can be
// DAY, IOC (immediate-or-cancel) or FOK (fill-or-kill). Resting orders can be
// cancelled or replaced. Every state change is reported as an Event that
// carries a per-book sequence number. Sequence numbers are gap-free, start at
// 1, and are shared by all event kinds, so the events form a complete,
// totally ordered audit trail of the book.
//
// Matching rules:
//
//   - An incoming order matches against the best-priced opposite level
//     first. Within a level it matches against the earliest-entered order.
//   - Trades execute at the resting order's price.
//   - A limit order never trades through its limit price.
//   - The remainder of a DAY limit order rests on the book. The remainder of
//     an IOC limit order or of a market order is cancelled. A FOK order fills
//     in full immediately or is cancelled without trading.
//   - Replacing an order keeps its time priority only when the price is
//     unchanged and the quantity is reduced. Any other replace re-enters the
//     order at the back of the queue for its new price, and the order may
//     trade immediately if the new price crosses the book.
//
// There is deliberately no self-trade prevention: the simulator must be able
// to produce self-matched (wash) trades for the detectors to find.
//
// A Book is not safe for concurrent use. The simulator drives each symbol's
// book from a single goroutine.
package orderbook

import "errors"

// Side is the side of an order. Values match tradewatch.v1.Side.
type Side int8

const (
	Buy  Side = 1
	Sell Side = 2
)

// Opposite returns the other side.
func (s Side) Opposite() Side {
	if s == Buy {
		return Sell
	}
	return Buy
}

func (s Side) String() string {
	switch s {
	case Buy:
		return "buy"
	case Sell:
		return "sell"
	default:
		return "invalid"
	}
}

// OrderType values match tradewatch.v1.OrderType.
type OrderType int8

const (
	Limit  OrderType = 1
	Market OrderType = 2
)

// TimeInForce values match tradewatch.v1.TimeInForce.
type TimeInForce int8

const (
	Day TimeInForce = 1
	IOC TimeInForce = 2
	FOK TimeInForce = 3
)

// CancelReason values match tradewatch.v1.CancelReason.
type CancelReason int8

const (
	ReasonUser         CancelReason = 1
	ReasonIOCRemainder CancelReason = 2
	ReasonFOKUnfilled  CancelReason = 3
	ReasonNoLiquidity  CancelReason = 4
	ReasonSessionEnd   CancelReason = 5
)

// EventKind identifies the kind of an Event.
type EventKind uint8

const (
	// EventAccepted reports a new order accepted by the book. It precedes
	// any trades or cancels that the order causes.
	EventAccepted EventKind = iota + 1
	// EventReplaced reports a cancel/replace of a resting order.
	EventReplaced
	// EventTrade reports one execution between an aggressor and a resting
	// order.
	EventTrade
	// EventCancelled reports open quantity removed without trading.
	EventCancelled
	// EventSnapshot reports the aggregated top of the book.
	EventSnapshot
)

func (k EventKind) String() string {
	switch k {
	case EventAccepted:
		return "accepted"
	case EventReplaced:
		return "replaced"
	case EventTrade:
		return "trade"
	case EventCancelled:
		return "cancelled"
	case EventSnapshot:
		return "snapshot"
	default:
		return "invalid"
	}
}

// Level is an aggregated price level.
type Level struct {
	Price int64
	Qty   int64
	Count int
}

// Event is a single entry of the book's audit trail. Which fields are set
// depends on Kind:
//
//   - Accepted: OrderID, Account, Side, Type, TIF, Price (0 for market
//     orders), Qty.
//   - Replaced: OrderID, Account, Side, Price and Qty (the new open
//     quantity), PrevPrice, PrevQty.
//   - Trade: TradeID, Price, Qty, Aggressor, Buy*/Sell* order IDs and
//     accounts, BuyLeaves/SellLeaves (open quantity after the fill).
//   - Cancelled: OrderID, Account, Side, Price, Qty (the cancelled quantity),
//     Reason, EntryTime.
//   - Snapshot: Bids, Asks, LastPrice, Volume.
type Event struct {
	Kind EventKind
	Seq  uint64
	Time int64

	OrderID   uint64
	Account   string
	Side      Side
	Type      OrderType
	TIF       TimeInForce
	Price     int64
	Qty       int64
	PrevPrice int64
	PrevQty   int64
	Reason    CancelReason
	EntryTime int64

	TradeID     uint64
	Aggressor   Side
	BuyOrderID  uint64
	SellOrderID uint64
	BuyAccount  string
	SellAccount string
	BuyLeaves   int64
	SellLeaves  int64

	Bids      []Level
	Asks      []Level
	LastPrice int64
	Volume    int64
}

// Request is a new-order request.
type Request struct {
	Account string
	Side    Side
	Type    OrderType
	TIF     TimeInForce
	// Price is the limit price, a positive multiple of the tick size. It must
	// be zero for market orders.
	Price int64
	// Qty is a positive multiple of the lot size.
	Qty int64
}

// OrderView is a read-only view of a resting order.
type OrderView struct {
	ID        uint64
	Account   string
	Side      Side
	Price     int64
	Qty       int64
	EntryTime int64
}

// Errors returned for rejected requests. A rejected request leaves the book
// unchanged, emits no events and consumes no sequence number.
var (
	ErrInvalidSide     = errors.New("orderbook: invalid side")
	ErrInvalidType     = errors.New("orderbook: invalid order type")
	ErrInvalidTIF      = errors.New("orderbook: invalid time in force")
	ErrInvalidPrice    = errors.New("orderbook: invalid price")
	ErrInvalidQuantity = errors.New("orderbook: invalid quantity")
	ErrTickSize        = errors.New("orderbook: price is not a multiple of the tick size")
	ErrLotSize         = errors.New("orderbook: quantity is not a multiple of the lot size")
	ErrUnknownOrder    = errors.New("orderbook: unknown order")
	ErrNotOwner        = errors.New("orderbook: order belongs to another account")
	ErrNoChange        = errors.New("orderbook: replace does not change the order")
)
