package sim

import (
	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
)

// Feed identifies the raw topic an event is published on.
type Feed uint8

const (
	FeedOrders Feed = iota + 1
	FeedCancels
	FeedTrades
	FeedBook
)

// FeedOf returns the feed a book event belongs to.
func FeedOf(k ob.EventKind) Feed {
	switch k {
	case ob.EventAccepted, ob.EventReplaced:
		return FeedOrders
	case ob.EventCancelled:
		return FeedCancels
	case ob.EventTrade:
		return FeedTrades
	default:
		return FeedBook
	}
}

// ToProto converts a book event of sym into the protobuf message published
// on its raw feed. publishedAt is the wall-clock publish time; pass 0 when
// the message is not being published (recordings, tests).
func ToProto(sessionID string, sym *Symbol, ev *ob.Event, publishedAt int64) proto.Message {
	h := &twpb.EventHeader{
		SessionId: sessionID, Symbol: sym.Symbol, Seq: ev.Seq,
		EventTimeNs: ev.Time, PublishedAtNs: publishedAt,
	}
	switch ev.Kind {
	case ob.EventAccepted:
		return &twpb.OrderEvent{
			Header: h, Action: twpb.OrderAction_ORDER_ACTION_NEW, OrderId: ev.OrderID,
			AccountId: ev.Account, Side: twpb.Side(ev.Side), Type: twpb.OrderType(ev.Type),
			TimeInForce: twpb.TimeInForce(ev.TIF), Price: ev.Price, Quantity: ev.Qty,
		}
	case ob.EventReplaced:
		return &twpb.OrderEvent{
			Header: h, Action: twpb.OrderAction_ORDER_ACTION_REPLACE, OrderId: ev.OrderID,
			AccountId: ev.Account, Side: twpb.Side(ev.Side), Type: twpb.OrderType(ev.Type),
			TimeInForce: twpb.TimeInForce(ev.TIF), Price: ev.Price, Quantity: ev.Qty,
			PreviousPrice: ev.PrevPrice, PreviousQuantity: ev.PrevQty,
		}
	case ob.EventCancelled:
		return &twpb.CancelEvent{
			Header: h, OrderId: ev.OrderID, AccountId: ev.Account, Side: twpb.Side(ev.Side),
			Price: ev.Price, CancelledQuantity: ev.Qty, Reason: twpb.CancelReason(ev.Reason),
			OrderEntryTimeNs: ev.EntryTime,
		}
	case ob.EventTrade:
		return &twpb.TradeEvent{
			Header: h, TradeId: ev.TradeID, Price: ev.Price, Quantity: ev.Qty,
			AggressorSide: twpb.Side(ev.Aggressor), BuyOrderId: ev.BuyOrderID, SellOrderId: ev.SellOrderID,
			BuyAccountId: ev.BuyAccount, SellAccountId: ev.SellAccount,
			BuyLeavesQuantity: ev.BuyLeaves, SellLeavesQuantity: ev.SellLeaves,
		}
	default:
		return &twpb.BookSnapshot{
			Header: h, Phase: sym.Phase, Bids: levels(ev.Bids), Asks: levels(ev.Asks),
			LastTradePrice: ev.LastPrice, SessionVolume: ev.Volume,
		}
	}
}

func levels(ls []ob.Level) []*twpb.PriceLevel {
	out := make([]*twpb.PriceLevel, len(ls))
	for i, l := range ls {
		out[i] = &twpb.PriceLevel{Price: l.Price, Quantity: l.Qty, OrderCount: uint32(l.Count)}
	}
	return out
}
