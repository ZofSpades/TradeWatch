// Package events provides encoding helpers for the TradeWatch protobuf
// schema (package tradewatch.v1, generated into ./twpb).
//
// All messages are marshalled deterministically so that identical inputs
// always produce identical bytes; recordings and replays rely on this to be
// comparable byte for byte.
package events

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

var marshalOptions = proto.MarshalOptions{Deterministic: true}

// Marshal encodes m deterministically.
func Marshal(m proto.Message) ([]byte, error) {
	return marshalOptions.Marshal(m)
}

// MarshalAppend appends the deterministic encoding of m to b.
func MarshalAppend(b []byte, m proto.Message) ([]byte, error) {
	return marshalOptions.MarshalAppend(b, m)
}

// Unmarshal decodes b into m.
func Unmarshal(b []byte, m proto.Message) error {
	return proto.Unmarshal(b, m)
}

// SchemaName returns the fully-qualified protobuf name of m, which is used as
// the value of the tw-schema record header.
func SchemaName(m proto.Message) string {
	return string(m.ProtoReflect().Descriptor().FullName())
}

// Schema names of the messages carried on each topic.
var (
	SchemaOrder        = SchemaName(&twpb.OrderEvent{})
	SchemaCancel       = SchemaName(&twpb.CancelEvent{})
	SchemaTrade        = SchemaName(&twpb.TradeEvent{})
	SchemaBookSnapshot = SchemaName(&twpb.BookSnapshot{})
	SchemaMarketEvent  = SchemaName(&twpb.MarketEvent{})
	SchemaRefData      = SchemaName(&twpb.RefDataRecord{})
	SchemaGroundTruth  = SchemaName(&twpb.GroundTruthLabel{})
	SchemaAlert        = SchemaName(&twpb.Alert{})
	SchemaDeadLetter   = SchemaName(&twpb.DeadLetter{})
	SchemaRuleSet      = SchemaName(&twpb.RuleSetSnapshot{})
)

// HeaderOf returns the EventHeader of the event wrapped by ev, or nil if ev
// has no body.
func HeaderOf(ev *twpb.MarketEvent) *twpb.EventHeader {
	switch b := ev.GetBody().(type) {
	case *twpb.MarketEvent_Order:
		return b.Order.GetHeader()
	case *twpb.MarketEvent_Cancel:
		return b.Cancel.GetHeader()
	case *twpb.MarketEvent_Trade:
		return b.Trade.GetHeader()
	case *twpb.MarketEvent_Book:
		return b.Book.GetHeader()
	case *twpb.MarketEvent_Gap:
		return b.Gap.GetHeader()
	default:
		return nil
	}
}

// Kind classifies a MarketEvent body.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindOrder
	KindCancel
	KindTrade
	KindBook
	KindGap
)

func (k Kind) String() string {
	switch k {
	case KindOrder:
		return "order"
	case KindCancel:
		return "cancel"
	case KindTrade:
		return "trade"
	case KindBook:
		return "book"
	case KindGap:
		return "gap"
	default:
		return "unknown"
	}
}

// KindOf returns the kind of ev's body.
func KindOf(ev *twpb.MarketEvent) Kind {
	switch ev.GetBody().(type) {
	case *twpb.MarketEvent_Order:
		return KindOrder
	case *twpb.MarketEvent_Cancel:
		return KindCancel
	case *twpb.MarketEvent_Trade:
		return KindTrade
	case *twpb.MarketEvent_Book:
		return KindBook
	case *twpb.MarketEvent_Gap:
		return KindGap
	default:
		return KindUnknown
	}
}

// Wrap returns a MarketEvent envelope around a raw event message. It returns
// an error for message types that cannot be carried in a MarketEvent.
func Wrap(m proto.Message) (*twpb.MarketEvent, error) {
	switch v := m.(type) {
	case *twpb.OrderEvent:
		return &twpb.MarketEvent{Body: &twpb.MarketEvent_Order{Order: v}}, nil
	case *twpb.CancelEvent:
		return &twpb.MarketEvent{Body: &twpb.MarketEvent_Cancel{Cancel: v}}, nil
	case *twpb.TradeEvent:
		return &twpb.MarketEvent{Body: &twpb.MarketEvent_Trade{Trade: v}}, nil
	case *twpb.BookSnapshot:
		return &twpb.MarketEvent{Body: &twpb.MarketEvent_Book{Book: v}}, nil
	case *twpb.SequenceGap:
		return &twpb.MarketEvent{Body: &twpb.MarketEvent_Gap{Gap: v}}, nil
	default:
		return nil, fmt.Errorf("events: %T cannot be wrapped in a MarketEvent", m)
	}
}

// Unwrap returns the raw event message carried by ev, or nil if ev has no
// body.
func Unwrap(ev *twpb.MarketEvent) proto.Message {
	switch b := ev.GetBody().(type) {
	case *twpb.MarketEvent_Order:
		return b.Order
	case *twpb.MarketEvent_Cancel:
		return b.Cancel
	case *twpb.MarketEvent_Trade:
		return b.Trade
	case *twpb.MarketEvent_Book:
		return b.Book
	case *twpb.MarketEvent_Gap:
		return b.Gap
	default:
		return nil
	}
}
