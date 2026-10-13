package events

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

func header(seq uint64) *twpb.EventHeader {
	return &twpb.EventHeader{
		SessionId:     "S1",
		Symbol:        "QZAP",
		Seq:           seq,
		EventTimeNs:   1_700_000_000_000_000_000 + int64(seq),
		PublishedAtNs: 1_700_000_000_000_000_500,
	}
}

func sampleMessages() []proto.Message {
	return []proto.Message{
		&twpb.OrderEvent{
			Header: header(1), Action: twpb.OrderAction_ORDER_ACTION_NEW, OrderId: 7,
			AccountId: "ACC-0001", Side: twpb.Side_SIDE_BUY, Type: twpb.OrderType_ORDER_TYPE_LIMIT,
			TimeInForce: twpb.TimeInForce_TIME_IN_FORCE_DAY, Price: 1_250_000, Quantity: 300,
		},
		&twpb.CancelEvent{
			Header: header(2), OrderId: 7, AccountId: "ACC-0001", Side: twpb.Side_SIDE_BUY,
			Price: 1_250_000, CancelledQuantity: 300, Reason: twpb.CancelReason_CANCEL_REASON_USER,
		},
		&twpb.TradeEvent{
			Header: header(3), TradeId: 1, Price: 1_250_000, Quantity: 100,
			AggressorSide: twpb.Side_SIDE_SELL, BuyOrderId: 7, SellOrderId: 8,
			BuyAccountId: "ACC-0001", SellAccountId: "ACC-0002", BuyLeavesQuantity: 200,
		},
		&twpb.BookSnapshot{
			Header: header(4), Phase: twpb.TradingPhase_TRADING_PHASE_CONTINUOUS,
			Bids: []*twpb.PriceLevel{{Price: 1_249_900, Quantity: 500, OrderCount: 2}},
			Asks: []*twpb.PriceLevel{{Price: 1_250_100, Quantity: 400, OrderCount: 1}},
		},
		&twpb.GroundTruthLabel{
			LabelId: "L1", SessionId: "S1", AbuseType: twpb.AbuseType_ABUSE_TYPE_SPOOFING,
			Symbol: "QZAP", AccountIds: []string{"ACC-0009"}, OrderIds: []uint64{11, 12},
			Params: map[string]string{"layers": "3", "size_multiple": "8", "side": "buy"},
		},
		&twpb.Alert{
			AlertKey: "k1", AbuseType: twpb.AbuseType_ABUSE_TYPE_SPOOFING, RuleId: "SPOOF-001",
			RuleVersion: 2, Severity: twpb.Severity_SEVERITY_HIGH, Score: 81.5,
			Evidence: &twpb.Evidence{
				OrderIds: []uint64{11, 12},
				Features: []*twpb.Feature{{Name: "size_vs_depth", Value: 6.2, Threshold: 3, Op: ">=", Breached: true}},
			},
		},
		&twpb.DeadLetter{
			SourceTopic: "orders", SourcePartition: 2, SourceOffset: 99,
			Value: []byte{0xff, 0x00}, Reason: "decode_error",
			Headers: map[string]string{"tw-seq": "5", "tw-schema": SchemaOrder},
		},
	}
}

func TestRoundTrip(t *testing.T) {
	for _, m := range sampleMessages() {
		b, err := Marshal(m)
		if err != nil {
			t.Fatalf("%T: marshal: %v", m, err)
		}
		got := m.ProtoReflect().New().Interface()
		if err := Unmarshal(b, got); err != nil {
			t.Fatalf("%T: unmarshal: %v", m, err)
		}
		if !proto.Equal(m, got) {
			t.Errorf("%T: round trip mismatch:\n want %v\n  got %v", m, m, got)
		}
	}
}

func TestMarshalIsDeterministic(t *testing.T) {
	// Map fields are where non-deterministic encoding would show up.
	for _, m := range sampleMessages() {
		first, err := Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 50; i++ {
			again, err := Marshal(proto.Clone(m))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first, again) {
				t.Fatalf("%T: encoding differs between runs", m)
			}
		}
	}
}

func TestWrapUnwrapAndHeader(t *testing.T) {
	raw := []proto.Message{
		&twpb.OrderEvent{Header: header(1)},
		&twpb.CancelEvent{Header: header(2)},
		&twpb.TradeEvent{Header: header(3)},
		&twpb.BookSnapshot{Header: header(4)},
		&twpb.SequenceGap{Header: header(5), LastSeq: 6},
	}
	wantKinds := []Kind{KindOrder, KindCancel, KindTrade, KindBook, KindGap}
	for i, m := range raw {
		ev, err := Wrap(m)
		if err != nil {
			t.Fatalf("wrap %T: %v", m, err)
		}
		if got := KindOf(ev); got != wantKinds[i] {
			t.Errorf("KindOf(%T) = %v, want %v", m, got, wantKinds[i])
		}
		if h := HeaderOf(ev); h.GetSeq() != uint64(i+1) {
			t.Errorf("HeaderOf(%T).Seq = %d, want %d", m, h.GetSeq(), i+1)
		}
		if Unwrap(ev) != m {
			t.Errorf("Unwrap(%T) did not return the wrapped message", m)
		}
	}
	if _, err := Wrap(&twpb.Alert{}); err == nil {
		t.Error("Wrap(Alert) should fail")
	}
	empty := &twpb.MarketEvent{}
	if HeaderOf(empty) != nil || KindOf(empty) != KindUnknown || Unwrap(empty) != nil {
		t.Error("empty MarketEvent should have no header, kind or body")
	}
}

func TestSchemaNames(t *testing.T) {
	cases := map[string]string{
		SchemaOrder:       "tradewatch.v1.OrderEvent",
		SchemaMarketEvent: "tradewatch.v1.MarketEvent",
		SchemaAlert:       "tradewatch.v1.Alert",
		SchemaDeadLetter:  "tradewatch.v1.DeadLetter",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("schema name %q, want %q", got, want)
		}
	}
}
