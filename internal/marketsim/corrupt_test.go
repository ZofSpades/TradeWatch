package marketsim

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/ingest"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

// validMessages returns one valid message per feed, built from the default
// universe, with the ingest feed each one is published on.
func validMessages(uni *sim.Universe) map[ingest.Feed]proto.Message {
	retail := uni.ByRole(sim.RoleRetail)
	h := func(seq uint64) *twpb.EventHeader {
		return &twpb.EventHeader{SessionId: "S1", Symbol: "QZAP", Seq: seq, EventTimeNs: 2_000}
	}
	return map[ingest.Feed]proto.Message{
		ingest.FeedOrders: &twpb.OrderEvent{
			Header: h(1), Action: twpb.OrderAction_ORDER_ACTION_NEW, OrderId: 1, AccountId: retail[0].ID,
			Side: twpb.Side_SIDE_BUY, Type: twpb.OrderType_ORDER_TYPE_LIMIT, TimeInForce: twpb.TimeInForce_TIME_IN_FORCE_DAY,
			Price: 1_254_000, Quantity: 100,
		},
		ingest.FeedCancels: &twpb.CancelEvent{
			Header: h(2), OrderId: 1, AccountId: retail[0].ID, Side: twpb.Side_SIDE_BUY, Price: 1_254_000,
			CancelledQuantity: 100, Reason: twpb.CancelReason_CANCEL_REASON_USER, OrderEntryTimeNs: 1_000,
		},
		ingest.FeedTrades: &twpb.TradeEvent{
			Header: h(3), TradeId: 1, Price: 1_254_000, Quantity: 10, AggressorSide: twpb.Side_SIDE_BUY,
			BuyOrderId: 2, SellOrderId: 3, BuyAccountId: retail[1].ID, SellAccountId: retail[2].ID,
		},
		ingest.FeedBook: &twpb.BookSnapshot{Header: h(4), Phase: twpb.TradingPhase_TRADING_PHASE_CONTINUOUS},
	}
}

func TestEveryCorruptionIsDeadLetteredWithItsReason(t *testing.T) {
	uni := sim.DefaultUniverse()
	ref := ingest.NewReferenceData(append(uni.RefData(),
		&twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: &twpb.TradingSession{SessionId: "S1"}}})...)
	wantReason := map[string]string{
		CorruptGarbage:        ingest.ReasonDecode,
		CorruptHeaderless:     ingest.ReasonDecode,
		CorruptNegativeQty:    ingest.ReasonInvalidField,
		CorruptUnknownAccount: ingest.ReasonUnknownAccount,
		CorruptOffTick:        ingest.ReasonOffTick,
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for feed, msg := range validMessages(uni) {
		value, err := events.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		h := msg.(interface{ GetHeader() *twpb.EventHeader }).GetHeader()
		meta := kafka.EventMeta{Schema: events.SchemaName(msg), Session: h.GetSessionId(), Seq: h.GetSeq(), HasSeq: true}
		if _, rej := ingest.Parse(ingest.Raw{Feed: feed, Key: []byte("QZAP"), Value: value, Meta: meta}, ref, 1); rej != nil {
			t.Fatalf("%v: the uncorrupted message must be valid: %v", feed, rej)
		}
		for _, kind := range kindsFor(msg) {
			v, hs := corrupt(kind, msg, value, meta.Headers(), rng)
			got := kafka.ParseEventMeta(hs)
			_, rej := ingest.Parse(ingest.Raw{Feed: feed, Key: []byte("QZAP"), Value: v, Meta: got}, ref, 1)
			if rej == nil {
				t.Errorf("%v/%s: corrupted record was accepted", feed, kind)
				continue
			}
			if rej.Reason != wantReason[kind] {
				t.Errorf("%v/%s: rejected as %q, want %q", feed, kind, rej.Reason, wantReason[kind])
			}
			if kind == CorruptHeaderless && got.HasSeq {
				t.Errorf("%v: headerless corruption kept its sequence header", feed)
			}
			if kind != CorruptHeaderless && got.Seq != h.GetSeq() {
				t.Errorf("%v/%s: sequence header lost, so ingest could not fill the gap", feed, kind)
			}
		}
	}
}

func TestCorrupterRespectsRate(t *testing.T) {
	msg := validMessages(sim.DefaultUniverse())[ingest.FeedOrders]
	value, _ := events.Marshal(msg)
	headers := kafka.EventMeta{Schema: events.SchemaOrder, Session: "S1", Seq: 1, HasSeq: true}.Headers()

	never := newCorrupter(0, 1, prometheus.NewRegistry())
	for i := 0; i < 1_000; i++ {
		if v, _ := never.apply(msg, value, headers); !bytes.Equal(v, value) {
			t.Fatal("rate 0 corrupted a record")
		}
	}
	always := newCorrupter(1, 1, prometheus.NewRegistry())
	for i := 0; i < 1_000; i++ {
		v, hs := always.apply(msg, value, headers)
		if bytes.Equal(v, value) && len(hs) == len(headers) {
			t.Fatal("rate 1 left a record intact")
		}
	}
}
