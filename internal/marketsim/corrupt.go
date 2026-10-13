package marketsim

import (
	"math/rand/v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/metrics"
)

// Corruption kinds marketsim can inject (TW_SIM_MALFORMED_RATE).
const (
	CorruptGarbage        = "garbage_value"
	CorruptHeaderless     = "garbage_without_headers"
	CorruptNegativeQty    = "negative_quantity"
	CorruptUnknownAccount = "unknown_account"
	CorruptOffTick        = "off_tick_price"
)

// corrupter deliberately damages a small fraction of published records so
// that a running stack exercises ingest validation, the dead-letter topic
// and sequence-gap handling. Recordings are never corrupted.
type corrupter struct {
	rate  float64
	rng   *rand.Rand
	count *prometheus.CounterVec
}

func newCorrupter(rate float64, seed uint64, reg prometheus.Registerer) *corrupter {
	c := &corrupter{
		rate: rate,
		rng:  rand.New(rand.NewPCG(seed, 0x6d616c66)),
		count: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace, Subsystem: "marketsim", Name: "malformed_records_total",
			Help: "Records deliberately corrupted before publishing, by kind.",
		}, []string{"kind"}),
	}
	reg.MustRegister(c.count)
	return c
}

// kindsFor lists the corruptions that make sense for a message.
func kindsFor(msg proto.Message) []string {
	kinds := []string{CorruptGarbage, CorruptHeaderless}
	switch m := msg.(type) {
	case *twpb.OrderEvent:
		kinds = append(kinds, CorruptNegativeQty, CorruptUnknownAccount)
		if m.GetPrice() > 0 {
			kinds = append(kinds, CorruptOffTick)
		}
	case *twpb.CancelEvent:
		kinds = append(kinds, CorruptUnknownAccount)
	case *twpb.TradeEvent:
		kinds = append(kinds, CorruptUnknownAccount, CorruptOffTick)
	}
	return kinds
}

// apply returns the value and headers to publish for msg, corrupting them
// with probability rate.
func (c *corrupter) apply(msg proto.Message, value []byte, headers []kgo.RecordHeader) ([]byte, []kgo.RecordHeader) {
	if c.rate <= 0 || c.rng.Float64() >= c.rate {
		return value, headers
	}
	kinds := kindsFor(msg)
	kind := kinds[c.rng.IntN(len(kinds))]
	c.count.WithLabelValues(kind).Inc()
	v, h := corrupt(kind, msg, value, headers, c.rng)
	return v, h
}

// corrupt applies one corruption kind.
func corrupt(kind string, msg proto.Message, value []byte, headers []kgo.RecordHeader, rng *rand.Rand) ([]byte, []kgo.RecordHeader) {
	garbage := func() []byte {
		b := make([]byte, 8+rng.IntN(24))
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		// A leading 0xff is never a valid protobuf tag, so the value is
		// guaranteed not to decode.
		b[0] = 0xff
		return b
	}
	switch kind {
	case CorruptGarbage:
		return garbage(), headers
	case CorruptHeaderless:
		return garbage(), nil
	}
	m := proto.Clone(msg)
	switch v := m.(type) {
	case *twpb.OrderEvent:
		switch kind {
		case CorruptNegativeQty:
			v.Quantity = -v.Quantity
		case CorruptUnknownAccount:
			v.AccountId = "ACC-9999"
		case CorruptOffTick:
			v.Price++
		}
	case *twpb.CancelEvent:
		v.AccountId = "ACC-9999"
	case *twpb.TradeEvent:
		switch kind {
		case CorruptUnknownAccount:
			v.BuyAccountId = "ACC-9999"
		case CorruptOffTick:
			v.Price++
		}
	}
	b, err := events.Marshal(m)
	if err != nil {
		return garbage(), headers
	}
	return b, headers
}
