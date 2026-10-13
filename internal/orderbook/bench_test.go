package orderbook

import (
	"math/rand/v2"
	"testing"
)

func benchBook(b *testing.B) *Book {
	b.Helper()
	book, err := New(Config{Symbol: "BENCH", TickSize: 100, LotSize: 10})
	if err != nil {
		b.Fatal(err)
	}
	return book
}

// BenchmarkAddCancel measures adding a passive order and cancelling the
// oldest one, with the book held at 1,000 resting orders over 40 levels.
func BenchmarkAddCancel(b *testing.B) {
	book := benchBook(b)
	evs := make([]Event, 0, 8)
	const resting = 1_000
	ring := make([]uint64, resting)
	var id uint64
	for i := 0; i < resting; i++ {
		side := Side(1 + i%2)
		price := int64(testMid - 100*(1+i%20))
		if side == Sell {
			price = int64(testMid + 100*(1+i%20))
		}
		_, id, _ = book.Submit(evs[:0], int64(i), Request{Account: "MM", Side: side, Type: Limit, TIF: Day, Price: price, Qty: 100})
		ring[i] = id
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slot := i % resting
		evs, _ = book.Cancel(evs[:0], int64(i), "MM", ring[slot])
		side := Side(1 + i%2)
		price := int64(testMid - 100*(1+i%20))
		if side == Sell {
			price = int64(testMid + 100*(1+i%20))
		}
		evs, id, _ = book.Submit(evs[:0], int64(i), Request{Account: "MM", Side: side, Type: Limit, TIF: Day, Price: price, Qty: 100})
		ring[slot] = id
	}
}

// BenchmarkAggressiveSingleFill measures an IOC order that fills completely
// against a single resting order, including re-posting that liquidity.
func BenchmarkAggressiveSingleFill(b *testing.B) {
	book := benchBook(b)
	evs := make([]Event, 0, 8)
	for i := 0; i < 20; i++ {
		evs, _, _ = book.Submit(evs[:0], 0, Request{Account: "MM", Side: Sell, Type: Limit, TIF: Day, Price: int64(testMid + 100*(1+i)), Qty: 1_000})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		evs, _, _ = book.Submit(evs[:0], int64(i), Request{Account: "MM", Side: Sell, Type: Limit, TIF: Day, Price: testMid, Qty: 100})
		evs, _, _ = book.Submit(evs[:0], int64(i), Request{Account: "T", Side: Buy, Type: Limit, TIF: IOC, Price: testMid, Qty: 100})
	}
}

// BenchmarkSweepFiveLevels measures a market order that sweeps 25 resting
// orders across five price levels, followed by rebuilding those levels.
func BenchmarkSweepFiveLevels(b *testing.B) {
	book := benchBook(b)
	evs := make([]Event, 0, 64)
	refill := func(t int64) {
		for lvl := 0; lvl < 5; lvl++ {
			for k := 0; k < 5; k++ {
				evs, _, _ = book.Submit(evs[:0], t, Request{Account: "MM", Side: Sell, Type: Limit, TIF: Day, Price: int64(testMid + 100*lvl), Qty: 100})
			}
		}
	}
	refill(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		evs, _, _ = book.Submit(evs[:0], int64(i), Request{Account: "T", Side: Buy, Type: Market, TIF: IOC, Qty: 2_500})
		refill(int64(i))
	}
}

// BenchmarkMixedWorkload replays a simulator-like operation mix (55% limit
// orders, 5% market orders, 22% cancels, 15% replaces, 3% snapshots) and
// reports throughput in operations and events per second.
func BenchmarkMixedWorkload(b *testing.B) {
	book := benchBook(b)
	rng := rand.New(rand.NewPCG(1, 2))
	accounts := []string{"MM1", "MM2", "N1", "N2", "N3", "INST"}
	ids := make([]uint64, 0, 1<<16)
	evs := make([]Event, 0, 64)
	var events int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		account := accounts[rng.IntN(len(accounts))]
		side := Side(1 + rng.IntN(2))
		evs = evs[:0]
		switch op := rng.IntN(100); {
		case op < 55:
			var id uint64
			evs, id, _ = book.Submit(evs, int64(i), Request{Account: account, Side: side, Type: Limit, TIF: Day, Price: int64(testMid + 100*(rng.IntN(21)-10)), Qty: int64(10 * (1 + rng.IntN(50)))})
			if len(ids) < cap(ids) {
				ids = append(ids, id)
			} else {
				ids[rng.IntN(len(ids))] = id
			}
		case op < 60:
			evs, _, _ = book.Submit(evs, int64(i), Request{Account: account, Side: side, Type: Market, TIF: IOC, Qty: int64(10 * (1 + rng.IntN(30)))})
		case op < 82:
			if len(ids) > 0 {
				id := ids[rng.IntN(len(ids))]
				if o, ok := book.Order(id); ok {
					evs, _ = book.Cancel(evs, int64(i), o.Account, id)
				}
			}
		case op < 97:
			if len(ids) > 0 {
				id := ids[rng.IntN(len(ids))]
				if o, ok := book.Order(id); ok {
					evs, _ = book.Replace(evs, int64(i), o.Account, id, int64(testMid+100*(rng.IntN(21)-10)), int64(10*(1+rng.IntN(50))))
				}
			}
		default:
			evs = book.Snapshot(evs, int64(i), 5)
		}
		events += len(evs)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
	b.ReportMetric(float64(events)/b.Elapsed().Seconds(), "events/s")
}
