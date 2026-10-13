package orderbook

import "sort"

// order is a resting order. Orders at one price form an intrusive
// doubly-linked FIFO, so cancels are O(1) once the order is found.
type order struct {
	id        uint64
	account   string
	side      Side
	price     int64
	qty       int64 // open quantity, always > 0 while resting
	entryTime int64
	priority  uint64 // arrival order; lower is earlier
	level     *level
	prev      *order
	next      *order
}

type level struct {
	price int64
	qty   int64 // sum of open quantity of the orders at this level
	count int
	head  *order
	tail  *order
}

func (l *level) push(o *order) {
	o.level = l
	o.prev = l.tail
	o.next = nil
	if l.tail != nil {
		l.tail.next = o
	} else {
		l.head = o
	}
	l.tail = o
	l.qty += o.qty
	l.count++
}

func (l *level) remove(o *order) {
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		l.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		l.tail = o.prev
	}
	l.qty -= o.qty
	l.count--
	o.level, o.prev, o.next = nil, nil, nil
}

// bookSide holds the price levels of one side. levels is sorted from the
// worst price to the best, so the best level is the last element and
// consuming it is O(1). Inserting or removing an inner level is O(levels),
// which is cheap for realistic book depths and keeps the hot path
// cache-friendly.
type bookSide struct {
	bid     bool
	levels  []*level
	byPrice map[int64]*level
}

func newBookSide(bid bool) bookSide {
	return bookSide{bid: bid, byPrice: make(map[int64]*level)}
}

// better reports whether price a is strictly better than price b on this
// side: higher for bids, lower for asks.
func (s *bookSide) better(a, b int64) bool {
	if s.bid {
		return a > b
	}
	return a < b
}

func (s *bookSide) best() *level {
	if n := len(s.levels); n > 0 {
		return s.levels[n-1]
	}
	return nil
}

func (s *bookSide) levelAt(price int64) *level {
	if l, ok := s.byPrice[price]; ok {
		return l
	}
	l := &level{price: price}
	// First index whose level is better than price: insert before it.
	i := sort.Search(len(s.levels), func(i int) bool { return s.better(s.levels[i].price, price) })
	s.levels = append(s.levels, nil)
	copy(s.levels[i+1:], s.levels[i:])
	s.levels[i] = l
	s.byPrice[price] = l
	return l
}

func (s *bookSide) removeLevel(l *level) {
	delete(s.byPrice, l.price)
	n := len(s.levels)
	if s.levels[n-1] == l {
		s.levels[n-1] = nil
		s.levels = s.levels[:n-1]
		return
	}
	// First index whose level is not worse than l, which is l itself.
	i := sort.Search(n, func(i int) bool { return !s.better(l.price, s.levels[i].price) })
	copy(s.levels[i:], s.levels[i+1:])
	s.levels[n-1] = nil
	s.levels = s.levels[:n-1]
}

// top returns up to depth aggregated levels, best first.
func (s *bookSide) top(depth int) []Level {
	n := min(depth, len(s.levels))
	out := make([]Level, n)
	for i := 0; i < n; i++ {
		l := s.levels[len(s.levels)-1-i]
		out[i] = Level{Price: l.price, Qty: l.qty, Count: l.count}
	}
	return out
}
