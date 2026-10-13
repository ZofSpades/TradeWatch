package ingest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func mkEvent(key SeqKey, seq uint64, f Feed, closing bool) *twpb.MarketEvent {
	h := &twpb.EventHeader{SessionId: key.Session, Symbol: key.Symbol, Seq: seq, EventTimeNs: int64(seq) * 1000}
	switch f {
	case FeedOrders:
		return &twpb.MarketEvent{Body: &twpb.MarketEvent_Order{Order: &twpb.OrderEvent{Header: h, OrderId: seq}}}
	case FeedCancels:
		return &twpb.MarketEvent{Body: &twpb.MarketEvent_Cancel{Cancel: &twpb.CancelEvent{Header: h, OrderId: seq}}}
	case FeedTrades:
		return &twpb.MarketEvent{Body: &twpb.MarketEvent_Trade{Trade: &twpb.TradeEvent{Header: h, TradeId: seq}}}
	default:
		phase := twpb.TradingPhase_TRADING_PHASE_CONTINUOUS
		if closing {
			phase = twpb.TradingPhase_TRADING_PHASE_CLOSED
		}
		return &twpb.MarketEvent{Body: &twpb.MarketEvent_Book{Book: &twpb.BookSnapshot{Header: h, Phase: phase}}}
	}
}

// label renders an emitted event as "seq:kind" for compact assertions.
func label(ev *twpb.MarketEvent) string {
	h := events.HeaderOf(ev)
	if g := ev.GetGap(); g != nil {
		return fmt.Sprintf("%d-%d:gap/%s", h.GetSeq(), g.GetLastSeq(), g.GetReason())
	}
	return fmt.Sprintf("%d:%s", h.GetSeq(), events.KindOf(ev))
}

type seqHarness struct {
	clock   *fakeClock
	s       *Sequencer
	out     []*twpb.MarketEvent
	offsets [numFeeds]int64
}

func newHarness() *seqHarness {
	h := &seqHarness{clock: &fakeClock{t: time.Unix(1_000, 0)}}
	h.s = NewSequencer(Checkpoint{}, 5*time.Second, h.clock.now, func(ev *twpb.MarketEvent) { h.out = append(h.out, ev) })
	return h
}

func (h *seqHarness) deliver(f Feed, ev *twpb.MarketEvent) {
	hd := events.HeaderOf(ev)
	h.s.Observe(f, h.offsets[f], SeqKey{hd.GetSessionId(), hd.GetSymbol()}, hd.GetSeq(), ev, "")
	h.offsets[f]++
}

func (h *seqHarness) deadLetter(f Feed, key SeqKey, seq uint64) {
	h.s.Observe(f, h.offsets[f], key, seq, nil, "decode_error")
	h.offsets[f]++
}

func (h *seqHarness) labels() string {
	var parts []string
	for _, ev := range h.out {
		parts = append(parts, label(ev))
	}
	return strings.Join(parts, " ")
}

var k1 = SeqKey{"S1", "QZAP"}

func TestReleasesAcrossFeedsInSequenceOrder(t *testing.T) {
	h := newHarness()
	h.deliver(FeedTrades, mkEvent(k1, 2, FeedTrades, false))
	h.deliver(FeedCancels, mkEvent(k1, 3, FeedCancels, false))
	if len(h.out) != 0 || h.s.Held() != 2 {
		t.Fatalf("released before seq 1: %s", h.labels())
	}
	if c := h.s.Committable(FeedTrades); c != 0 {
		t.Errorf("held trade must not be committable, got %d", c)
	}
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))
	if got := h.labels(); got != "1:order 2:trade 3:cancel" {
		t.Fatalf("output %q", got)
	}
	for f, want := range map[Feed]int64{FeedOrders: 1, FeedTrades: 1, FeedCancels: 1, FeedBook: 0} {
		if c := h.s.Committable(f); c != want {
			t.Errorf("committable %v = %d, want %d", f, c, want)
		}
	}
}

func TestDropsDuplicates(t *testing.T) {
	h := newHarness()
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false)) // already released
	h.deliver(FeedTrades, mkEvent(k1, 3, FeedTrades, false))
	h.deliver(FeedTrades, mkEvent(k1, 3, FeedTrades, false)) // already held
	h.deliver(FeedOrders, mkEvent(k1, 2, FeedOrders, false))
	if got := h.labels(); got != "1:order 2:order 3:trade" {
		t.Fatalf("output %q", got)
	}
	if st := h.s.Stats(); st.Duplicates != 2 {
		t.Errorf("duplicates = %d", st.Duplicates)
	}
	if h.s.Committable(FeedOrders) != 3 || h.s.Committable(FeedTrades) != 2 {
		t.Error("duplicate offsets must count as done")
	}
}

func TestDeadLetteredRecordBecomesGapAtItsPosition(t *testing.T) {
	h := newHarness()
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))
	h.deadLetter(FeedTrades, k1, 2)
	h.deliver(FeedOrders, mkEvent(k1, 3, FeedOrders, false))
	if got := h.labels(); got != "1:order 2-2:gap/decode_error 3:order" {
		t.Fatalf("output %q", got)
	}
	gap := h.out[1].GetGap()
	if gap.GetHeader().GetEventTimeNs() != 1000 {
		t.Errorf("gap must inherit the previous event time, got %d", gap.GetHeader().GetEventTimeNs())
	}
}

func TestLostRecordDeclaredWhenEveryFeedHasPassedIt(t *testing.T) {
	h := newHarness()
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))
	// seq 2 never arrives. Three feeds move past it...
	h.deliver(FeedOrders, mkEvent(k1, 3, FeedOrders, false))
	h.deliver(FeedCancels, mkEvent(k1, 4, FeedCancels, false))
	h.deliver(FeedBook, mkEvent(k1, 5, FeedBook, false))
	h.s.CheckGaps(func() bool { return false })
	if got := h.labels(); got != "1:order" {
		t.Fatalf("declared lost while the trades feed could still deliver it: %q", got)
	}
	// ...and once the fourth does too, seq 2 can no longer arrive.
	h.deliver(FeedTrades, mkEvent(k1, 6, FeedTrades, false))
	h.s.CheckGaps(func() bool { return false })
	if got := h.labels(); got != "1:order 2-2:gap/lost 3:order 4:cancel 5:book 6:trade" {
		t.Fatalf("output %q", got)
	}
	if st := h.s.Stats(); st.LostGaps != 1 || st.LostEvents != 1 {
		t.Errorf("stats = %+v", st)
	}
}

func TestLostRecordDeclaredAfterTimeoutOnlyWhenCaughtUp(t *testing.T) {
	h := newHarness()
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))
	h.deliver(FeedOrders, mkEvent(k1, 4, FeedOrders, false)) // 2 and 3 missing
	h.clock.t = h.clock.t.Add(10 * time.Second)
	h.s.CheckGaps(func() bool { return false })
	if len(h.out) != 1 {
		t.Fatalf("declared a gap while feeds were lagging: %s", h.labels())
	}
	h.s.CheckGaps(func() bool { return true })
	if got := h.labels(); got != "1:order 2-3:gap/lost 4:order" {
		t.Fatalf("output %q", got)
	}
}

// Found by TestSequencerProperties: two separate gaps in one key must not
// cost two timeouts. Once the first wait has expired with every feed
// caught up, the second gap cannot be in flight either.
func TestConsecutiveGapsDeclaredInOnePass(t *testing.T) {
	h := newHarness()
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))
	h.deliver(FeedOrders, mkEvent(k1, 3, FeedOrders, false)) // 2 missing
	h.deliver(FeedOrders, mkEvent(k1, 5, FeedOrders, false)) // 4 missing
	h.clock.t = h.clock.t.Add(10 * time.Second)
	h.s.CheckGaps(func() bool { return true })
	if got := h.labels(); got != "1:order 2-2:gap/lost 3:order 4-4:gap/lost 5:order" {
		t.Fatalf("output %q", got)
	}
	if h.s.Held() != 0 {
		t.Errorf("%d still held", h.s.Held())
	}
}

func TestTimeoutRuleWaitsForTheTimeout(t *testing.T) {
	h := newHarness()
	h.deliver(FeedOrders, mkEvent(k1, 2, FeedOrders, false))
	h.clock.t = h.clock.t.Add(4 * time.Second)
	h.s.CheckGaps(func() bool { return true })
	if len(h.out) != 0 {
		t.Fatalf("gap declared before the timeout: %s", h.labels())
	}
}

func TestClosedSessionIsForgottenAndNextSessionStartsAtOne(t *testing.T) {
	h := newHarness()
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))
	h.deliver(FeedBook, mkEvent(k1, 2, FeedBook, true))
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false)) // late duplicate of a closed session
	k2 := SeqKey{"S2", "QZAP"}
	h.deliver(FeedOrders, mkEvent(k2, 1, FeedOrders, false))
	if got := h.labels(); got != "1:order 2:book 1:order" {
		t.Fatalf("output %q", got)
	}
	if st := h.s.Stats(); st.StaleRecords != 1 {
		t.Errorf("stale = %d", st.StaleRecords)
	}
	cp := h.s.Checkpoint(1)
	if _, ok := cp.Next[k1.encode()]; ok {
		t.Error("closed key still in checkpoint.next")
	}
	if len(cp.Closed) != 1 || cp.Closed[0].Final != 2 || cp.Next[k2.encode()] != 2 {
		t.Errorf("checkpoint = %+v", cp)
	}
}

func TestCheckpointRoundTripAndSizeLimit(t *testing.T) {
	h := newHarness()
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))
	for i := 0; i < 200; i++ {
		k := SeqKey{fmt.Sprintf("live-20261013T090000Z-%03d", i), "QZAP"}
		h.deliver(FeedOrders, mkEvent(k, 1, FeedOrders, false))
		h.deliver(FeedBook, mkEvent(k, 2, FeedBook, true))
	}
	cp := h.s.Checkpoint(99)
	if len(cp.Closed) != maxClosed {
		t.Errorf("closed sessions remembered = %d, want %d", len(cp.Closed), maxClosed)
	}
	enc, err := cp.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) > maxCheckpointBytes {
		t.Fatalf("encoded checkpoint is %d bytes", len(enc))
	}
	back, ok := DecodeCheckpoint(enc)
	if !ok || back.At != 99 || back.Next[k1.encode()] != 2 {
		t.Fatalf("decoded = %+v %v", back, ok)
	}
	restored := NewSequencer(back, time.Second, time.Now, func(*twpb.MarketEvent) {})
	restored.Observe(FeedOrders, 0, k1, 1, mkEvent(k1, 1, FeedOrders, false), "")
	if restored.Stats().Duplicates != 1 {
		t.Error("restored sequencer must treat released sequence numbers as duplicates")
	}
	for _, bad := range []string{"", "member-1", `{"v":2}`, `{"v":1,`} {
		if _, ok := DecodeCheckpoint(bad); ok {
			t.Errorf("DecodeCheckpoint(%q) accepted foreign metadata", bad)
		}
	}
}

// ADR-0004 states that roughly 90 open sequences with live session IDs and
// seven-digit sequence numbers fit in the commit metadata.
func TestCheckpointCapacity(t *testing.T) {
	cp := Checkpoint{Version: 1, At: time.Now().UnixNano(), Next: map[string]uint64{}}
	symbols := []string{"QZAP", "MOSS", "VANT", "KELP"}
	for n := 0; ; n++ {
		if _, err := cp.Encode(); err != nil {
			t.Logf("%d open sequences fit in %d bytes", n-1, maxCheckpointBytes)
			if n-1 < 85 || n-1 > 95 {
				t.Errorf("capacity is %d sequences; update ADR-0004", n-1)
			}
			return
		}
		key := SeqKey{fmt.Sprintf("live-20261013T090000Z-%03d", n/4), symbols[n%4]}
		cp.Next[key.encode()] = 1_234_567
	}
}

func TestPausableFeeds(t *testing.T) {
	h := newHarness()
	h.deliver(FeedOrders, mkEvent(k1, 5, FeedOrders, false))
	h.deliver(FeedBook, mkEvent(k1, 6, FeedBook, false))
	got := h.s.PausableFeeds()
	want := [numFeeds]bool{FeedOrders: true, FeedBook: true}
	if got != want {
		t.Errorf("pausable = %v, want orders and book_snapshots", got)
	}
}

// Found by the detector integration test: with two symbols on a partition,
// a feed that is ahead for one blocked symbol can still hold the record the
// other is waiting for. Pausing it starved that symbol forever.
func TestFeedNeededByAnyBlockedKeyIsNotPausable(t *testing.T) {
	h := newHarness()
	k2 := SeqKey{"S1", "MOSS"}
	// QZAP waits for seq 1 (a trade); orders and book are ahead for it.
	h.deliver(FeedOrders, mkEvent(k1, 2, FeedOrders, false))
	h.deliver(FeedBook, mkEvent(k1, 3, FeedBook, false))
	// MOSS waits for seq 1 too, but has only been seen on the book feed;
	// its missing record may still come on the orders feed.
	h.deliver(FeedBook, mkEvent(k2, 2, FeedBook, false))
	got := h.s.PausableFeeds()
	if got[FeedOrders] {
		t.Fatal("orders is still needed by MOSS and must not be paused")
	}
	if !got[FeedBook] {
		t.Error("book_snapshots is ahead for every blocked key and can be paused")
	}
}

// A marketsim restart abandons its session without a CLOSED snapshot. Its
// keys must leave the checkpoint, or every restart would add to commit
// metadata until it exceeds the broker's size limit.
func TestForgetIdleDropsAbandonedSequences(t *testing.T) {
	h := newHarness()
	live := SeqKey{"S2", "QZAP"}
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))
	h.deliver(FeedOrders, mkEvent(live, 1, FeedOrders, false))
	if n := h.s.ForgetIdle(10 * time.Minute); n != 0 {
		t.Fatalf("forgot %d keys at the first sweep", n)
	}
	for seq := uint64(2); seq <= 11; seq++ { // k1 stops, live goes on
		h.clock.t = h.clock.t.Add(time.Minute)
		h.deliver(FeedOrders, mkEvent(live, seq, FeedOrders, false))
		h.s.ForgetIdle(10 * time.Minute)
	}
	cp := h.s.Checkpoint(1)
	if _, ok := cp.Next[k1.encode()]; ok || cp.Next[live.encode()] != 12 {
		t.Fatalf("checkpoint.next = %v, want only the live key at 12", cp.Next)
	}
	if st := h.s.Stats(); st.Abandoned != 1 {
		t.Errorf("abandoned = %d, want 1", st.Abandoned)
	}

	// A forgotten key that resumes starts over: once every feed has passed
	// sequence number 1, it is reported lost.
	h.out = nil
	for f := Feed(0); f < numFeeds; f++ {
		h.deliver(f, mkEvent(k1, uint64(f)+2, f, false))
	}
	h.s.CheckGaps(func() bool { return true })
	if got := h.labels(); got != "1-1:gap/lost 2:order 3:cancel 4:trade 5:book" {
		t.Fatalf("output after resuming %q", got)
	}
}

// A key stays while records of it are held, and while a restart from the
// committable offsets would read its records again.
func TestForgetIdleKeepsKeysThatMayStillBeNeeded(t *testing.T) {
	h := newHarness()
	pinned := SeqKey{"S1", "MOSS"}
	h.deliver(FeedOrders, mkEvent(pinned, 2, FeedOrders, false)) // waits for 1, holding the orders offset at 0
	h.deliver(FeedOrders, mkEvent(k1, 1, FeedOrders, false))     // done, but above the committable offset
	waiting := SeqKey{"S1", "VANT"}
	h.deliver(FeedBook, mkEvent(waiting, 1, FeedBook, false))
	h.deliver(FeedBook, mkEvent(waiting, 3, FeedBook, false)) // holds 3 until 2 is declared lost
	h.s.ForgetIdle(time.Minute)
	h.clock.t = h.clock.t.Add(2 * time.Minute)
	if n := h.s.ForgetIdle(time.Minute); n != 0 {
		t.Fatalf("forgot %d keys that are still needed", n)
	}
	h.s.CheckGaps(func() bool { return true }) // releases pinned and waiting
	if h.s.Committable(FeedOrders) != 2 {
		t.Fatalf("orders committable = %d, want 2", h.s.Committable(FeedOrders))
	}
	if n := h.s.ForgetIdle(time.Minute); n != 1 {
		t.Fatalf("forgot %d keys, want only %v", n, k1)
	}
	h.clock.t = h.clock.t.Add(time.Minute)
	if n := h.s.ForgetIdle(time.Minute); n != 2 {
		t.Fatalf("forgot %d keys, want the two released ones", n)
	}
	if cp := h.s.Checkpoint(1); len(cp.Next) != 0 {
		t.Errorf("checkpoint.next = %v, want empty", cp.Next)
	}
}

func TestOffsetTrackerSkipsOffsetsWithoutRecords(t *testing.T) {
	var tr offsetTracker
	tr.base = 10
	tr.push(10)
	tr.push(13) // 11 and 12 carry no records
	tr.markDone(10)
	if tr.base != 13 {
		t.Fatalf("base = %d, want 13", tr.base)
	}
	tr.markDone(13)
	if tr.base != 14 {
		t.Fatalf("base = %d, want 14", tr.base)
	}
}

// --- Property test -----------------------------------------------------

type fate uint8

const (
	fateNormal fate = iota
	fateDeadLetter
	fateLost
)

type prodEvent struct {
	key   SeqKey
	seq   uint64
	feed  Feed
	fate  fate
	event *twpb.MarketEvent
}

// feedRecord is one record on a feed: an event or a duplicate of one.
type feedRecord struct{ ev *prodEvent }

// TestSequencerProperties drives a sequencer with random productions and
// consumption orders, including duplicates, dead-lettered and lost records
// and a crash with restore from a checkpoint, and checks that the output is
// exactly the produced sequence per key.
func TestSequencerProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Keys: up to three symbols, the first possibly with a second session
		// that starts after the first closes.
		nSym := rapid.IntRange(1, 3).Draw(rt, "symbols")
		type stream struct {
			events []*prodEvent
		}
		var streams []*stream
		var expected []*prodEvent
		for i := 0; i < nSym; i++ {
			sym := fmt.Sprintf("SYM%d", i)
			st := &stream{}
			sessions := 1
			if i == 0 {
				sessions = rapid.IntRange(1, 2).Draw(rt, "sessions")
			}
			for s := 0; s < sessions; s++ {
				key := SeqKey{fmt.Sprintf("S%d", s+1), sym}
				n := rapid.IntRange(1, 40).Draw(rt, "length")
				closes := s < sessions-1 || rapid.Bool().Draw(rt, "closes")
				for seq := 1; seq <= n; seq++ {
					last := seq == n && closes
					f := Feed(rapid.IntRange(0, int(numFeeds)-1).Draw(rt, "feed"))
					fa := fate(rapid.SampledFrom([]fate{fateNormal, fateNormal, fateNormal, fateNormal, fateNormal, fateNormal, fateNormal, fateDeadLetter, fateLost}).Draw(rt, "fate"))
					if last {
						f, fa = FeedBook, fateNormal
					}
					pe := &prodEvent{key: key, seq: uint64(seq), feed: f, fate: fa, event: mkEvent(key, uint64(seq), f, last)}
					st.events = append(st.events, pe)
					expected = append(expected, pe)
				}
			}
			streams = append(streams, st)
		}

		// Production: interleave the symbol streams, preserving each one's order.
		var produced []*prodEvent
		pos := make([]int, len(streams))
		for {
			var open []int
			for i, st := range streams {
				if pos[i] < len(st.events) {
					open = append(open, i)
				}
			}
			if len(open) == 0 {
				break
			}
			i := rapid.SampledFrom(open).Draw(rt, "produce")
			produced = append(produced, streams[i].events[pos[i]])
			pos[i]++
		}

		// Feeds: each produced, non-lost event lands on its feed; some get a
		// duplicate later on the same feed.
		var feeds [numFeeds][]feedRecord
		var dups [numFeeds][]*prodEvent
		for _, pe := range produced {
			if pe.fate == fateLost {
				continue
			}
			f := pe.feed
			feeds[f] = append(feeds[f], feedRecord{pe})
			if rapid.IntRange(0, 9).Draw(rt, "dupe") == 0 {
				dups[f] = append(dups[f], pe)
			}
			if len(dups[f]) > 0 && rapid.IntRange(0, 3).Draw(rt, "flushDupe") == 0 {
				feeds[f] = append(feeds[f], feedRecord{dups[f][0]})
				dups[f] = dups[f][1:]
			}
		}
		for f := range dups {
			for _, pe := range dups[f] {
				feeds[f] = append(feeds[f], feedRecord{pe})
			}
		}

		clock := &fakeClock{t: time.Unix(1_000, 0)}
		type emitted struct {
			instance int
			ev       *twpb.MarketEvent
		}
		var out []emitted
		instance := 0
		newSeq := func(cp Checkpoint) *Sequencer {
			n := instance
			return NewSequencer(cp, time.Second, clock.now, func(ev *twpb.MarketEvent) { out = append(out, emitted{n, ev}) })
		}
		seqr := newSeq(Checkpoint{})
		var next [numFeeds]int64

		total := 0
		for f := range feeds {
			total += len(feeds[f])
		}
		crashAt := rapid.IntRange(-1, total).Draw(rt, "crashAt")
		step := 0
		deliverAll := func() {
			for {
				var open []int
				for f := range feeds {
					if next[f] < int64(len(feeds[f])) {
						open = append(open, f)
					}
				}
				if len(open) == 0 {
					return
				}
				if step == crashAt {
					// Crash: keep only what a commit at this point would
					// persist, then resume a fresh sequencer from it.
					cp := seqr.Checkpoint(1)
					enc, err := cp.Encode()
					if err != nil {
						rt.Fatal(err)
					}
					restoredCP, ok := DecodeCheckpoint(enc)
					if !ok {
						rt.Fatal("checkpoint did not round-trip")
					}
					var committed [numFeeds]int64
					for f := range committed {
						committed[f] = seqr.Committable(Feed(f))
					}
					instance++
					seqr = newSeq(restoredCP)
					for f := range committed {
						seqr.SetBase(Feed(f), committed[f])
						next[f] = committed[f]
					}
				}
				step++
				f := Feed(rapid.SampledFrom(open).Draw(rt, "consume"))
				rec := feeds[f][next[f]]
				pe := rec.ev
				if pe.fate == fateDeadLetter {
					seqr.Observe(f, next[f], pe.key, pe.seq, nil, "decode_error")
				} else {
					seqr.Observe(f, next[f], pe.key, pe.seq, pe.event, "")
				}
				next[f]++
				seqr.CheckGaps(func() bool { return false })
			}
		}
		deliverAll()
		clock.t = clock.t.Add(time.Minute)
		seqr.CheckGaps(func() bool { return true })

		if seqr.Held() != 0 {
			rt.Fatalf("%d records still held after everything was delivered", seqr.Held())
		}
		for f := range feeds {
			if c := seqr.Committable(Feed(f)); c != int64(len(feeds[f])) {
				rt.Fatalf("feed %v committable %d, want %d", Feed(f), c, len(feeds[f]))
			}
		}

		// Expand the output into one entry per (key, seq).
		type entry struct {
			label string
		}
		got := map[SeqKey]map[uint64]entry{}
		lastSeq := map[[2]any]uint64{}
		for _, e := range out {
			h := events.HeaderOf(e.ev)
			key := SeqKey{h.GetSessionId(), h.GetSymbol()}
			first, last := h.GetSeq(), h.GetSeq()
			lab := events.KindOf(e.ev).String()
			if g := e.ev.GetGap(); g != nil {
				last = g.GetLastSeq()
				lab = "gap/" + g.GetReason()
			}
			ik := [2]any{e.instance, key}
			if prev, ok := lastSeq[ik]; ok && first != prev+1 {
				rt.Fatalf("instance %d emitted %v seq %d after %d", e.instance, key, first, prev)
			}
			lastSeq[ik] = last
			if got[key] == nil {
				got[key] = map[uint64]entry{}
			}
			for s := first; s <= last; s++ {
				if prev, dup := got[key][s]; dup && prev.label != lab {
					rt.Fatalf("%v seq %d emitted as both %s and %s", key, s, prev.label, lab)
				}
				got[key][s] = entry{lab}
			}
		}
		// Trailing lost events (nothing after them was delivered) are never
		// declared; everything else must be present exactly as produced.
		lastDelivered := map[SeqKey]uint64{}
		for _, pe := range expected {
			if pe.fate != fateLost {
				lastDelivered[pe.key] = max(lastDelivered[pe.key], pe.seq)
			}
		}
		for _, pe := range expected {
			want := events.KindOf(pe.event).String()
			switch pe.fate {
			case fateDeadLetter:
				want = "gap/decode_error"
			case fateLost:
				want = "gap/" + GapLost
			}
			e, ok := got[pe.key][pe.seq]
			if pe.fate == fateLost && pe.seq > lastDelivered[pe.key] {
				if ok {
					rt.Fatalf("%v seq %d: trailing lost event was declared", pe.key, pe.seq)
				}
				continue
			}
			if !ok || e.label != want {
				rt.Fatalf("%v seq %d: got %q (present=%v), want %q", pe.key, pe.seq, e.label, ok, want)
			}
		}
	})
}
