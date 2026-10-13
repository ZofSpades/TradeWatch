package ingest

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// SeqKey identifies one sequence: a symbol within a session.
type SeqKey struct {
	Session string
	Symbol  string
}

func (k SeqKey) encode() string { return k.Session + "|" + k.Symbol }

func decodeKey(s string) (SeqKey, bool) {
	i := strings.LastIndexByte(s, '|')
	if i <= 0 || i == len(s)-1 {
		return SeqKey{}, false
	}
	return SeqKey{Session: s[:i], Symbol: s[i+1:]}, true
}

// Gap reasons carried in SequenceGap.reason.
const (
	GapDeadLetter = "dead_letter" // the record was rejected; its position is known from its headers
	GapLost       = "lost"        // the record never arrived
)

// held is a record waiting for its turn.
type held struct {
	feed   Feed
	offset int64
	event  *twpb.MarketEvent // nil when the record was dead-lettered
	reason string
}

// SequencerStats counts what a sequencer did.
type SequencerStats struct {
	Released     uint64
	Duplicates   uint64
	DeadLetter   uint64 // positions filled for dead-lettered records
	LostGaps     uint64 // gap markers emitted for records that never arrived
	LostEvents   uint64 // sequence numbers covered by those markers
	StaleRecords uint64 // records for a session that already closed
}

// Sequencer restores per-(session, symbol) sequence order for one
// co-partitioned group: partition p of the four raw feeds.
//
// Records are observed in offset order per feed. A record is released when
// every lower sequence number of its key has been released. A
// dead-lettered record is released as a SequenceGap at its position.
// Duplicates (sequence numbers already released or already held) are
// dropped. A sequence number that never arrives is declared lost once
// every feed has delivered a later record of the key (each feed is ordered
// per key, so it can no longer arrive), or after GapTimeout once every feed
// is caught up to its high watermark.
//
// For commits the sequencer reports, per feed, the lowest offset that is
// not yet done. The Checkpoint of the next sequence per key is consistent
// with those offsets: every record with a sequence number at or above a
// key's next value sits at or after its feed's committable offset.
type Sequencer struct {
	emit       func(*twpb.MarketEvent)
	now        func() time.Time
	gapTimeout time.Duration

	next     map[SeqKey]uint64
	lastTime map[SeqKey]int64
	held     map[SeqKey]map[uint64]held
	nHeld    int
	blocked  map[SeqKey]time.Time
	lastSeen [numFeeds]map[SeqKey]uint64
	offsets  [numFeeds]offsetTracker

	closed      map[SeqKey]uint64
	closedOrder []SeqKey

	stats SequencerStats
}

// maxClosed bounds how many closed sessions per partition are remembered
// (to drop late duplicates of a finished session).
const maxClosed = 32

// NewSequencer returns a sequencer resuming from cp (which may be empty).
// emit receives released events in order.
func NewSequencer(cp Checkpoint, gapTimeout time.Duration, now func() time.Time, emit func(*twpb.MarketEvent)) *Sequencer {
	s := &Sequencer{
		emit: emit, now: now, gapTimeout: gapTimeout,
		next: map[SeqKey]uint64{}, lastTime: map[SeqKey]int64{},
		held: map[SeqKey]map[uint64]held{}, blocked: map[SeqKey]time.Time{},
		closed: map[SeqKey]uint64{},
	}
	for f := range s.lastSeen {
		s.lastSeen[f] = map[SeqKey]uint64{}
	}
	for k, n := range cp.Next {
		if key, ok := decodeKey(k); ok {
			s.next[key] = n
		}
	}
	for _, c := range cp.Closed {
		if key, ok := decodeKey(c.Key); ok {
			s.closed[key] = c.Final
			s.closedOrder = append(s.closedOrder, key)
		}
	}
	return s
}

// SetBase sets the offset a feed starts consuming from. It must be called
// before the first record of that feed is observed.
func (s *Sequencer) SetBase(f Feed, offset int64) { s.offsets[f] = offsetTracker{base: offset} }

// Committable returns the offset of the feed to commit: the lowest offset
// whose record is not yet done.
func (s *Sequencer) Committable(f Feed) int64 { return s.offsets[f].base }

// Held returns the number of records waiting for their turn.
func (s *Sequencer) Held() int { return s.nHeld }

// Stats returns cumulative counters.
func (s *Sequencer) Stats() SequencerStats { return s.stats }

func (s *Sequencer) nextOf(k SeqKey) uint64 {
	if n, ok := s.next[k]; ok {
		return n
	}
	return 1
}

// ObserveUnsequenced records a feed offset that carries no usable sequence
// information (a dead-lettered record without headers). It never blocks
// anything, so its offset is done immediately.
func (s *Sequencer) ObserveUnsequenced(f Feed, offset int64) {
	s.offsets[f].push(offset)
	s.offsets[f].markDone(offset)
}

// Observe hands the sequencer one record of feed f. ev is the parsed event,
// or nil if the record was dead-lettered with the given reason.
func (s *Sequencer) Observe(f Feed, offset int64, key SeqKey, seq uint64, ev *twpb.MarketEvent, reason string) {
	s.offsets[f].push(offset)
	if seq > s.lastSeen[f][key] {
		s.lastSeen[f][key] = seq
	}
	if _, ok := s.closed[key]; ok {
		s.stats.StaleRecords++
		s.offsets[f].markDone(offset)
		return
	}
	if seq < s.nextOf(key) {
		s.stats.Duplicates++
		s.offsets[f].markDone(offset)
		return
	}
	hk := s.held[key]
	if hk == nil {
		hk = map[uint64]held{}
		s.held[key] = hk
	}
	if _, dup := hk[seq]; dup {
		s.stats.Duplicates++
		s.offsets[f].markDone(offset)
		return
	}
	hk[seq] = held{feed: f, offset: offset, event: ev, reason: reason}
	s.nHeld++
	s.release(key)
}

// release emits every consecutive held record of key starting at its next
// sequence number.
func (s *Sequencer) release(key SeqKey) {
	hk := s.held[key]
	n := s.nextOf(key)
	for {
		h, ok := hk[n]
		if !ok {
			break
		}
		delete(hk, n)
		s.nHeld--
		out := h.event
		if out == nil {
			out = s.gapEvent(key, n, n, h.reason)
			s.stats.DeadLetter++
		} else {
			s.lastTime[key] = events.HeaderOf(out).GetEventTimeNs()
			s.stats.Released++
		}
		s.emit(out)
		s.offsets[h.feed].markDone(h.offset)
		n++
		s.next[key] = n
		if b := out.GetBook(); b != nil && b.GetPhase() == twpb.TradingPhase_TRADING_PHASE_CLOSED {
			s.close(key, n-1)
			return
		}
	}
	if len(hk) > 0 {
		if _, ok := s.blocked[key]; !ok {
			s.blocked[key] = s.now()
		}
	} else {
		delete(s.blocked, key)
	}
}

// close forgets a finished session of a symbol, remembering only its final
// sequence number to recognise late duplicates.
func (s *Sequencer) close(key SeqKey, final uint64) {
	for seq, h := range s.held[key] {
		s.stats.StaleRecords++
		s.offsets[h.feed].markDone(h.offset)
		delete(s.held[key], seq)
		s.nHeld--
	}
	delete(s.held, key)
	delete(s.next, key)
	delete(s.lastTime, key)
	delete(s.blocked, key)
	for f := range s.lastSeen {
		delete(s.lastSeen[f], key)
	}
	s.closed[key] = final
	s.closedOrder = append(s.closedOrder, key)
	if len(s.closedOrder) > maxClosed {
		delete(s.closed, s.closedOrder[0])
		s.closedOrder = s.closedOrder[1:]
	}
}

func (s *Sequencer) gapEvent(key SeqKey, first, last uint64, reason string) *twpb.MarketEvent {
	return &twpb.MarketEvent{Body: &twpb.MarketEvent_Gap{Gap: &twpb.SequenceGap{
		Header: &twpb.EventHeader{
			SessionId: key.Session, Symbol: key.Symbol, Seq: first,
			// A gap has no event time of its own; it inherits the time of
			// the last released event so event time never moves backwards.
			EventTimeNs: s.lastTime[key],
		},
		LastSeq: last,
		Reason:  reason,
	}}}
}

// CheckGaps declares missing sequence numbers lost when they can no longer
// arrive. caughtUp reports, per feed, whether consumption has reached the
// high watermark; it is only consulted for the timeout rule.
func (s *Sequencer) CheckGaps(caughtUp func() bool) {
	if len(s.blocked) == 0 {
		return
	}
	keys := make([]SeqKey, 0, len(s.blocked))
	for k := range s.blocked {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b SeqKey) int { return strings.Compare(a.encode(), b.encode()) })
	allCaughtUp, asked := false, false
	for _, key := range keys {
		// Once a key's wait has timed out with every feed caught up, no
		// further records of it are in flight, so any gap that surfaces
		// after releasing is declared in the same pass instead of waiting
		// for another timeout.
		timedOut := false
		for {
			since, blocked := s.blocked[key]
			if !blocked {
				break
			}
			next := s.nextOf(key)
			passed := true
			for f := range s.lastSeen {
				if s.lastSeen[f][key] <= next {
					passed = false
					break
				}
			}
			if !passed && !timedOut {
				if s.now().Sub(since) < s.gapTimeout {
					break
				}
				if !asked {
					allCaughtUp, asked = caughtUp(), true
				}
				if !allCaughtUp {
					break
				}
				timedOut = true
			}
			s.declareLost(key, next)
		}
	}
}

// declareLost emits a gap from next up to the lowest held sequence number
// of key and releases what follows it.
func (s *Sequencer) declareLost(key SeqKey, next uint64) {
	first := uint64(0)
	for seq := range s.held[key] {
		if first == 0 || seq < first {
			first = seq
		}
	}
	s.emit(s.gapEvent(key, next, first-1, GapLost))
	s.stats.LostGaps++
	s.stats.LostEvents += first - next
	s.next[key] = first
	delete(s.blocked, key)
	s.release(key)
}

// PausableFeeds reports, per feed, whether fetching from it can be paused
// without starving a blocked key: a feed qualifies only if it has already
// delivered a record beyond the missing position of every blocked key, so
// that none of the missing records can be on it. Pausing such feeds lets
// the others catch up without the held buffer growing; pausing any other
// feed could deadlock a key waiting for that feed.
func (s *Sequencer) PausableFeeds() [numFeeds]bool {
	var ok [numFeeds]bool
	if len(s.blocked) == 0 {
		return ok
	}
	for f := range s.lastSeen {
		ok[f] = true
		for k := range s.blocked {
			if s.lastSeen[f][k] <= s.nextOf(k) {
				ok[f] = false
				break
			}
		}
	}
	return ok
}

// ClosedEntry is a closed session remembered in a checkpoint.
type ClosedEntry struct {
	Key   string `json:"k"`
	Final uint64 `json:"f"`
}

// Checkpoint is the sequencer state stored in offset-commit metadata.
type Checkpoint struct {
	Version int `json:"v"`
	// At is the wall-clock time of the commit; on restore the newest
	// checkpoint among a partition's feeds wins.
	At     int64             `json:"at"`
	Next   map[string]uint64 `json:"next"`
	Closed []ClosedEntry     `json:"closed,omitempty"`
}

// maxCheckpointBytes keeps metadata below Kafka's default
// offset.metadata.max.bytes (4096).
const maxCheckpointBytes = 3800

// Checkpoint snapshots the state needed to resume at the committable
// offsets.
func (s *Sequencer) Checkpoint(at int64) Checkpoint {
	cp := Checkpoint{Version: 1, At: at, Next: make(map[string]uint64, len(s.next))}
	for k, n := range s.next {
		cp.Next[k.encode()] = n
	}
	for _, k := range s.closedOrder {
		cp.Closed = append(cp.Closed, ClosedEntry{Key: k.encode(), Final: s.closed[k]})
	}
	return cp
}

// Encode serializes the checkpoint, dropping the oldest closed sessions if
// needed to stay within the metadata size limit.
func (cp Checkpoint) Encode() (string, error) {
	for {
		b, err := json.Marshal(cp)
		if err != nil {
			return "", err
		}
		if len(b) <= maxCheckpointBytes {
			return string(b), nil
		}
		if len(cp.Closed) == 0 {
			return "", fmt.Errorf("ingest: checkpoint of %d bytes exceeds the metadata limit", len(b))
		}
		cp.Closed = cp.Closed[1:]
	}
}

// DecodeCheckpoint parses offset-commit metadata. Metadata that is empty or
// was not written by ingest yields an empty checkpoint and false.
func DecodeCheckpoint(s string) (Checkpoint, bool) {
	var cp Checkpoint
	if s == "" || json.Unmarshal([]byte(s), &cp) != nil || cp.Version != 1 {
		return Checkpoint{}, false
	}
	return cp, true
}

// offsetTracker tracks which offsets of one feed are done. Offsets are
// pushed in increasing order and marked done in any order. base is the
// lowest offset that is not done.
type offsetTracker struct {
	base int64
	done []bool // done[i] is the state of offset base+i
}

func (t *offsetTracker) push(o int64) {
	end := t.base + int64(len(t.done))
	switch {
	case o < end:
		return
	case len(t.done) == 0 && o-t.base > 1<<20:
		// The consumer started far beyond the recorded base (for example
		// after retention removed old data): restart tracking at o.
		t.base = o
	default:
		for ; end < o; end++ {
			t.done = append(t.done, true) // offsets without records
		}
	}
	t.done = append(t.done, false)
}

func (t *offsetTracker) markDone(o int64) {
	if i := o - t.base; i >= 0 && i < int64(len(t.done)) {
		t.done[i] = true
	}
	n := 0
	for n < len(t.done) && t.done[n] {
		n++
	}
	if n > 0 {
		t.done = t.done[n:]
		t.base += int64(n)
	}
}
