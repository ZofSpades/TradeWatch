// Package sim is a deterministic discrete-event simulator of a multi-symbol
// exchange.
//
// A Sim owns one order book per symbol and a set of agents (market makers,
// noise traders, institutional slicers, momentum traders and the session
// machinery). Agents wake up at scheduled points of a single virtual clock,
// look at the books and send orders. The resulting book events are handed,
// in sequence order, to an Emitter.
//
// Given the same Config and Universe, a Sim always produces exactly the same
// event stream. Nothing in the simulation reads the wall clock, and every
// agent draws from its own seeded random stream. Pacing against real time is
// the caller's business (see internal/marketsim).
package sim

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/orderbook"
)

// Config parameterizes one simulated trading session.
type Config struct {
	// Seed drives every random decision of the session.
	Seed uint64
	// SessionID identifies the session in every published event.
	SessionID string
	// Open is the session's opening time (event time).
	Open time.Time
	// Duration is the length of continuous trading.
	Duration time.Duration
	// ClosingWindow is the final part of the session during which the
	// closing reference price forms (TRADING_PHASE_CLOSING).
	ClosingWindow time.Duration
	// SnapshotInterval is the period of book snapshots per symbol.
	SnapshotInterval time.Duration
	// SnapshotDepth is the number of levels per side in a snapshot.
	SnapshotDepth int
	// Activity scales the arrival rates of all participants (1 = normal).
	Activity float64
	// Symbols restricts the session to a subset of the universe; empty
	// means all instruments.
	Symbols []string
	// Injection sets the rates of injected market abuse. The zero value
	// injects nothing.
	Injection Injection
}

// DefaultConfig returns a full 6.5-hour session with a 10-minute closing
// window.
func DefaultConfig() Config {
	return Config{
		Seed:             1,
		SessionID:        "sim-1",
		Open:             time.Date(2026, 1, 5, 14, 30, 0, 0, time.UTC),
		Duration:         6*time.Hour + 30*time.Minute,
		ClosingWindow:    10 * time.Minute,
		SnapshotInterval: time.Second,
		SnapshotDepth:    10,
		Activity:         1,
	}
}

func (c Config) validate() error {
	switch {
	case c.SessionID == "":
		return errors.New("sim: session ID is required")
	case c.Duration <= 0:
		return errors.New("sim: duration must be positive")
	case c.ClosingWindow < 0 || c.ClosingWindow >= c.Duration:
		return errors.New("sim: closing window must be shorter than the session")
	case c.SnapshotInterval <= 0:
		return errors.New("sim: snapshot interval must be positive")
	case c.SnapshotDepth <= 0:
		return errors.New("sim: snapshot depth must be positive")
	case c.Activity <= 0:
		return errors.New("sim: activity must be positive")
	}
	return c.Injection.Validate()
}

// Session describes the trading session being simulated.
type Session struct {
	ID           string
	Seed         uint64
	Open         int64
	ClosingStart int64
	Close        int64
}

// Proto returns the session as a reference-data message.
func (s Session) Proto() *twpb.TradingSession {
	return &twpb.TradingSession{
		SessionId: s.ID, OpenTimeNs: s.Open, ClosingPhaseTimeNs: s.ClosingStart,
		CloseTimeNs: s.Close, Seed: s.Seed,
	}
}

// Symbol is the simulator's state for one instrument.
type Symbol struct {
	Instrument
	Index int
	Book  *orderbook.Book
	Phase twpb.TradingPhase

	fair   float64 // fair value, in price units
	impact float64
}

// Fair returns the symbol's current fair value in price units.
func (s *Symbol) Fair() float64 { return s.fair }

// Emitter receives every book event of a session in sequence order. The
// event is only valid for the duration of the call. A non-nil error stops
// the simulation.
type Emitter interface {
	Emit(sym *Symbol, ev *orderbook.Event) error
}

// LabelEmitter is implemented by emitters that also receive the ground-truth
// labels of injected abuse. Labels never travel with market events: they are
// published separately, for scoring only.
type LabelEmitter interface {
	Label(l *twpb.GroundTruthLabel) error
}

// Agent is a simulated participant or piece of session machinery.
type Agent interface {
	// Start schedules the agent's first wake-up with Sim.Schedule.
	Start(s *Sim)
	// Wake runs the agent at the current time and returns the time of its
	// next wake-up, or a negative value to stop.
	Wake(s *Sim) int64
}

// Fill is delivered to the owner of an order that traded.
type Fill struct {
	Symbol    *Symbol
	OrderID   uint64
	TradeID   uint64
	Side      orderbook.Side
	Price     int64
	Qty       int64
	Leaves    int64
	Aggressor bool
}

// Cancel is delivered to the owner of an order whose open quantity was
// removed without trading.
type Cancel struct {
	Symbol  *Symbol
	OrderID uint64
	Qty     int64
	Reason  orderbook.CancelReason
}

// FillHandler is implemented by agents that track their executions.
type FillHandler interface{ OnFill(s *Sim, f Fill) }

// CancelHandler is implemented by agents that track their cancellations.
type CancelHandler interface{ OnCancel(s *Sim, c Cancel) }

// ErrClosed is returned for orders sent after the session closed.
var ErrClosed = errors.New("sim: session is closed")

// Stats counts what a session produced.
type Stats struct {
	Events    uint64
	Orders    uint64
	Rejected  uint64
	Trades    uint64
	Volume    int64
	Cancels   uint64
	Replaces  uint64
	Snapshots uint64
	Labels    uint64
}

type ownerKey struct {
	symbol int
	order  uint64
}

// Sim is one simulated trading session.
type Sim struct {
	cfg      Config
	uni      *Universe
	session  Session
	symbols  []*Symbol
	bySymbol map[string]*Symbol
	agents   []Agent
	emit     Emitter

	queue       actionQueue
	now         int64
	tie         uint64
	evs         []orderbook.Event
	owners      map[ownerKey]Agent
	dispatching bool
	closed      bool
	err         error
	stats       Stats

	injection Injection
	inbox     chan func(*Sim)
	trigger   *rand.Rand
	episodes  int
	active    map[activeKey]bool
	busy      map[string]bool
}

// New builds a session. Agents are created from the universe; call Run to
// simulate it.
func New(cfg Config, uni *Universe, emit Emitter) (*Sim, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	open := cfg.Open.UnixNano()
	s := &Sim{
		cfg: cfg,
		uni: uni,
		session: Session{
			ID: cfg.SessionID, Seed: cfg.Seed, Open: open,
			ClosingStart: open + int64(cfg.Duration-cfg.ClosingWindow),
			Close:        open + int64(cfg.Duration),
		},
		bySymbol:  map[string]*Symbol{},
		emit:      emit,
		owners:    map[ownerKey]Agent{},
		evs:       make([]orderbook.Event, 0, 64),
		injection: cfg.Injection,
		inbox:     make(chan func(*Sim), 64),
		active:    map[activeKey]bool{},
		busy:      map[string]bool{},
	}
	want := map[string]bool{}
	for _, sym := range cfg.Symbols {
		want[sym] = true
	}
	for _, in := range uni.Instruments {
		if len(want) > 0 && !want[in.Symbol] {
			continue
		}
		book, err := orderbook.New(orderbook.Config{Symbol: in.Symbol, TickSize: in.TickSize, LotSize: in.LotSize})
		if err != nil {
			return nil, fmt.Errorf("sim: %s: %w", in.Symbol, err)
		}
		sym := &Symbol{Instrument: in, Index: len(s.symbols), Book: book, fair: float64(in.RefPrice), impact: paramsFor(in.Tier).impact}
		s.symbols = append(s.symbols, sym)
		s.bySymbol[in.Symbol] = sym
	}
	if len(s.symbols) == 0 {
		return nil, errors.New("sim: no symbols selected")
	}
	s.agents = defaultAgents(s)
	return s, nil
}

// AddAgent registers an additional agent. It must be called before Run.
func (s *Sim) AddAgent(a Agent) { s.agents = append(s.agents, a) }

// Session returns the session being simulated.
func (s *Sim) Session() Session { return s.session }

// Config returns the session configuration.
func (s *Sim) Config() Config { return s.cfg }

// Universe returns the universe the session runs over.
func (s *Sim) Universe() *Universe { return s.uni }

// Symbols returns the simulated symbols in a stable order.
func (s *Sim) Symbols() []*Symbol { return s.symbols }

// Symbol returns the state of one symbol.
func (s *Sim) Symbol(name string) (*Symbol, bool) {
	sym, ok := s.bySymbol[name]
	return sym, ok
}

// Now returns the current event time.
func (s *Sim) Now() int64 { return s.now }

// Closed reports whether the session has closed.
func (s *Sim) Closed() bool { return s.closed }

// Stats returns counters for the events produced so far.
func (s *Sim) Stats() Stats { return s.stats }

// Schedule queues a wake-up of a at time at.
func (s *Sim) Schedule(at int64, a Agent) {
	if at < s.now {
		at = s.now
	}
	s.tie++
	heap.Push(&s.queue, action{at: at, tie: s.tie, agent: a})
}

// Run simulates the session until it closes, the emitter fails or ctx is
// cancelled.
func (s *Sim) Run(ctx context.Context) error {
	for _, a := range s.agents {
		a.Start(s)
	}
	for n := 0; s.queue.Len() > 0; n++ {
		if s.err != nil {
			return s.err
		}
		if n%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		s.drainInbox()
		act := heap.Pop(&s.queue).(action)
		s.now = act.at
		if next := act.agent.Wake(s); next >= 0 {
			if next <= s.now {
				next = s.now + 1
			}
			s.Schedule(next, act.agent)
		}
	}
	return s.err
}

// ErrBusy is returned by Do when too many commands are already queued.
var ErrBusy = errors.New("sim: command queue is full")

// Do runs fn on the simulation goroutine between two actions, at the
// current simulated time. It is safe to call from any goroutine and is how
// live controls (injection rates, triggered scenarios) reach a running
// session. Commands make a run depend on when they arrive, so reproducible
// runs do not use them.
func (s *Sim) Do(fn func(*Sim)) error {
	select {
	case s.inbox <- fn:
		return nil
	default:
		return ErrBusy
	}
}

func (s *Sim) drainInbox() {
	for {
		select {
		case fn := <-s.inbox:
			fn(s)
		default:
			return
		}
	}
}

// Injection returns the abuse rates currently in effect.
func (s *Sim) Injection() Injection { return s.injection }

// SetInjection changes the abuse rates from the next planning interval
// on. It must be called on the simulation goroutine (for example via Do).
func (s *Sim) SetInjection(in Injection) { s.injection = in }

// Label publishes the ground-truth label of an injected episode. The label
// ID is derived from the session and a per-session counter, so it is
// reproducible.
func (s *Sim) Label(l *twpb.GroundTruthLabel) {
	s.stats.Labels++
	l.SessionId = s.session.ID
	l.LabelId = fmt.Sprintf("%s/%04d", s.session.ID, s.stats.Labels)
	if le, ok := s.emit.(LabelEmitter); ok && s.err == nil {
		if err := le.Label(l); err != nil {
			s.err = err
		}
	}
}

// Submit sends a new order on behalf of agent a and returns its order ID.
func (s *Sim) Submit(a Agent, sym *Symbol, r orderbook.Request) (uint64, error) {
	s.mustNotDispatch()
	if s.closed {
		return 0, ErrClosed
	}
	evs, id, err := sym.Book.Submit(s.evs[:0], s.now, r)
	if err != nil {
		s.stats.Rejected++
		return 0, err
	}
	s.stats.Orders++
	s.owners[ownerKey{sym.Index, id}] = a
	s.dispatch(sym, evs)
	return id, nil
}

// Cancel cancels one of agent a's resting orders.
func (s *Sim) Cancel(a Agent, sym *Symbol, account string, id uint64) error {
	s.mustNotDispatch()
	if s.closed {
		return ErrClosed
	}
	evs, err := sym.Book.Cancel(s.evs[:0], s.now, account, id)
	if err != nil {
		return err
	}
	s.dispatch(sym, evs)
	return nil
}

// Replace changes the price and open quantity of one of agent a's resting
// orders.
func (s *Sim) Replace(a Agent, sym *Symbol, account string, id uint64, price, qty int64) error {
	s.mustNotDispatch()
	if s.closed {
		return ErrClosed
	}
	evs, err := sym.Book.Replace(s.evs[:0], s.now, account, id, price, qty)
	if err != nil {
		return err
	}
	s.dispatch(sym, evs)
	return nil
}

// snapshot emits a book snapshot for sym.
func (s *Sim) snapshot(sym *Symbol) {
	s.dispatch(sym, sym.Book.Snapshot(s.evs[:0], s.now, s.cfg.SnapshotDepth))
}

// mustNotDispatch guards against agents sending orders from inside a fill
// or cancel notification, which would interleave two book operations.
func (s *Sim) mustNotDispatch() {
	if s.dispatching {
		panic("sim: orders must not be sent from OnFill or OnCancel")
	}
}

// dispatch hands events to the emitter in order and notifies order owners.
func (s *Sim) dispatch(sym *Symbol, evs []orderbook.Event) {
	s.dispatching = true
	defer func() { s.dispatching = false }()
	for i := range evs {
		ev := &evs[i]
		s.count(ev)
		if s.err == nil {
			if err := s.emit.Emit(sym, ev); err != nil {
				s.err = err
			}
		}
		switch ev.Kind {
		case orderbook.EventTrade:
			// Aggressive flow moves the fair value permanently (linear impact),
			// so large one-sided trading moves prices for every participant.
			signed := float64(ev.Qty)
			if ev.Aggressor == orderbook.Sell {
				signed = -signed
			}
			sym.fair *= math.Exp(sym.impact * signed)
			s.notifyFill(sym, ev.BuyOrderID, orderbook.Buy, ev, ev.BuyLeaves)
			s.notifyFill(sym, ev.SellOrderID, orderbook.Sell, ev, ev.SellLeaves)
		case orderbook.EventCancelled:
			key := ownerKey{sym.Index, ev.OrderID}
			if owner, ok := s.owners[key]; ok {
				delete(s.owners, key)
				if h, ok := owner.(CancelHandler); ok {
					h.OnCancel(s, Cancel{Symbol: sym, OrderID: ev.OrderID, Qty: ev.Qty, Reason: ev.Reason})
				}
			}
		}
	}
}

func (s *Sim) notifyFill(sym *Symbol, id uint64, side orderbook.Side, ev *orderbook.Event, leaves int64) {
	key := ownerKey{sym.Index, id}
	owner, ok := s.owners[key]
	if !ok {
		return
	}
	if leaves == 0 {
		delete(s.owners, key)
	}
	if h, ok := owner.(FillHandler); ok {
		h.OnFill(s, Fill{
			Symbol: sym, OrderID: id, TradeID: ev.TradeID, Side: side, Price: ev.Price, Qty: ev.Qty,
			Leaves: leaves, Aggressor: ev.Aggressor == side,
		})
	}
}

func (s *Sim) count(ev *orderbook.Event) {
	s.stats.Events++
	switch ev.Kind {
	case orderbook.EventTrade:
		s.stats.Trades++
		s.stats.Volume += ev.Qty
	case orderbook.EventCancelled:
		s.stats.Cancels++
	case orderbook.EventReplaced:
		s.stats.Replaces++
	case orderbook.EventSnapshot:
		s.stats.Snapshots++
	}
}

type action struct {
	at    int64
	tie   uint64
	agent Agent
}

// actionQueue is a min-heap ordered by time, then by scheduling order, which
// makes simultaneous wake-ups deterministic.
type actionQueue []action

func (q actionQueue) Len() int { return len(q) }
func (q actionQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].tie < q[j].tie
}
func (q actionQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *actionQueue) Push(x any)   { *q = append(*q, x.(action)) }
func (q *actionQueue) Pop() any {
	old := *q
	n := len(old)
	a := old[n-1]
	old[n-1] = action{}
	*q = old[:n-1]
	return a
}
