package sim

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
)

// Injection sets how often each kind of market abuse is injected. The
// continuous behaviours are expected episodes per symbol per trading hour.
// Marking the close is the probability that a symbol's close is marked in a
// session.
type Injection struct {
	WashTrading      float64 `json:"wash_trading"`
	Spoofing         float64 `json:"spoofing"`
	MomentumIgnition float64 `json:"momentum_ignition"`
	QuoteStuffing    float64 `json:"quote_stuffing"`
	MarkingTheClose  float64 `json:"marking_the_close"`
}

// Validate reports invalid rates.
func (in Injection) Validate() error {
	for name, r := range map[string]float64{
		"wash_trading": in.WashTrading, "spoofing": in.Spoofing, "momentum_ignition": in.MomentumIgnition,
		"quote_stuffing": in.QuoteStuffing, "marking_the_close": in.MarkingTheClose,
	} {
		if r < 0 || math.IsNaN(r) || math.IsInf(r, 0) {
			return fmt.Errorf("sim: injection rate %s must be a non-negative number, got %v", name, r)
		}
	}
	if in.MarkingTheClose > 1 {
		return fmt.Errorf("sim: marking_the_close is a probability, got %v", in.MarkingTheClose)
	}
	return nil
}

// Scenario names, recorded in GroundTruthLabel.scenario.
const (
	ScenarioWashDirect      = "wash.direct"
	ScenarioWashRing        = "wash.ring"
	ScenarioSpoofing        = "spoofing.layering"
	ScenarioMomentum        = "momentum.ignition"
	ScenarioQuoteStuffing   = "quote_stuffing.burst"
	ScenarioMarkingTheClose = "marking_the_close"
)

// Scenarios lists every scenario the injector can run.
var Scenarios = []string{
	ScenarioWashDirect, ScenarioWashRing, ScenarioSpoofing,
	ScenarioMomentum, ScenarioQuoteStuffing, ScenarioMarkingTheClose,
}

// ErrUnknownScenario is returned by Trigger for an unknown scenario name.
var ErrUnknownScenario = errors.New("sim: unknown scenario")

// planInterval is how much simulated time the planner draws episodes for at
// a time. Rate changes take effect at the next planning tick.
const planInterval = time.Minute

// planner schedules injected episodes from Poisson processes at the
// current injection rates.
type planner struct {
	rng *rand.Rand
}

func newPlanner(s *Sim) *planner { return &planner{rng: streamRNG(s.cfg.Seed, "planner")} }

func (p *planner) Start(s *Sim) {
	// The first minute lets the books fill before anything is injected.
	s.Schedule(s.session.Open+int64(planInterval), p)
	// Closes are marked with a per-session probability, so they are
	// planned up front.
	window := min(s.session.Close-s.session.ClosingStart, int64(150*time.Second))
	for _, sym := range s.symbols {
		if p.rng.Float64() < s.injection.MarkingTheClose && window > int64(10*time.Second) {
			lead := int64(60*time.Second) + p.rng.Int64N(int64(90*time.Second))
			s.schedule(ScenarioMarkingTheClose, sym, s.session.Close-min(lead, window))
		}
	}
}

func (p *planner) Wake(s *Sim) int64 {
	if s.closed || s.now >= s.session.ClosingStart {
		return -1
	}
	// Continuous abuse is only planned before the closing phase.
	end := min(s.now+int64(planInterval), s.session.ClosingStart)
	span := end - s.now
	hours := float64(span) / float64(time.Hour)
	in := s.injection
	for _, sym := range s.symbols {
		for _, k := range []struct {
			rate     float64
			scenario func() string
		}{
			{in.WashTrading, func() string {
				if p.rng.Float64() < 0.6 {
					return ScenarioWashDirect
				}
				return ScenarioWashRing
			}},
			{in.Spoofing, func() string { return ScenarioSpoofing }},
			{in.MomentumIgnition, func() string { return ScenarioMomentum }},
			{in.QuoteStuffing, func() string { return ScenarioQuoteStuffing }},
		} {
			for n := poisson(p.rng, k.rate*hours); n > 0; n-- {
				s.schedule(k.scenario(), sym, s.now+p.rng.Int64N(span))
			}
		}
	}
	return end
}

// poisson draws from a Poisson distribution with the given mean (Knuth).
func poisson(r *rand.Rand, mean float64) int {
	if mean <= 0 {
		return 0
	}
	l, k, prod := math.Exp(-mean), 0, r.Float64()
	for prod > l {
		k++
		prod *= r.Float64()
	}
	return k
}

// Trigger runs a scenario on a symbol now (marking the close: at this
// session's close). It must be called on the simulation goroutine, for
// example through Do. An empty symbol picks one at random.
func (s *Sim) Trigger(scenario, symbol string) error {
	if !slices.Contains(Scenarios, scenario) {
		return fmt.Errorf("%w %q", ErrUnknownScenario, scenario)
	}
	if s.closed {
		return ErrClosed
	}
	var sym *Symbol
	if symbol == "" {
		sym = s.symbols[s.triggerRNG().IntN(len(s.symbols))]
	} else if sym = s.bySymbol[symbol]; sym == nil {
		return fmt.Errorf("sim: unknown symbol %q", symbol)
	}
	at := s.now
	if scenario == ScenarioMarkingTheClose {
		at = max(s.now, s.session.Close-int64(90*time.Second))
	}
	s.schedule(scenario, sym, at)
	return nil
}

func (s *Sim) triggerRNG() *rand.Rand {
	if s.trigger == nil {
		s.trigger = streamRNG(s.cfg.Seed, "trigger")
	}
	return s.trigger
}

// schedule creates an episode of the scenario on sym starting at at.
func (s *Sim) schedule(scenario string, sym *Symbol, at int64) {
	s.episodes++
	rng := streamRNG(s.cfg.Seed, "episode/"+strconv.Itoa(s.episodes))
	base := episode{scenario: scenario, sym: sym, rng: rng, mine: map[uint64]bool{}, fills: map[uint64]int{}, params: map[string]string{}}
	var a Agent
	switch scenario {
	case ScenarioWashDirect, ScenarioWashRing:
		base.kind = twpb.AbuseType_ABUSE_TYPE_WASH_TRADING
		a = &washEpisode{episode: base, ring: scenario == ScenarioWashRing}
	case ScenarioSpoofing:
		base.kind = twpb.AbuseType_ABUSE_TYPE_SPOOFING
		a = &spoofEpisode{episode: base}
	case ScenarioMomentum:
		base.kind = twpb.AbuseType_ABUSE_TYPE_MOMENTUM_IGNITION
		a = &momentumEpisode{episode: base}
	case ScenarioQuoteStuffing:
		base.kind = twpb.AbuseType_ABUSE_TYPE_QUOTE_STUFFING
		a = &stuffingEpisode{episode: base}
	default:
		base.kind = twpb.AbuseType_ABUSE_TYPE_MARKING_THE_CLOSE
		a = &closeEpisode{episode: base}
	}
	s.Schedule(at, a)
}

// activeKey identifies a running episode type on a symbol; at most one
// episode of each abuse type runs on a symbol at a time.
type activeKey struct {
	symbol int
	kind   twpb.AbuseType
}

// episode is the bookkeeping shared by all injected scenarios: which
// accounts it uses, every order it sent and every trade it took part in,
// so its ground-truth label lists exactly what happened.
type episode struct {
	kind     twpb.AbuseType
	scenario string
	sym      *Symbol
	rng      *rand.Rand
	accounts []*Account
	orders   []uint64
	mine     map[uint64]bool
	trades   []uint64
	fills    map[uint64]int // fills per trade ID (2 = both sides are ours)
	first    int64
	last     int64
	params   map[string]string
	started  bool
	done     bool
}

// begin claims the episode's slot on the symbol and its accounts. It returns
// false if another episode of the same type is running on the symbol or the
// accounts are in use, in which case the episode is skipped.
func (e *episode) begin(s *Sim, accounts []*Account) bool {
	key := activeKey{e.sym.Index, e.kind}
	if s.active[key] || len(accounts) == 0 {
		return false
	}
	for _, a := range accounts {
		if s.busy[a.ID] {
			return false
		}
	}
	s.active[key] = true
	for _, a := range accounts {
		s.busy[a.ID] = true
	}
	e.accounts = accounts
	e.started = true
	return true
}

// end releases the slot and accounts.
func (e *episode) end(s *Sim) {
	e.done = true
	if !e.started {
		return
	}
	delete(s.active, activeKey{e.sym.Index, e.kind})
	for _, a := range e.accounts {
		delete(s.busy, a.ID)
	}
}

// pick returns a free account of the role, or nil.
func (e *episode) pick(s *Sim, role Role) *Account {
	var free []*Account
	for _, a := range s.uni.ByRole(role) {
		if !s.busy[a.ID] {
			free = append(free, a)
		}
	}
	if len(free) == 0 {
		return nil
	}
	return free[e.rng.IntN(len(free))]
}

func (e *episode) submit(s *Sim, a Agent, acct *Account, r ob.Request) (uint64, bool) {
	r.Account = acct.ID
	id, err := s.Submit(a, e.sym, r)
	if err != nil {
		return 0, false
	}
	e.orders = append(e.orders, id)
	e.mine[id] = true
	if e.first == 0 {
		e.first = s.now
	}
	e.last = s.now
	return id, true
}

func (e *episode) cancel(s *Sim, a Agent, acct *Account, id uint64) {
	if _, ok := e.sym.Book.Order(id); !ok {
		return
	}
	if s.Cancel(a, e.sym, acct.ID, id) == nil {
		e.last = s.now
	}
}

func (e *episode) recordFill(s *Sim, f Fill) {
	if e.fills[f.TradeID] == 0 {
		e.trades = append(e.trades, f.TradeID)
	}
	e.fills[f.TradeID]++
	e.last = s.now
}

// label publishes the episode's ground truth, restricted to the given trades.
func (e *episode) label(s *Sim, trades []uint64) {
	l := &twpb.GroundTruthLabel{
		AbuseType: e.kind, Scenario: e.scenario, Symbol: e.sym.Symbol,
		StartTimeNs: e.first, EndTimeNs: e.last,
		OrderIds: append([]uint64(nil), e.orders...), TradeIds: append([]uint64(nil), trades...),
		Params: e.params,
	}
	owners := map[string]bool{}
	for _, a := range e.accounts {
		l.AccountIds = append(l.AccountIds, a.ID)
		if !owners[a.BeneficialOwner] {
			owners[a.BeneficialOwner] = true
			l.BeneficialOwnerIds = append(l.BeneficialOwnerIds, a.BeneficialOwner)
		}
	}
	s.Label(l)
}

// typicalLevelQty is the mean quantity of the top levels on one side, or
// fallback when the side is empty.
func typicalLevelQty(b *ob.Book, side ob.Side, levels int, fallback int64) int64 {
	ls := b.Depth(side, levels)
	if len(ls) == 0 {
		return fallback
	}
	var sum int64
	for _, l := range ls {
		sum += l.Qty
	}
	return sum / int64(len(ls))
}

func midPrice(b *ob.Book) (float64, bool) {
	bid, okb := b.BestBid()
	ask, oka := b.BestAsk()
	if !okb || !oka {
		return 0, false
	}
	return float64(bid.Price+ask.Price) / 2, true
}

func ms(d int64) string { return strconv.FormatInt(d/int64(time.Millisecond), 10) }
