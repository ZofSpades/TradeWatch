package sim

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// sessionAgent drives the trading phases: continuous trading at the open,
// the closing phase, and the close, which cancels all resting orders.
type sessionAgent struct{ stage int }

func (a *sessionAgent) Start(s *Sim) { s.Schedule(s.session.Open, a) }

func (a *sessionAgent) Wake(s *Sim) int64 {
	switch a.stage {
	case 0:
		a.stage = 1
		s.setPhase(twpb.TradingPhase_TRADING_PHASE_CONTINUOUS)
		if s.session.ClosingStart > s.session.Open {
			return s.session.ClosingStart
		}
		return s.session.Close
	case 1:
		a.stage = 2
		if s.now < s.session.Close {
			s.setPhase(twpb.TradingPhase_TRADING_PHASE_CLOSING)
			return s.session.Close
		}
		fallthrough
	default:
		s.closeSession()
		return -1
	}
}

// setPhase changes every symbol's phase and publishes a snapshot so
// consumers learn about the transition at its exact sequence position.
func (s *Sim) setPhase(p twpb.TradingPhase) {
	for _, sym := range s.symbols {
		sym.Phase = p
		s.snapshot(sym)
	}
}

func (s *Sim) closeSession() {
	for _, sym := range s.symbols {
		s.dispatch(sym, sym.Book.CloseSession(s.evs[:0], s.now))
		sym.Phase = twpb.TradingPhase_TRADING_PHASE_CLOSED
		s.snapshot(sym)
	}
	s.closed = true
}

// snapshotAgent publishes a periodic snapshot of one symbol's book.
type snapshotAgent struct {
	sym    *Symbol
	offset int64
}

func (a *snapshotAgent) Start(s *Sim) { s.Schedule(s.session.Open+a.offset, a) }

func (a *snapshotAgent) Wake(s *Sim) int64 {
	if s.closed {
		return -1
	}
	if a.sym.Phase != twpb.TradingPhase_TRADING_PHASE_UNSPECIFIED {
		s.snapshot(a.sym)
	}
	return s.now + int64(s.cfg.SnapshotInterval)
}

// fundamentalAgent moves a symbol's fair value along a geometric random walk
// with rare jumps (news). Market makers quote around the fair value, so it is
// the latent driver of prices.
type fundamentalAgent struct {
	sym  *Symbol
	rng  *rand.Rand
	step int64
	// sigma is the volatility per sqrt(step).
	sigma float64
	// jumpProb is the per-step probability of a news jump.
	jumpProb float64
}

const fundamentalStep = 500 * time.Millisecond

// tradingSecondsPerDay is the length of a standard 6.5-hour session; daily
// volatilities are quoted per such day.
const tradingSecondsPerDay = 23_400

func newFundamentalAgent(s *Sim, sym *Symbol) *fundamentalAgent {
	stepSec := fundamentalStep.Seconds()
	return &fundamentalAgent{
		sym:   sym,
		rng:   streamRNG(s.cfg.Seed, "fundamental/"+sym.Symbol),
		step:  int64(fundamentalStep),
		sigma: sym.DailyVol * math.Sqrt(stepSec/tradingSecondsPerDay),
		// About one news event per four trading hours per symbol.
		jumpProb: stepSec / (4 * 3600),
	}
}

func (a *fundamentalAgent) Start(s *Sim) { s.Schedule(s.session.Open+a.step, a) }

func (a *fundamentalAgent) Wake(s *Sim) int64 {
	if s.closed {
		return -1
	}
	r := a.sigma * a.rng.NormFloat64()
	if a.rng.Float64() < a.jumpProb {
		// A jump of 0.4%-1.2% in a random direction.
		size := 0.004 + 0.008*a.rng.Float64()
		if a.rng.IntN(2) == 0 {
			size = -size
		}
		r += size
	}
	a.sym.fair *= math.Exp(r)
	if floor := float64(20 * a.sym.TickSize); a.sym.fair < floor {
		a.sym.fair = floor
	}
	return s.now + a.step
}

// tierParams are the participant intensities for one liquidity tier.
type tierParams struct {
	marketMakers    int
	mmWake          time.Duration
	mmHalfSpread    float64 // ticks
	mmSize          float64 // shares
	mmMaxInventory  int64
	noiseArrival    time.Duration
	noiseMedianSize float64
	// impact is the permanent change in fair value, as a fraction, per share
	// traded aggressively (signed by the aggressor's side).
	impact           float64
	parents          int // institutional parent orders per session
	parentMedianSize float64
	momentumTraders  int
	momentumSize     int64
}

func paramsFor(tier twpb.LiquidityTier) tierParams {
	switch tier {
	case twpb.LiquidityTier_LIQUIDITY_TIER_HIGH:
		return tierParams{
			marketMakers: 3, mmWake: 400 * time.Millisecond, mmHalfSpread: 1.2, mmSize: 400, mmMaxInventory: 6_000, impact: 4e-7,
			noiseArrival: 700 * time.Millisecond, noiseMedianSize: 120, parents: 3, parentMedianSize: 60_000,
			momentumTraders: 2, momentumSize: 1_500,
		}
	case twpb.LiquidityTier_LIQUIDITY_TIER_MEDIUM:
		return tierParams{
			marketMakers: 2, mmWake: 700 * time.Millisecond, mmHalfSpread: 2.2, mmSize: 300, mmMaxInventory: 4_000, impact: 1e-6,
			noiseArrival: 1600 * time.Millisecond, noiseMedianSize: 90, parents: 2, parentMedianSize: 25_000,
			momentumTraders: 1, momentumSize: 800,
		}
	default:
		return tierParams{
			marketMakers: 1, mmWake: 1200 * time.Millisecond, mmHalfSpread: 4, mmSize: 200, mmMaxInventory: 2_500, impact: 2.5e-6,
			noiseArrival: 4 * time.Second, noiseMedianSize: 70, parents: 1, parentMedianSize: 8_000,
			momentumTraders: 1, momentumSize: 400,
		}
	}
}

// defaultAgents builds the session machinery and the normal participants.
func defaultAgents(s *Sim) []Agent {
	agents := []Agent{&sessionAgent{}, newPlanner(s)}
	mms := s.uni.ByRole(RoleMarketMaker)
	insts := s.uni.ByRole(RoleInstitutional)
	props := s.uni.ByRole(RoleProp)
	retail := s.uni.ByRole(RoleRetail)
	for i, sym := range s.symbols {
		p := paramsFor(sym.Tier)
		interval := int64(s.cfg.SnapshotInterval)
		agents = append(agents,
			newFundamentalAgent(s, sym),
			&snapshotAgent{sym: sym, offset: interval * int64(i+1) / int64(len(s.symbols)+1)},
		)
		for k := 0; k < p.marketMakers && k < len(mms); k++ {
			agents = append(agents, newMarketMaker(s, sym, mms[(i+k)%len(mms)], p))
		}
		agents = append(agents, newNoiseTrader(s, sym, retail, p))
		rng := streamRNG(s.cfg.Seed, "parents/"+sym.Symbol)
		for k := 0; k < p.parents; k++ {
			agents = append(agents, newParentOrder(s, sym, insts[rng.IntN(len(insts))], k, rng, p))
		}
		for k := 0; k < p.momentumTraders && k < len(props); k++ {
			agents = append(agents, newMomentumTrader(s, sym, props[(i+k)%len(props)], p))
		}
	}
	return agents
}
