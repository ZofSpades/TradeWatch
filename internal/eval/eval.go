// Package eval measures how well rules detect the abuse the market
// simulator injects. It simulates seeded sessions in-process, runs each
// event through the ingest stage and a detection engine, exactly as the
// services do, matches each rule group's alerts against the ground-truth
// labels, and aggregates precision, recall, time to detect and the
// false-alert rate on sessions without any injected abuse.
//
// Everything is deterministic: the same plan produces the same numbers.
package eval

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/ingest"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/rules"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

// Plan describes the sessions to simulate.
type Plan struct {
	// FirstSeed is the seed of the first session; session i uses
	// FirstSeed+i. Clean sessions continue the sequence.
	FirstSeed uint64
	// Sessions is the number of sessions with injected abuse; Clean the
	// number of sessions without any.
	Sessions, Clean int
	Duration        time.Duration
	ClosingWindow   time.Duration
	Injection       sim.Injection
	// Workers bounds how many sessions are simulated at once (default: the
	// number of CPUs, at most 4).
	Workers int
}

// Variant is a group of rules evaluated together, named by their IDs in
// the rule set passed to Run.
type Variant struct {
	Name  string
	Rules []string
}

// Result is the evaluation of one variant over every session.
type Result struct {
	Variant string
	// ByType holds the measurements per abuse type name (for example
	// "spoofing"), and ByScenario the recall per injector scenario.
	ByType     map[string]*TypeStats
	ByScenario map[string]*ScenarioStats
}

// TypeStats are the measurements of one abuse type.
type TypeStats struct {
	Labels   int // injected episodes
	Detected int // episodes with at least one matching alert
	Alerts   int // alerts on injected sessions
	Matched  int // alerts matching an episode
	// TTD holds, per detected episode, the event time from the episode's
	// start to its first matching alert, in seconds, sorted.
	TTD []float64
	// CleanAlerts counts alerts on sessions without injected abuse, over
	// CleanSymbolHours of trading.
	CleanAlerts      int
	CleanSymbolHours float64
}

// ScenarioStats is the recall of one injector scenario, such as wash.ring.
type ScenarioStats struct {
	AbuseType string
	Labels    int
	Detected  int
}

// Grace is how long after an episode's end a matching alert's evidence
// window may start, to allow for detection that completes after the
// episode.
const Grace = time.Minute

// Matches reports whether an alert matches a ground-truth label: same
// session, symbol and abuse type, at least one account in common, detected
// no earlier than the episode began, with an evidence window starting no
// later than Grace after the episode ended.
func Matches(a *twpb.Alert, l *twpb.GroundTruthLabel) bool {
	if a.GetSessionId() != l.GetSessionId() || a.GetSymbol() != l.GetSymbol() || a.GetAbuseType() != l.GetAbuseType() {
		return false
	}
	if a.GetDetectedAtNs() < l.GetStartTimeNs() || a.GetWindowStartNs() > l.GetEndTimeNs()+int64(Grace) {
		return false
	}
	for _, acct := range a.GetAccountIds() {
		if slices.Contains(l.GetAccountIds(), acct) {
			return true
		}
	}
	return false
}

// session is one simulated session's outcome.
type session struct {
	clean       bool
	symbolHours float64
	labels      []*twpb.GroundTruthLabel
	alerts      map[string][]*twpb.Alert // by rule ID
}

// collector feeds a simulation through the ingest stage into an engine.
type collector struct {
	id      string
	ref     *ingest.ReferenceData
	engine  *detect.Engine
	alerts  map[string][]*twpb.Alert
	labels  []*twpb.GroundTruthLabel
	symbols map[string]bool
	pos     int64
}

func (c *collector) Emit(sym *sim.Symbol, ev *ob.Event) error {
	mev, rej := ingest.Normalize(sim.ToProto(c.id, sym, ev, 0), c.ref, ev.Time)
	if rej != nil {
		return fmt.Errorf("eval: ingest rejected a simulated event: %v", rej)
	}
	c.symbols[sym.Symbol] = true
	for _, a := range c.engine.Process(mev, c.pos) {
		c.alerts[a.GetRuleId()] = append(c.alerts[a.GetRuleId()], a)
	}
	c.pos++
	return nil
}

func (c *collector) Label(l *twpb.GroundTruthLabel) error {
	c.labels = append(c.labels, l)
	return nil
}

func (p Plan) runSession(ctx context.Context, n int, clean bool, set *rules.Set) (*session, error) {
	cfg := sim.DefaultConfig()
	cfg.Seed = p.FirstSeed + uint64(n)
	cfg.SessionID = fmt.Sprintf("eval-%d", cfg.Seed)
	cfg.Duration, cfg.ClosingWindow = p.Duration, p.ClosingWindow
	if !clean {
		cfg.Injection = p.Injection
	}
	uni := sim.DefaultUniverse()
	engine, err := detect.NewEngine(set, detect.Config{Instance: "eval"})
	if err != nil {
		return nil, err
	}
	c := &collector{id: cfg.SessionID, engine: engine, alerts: map[string][]*twpb.Alert{}, symbols: map[string]bool{}}
	s, err := sim.New(cfg, uni, c)
	if err != nil {
		return nil, err
	}
	c.ref = ingest.NewReferenceData(append(uni.RefData(),
		&twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: s.Session().Proto()}})...)
	if err := s.Run(ctx); err != nil {
		return nil, err
	}
	trading := (p.Duration + p.ClosingWindow).Hours()
	return &session{clean: clean, symbolHours: float64(len(c.symbols)) * trading, labels: c.labels, alerts: c.alerts}, nil
}

// Run simulates the plan's sessions, running every event through one
// engine holding set, and evaluates each variant's rules. Results are in
// the order of variants.
func Run(ctx context.Context, p Plan, set *rules.Set, variants []Variant) ([]*Result, error) {
	for _, v := range variants {
		for _, id := range v.Rules {
			if _, ok := set.Rule(id); !ok {
				return nil, fmt.Errorf("eval: variant %s refers to rule %s, which is not in the set", v.Name, id)
			}
		}
	}
	workers := p.Workers
	if workers <= 0 {
		workers = min(runtime.NumCPU(), 4)
	}
	type job struct {
		n     int
		clean bool
	}
	jobs := make(chan job)
	sessions := make([]*session, p.Sessions+p.Clean)
	errs := make([]error, len(sessions))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for j := range jobs {
				sessions[j.n], errs[j.n] = p.runSession(ctx, j.n, j.clean, set)
			}
		})
	}
	for n := range sessions {
		jobs <- job{n: n, clean: n >= p.Sessions}
	}
	close(jobs)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	results := make([]*Result, len(variants))
	for i, v := range variants {
		results[i] = score(v, sessions)
	}
	return results, nil
}

// ScoreAlerts scores the alerts raised on one session against its labels.
func ScoreAlerts(name string, alerts []*twpb.Alert, labels []*twpb.GroundTruthLabel) *Result {
	s := &session{labels: labels, alerts: map[string][]*twpb.Alert{}}
	v := Variant{Name: name}
	for _, a := range alerts {
		if _, seen := s.alerts[a.GetRuleId()]; !seen {
			v.Rules = append(v.Rules, a.GetRuleId())
		}
		s.alerts[a.GetRuleId()] = append(s.alerts[a.GetRuleId()], a)
	}
	return score(v, []*session{s})
}

// score aggregates one variant's alerts over every session.
func score(v Variant, sessions []*session) *Result {
	r := &Result{Variant: v.Name, ByType: map[string]*TypeStats{}, ByScenario: map[string]*ScenarioStats{}}
	stats := func(t twpb.AbuseType) *TypeStats {
		key := typeName(t)
		if r.ByType[key] == nil {
			r.ByType[key] = &TypeStats{}
		}
		return r.ByType[key]
	}
	for _, s := range sessions {
		var alerts []*twpb.Alert
		for _, id := range v.Rules {
			alerts = append(alerts, s.alerts[id]...)
		}
		if s.clean {
			for _, a := range alerts {
				stats(a.GetAbuseType()).CleanAlerts++
			}
			for _, t := range abuseTypes {
				stats(t).CleanSymbolHours += s.symbolHours
			}
			continue
		}
		for _, a := range alerts {
			st := stats(a.GetAbuseType())
			st.Alerts++
			for _, l := range s.labels {
				if Matches(a, l) {
					st.Matched++
					break
				}
			}
		}
		for _, l := range s.labels {
			st := stats(l.GetAbuseType())
			st.Labels++
			sc := r.ByScenario[l.GetScenario()]
			if sc == nil {
				sc = &ScenarioStats{AbuseType: typeName(l.GetAbuseType())}
				r.ByScenario[l.GetScenario()] = sc
			}
			sc.Labels++
			first := int64(-1)
			for _, a := range alerts {
				if Matches(a, l) && (first < 0 || a.GetDetectedAtNs() < first) {
					first = a.GetDetectedAtNs()
				}
			}
			if first >= 0 {
				st.Detected++
				sc.Detected++
				st.TTD = append(st.TTD, float64(first-l.GetStartTimeNs())/1e9)
			}
		}
	}
	for _, st := range r.ByType {
		slices.Sort(st.TTD)
	}
	return r
}

var abuseTypes = []twpb.AbuseType{
	twpb.AbuseType_ABUSE_TYPE_WASH_TRADING, twpb.AbuseType_ABUSE_TYPE_SPOOFING,
	twpb.AbuseType_ABUSE_TYPE_MOMENTUM_IGNITION, twpb.AbuseType_ABUSE_TYPE_QUOTE_STUFFING,
	twpb.AbuseType_ABUSE_TYPE_MARKING_THE_CLOSE,
}

// TypeNames lists the abuse type names in report order.
func TypeNames() []string {
	out := make([]string, len(abuseTypes))
	for i, t := range abuseTypes {
		out[i] = typeName(t)
	}
	return out
}

// typeName is an abuse type's name as the store and the API use it, for
// example "wash_trading".
func typeName(t twpb.AbuseType) string {
	return strings.ToLower(strings.TrimPrefix(t.String(), "ABUSE_TYPE_"))
}
