// Package demo drives `make demo`. It injects labelled abuse scenarios into
// the running simulator through the API, as an admin can from the
// dashboard's Simulator page, and reports the alert each one raised.
//
// A scenario is paired with its alert through the episode's ground-truth
// label, by the rule the evaluator scores with (eval.Matches). The pipeline
// never reads labels. The demo reads them, like the evaluator, only to
// check the detectors' work.
package demo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ZofSpades/tradewatch/internal/eval"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/marketsim"
	"github.com/ZofSpades/tradewatch/internal/rules"
	"github.com/ZofSpades/tradewatch/internal/sim"
	"github.com/ZofSpades/tradewatch/internal/store"
)

// DefaultScenarios are injected unless others are named. The naive
// baseline in the evaluation detects no wash ring.
var DefaultScenarios = []string{sim.ScenarioWashRing, sim.ScenarioSpoofing, sim.ScenarioQuoteStuffing}

// descriptions say what each scenario does, after docs/manipulation.md.
var descriptions = map[string]string{
	sim.ScenarioWashDirect:      "two accounts of one beneficial owner trade with each other",
	sim.ScenarioWashRing:        "three accounts with different owners trade in a circle, A to B to C to A",
	sim.ScenarioSpoofing:        "large orders behind the touch, cancelled once a genuine order on the other side executes",
	sim.ScenarioMomentum:        "aggressive orders push the price, then the position is traded out the other way",
	sim.ScenarioQuoteStuffing:   "hundreds of orders a second, each cancelled within milliseconds",
	sim.ScenarioMarkingTheClose: "aggressive one-way orders in the last minutes before the close",
}

// LabelSource yields ground-truth labels as episodes finish.
type LabelSource interface {
	Next(ctx context.Context) (*twpb.GroundTruthLabel, error)
}

// Config configures a demo run.
type Config struct {
	API       *API
	Labels    LabelSource
	Scenarios []string
	// Attempts is how many times a scenario is injected before the demo
	// gives up on it: not every episode forms its pattern, and the rules
	// do not detect every one.
	Attempts int
	// LabelTimeout bounds the wait for an injected episode to finish, and
	// AlertTimeout the wait for a matching alert after it has.
	LabelTimeout time.Duration
	AlertTimeout time.Duration
	// MinSessionLeft is the simulated time a session must have left for
	// the scenarios to finish before its close.
	MinSessionLeft time.Duration
	// Poll is the interval between checks of the API.
	Poll time.Duration
	// Dashboard is the dashboard's URL as a browser reaches it.
	Dashboard string
	Out       io.Writer
}

// Run injects the scenarios at once, on different symbols, and reports
// each one's ground truth and alert as it completes. It fails if a
// scenario raised no matching alert in any attempt.
func Run(ctx context.Context, cfg Config) error {
	if len(cfg.Scenarios) == 0 {
		return errors.New("no scenarios to inject")
	}
	for _, s := range cfg.Scenarios {
		if !slices.Contains(sim.Scenarios, s) {
			return fmt.Errorf("unknown scenario %q; the simulator has %s", s, strings.Join(sim.Scenarios, ", "))
		}
	}
	d := &run{cfg: cfg, board: newBoard(), inUse: map[string]bool{}}
	st, err := d.readySession(ctx)
	if err != nil {
		return err
	}
	if len(st.Symbols) < len(cfg.Scenarios) {
		return fmt.Errorf("%d scenarios need as many symbols, and the session trades %d", len(cfg.Scenarios), len(st.Symbols))
	}
	d.symbols = slices.Clone(st.Symbols)
	rand.Shuffle(len(d.symbols), func(i, j int) { d.symbols[i], d.symbols[j] = d.symbols[j], d.symbols[i] })
	picks := make([]string, len(cfg.Scenarios))
	for i := range picks {
		picks[i] = d.takeSymbol(nil)
	}
	d.header(st, picks)

	lctx, stop := context.WithCancel(ctx)
	followed := make(chan struct{})
	go func() {
		defer close(followed)
		for {
			l, err := cfg.Labels.Next(lctx)
			d.board.add(l, err)
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		stop()
		<-followed
	}()

	detected := make([]bool, len(cfg.Scenarios))
	var wg sync.WaitGroup
	for i, sc := range cfg.Scenarios {
		wg.Go(func() { detected[i] = d.scenario(ctx, sc, picks[i]) })
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	n := 0
	for _, ok := range detected {
		if ok {
			n++
		}
	}
	w := cfg.Out
	fmt.Fprintf(w, "\n%d of %d scenarios raised an alert that matches their ground truth.\n\n", n, len(cfg.Scenarios))
	para(w, fmt.Sprintf("Open the dashboard on %s and sign in as analyst to work the alerts. "+
		"`make env` wrote the password to .env as TW_API_ANALYST_PASSWORD. "+
		"The dashboard's System page links to Grafana and to the traces in Jaeger.", cfg.Dashboard))
	if n < len(cfg.Scenarios) {
		return fmt.Errorf("%d of %d scenarios raised no matching alert in %d attempts", len(cfg.Scenarios)-n, len(cfg.Scenarios), cfg.Attempts)
	}
	return nil
}

type run struct {
	cfg   Config
	board *board

	mu      sync.Mutex // guards the output and the symbols
	symbols []string
	next    int
	inUse   map[string]bool
}

// readySession returns the status of a running session with at least
// MinSessionLeft of simulated time left. It starts the simulator if it is
// stopped, and waits for the next session if the current one closes too
// soon.
func (d *run) readySession(ctx context.Context) (marketsim.Status, error) {
	limit := d.cfg.MinSessionLeft + 5*time.Minute
	deadline := time.Now().Add(limit)
	started, waited := false, ""
	for {
		st, err := d.cfg.API.SimStatus(ctx)
		if err != nil {
			return st, fmt.Errorf("simulator status: %w", err)
		}
		switch st.State {
		case "stopped":
			if !started {
				fmt.Fprintln(d.cfg.Out, "The simulator is stopped. Starting it.")
				if err := d.cfg.API.StartSim(ctx); err != nil {
					return st, fmt.Errorf("start the simulator: %w", err)
				}
				started = true
			}
		case "finished":
			return st, errors.New("the simulator has run all its configured sessions; `make up` restarts it")
		case "running":
			left, ok := sessionLeft(st)
			if ok && left >= d.cfg.MinSessionLeft {
				return st, nil
			}
			if ok && waited != st.SessionID {
				fmt.Fprintf(d.cfg.Out, "Session %s closes in %s of simulated time, too soon for every scenario to finish. Waiting for the next session.\n",
					st.SessionID, human(left))
				waited = st.SessionID
			}
		}
		if time.Now().After(deadline) {
			return st, fmt.Errorf("no session with %s left to run started within %s", human(d.cfg.MinSessionLeft), human(limit))
		}
		if err := sleep(ctx, d.cfg.Poll); err != nil {
			return st, err
		}
	}
}

// sessionLeft is the simulated time from the last published event to the
// session's close.
func sessionLeft(st marketsim.Status) (time.Duration, bool) {
	now, err1 := time.Parse(time.RFC3339Nano, st.SimTime)
	end, err2 := time.Parse(time.RFC3339Nano, st.SessionClose)
	if st.SessionID == "" || err1 != nil || err2 != nil {
		return 0, false
	}
	return end.Sub(now), true
}

// takeSymbol reserves a symbol no running scenario uses, preferring one
// not in tried. Each scenario holds at most one symbol, and there are at
// least as many symbols as scenarios, so one is always free.
func (d *run) takeSymbol(tried map[string]bool) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	pick := ""
	for range d.symbols {
		s := d.symbols[d.next%len(d.symbols)]
		d.next++
		if d.inUse[s] {
			continue
		}
		if !tried[s] {
			pick = s
			break
		}
		if pick == "" {
			pick = s
		}
	}
	d.inUse[pick] = true
	return pick
}

func (d *run) releaseSymbol(s string) {
	d.mu.Lock()
	delete(d.inUse, s)
	d.mu.Unlock()
}

// result is the outcome of one injection.
type result struct {
	scenario, symbol string
	// label is nil if the episode published none in time.
	label *twpb.GroundTruthLabel
	// alert is the earliest matching alert, or nil; more counts the others.
	alert *live.Summary
	more  int
	trace string
}

// scenario injects a scenario until an attempt raises a matching alert,
// moving to another symbol after each miss.
func (d *run) scenario(ctx context.Context, scenario, symbol string) bool {
	tried := map[string]bool{}
	for attempt := 1; ; attempt++ {
		tried[symbol] = true
		res, err := d.attempt(ctx, scenario, symbol)
		d.releaseSymbol(symbol)
		next := ""
		if err == nil && res.alert == nil && attempt < d.cfg.Attempts {
			next = d.takeSymbol(tried)
		}
		if ctx.Err() != nil {
			return false
		}
		d.report(res, err, next)
		if next == "" {
			return err == nil && res.alert != nil
		}
		symbol = next
	}
}

// attempt triggers one episode, waits for its label, then for an alert
// that matches it.
func (d *run) attempt(ctx context.Context, scenario, symbol string) (result, error) {
	res := result{scenario: scenario, symbol: symbol}
	st, err := d.cfg.API.SimStatus(ctx)
	if err != nil {
		return res, fmt.Errorf("simulator status: %w", err)
	}
	// The episode starts at the simulated time of the trigger, which is
	// no earlier than the last event published before it.
	var since int64
	if t, err := time.Parse(time.RFC3339Nano, st.SimTime); err == nil {
		since = t.UnixNano()
	}
	if err := d.trigger(ctx, scenario, symbol); err != nil {
		return res, fmt.Errorf("trigger: %w", err)
	}
	lctx, cancel := context.WithTimeout(ctx, d.cfg.LabelTimeout)
	defer cancel()
	res.label, err = d.board.wait(lctx, func(l *twpb.GroundTruthLabel) bool {
		return l.GetSessionId() == st.SessionID && l.GetScenario() == scenario && l.GetSymbol() == symbol && l.GetStartTimeNs() >= since
	})
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return res, nil
	}
	if err != nil {
		return res, err
	}
	deadline := time.Now().Add(d.cfg.AlertTimeout)
	for {
		alerts, err := d.cfg.API.Alerts(ctx, res.label.GetSessionId(), symbol, store.AbuseTypeName(res.label.GetAbuseType()))
		if err != nil {
			return res, fmt.Errorf("list alerts: %w", err)
		}
		if m := matching(alerts, res.label); len(m) > 0 {
			res.alert, res.more = &m[0], len(m)-1
			// The trace link is a convenience: without one, the alert is
			// still reported.
			res.trace, _ = d.cfg.API.TraceURL(ctx, m[0].Key)
			return res, nil
		}
		if time.Now().After(deadline) {
			return res, nil
		}
		if err := sleep(ctx, d.cfg.Poll); err != nil {
			return res, err
		}
	}
}

// trigger starts the scenario. The simulator answers 409 between sessions
// and when its command queue is full, so those are retried for a while.
func (d *run) trigger(ctx context.Context, scenario, symbol string) error {
	for i := 0; ; i++ {
		err := d.cfg.API.Trigger(ctx, scenario, symbol)
		var se *StatusError
		if !errors.As(err, &se) || se.Code != http.StatusConflict || i == 10 {
			return err
		}
		if err := sleep(ctx, d.cfg.Poll); err != nil {
			return err
		}
	}
}

// matching returns the alerts that match the label by the evaluator's
// rule, the earliest detected first.
func matching(alerts []live.Summary, l *twpb.GroundTruthLabel) []live.Summary {
	var out []live.Summary
	for _, a := range alerts {
		pa := &twpb.Alert{
			SessionId: a.SessionID, Symbol: a.Symbol, AbuseType: rules.AbuseTypes[a.AbuseType], AccountIds: a.AccountIDs,
			DetectedAtNs: a.DetectedAt.UnixNano(), WindowStartNs: a.WindowStart.UnixNano(),
		}
		if eval.Matches(pa, l) {
			out = append(out, a)
		}
	}
	slices.SortStableFunc(out, func(x, y live.Summary) int { return x.DetectedAt.Compare(y.DetectedAt) })
	return out
}

func (d *run) header(st marketsim.Status, picks []string) {
	w := d.cfg.Out
	fmt.Fprintln(w, "TradeWatch demo. All market data is simulated.")
	fmt.Fprintln(w)
	if t, err := time.Parse(time.RFC3339Nano, st.SimTime); err == nil {
		fmt.Fprintf(w, "Session %s is running. Simulated time is %s UTC.\n", st.SessionID, t.Format("15:04:05"))
	}
	para(w, fmt.Sprintf("Injecting %d labelled scenarios through the API, as an admin can from the dashboard's Simulator page:", len(picks)))
	for i, sc := range d.cfg.Scenarios {
		wrap(w, fmt.Sprintf("  %-21s on %-4s  ", sc, picks[i]), descriptions[sc])
	}
	fmt.Fprintln(w)
	para(w, "The simulator publishes an episode's ground-truth label when the episode ends, "+
		"within a few minutes. The detectors never see labels. The demo pairs each label "+
		"with the alert that matches it, by the rule the evaluator scores with.")
}

func (d *run) report(r result, err error, next string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	w := d.cfg.Out
	fmt.Fprintf(w, "\n%s on %s\n", r.scenario, r.symbol)
	switch l := r.label; {
	case err != nil:
		wrap(w, "  Failed        ", err.Error())
	case l == nil:
		wrap(w, "  Ground truth  ", fmt.Sprintf("no label within %s: the episode was skipped or never formed its pattern", human(d.cfg.LabelTimeout)))
	default:
		wrap(w, "  Ground truth  ", fmt.Sprintf("label %s: %s %s, %d orders, %s to %s", l.GetLabelId(),
			plural(len(l.GetAccountIds()), "account"), strings.Join(l.GetAccountIds(), ", "), len(l.GetOrderIds()),
			clock(l.GetStartTimeNs()), clock(l.GetEndTimeNs())))
		if a := r.alert; a != nil {
			wrap(w, "  Alert         ", fmt.Sprintf("%s by rule %s v%d (%s), severity %s, score %.0f, detected %s into the episode in simulated time",
				a.AbuseType, a.RuleID, a.RuleVersion, a.TaxonomyCode, a.Severity, a.Score,
				human(time.Duration(a.DetectedAt.UnixNano()-l.GetStartTimeNs()))))
			wrap(w, "  Why           ", a.Explanation)
			fmt.Fprintf(w, "  Dashboard     %s/#/alerts/%s\n", strings.TrimRight(d.cfg.Dashboard, "/"), url.PathEscape(a.Key))
			if r.trace != "" {
				fmt.Fprintf(w, "  Trace         %s\n", r.trace)
			}
			if r.more > 0 {
				fmt.Fprintf(w, "  Also          %s matching this episode\n", plural(r.more, "more alert"))
			}
		} else {
			wrap(w, "  Alert         ", fmt.Sprintf("none matched within %s; the rules miss some episodes (docs/results/eval)", human(d.cfg.AlertTimeout)))
		}
	}
	if next != "" {
		fmt.Fprintf(w, "  Injecting it again, on %s.\n", next)
	}
}

// board collects labels for the scenarios waiting on them.
type board struct {
	mu      sync.Mutex
	labels  []*twpb.GroundTruthLabel
	err     error
	changed chan struct{} // closed and replaced when a label or error arrives
}

func newBoard() *board { return &board{changed: make(chan struct{})} }

func (b *board) add(l *twpb.GroundTruthLabel, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.err = err
	} else {
		b.labels = append(b.labels, l)
	}
	close(b.changed)
	b.changed = make(chan struct{})
}

// wait returns the first label that satisfies match, waiting for one.
func (b *board) wait(ctx context.Context, match func(*twpb.GroundTruthLabel) bool) (*twpb.GroundTruthLabel, error) {
	seen := 0
	for {
		b.mu.Lock()
		for ; seen < len(b.labels); seen++ {
			if l := b.labels[seen]; match(l) {
				b.mu.Unlock()
				return l, nil
			}
		}
		err, changed := b.err, b.changed
		b.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("ground-truth labels: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func clock(ns int64) string { return time.Unix(0, ns).UTC().Format("15:04:05.000") }

func human(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%d ms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1f s", d.Seconds())
	}
	d = d.Round(time.Second)
	if s := d % time.Minute; s != 0 {
		return fmt.Sprintf("%d min %d s", d/time.Minute, s/time.Second)
	}
	return fmt.Sprintf("%d min", d/time.Minute)
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

const width = 78

// para writes text wrapped to the terminal width.
func para(w io.Writer, text string) { wrap(w, "", text) }

// wrap writes text after first, wrapped and indented to first's width.
func wrap(w io.Writer, first, text string) {
	indent := strings.Repeat(" ", len(first))
	line := first
	for _, word := range strings.Fields(text) {
		if len(line) > len(indent) && len(line)+1+len(word) > width {
			fmt.Fprintln(w, line)
			line = indent
		}
		if len(line) > len(indent) {
			line += " "
		}
		line += word
	}
	fmt.Fprintln(w, line)
}
