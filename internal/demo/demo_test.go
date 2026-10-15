package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/eval"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/marketsim"
	"github.com/ZofSpades/tradewatch/internal/sim"
	"github.com/ZofSpades/tradewatch/internal/store"
)

var simNow = time.Date(2026, 10, 15, 14, 0, 0, 0, time.UTC)

func running(session string, left time.Duration) marketsim.Status {
	return marketsim.Status{
		State: "running", SessionID: session,
		SimTime: simNow.Format(time.RFC3339Nano), SessionClose: simNow.Add(left).Format(time.RFC3339Nano),
		Symbols: []string{"AAA", "BBB", "CCC", "DDD", "EEE", "FFF"},
	}
}

var abuseOf = map[string]twpb.AbuseType{
	sim.ScenarioWashDirect: twpb.AbuseType_ABUSE_TYPE_WASH_TRADING, sim.ScenarioWashRing: twpb.AbuseType_ABUSE_TYPE_WASH_TRADING,
	sim.ScenarioSpoofing: twpb.AbuseType_ABUSE_TYPE_SPOOFING, sim.ScenarioMomentum: twpb.AbuseType_ABUSE_TYPE_MOMENTUM_IGNITION,
	sim.ScenarioQuoteStuffing: twpb.AbuseType_ABUSE_TYPE_QUOTE_STUFFING, sim.ScenarioMarkingTheClose: twpb.AbuseType_ABUSE_TYPE_MARKING_THE_CLOSE,
}

// fakeStack serves the API calls the demo makes. react decides what the
// nth accepted trigger causes: a label, alerts, or nothing.
type fakeStack struct {
	mu          sync.Mutex
	status      func(calls int, started bool) marketsim.Status
	statusCalls int
	started     bool
	conflicts   int // triggers to refuse with 409 first
	triggers    []marketsim.ScenarioRequest
	alerts      []live.Summary
	labels      chan *twpb.GroundTruthLabel
	react       func(f *fakeStack, n int, req marketsim.ScenarioRequest)
}

// settle orders the handlers' writes before the test reads them.
func (f *fakeStack) settle() {
	f.mu.Lock()
	//lint:ignore SA2001 the empty critical section is the synchronization
	f.mu.Unlock()
}

func newFakeStack(t *testing.T) (*fakeStack, *API) {
	f := &fakeStack{
		labels: make(chan *twpb.GroundTruthLabel, 16),
		status: func(int, bool) marketsim.Status { return running("demo-1", 20*time.Minute) },
	}
	mux := http.NewServeMux()
	authed := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer tok" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			h(w, r)
		}
	}
	reply := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("POST /api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var c struct{ Username, Password string }
		if json.NewDecoder(r.Body).Decode(&c) != nil || c.Username != "admin" || c.Password != "secret" {
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}
		reply(w, http.StatusOK, map[string]any{"access_token": "tok", "token_type": "Bearer"})
	})
	mux.HandleFunc("GET /api/v1/sim/status", authed(func(w http.ResponseWriter, _ *http.Request) {
		f.statusCalls++
		reply(w, http.StatusOK, f.status(f.statusCalls, f.started))
	}))
	mux.HandleFunc("POST /api/v1/sim/start", authed(func(w http.ResponseWriter, _ *http.Request) {
		f.started = true
		reply(w, http.StatusAccepted, map[string]string{})
	}))
	mux.HandleFunc("POST /api/v1/sim/scenarios", authed(func(w http.ResponseWriter, r *http.Request) {
		var req marketsim.ScenarioRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("trigger body: %v", err)
		}
		if f.conflicts > 0 {
			f.conflicts--
			reply(w, http.StatusConflict, map[string]string{"error": "marketsim: no session is running"})
			return
		}
		f.triggers = append(f.triggers, req)
		if f.react != nil {
			f.react(f, len(f.triggers)-1, req)
		}
		reply(w, http.StatusAccepted, map[string]string{"status": "accepted"})
	}))
	mux.HandleFunc("GET /api/v1/alerts", authed(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		out := []live.Summary{}
		for _, a := range f.alerts {
			if a.SessionID == q.Get("session") && a.Symbol == q.Get("symbol") && a.AbuseType == q.Get("abuse_type") {
				out = append(out, a)
			}
		}
		reply(w, http.StatusOK, map[string]any{"alerts": out})
	}))
	mux.HandleFunc("GET /api/v1/alerts/{key}", authed(func(w http.ResponseWriter, r *http.Request) {
		reply(w, http.StatusOK, map[string]string{"key": r.PathValue("key"), "trace_url": "http://jaeger/trace/" + r.PathValue("key")})
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	api := NewAPI(srv.URL)
	if err := api.Login(context.Background(), "admin", "secret"); err != nil {
		t.Fatal(err)
	}
	return f, api
}

// episode is the label of the nth triggered episode: one account, starting
// a second after the trigger.
func episode(n int, session string, req marketsim.ScenarioRequest) *twpb.GroundTruthLabel {
	start := simNow.Add(time.Second).UnixNano()
	return &twpb.GroundTruthLabel{
		LabelId: fmt.Sprintf("%s/%04d", session, n+1), SessionId: session, AbuseType: abuseOf[req.Scenario],
		Scenario: req.Scenario, Symbol: req.Symbol, AccountIds: []string{"ACC-" + req.Symbol},
		StartTimeNs: start, EndTimeNs: start + int64(5*time.Second), OrderIds: []uint64{11, 12, 13},
	}
}

// alertOn is an alert on the label's symbol for the given account,
// detected two seconds into the episode.
func alertOn(l *twpb.GroundTruthLabel, key, account string) live.Summary {
	start := time.Unix(0, l.GetStartTimeNs()).UTC()
	return live.Summary{
		Key: key, SessionID: l.GetSessionId(), Symbol: l.GetSymbol(), AbuseType: store.AbuseTypeName(l.GetAbuseType()),
		Severity: "high", Score: 87, RuleID: "rule-1", RuleVersion: 2, TaxonomyCode: "MAR-X",
		AccountIDs: []string{account}, WindowStart: start, WindowEnd: start.Add(3 * time.Second),
		DetectedAt: start.Add(2 * time.Second), Explanation: "The account did what the rule looks for.",
	}
}

// detected makes every trigger publish a label with one matching alert,
// next to an alert for another account that must not match.
func detected(f *fakeStack, n int, req marketsim.ScenarioRequest) {
	l := episode(n, "demo-1", req)
	f.alerts = append(f.alerts, alertOn(l, "other-"+req.Symbol, "ACC-ZZZ"), alertOn(l, "key-"+req.Symbol, "ACC-"+req.Symbol))
	f.labels <- l
}

// flat collapses the wrapped output's whitespace.
func flat(s string) string { return strings.Join(strings.Fields(s), " ") }

type labelChan chan *twpb.GroundTruthLabel

func (c labelChan) Next(ctx context.Context) (*twpb.GroundTruthLabel, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case l := <-c:
		return l, nil
	}
}

func config(f *fakeStack, api *API, out *bytes.Buffer, scenarios ...string) Config {
	return Config{
		API: api, Labels: labelChan(f.labels), Scenarios: scenarios, Attempts: 3,
		LabelTimeout: 300 * time.Millisecond, AlertTimeout: 150 * time.Millisecond, MinSessionLeft: 5 * time.Minute,
		Poll: 5 * time.Millisecond, Dashboard: "http://dash/", Out: out,
	}
}

func TestRunPairsEachScenarioWithTheAlertItRaised(t *testing.T) {
	f, api := newFakeStack(t)
	f.react = detected
	var out bytes.Buffer
	if err := Run(context.Background(), config(f, api, &out, DefaultScenarios...)); err != nil {
		t.Fatalf("Run: %v\n%s", err, out.String())
	}
	f.settle()
	got := flat(out.String())
	if len(f.triggers) != len(DefaultScenarios) {
		t.Fatalf("triggered %v, want each default scenario once", f.triggers)
	}
	symbols := map[string]bool{}
	for _, tr := range f.triggers {
		symbols[tr.Symbol] = true
		for _, want := range []string{
			tr.Scenario + " on " + tr.Symbol,
			"Ground truth label demo-1/",
			"1 account ACC-" + tr.Symbol + ", 3 orders, 14:00:01.000 to 14:00:06.000",
			"Dashboard http://dash/#/alerts/key-" + tr.Symbol,
			"Trace http://jaeger/trace/key-" + tr.Symbol,
		} {
			if !strings.Contains(got, want) {
				t.Errorf("output lacks %q:\n%s", want, out.String())
			}
		}
	}
	if len(symbols) != len(DefaultScenarios) {
		t.Errorf("scenarios shared symbols: %v", f.triggers)
	}
	for _, want := range []string{
		"All market data is simulated.",
		"by rule rule-1 v2 (MAR-X), severity high, score 87, detected 2.0 s into the episode in simulated time",
		"3 of 3 scenarios raised an alert that matches their ground truth.",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(got, "other-") {
		t.Errorf("reported an alert for an account outside the episode:\n%s", got)
	}
}

func TestRunInjectsAMissedScenarioAgainOnAnotherSymbol(t *testing.T) {
	f, api := newFakeStack(t)
	f.react = func(f *fakeStack, n int, req marketsim.ScenarioRequest) {
		if n == 0 { // a label, but only an alert for an account outside it
			l := episode(n, "demo-1", req)
			f.alerts = append(f.alerts, alertOn(l, "other", "ACC-ZZZ"))
			f.labels <- l
			return
		}
		detected(f, n, req)
	}
	var out bytes.Buffer
	if err := Run(context.Background(), config(f, api, &out, sim.ScenarioSpoofing)); err != nil {
		t.Fatalf("Run: %v\n%s", err, out.String())
	}
	f.settle()
	if len(f.triggers) != 2 || f.triggers[0].Symbol == f.triggers[1].Symbol {
		t.Fatalf("triggers = %v, want a second one on another symbol", f.triggers)
	}
	got := flat(out.String())
	for _, want := range []string{
		"Alert none matched within 150 ms",
		"Injecting it again, on " + f.triggers[1].Symbol + ".",
		"1 of 1 scenarios raised an alert",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
}

func TestRunFailsWhenNoAttemptRaisesAnAlert(t *testing.T) {
	f, api := newFakeStack(t) // triggered episodes publish no label
	var out bytes.Buffer
	cfg := config(f, api, &out, sim.ScenarioQuoteStuffing)
	cfg.Attempts = 2
	err := Run(context.Background(), cfg)
	f.settle()
	if err == nil || !strings.Contains(err.Error(), "1 of 1 scenarios raised no matching alert in 2 attempts") {
		t.Fatalf("Run error = %v", err)
	}
	if len(f.triggers) != 2 {
		t.Errorf("triggers = %v, want 2", f.triggers)
	}
	if n := strings.Count(flat(out.String()), "no label within 300 ms"); n != 2 {
		t.Errorf("reported %d missing labels, want 2:\n%s", n, out.String())
	}
}

func TestRunWaitsForTheNextSessionWhenTheCloseIsNear(t *testing.T) {
	f, api := newFakeStack(t)
	f.status = func(calls int, _ bool) marketsim.Status {
		if calls < 3 {
			return running("demo-1", 2*time.Minute)
		}
		return running("demo-2", 30*time.Minute)
	}
	f.react = func(f *fakeStack, n int, req marketsim.ScenarioRequest) {
		l := episode(n, "demo-2", req)
		f.alerts = append(f.alerts, alertOn(l, "key", "ACC-"+req.Symbol))
		f.labels <- l
	}
	var out bytes.Buffer
	if err := Run(context.Background(), config(f, api, &out, sim.ScenarioWashDirect)); err != nil {
		t.Fatalf("Run: %v\n%s", err, out.String())
	}
	f.settle()
	got := flat(out.String())
	if n := strings.Count(got, "Session demo-1 closes in 2 min of simulated time"); n != 1 {
		t.Errorf("announced the wait %d times, want once:\n%s", n, got)
	}
	if !strings.Contains(got, "Session demo-2 is running.") || !strings.Contains(got, "label demo-2/0001") {
		t.Errorf("did not run in the next session:\n%s", got)
	}
}

func TestRunStartsAStoppedSimulator(t *testing.T) {
	f, api := newFakeStack(t)
	f.status = func(_ int, started bool) marketsim.Status {
		if !started {
			return marketsim.Status{State: "stopped"}
		}
		return running("demo-1", 20*time.Minute)
	}
	f.react = detected
	var out bytes.Buffer
	if err := Run(context.Background(), config(f, api, &out, sim.ScenarioMomentum)); err != nil {
		t.Fatalf("Run: %v\n%s", err, out.String())
	}
	f.settle()
	if !f.started || !strings.Contains(out.String(), "The simulator is stopped. Starting it.") {
		t.Errorf("started = %v:\n%s", f.started, out.String())
	}
}

func TestRunRetriesATriggerRefusedBetweenSessions(t *testing.T) {
	f, api := newFakeStack(t)
	f.conflicts = 2
	f.react = detected
	var out bytes.Buffer
	if err := Run(context.Background(), config(f, api, &out, sim.ScenarioMarkingTheClose)); err != nil {
		t.Fatalf("Run: %v\n%s", err, out.String())
	}
	f.settle()
	if f.conflicts != 0 || len(f.triggers) != 1 {
		t.Errorf("conflicts left %d, accepted triggers %v", f.conflicts, f.triggers)
	}
}

type failingLabels struct{ err error }

func (f failingLabels) Next(context.Context) (*twpb.GroundTruthLabel, error) { return nil, f.err }

func TestRunFailsWhenTheLabelsCannotBeRead(t *testing.T) {
	f, api := newFakeStack(t)
	var out bytes.Buffer
	cfg := config(f, api, &out, sim.ScenarioSpoofing)
	cfg.Labels = failingLabels{errors.New("broker unreachable")}
	if err := Run(context.Background(), cfg); err == nil {
		t.Fatal("Run succeeded without labels")
	}
	f.settle()
	if !strings.Contains(flat(out.String()), "Failed ground-truth labels: broker unreachable") {
		t.Errorf("output:\n%s", out.String())
	}
	if len(f.triggers) != 1 {
		t.Errorf("triggers = %v, want no retry after a failure", f.triggers)
	}
}

func TestRunRejectsUnknownScenarios(t *testing.T) {
	f, api := newFakeStack(t)
	var out bytes.Buffer
	err := Run(context.Background(), config(f, api, &out, "pump.and.dump"))
	f.settle()
	if err == nil || !strings.Contains(err.Error(), `unknown scenario "pump.and.dump"`) {
		t.Fatalf("Run error = %v", err)
	}
	if f.statusCalls != 0 {
		t.Error("called the API before validating the scenarios")
	}
}

func TestMatchingAppliesTheEvaluatorsRule(t *testing.T) {
	l := episode(0, "demo-1", marketsim.ScenarioRequest{Scenario: sim.ScenarioWashRing, Symbol: "AAA"})
	ok := alertOn(l, "ok", "ACC-AAA")
	earlier := alertOn(l, "earlier", "ACC-AAA")
	earlier.DetectedAt = earlier.DetectedAt.Add(-time.Second)
	otherSession := alertOn(l, "other-session", "ACC-AAA")
	otherSession.SessionID = "demo-2"
	otherType := alertOn(l, "other-type", "ACC-AAA")
	otherType.AbuseType = "spoofing"
	beforeStart := alertOn(l, "before-start", "ACC-AAA")
	beforeStart.DetectedAt = time.Unix(0, l.GetStartTimeNs()-1).UTC()
	tooLate := alertOn(l, "too-late", "ACC-AAA")
	tooLate.WindowStart = time.Unix(0, l.GetEndTimeNs()).Add(eval.Grace + time.Nanosecond).UTC()

	got := matching([]live.Summary{ok, otherSession, otherType, beforeStart, tooLate, earlier}, l)
	var keys []string
	for _, a := range got {
		keys = append(keys, a.Key)
	}
	if strings.Join(keys, ",") != "earlier,ok" {
		t.Errorf("matching = %v, want earlier,ok", keys)
	}
}

func TestWrapIndentsContinuationLines(t *testing.T) {
	var b bytes.Buffer
	wrap(&b, "  Why           ", strings.Repeat("evidence ", 30))
	lines := strings.Split(strings.TrimSuffix(b.String(), "\n"), "\n")
	if len(lines) < 3 {
		t.Fatalf("wrapped into %d lines:\n%s", len(lines), b.String())
	}
	for i, line := range lines {
		if len(line) > width {
			t.Errorf("line %d is %d characters long", i, len(line))
		}
		if i > 0 && !strings.HasPrefix(line, strings.Repeat(" ", 16)+"evidence") {
			t.Errorf("line %d is not indented: %q", i, line)
		}
	}
}

func TestHuman(t *testing.T) {
	for d, want := range map[time.Duration]string{
		250 * time.Millisecond: "250 ms",
		2 * time.Second:        "2.0 s",
		90 * time.Second:       "1 min 30 s",
		8 * time.Minute:        "8 min",
	} {
		if got := human(d); got != want {
			t.Errorf("human(%v) = %q, want %q", d, got, want)
		}
	}
}
