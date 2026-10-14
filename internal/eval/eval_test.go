package eval

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

const repoRules = "../../rules"

var smallPlan = Plan{
	FirstSeed: 11, Sessions: 1, Clean: 1, Duration: 25 * time.Minute, ClosingWindow: 5 * time.Minute, Workers: 2,
	Injection: sim.Injection{WashTrading: 2, Spoofing: 2, MomentumIgnition: 2, QuoteStuffing: 2, MarkingTheClose: 1},
}

// The bundle runs every rule in one engine. Each rule must raise exactly
// what it raises alone, and a sweep point at the production value must
// reproduce the production rule.
func TestBundleGivesEveryRuleTheResultsItHasAlone(t *testing.T) {
	if testing.Short() {
		t.Skip("simulates two sessions")
	}
	ctx := context.Background()
	b, err := NewBundle(repoRules, repoRules+"/baselines", t.TempDir(), DefaultSweeps)
	if err != nil {
		t.Fatal(err)
	}
	results, err := Run(ctx, smallPlan, b.Set, b.Variants())
	if err != nil {
		t.Fatal(err)
	}
	rep := b.Report(smallPlan, 8, "test", results)

	prod, err := rules.Load(repoRules)
	if err != nil {
		t.Fatal(err)
	}
	alone, err := Run(ctx, smallPlan, prod, []Variant{b.Production})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(alone[0].ByType, rep.Production.ByType) {
		t.Fatalf("production rules alone and in the bundle differ:\nalone  %+v\nbundle %+v", alone[0].ByType, rep.Production.ByType)
	}
	total := 0
	for _, s := range rep.Production.ByType {
		total += s.Alerts + s.Labels
	}
	if total == 0 {
		t.Fatal("no labels or alerts; the test does not exercise detection")
	}
	for _, sw := range rep.Sweeps {
		found := false
		for i, v := range sw.Values {
			if v != sw.Production {
				continue
			}
			found = true
			if got, want := sw.Points[i].stats(sw.Type), rep.Production.stats(sw.Type); !reflect.DeepEqual(got, want) {
				t.Errorf("%s at its production value %s=%s: %+v, production %+v", sw.RuleID, sw.Param, v, got, want)
			}
		}
		if !found {
			t.Errorf("%s: the sweep does not include the production value %s", sw.RuleID, sw.Production)
		}
	}
	if md := rep.Markdown(); !strings.Contains(md, "## Parameter sweeps") || strings.Contains(md, "NaN") {
		t.Error("report rendering")
	}
	if _, err := rep.JSON(); err != nil {
		t.Fatal(err)
	}
}

func TestMatches(t *testing.T) {
	label := &twpb.GroundTruthLabel{SessionId: "S", Symbol: "QZAP", AbuseType: twpb.AbuseType_ABUSE_TYPE_SPOOFING,
		AccountIds: []string{"A", "B"}, StartTimeNs: 100e9, EndTimeNs: 200e9}
	alert := func(mod func(*twpb.Alert)) *twpb.Alert {
		a := &twpb.Alert{SessionId: "S", Symbol: "QZAP", AbuseType: twpb.AbuseType_ABUSE_TYPE_SPOOFING,
			AccountIds: []string{"B"}, WindowStartNs: 150e9, DetectedAtNs: 210e9}
		mod(a)
		return a
	}
	for name, c := range map[string]struct {
		a    *twpb.Alert
		want bool
	}{
		"match":                     {alert(func(*twpb.Alert) {}), true},
		"other account":             {alert(func(a *twpb.Alert) { a.AccountIds = []string{"C"} }), false},
		"other symbol":              {alert(func(a *twpb.Alert) { a.Symbol = "MOSS" }), false},
		"other session":             {alert(func(a *twpb.Alert) { a.SessionId = "T" }), false},
		"other type":                {alert(func(a *twpb.Alert) { a.AbuseType = twpb.AbuseType_ABUSE_TYPE_WASH_TRADING }), false},
		"detected before the start": {alert(func(a *twpb.Alert) { a.DetectedAtNs = 90e9 }), false},
		"window within the grace":   {alert(func(a *twpb.Alert) { a.WindowStartNs = 250e9; a.DetectedAtNs = 260e9 }), true},
		"window after the grace":    {alert(func(a *twpb.Alert) { a.WindowStartNs = 261e9; a.DetectedAtNs = 270e9 }), false},
	} {
		if got := Matches(c.a, label); got != c.want {
			t.Errorf("%s: Matches = %v, want %v", name, got, c.want)
		}
	}
}

func TestWilsonInterval(t *testing.T) {
	for _, c := range []struct {
		k, n           int
		value, lo, hi  float64
		defined, exact bool
	}{
		{0, 10, 0, 0, 0.2775, true, false},
		{10, 10, 1, 0.7225, 1, true, false},
		{5, 10, 0.5, 0.2366, 0.7634, true, false},
		{0, 0, 0, 0, 0, false, true},
	} {
		r := wilson(c.k, c.n)
		if r.Defined() != c.defined || math.Abs(r.Value-c.value) > 1e-4 || math.Abs(r.Lo-c.lo) > 1e-4 || math.Abs(r.Hi-c.hi) > 1e-4 {
			t.Errorf("wilson(%d, %d) = %+v", c.k, c.n, r)
		}
	}
}
