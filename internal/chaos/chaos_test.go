package chaos

import (
	"strings"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

func alert(key string, score float64) *twpb.Alert {
	return &twpb.Alert{AlertKey: key, SessionId: "chaos-1", RuleId: "TW-SPOOF-001", Score: score, Symbol: "QZAP"}
}

// Live alerts differ from replayed ones only in wall-clock times, the
// detector instance and the trace; anything else is a difference.
func TestCompare(t *testing.T) {
	live := func(key string, score float64) *twpb.Alert {
		a := alert(key, score)
		a.EmittedAtNs, a.TriggerPublishedAtNs, a.TraceId, a.DetectorInstance = 99, 98, "4bf92f3577b34da6a3ce929d0e0e4736", "det-7"
		return a
	}
	expected := []*twpb.Alert{alert("a", 60), alert("b", 70), alert("c", 80)}
	c := Compare(expected, map[string]*twpb.Alert{"a": live("a", 60), "b": live("b", 71), "x": live("x", 50)})
	if c.Expected != 3 || c.Stored != 3 || strings.Join(c.Missing, ",") != "c" || strings.Join(c.Unexpected, ",") != "x" ||
		strings.Join(c.Differing, ",") != "b" || c.OK() {
		t.Errorf("comparison = %+v", c)
	}
	if c := Compare(expected[:1], map[string]*twpb.Alert{"a": live("a", 60)}); !c.OK() {
		t.Errorf("an alert equal up to wall-clock fields counts as different: %+v", c)
	}
}

func TestParseFaults(t *testing.T) {
	faults, err := ParseFaults(strings.NewReader("2026-10-14T16:00:30Z SIGKILL detector-1\n\n2026-10-14T16:00:50Z detector-1 started again\n"))
	if err != nil || len(faults) != 2 || faults[1].What != "detector-1 started again" ||
		!faults[0].At.Equal(time.Date(2026, 10, 14, 16, 0, 30, 0, time.UTC)) {
		t.Fatalf("faults = %+v, %v", faults, err)
	}
	if _, err := ParseFaults(strings.NewReader("yesterday SIGKILL detector-1\n")); err == nil {
		t.Error("a line without a time was accepted")
	}
}

func TestReport(t *testing.T) {
	at := time.Date(2026, 10, 14, 16, 0, 30, 0, time.UTC)
	r := &Result{
		Session: "chaos-1", Commit: "abc1234", Finished: at.Add(8 * time.Minute), Started: at, Events: 600_123, Labels: 400,
		Faults:       []Fault{{at, "SIGKILL detector-1"}},
		Comparison:   Comparison{Expected: 1234, Stored: 1234, Missing: []string{}, Unexpected: []string{}, Differing: []string{}},
		AlertRecords: 1500, AlertKeys: 1234, Deliveries: 1500, RaisedTwice: []string{}, Passed: true,
		Recovery: []Recovery{{"raw records ingest dropped as duplicates", 20_000}},
	}
	md := Markdown(r)
	for _, want := range []string{
		"**Passed.**", "exactly the 1,234 alerts that an offline replay", "1,500 deliveries of the session's alerts, so 266 duplicate deliveries",
		"| 16:00:30 | SIGKILL detector-1 |", "| Events in the session's recording | 600,123 |", "| raw records ingest dropped as duplicates | 20,000 |",
		"commit `abc1234`",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q:\n%s", want, md)
		}
	}
	r.Passed, r.Comparison.Missing = false, []string{"k1"}
	if md := Markdown(r); !strings.Contains(md, "**Failed.**") || !strings.Contains(md, "Missing: k1") {
		t.Errorf("failed report:\n%s", md)
	}
}
