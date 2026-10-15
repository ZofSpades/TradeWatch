package eval

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestSummaryLeavesUndefinedRatesNull(t *testing.T) {
	r := &Result{ByType: map[string]*TypeStats{
		"wash_trading":   {Labels: 4, Detected: 3, Alerts: 5, Matched: 4, TTD: []float64{1, 2, 9}},
		"spoofing":       {Labels: 2},
		"quote_stuffing": {},
	}}
	got := r.Summary()
	if len(got) != 2 || got[0].AbuseType != "wash_trading" || got[1].AbuseType != "spoofing" {
		t.Fatalf("summary %+v, want wash trading then spoofing", got)
	}
	w := got[0]
	if *w.Recall != 0.75 || *w.Precision != 0.8 || *w.MedianTTDSeconds != 2 || math.Abs(*w.F1-1.2/1.55) > 1e-12 {
		t.Errorf("wash trading: recall %v precision %v f1 %v median %v", *w.Recall, *w.Precision, *w.F1, *w.MedianTTDSeconds)
	}
	s := got[1]
	if s.Recall == nil || *s.Recall != 0 || s.Precision != nil || s.F1 != nil || s.MedianTTDSeconds != nil {
		t.Errorf("spoofing without alerts: %+v", s)
	}
	js, err := json.Marshal(s)
	if err != nil || !strings.Contains(string(js), `"precision":null`) {
		t.Errorf("JSON %s (%v)", js, err)
	}
}
