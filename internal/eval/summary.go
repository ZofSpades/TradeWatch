package eval

import "math"

// TypeSummary is one abuse type's figures, as an evaluation run stores and
// the API serves them.
type TypeSummary struct {
	AbuseType string `json:"abuse_type"`
	Episodes  int    `json:"episodes"`
	Detected  int    `json:"detected"`
	Alerts    int    `json:"alerts"`
	Matched   int    `json:"matched"`
	// The rates are null when undefined: without episodes, without alerts,
	// or with nothing detected.
	Recall           *float64 `json:"recall"`
	Precision        *float64 `json:"precision"`
	F1               *float64 `json:"f1"`
	MedianTTDSeconds *float64 `json:"median_ttd_seconds"`
}

// Summary lists the result's figures per abuse type, in report order. Types
// with neither episodes nor alerts are left out.
func (r *Result) Summary() []TypeSummary {
	out := []TypeSummary{}
	for _, name := range TypeNames() {
		s := r.ByType[name]
		if s == nil || (s.Labels == 0 && s.Alerts == 0) {
			continue
		}
		t := TypeSummary{AbuseType: name, Episodes: s.Labels, Detected: s.Detected, Alerts: s.Alerts, Matched: s.Matched}
		if rc := s.Recall(); rc.Defined() {
			t.Recall = &rc.Value
		}
		if p := s.Precision(); p.Defined() {
			t.Precision = &p.Value
		}
		if f := s.F1(); !math.IsNaN(f) {
			t.F1 = &f
		}
		if m, ok := s.TTDQuantile(0.5); ok {
			t.MedianTTDSeconds = &m
		}
		out = append(out, t)
	}
	return out
}
