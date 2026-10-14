package eval

import (
	"math"
)

// Rate is a proportion k/n with its 95% Wilson score interval.
type Rate struct {
	K, N          int
	Value, Lo, Hi float64
}

// Defined reports whether the rate has a denominator.
func (r Rate) Defined() bool { return r.N > 0 }

// wilson is the proportion k/n with its 95% Wilson score interval, which
// stays inside [0, 1] and behaves for small n and for proportions near 0
// or 1, unlike the normal approximation.
func wilson(k, n int) Rate {
	r := Rate{K: k, N: n}
	if n == 0 {
		return r
	}
	const z = 1.959963984540054
	p, nf := float64(k)/float64(n), float64(n)
	denom := 1 + z*z/nf
	centre := (p + z*z/(2*nf)) / denom
	half := z * math.Sqrt(p*(1-p)/nf+z*z/(4*nf*nf)) / denom
	r.Value, r.Lo, r.Hi = p, math.Max(0, centre-half), math.Min(1, centre+half)
	return r
}

// Recall is the share of injected episodes with at least one matching alert.
func (s *TypeStats) Recall() Rate { return wilson(s.Detected, s.Labels) }

// Precision is the share of alerts on injected sessions that match an
// episode.
func (s *TypeStats) Precision() Rate { return wilson(s.Matched, s.Alerts) }

// F1 is the harmonic mean of precision and recall, or NaN when either is
// undefined.
func (s *TypeStats) F1() float64 {
	p, r := s.Precision(), s.Recall()
	if !p.Defined() || !r.Defined() {
		return math.NaN()
	}
	if p.Value+r.Value == 0 {
		return 0
	}
	return 2 * p.Value * r.Value / (p.Value + r.Value)
}

// TTD returns the q-quantile (nearest rank) of the times to detect, in
// seconds, and false if no episode was detected.
func (s *TypeStats) TTDQuantile(q float64) (float64, bool) {
	if len(s.TTD) == 0 {
		return 0, false
	}
	i := int(math.Ceil(q*float64(len(s.TTD)))) - 1
	return s.TTD[max(0, min(i, len(s.TTD)-1))], true
}

// CleanPer100 is the number of alerts per 100 symbol-hours of trading
// without injected abuse, or NaN without clean sessions.
func (s *TypeStats) CleanPer100() float64 {
	if s.CleanSymbolHours == 0 {
		return math.NaN()
	}
	return 100 * float64(s.CleanAlerts) / s.CleanSymbolHours
}
