package bench

import (
	"encoding/json"
	"math"
	"sort"
)

// Value is a measurement that may be missing (NaN); JSON encodes a missing
// value as null.
type Value float64

// Missing is the value of a measurement that could not be taken.
func Missing() Value { return Value(math.NaN()) }

// OK reports whether the value was measured.
func (v Value) OK() bool { return !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) }

func (v Value) MarshalJSON() ([]byte, error) {
	if !v.OK() {
		return []byte("null"), nil
	}
	return json.Marshal(float64(v))
}

func (v *Value) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*v = Missing()
		return nil
	}
	var f float64
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	*v = Value(f)
	return nil
}

// Quantiles summarizes a latency distribution, in seconds.
type Quantiles struct {
	// N is the number of observations; zero means none were made.
	N   int   `json:"n"`
	P50 Value `json:"p50"`
	P95 Value `json:"p95"`
	P99 Value `json:"p99"`
	Max Value `json:"max"`
}

// ExactQuantiles uses the nearest-rank method: the q-quantile of n sorted
// observations is the one at rank ceil(q*n). With fewer than 100
// observations the 99th percentile is therefore the largest.
func ExactQuantiles(xs []float64) Quantiles {
	if len(xs) == 0 {
		return Quantiles{P50: Missing(), P95: Missing(), P99: Missing(), Max: Missing()}
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	rank := func(q float64) Value {
		i := int(math.Ceil(q*float64(len(s)))) - 1
		return Value(s[max(i, 0)])
	}
	return Quantiles{N: len(s), P50: rank(0.5), P95: rank(0.95), P99: rank(0.99), Max: Value(s[len(s)-1])}
}

// HistogramQuantiles reads the quantiles of a histogram of the
// observations of one window. The maximum is not known from a histogram.
func HistogramQuantiles(h Histogram) Quantiles {
	return Quantiles{
		N: int(math.Round(h.Count)), P50: Value(h.Quantile(0.5)), P95: Value(h.Quantile(0.95)), P99: Value(h.Quantile(0.99)),
		Max: Missing(),
	}
}

// LagStats summarizes the samples of one consumer group's lag over a
// window, in records.
type LagStats struct {
	End  float64 `json:"end"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
	// Slope is the least-squares trend, in records per second: positive
	// when the group keeps falling further behind.
	Slope float64 `json:"slope_per_second"`
}

// lagSample is one reading of a lag, t seconds into the window.
type lagSample struct{ t, v float64 }

func summarizeLag(samples []lagSample) LagStats {
	if len(samples) == 0 {
		return LagStats{}
	}
	var st LagStats
	var sumT, sumV, sumTT, sumTV float64
	for _, s := range samples {
		st.Max = max(st.Max, s.v)
		sumT += s.t
		sumV += s.v
		sumTT += s.t * s.t
		sumTV += s.t * s.v
	}
	n := float64(len(samples))
	st.End = samples[len(samples)-1].v
	st.Mean = sumV / n
	if d := n*sumTT - sumT*sumT; d > 0 {
		st.Slope = (n*sumTV - sumT*sumV) / d
	}
	return st
}
