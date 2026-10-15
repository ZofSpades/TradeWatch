package bench

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// detectorMetrics is one detector instance's exposition with the given
// counter, gauge and cumulative histogram bucket counts (le 0.1, 0.2, 0.5
// and +Inf).
func detectorMetrics(t *testing.T, events, lag int, buckets [4]int) map[string]*dto.MetricFamily {
	t.Helper()
	text := fmt.Sprintf(`# TYPE tradewatch_detector_events_total counter
tradewatch_detector_events_total %d
# TYPE tradewatch_detector_consumer_lag_records gauge
tradewatch_detector_consumer_lag_records %d
# TYPE tradewatch_detector_event_latency_seconds histogram
tradewatch_detector_event_latency_seconds_bucket{le="0.1"} %d
tradewatch_detector_event_latency_seconds_bucket{le="0.2"} %d
tradewatch_detector_event_latency_seconds_bucket{le="0.5"} %d
tradewatch_detector_event_latency_seconds_bucket{le="+Inf"} %d
tradewatch_detector_event_latency_seconds_sum 1
tradewatch_detector_event_latency_seconds_count %d
# TYPE tradewatch_ingest_records_total counter
tradewatch_ingest_records_total{feed="orders",outcome="accepted"} 5
tradewatch_ingest_records_total{feed="orders",outcome="rejected"} 2
`, events, lag, buckets[0], buckets[1], buckets[2], buckets[3], buckets[3])
	fams, err := parseMetrics(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	return fams
}

func snapshot(instances ...map[string]*dto.MetricFamily) *Snapshot {
	s := &Snapshot{At: time.Now(), Instances: map[string]map[string]map[string]*dto.MetricFamily{"detector": {}}}
	for i, fams := range instances {
		s.Instances["detector"][fmt.Sprintf("10.0.0.%d:8092", i)] = fams
	}
	return s
}

func TestSnapshotSumsAndMergesAcrossInstances(t *testing.T) {
	before := snapshot(detectorMetrics(t, 100, 5, [4]int{1, 2, 3, 3}), detectorMetrics(t, 50, 7, [4]int{0, 1, 1, 1}))
	after := snapshot(detectorMetrics(t, 400, 9, [4]int{11, 22, 33, 33}), detectorMetrics(t, 250, 1, [4]int{10, 21, 21, 21}))

	if got := after.Sum("detector", "tradewatch_detector_events_total", nil) - before.Sum("detector", "tradewatch_detector_events_total", nil); got != 500 {
		t.Errorf("events delta = %v, want 500", got)
	}
	if got := after.Sum("detector", "tradewatch_detector_consumer_lag_records", nil); got != 10 {
		t.Errorf("lag = %v, want 10", got)
	}
	if got := after.Sum("detector", "tradewatch_ingest_records_total", map[string]string{"outcome": "rejected"}); got != 4 {
		t.Errorf("filtered sum = %v, want 4", got)
	}
	d := after.Histogram("detector", "tradewatch_detector_event_latency_seconds").
		Delta(before.Histogram("detector", "tradewatch_detector_event_latency_seconds"))
	if d.Count != 50 || fmt.Sprint(d.Bounds) != "[0.1 0.2 0.5]" || fmt.Sprint(d.Counts) != "[20 40 50]" {
		t.Errorf("delta histogram = %+v", d)
	}
	if after.InstanceCount("detector") != 2 || after.InstanceCount("ingest") != 0 {
		t.Error("instance counts are wrong")
	}
}

// The quantile matches Prometheus's histogram_quantile: linear within the
// bucket, the first bucket starting at zero, and the highest finite bound
// for a rank in the +Inf bucket.
func TestHistogramQuantileMatchesPrometheus(t *testing.T) {
	h := Histogram{Bounds: []float64{0.1, 0.2, 0.5}, Counts: []float64{10, 30, 40}, Count: 40}
	for _, c := range []struct{ q, want float64 }{
		{0.125, 0.05},  // rank 5 in (0, 0.1]
		{0.5, 0.15},    // rank 20 in (0.1, 0.2]
		{0.99, 0.4880}, // rank 39.6 in (0.2, 0.5]
	} {
		if got := h.Quantile(c.q); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("q%.3f = %v, want %v", c.q, got, c.want)
		}
	}
	h.Count = 50 // ten observations above the last finite bound
	if got := h.Quantile(0.99); got != 0.5 {
		t.Errorf("rank in +Inf: %v, want 0.5", got)
	}
	if !math.IsNaN((Histogram{}).Quantile(0.5)) {
		t.Error("an empty histogram has a quantile")
	}
}

func TestExactQuantilesUseNearestRank(t *testing.T) {
	var xs []float64
	for i := 100; i >= 1; i-- {
		xs = append(xs, float64(i))
	}
	q := ExactQuantiles(xs)
	if q.N != 100 || q.P50 != 50 || q.P95 != 95 || q.P99 != 99 || q.Max != 100 {
		t.Errorf("1..100: %+v", q)
	}
	if q := ExactQuantiles(xs[90:]); q.P99 != 10 || q.Max != 10 || q.P50 != 5 {
		t.Errorf("ten values: %+v, want p99 = max = 10, p50 = 5", q)
	}
	if q := ExactQuantiles(nil); q.N != 0 || q.P50.OK() || q.Max.OK() {
		t.Errorf("no values: %+v", q)
	}
}

func TestLagSummary(t *testing.T) {
	var s []lagSample
	for i := 0; i <= 10; i++ {
		s = append(s, lagSample{float64(i), 100 + 2*float64(i)})
	}
	st := summarizeLag(s)
	if st.End != 120 || st.Max != 120 || st.Mean != 110 || math.Abs(st.Slope-2) > 1e-9 {
		t.Errorf("summary = %+v", st)
	}
	if (summarizeLag(nil) != LagStats{}) {
		t.Error("no samples gave a summary")
	}
}

func TestJudge(t *testing.T) {
	step := func(target, published, processed, ingestLag, detectorLag float64) Step {
		s := Step{TargetRate: target, Published: published, Processed: processed,
			Lag: map[string]LagStats{"ingest": {End: ingestLag}, "detector": {End: detectorLag}}}
		s.judge()
		return s
	}
	for _, c := range []struct {
		name string
		s    Step
		want string
	}{
		{"keeps up", step(10_000, 9_990, 9_900, 500, 8_000), "sustained"},
		{"lag of more than a second", step(10_000, 9_990, 9_990, 500, 12_000), "fell behind"},
		{"ingest behind", step(10_000, 9_990, 9_990, 10_500, 0), "fell behind"},
		{"processed too little", step(10_000, 9_990, 9_500, 0, 0), "fell behind"},
		{"slow rates get 1,000 records of slack", step(500, 500, 495, 0, 900), "sustained"},
		{"marketsim cannot offer the rate", step(100_000, 80_000, 79_500, 0, 100), "kept up; marketsim could not offer more"},
	} {
		if got := c.s.Verdict(); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

func TestMissingValuesAreJSONNull(t *testing.T) {
	b, err := json.Marshal(Quantiles{N: 0, P50: Missing(), P95: 0.25, P99: Missing(), Max: Missing()})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"n":0,"p50":null,"p95":0.25,"p99":null,"max":null}` {
		t.Errorf("encoded %s", b)
	}
	var back Quantiles
	if err := json.Unmarshal(b, &back); err != nil || back.P50.OK() || back.P95 != 0.25 {
		t.Errorf("decoded %+v (%v)", back, err)
	}
}

func TestReportStatesTheSustainedRate(t *testing.T) {
	lag := func(ingest, detector float64) map[string]LagStats {
		return map[string]LagStats{"ingest": {End: ingest, Max: ingest}, "detector": {End: detector, Max: detector}, "alertsink": {}}
	}
	ok := Step{TargetRate: 20_000, Published: 19_990, Released: 19_985, Processed: 19_980, Alerts: 120, Lag: lag(800, 3_000),
		EndToEnd:        Quantiles{N: 120, P50: 0.031, P95: 0.08, P99: 0.12, Max: 0.15},
		EventToDetector: Quantiles{N: 1_200_000, P50: 0.004, P95: 0.012, P99: 0.03, Max: Missing()},
		CPU:             map[string]float64{"ingest": 0.9}, RSS: map[string]float64{"ingest": 64 << 20}}
	ok.judge()
	behind := Step{TargetRate: 40_000, Published: 39_900, Processed: 30_000, Lag: lag(1_000, 400_000),
		EndToEnd: ExactQuantiles(nil), EventToDetector: Quantiles{P50: Missing(), P95: Missing(), P99: Missing(), Max: Missing()}}
	behind.judge()
	r := &Result{Started: time.Date(2026, 10, 14, 16, 0, 0, 0, time.UTC), Finished: time.Date(2026, 10, 14, 16, 3, 0, 0, time.UTC),
		Warmup: 20, Measure: 60, Steps: []Step{ok, behind},
		Environment: Environment{CPU: "Test CPU", LogicalCPUs: 16, MemoryBytes: 8 << 30, Kernel: "6.1", Go: "go1.26", Commit: "abc1234"},
		Deployment:  Deployment{Session: "bench-1", DetectorInstances: 2, Partitions: 6, Symbols: 8}}

	md := Markdown(r)
	for _, want := range []string{
		"**Sustained throughput: 19,990 events per second**, published by marketsim and kept up with at the 20k step",
		"stored 31.0 ms after the event that triggered it at the median and 120 ms at the 99th percentile (120 alerts)",
		"At 40,000 events per second the pipeline fell behind. Of 39,900 events per second published, ingest released 0 and the detectors processed 30,000; at the end of the window ingest was 1,000 records behind and the detectors 400,000.",
		"| 20,000 | 19,990 | 19,985 | 19,980 | 800 / 800 | 3,000 / 3,000 | sustained |",
		"| 40,000 | 0 | – | – | – | – | – | – | – |",
		"commit `abc1234`",
		"16 logical CPUs and 8.0 GiB",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q:\n%s", want, md)
		}
	}
	for _, svg := range []string{throughputChart(r), latencyChart(r), lagChart(r)} {
		if !strings.HasPrefix(svg, "<svg") || strings.Contains(svg, "NaN") || strings.Contains(svg, "Inf") {
			t.Errorf("bad chart:\n%s", svg)
		}
	}
}
