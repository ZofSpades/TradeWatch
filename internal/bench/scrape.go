// Package bench load-tests the running pipeline: it drives marketsim at
// increasing event rates and measures, at each rate, the throughput of every
// stage, the consumer lag, and the latency from an event's publication to
// its alert being stored. `make bench` runs it against the compose stack and
// writes the report to docs/results/bench.
package bench

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Target is a service whose instances are scraped: every address its
// compose service name resolves to.
type Target struct {
	Job  string
	Host string
	Port int
}

// DefaultTargets are the compose stack's services.
var DefaultTargets = []Target{
	{"marketsim", "marketsim", 8090},
	{"ingest", "ingest", 8091},
	{"detector", "detector", 8092},
	{"alertsink", "alertsink", 8093},
	{"api", "api", 8094},
}

// Snapshot is one scrape of every instance of every target.
type Snapshot struct {
	At time.Time
	// Instances holds the metric families of each instance, by job and
	// address.
	Instances map[string]map[string]map[string]*dto.MetricFamily
}

// scraper fetches /metrics from service instances.
type scraper struct {
	targets []Target
	http    *http.Client
	resolve func(ctx context.Context, host string) ([]string, error)
}

func newScraper(targets []Target) *scraper {
	return &scraper{
		targets: targets,
		http:    &http.Client{Timeout: 10 * time.Second},
		resolve: net.DefaultResolver.LookupHost,
	}
}

// scrape takes a snapshot of every instance.
func (s *scraper) scrape(ctx context.Context) (*Snapshot, error) {
	snap := &Snapshot{At: time.Now(), Instances: map[string]map[string]map[string]*dto.MetricFamily{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errs []error
	for _, t := range s.targets {
		addrs, err := s.resolve(ctx, t.Host)
		if err != nil {
			return nil, fmt.Errorf("resolve %s: %w", t.Host, err)
		}
		snap.Instances[t.Job] = map[string]map[string]*dto.MetricFamily{}
		for _, a := range addrs {
			addr := net.JoinHostPort(a, strconv.Itoa(t.Port))
			wg.Add(1)
			go func() {
				defer wg.Done()
				fams, err := s.fetch(ctx, "http://"+addr+"/metrics")
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, fmt.Errorf("%s at %s: %w", t.Job, addr, err))
					return
				}
				snap.Instances[t.Job][addr] = fams
			}()
		}
	}
	wg.Wait()
	if len(errs) > 0 {
		return nil, errs[0]
	}
	return snap, nil
}

func (s *scraper) fetch(ctx context.Context, url string) (map[string]*dto.MetricFamily, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	// Ask for the text format, which the parser reads.
	req.Header.Set("Accept", "text/plain; version=0.0.4")
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return parseMetrics(resp.Body)
}

func parseMetrics(r io.Reader) (map[string]*dto.MetricFamily, error) {
	p := expfmt.NewTextParser(model.UTF8Validation)
	return p.TextToMetricFamilies(r)
}

// matches reports whether a series carries every label in want.
func matches(m *dto.Metric, want map[string]string) bool {
	for k, v := range want {
		found := false
		for _, lp := range m.GetLabel() {
			if lp.GetName() == k && lp.GetValue() == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Sum adds up the value of every series of a counter or gauge named name
// across all instances of job, optionally restricted to series with the
// given labels.
func (s *Snapshot) Sum(job, name string, labels map[string]string) float64 {
	total := 0.0
	for _, fams := range s.Instances[job] {
		f := fams[name]
		if f == nil {
			continue
		}
		for _, m := range f.GetMetric() {
			if !matches(m, labels) {
				continue
			}
			switch {
			case m.GetCounter() != nil:
				total += m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				total += m.GetGauge().GetValue()
			case m.GetUntyped() != nil:
				total += m.GetUntyped().GetValue()
			}
		}
	}
	return total
}

// InstanceCount is how many instances of job answered.
func (s *Snapshot) InstanceCount(job string) int { return len(s.Instances[job]) }

// Histogram is a cumulative histogram: Counts[i] observations at or below
// Bounds[i], and Count in total (the +Inf bucket).
type Histogram struct {
	Bounds []float64
	Counts []float64
	Count  float64
	Sum    float64
}

// Histogram merges the histogram named name across every instance of job.
func (s *Snapshot) Histogram(job, name string) Histogram {
	byBound := map[float64]float64{}
	var h Histogram
	for _, fams := range s.Instances[job] {
		f := fams[name]
		if f == nil {
			continue
		}
		for _, m := range f.GetMetric() {
			hist := m.GetHistogram()
			if hist == nil {
				continue
			}
			h.Count += float64(hist.GetSampleCount())
			h.Sum += hist.GetSampleSum()
			for _, b := range hist.GetBucket() {
				if !math.IsInf(b.GetUpperBound(), 1) {
					byBound[b.GetUpperBound()] += float64(b.GetCumulativeCount())
				}
			}
		}
	}
	for b := range byBound {
		h.Bounds = append(h.Bounds, b)
	}
	sort.Float64s(h.Bounds)
	for _, b := range h.Bounds {
		h.Counts = append(h.Counts, byBound[b])
	}
	return h
}

// Delta is the histogram of the observations made between two snapshots.
func (h Histogram) Delta(earlier Histogram) Histogram {
	prev := map[float64]float64{}
	for i, b := range earlier.Bounds {
		prev[b] = earlier.Counts[i]
	}
	d := Histogram{Bounds: h.Bounds, Count: h.Count - earlier.Count, Sum: h.Sum - earlier.Sum}
	for i, b := range h.Bounds {
		d.Counts = append(d.Counts, h.Counts[i]-prev[b])
	}
	return d
}

// Quantile estimates the q-quantile the way Prometheus's histogram_quantile
// does: by linear interpolation within the bucket the rank falls in,
// assuming the first bucket starts at zero. A rank in the +Inf bucket
// returns the highest finite bound. It returns NaN without observations.
func (h Histogram) Quantile(q float64) float64 {
	if h.Count <= 0 || len(h.Bounds) == 0 {
		return math.NaN()
	}
	rank := q * h.Count
	lower, below := 0.0, 0.0
	for i, b := range h.Bounds {
		if h.Counts[i] >= rank {
			inBucket := h.Counts[i] - below
			if inBucket <= 0 {
				return b
			}
			return lower + (b-lower)*(rank-below)/inBucket
		}
		lower, below = b, h.Counts[i]
	}
	return h.Bounds[len(h.Bounds)-1]
}
