package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// Config configures a load test.
type Config struct {
	// Rates are the event rates to offer, in events per second, in order.
	Rates []float64
	// Warmup is how long each rate runs before it is measured.
	Warmup time.Duration
	// Measure is the length of each measurement window.
	Measure time.Duration
	// SampleEvery is how often consumer lag is read within a window.
	SampleEvery  time.Duration
	MarketsimURL string
	PostgresURL  string
	Targets      []Target
	Log          *slog.Logger
}

// Step is the measurement of one offered rate.
type Step struct {
	TargetRate float64 `json:"target_rate"`
	Seconds    float64 `json:"seconds"`
	// Events per second over the window, at each stage.
	Published float64 `json:"published_per_second"`
	Released  float64 `json:"released_per_second"`
	Processed float64 `json:"processed_per_second"`
	// Alerts is the number of alerts the session stored in the window.
	Alerts int `json:"alerts_stored"`
	// Lag summarizes each consumer group's lag: ingest, detector, alertsink.
	Lag map[string]LagStats `json:"lag_records"`
	// EndToEnd is exact, from every alert stored in the window: the time
	// from marketsim publishing the triggering event to the alert's insert
	// into Postgres.
	EndToEnd Quantiles `json:"end_to_end_seconds"`
	// EventToIngest and EventToDetector are read from the services'
	// histograms: publication to release by ingest, and to processing by a
	// detector, for every event of the window.
	EventToIngest   Quantiles `json:"event_to_ingest_seconds"`
	EventToDetector Quantiles `json:"event_to_detector_seconds"`
	// CPU is the cores each service used, summed over its instances.
	CPU map[string]float64 `json:"cpu_cores"`
	// RSS is each service's resident memory at the end of the window, in
	// bytes, summed over its instances.
	RSS map[string]float64 `json:"rss_bytes"`
	// KeptUp reports whether the detectors processed what was published
	// without building up lag; SourceLimited whether marketsim could not
	// offer the target rate.
	KeptUp        bool `json:"kept_up"`
	SourceLimited bool `json:"source_limited"`
}

// Verdict describes the step in words.
func (s Step) Verdict() string {
	switch {
	case !s.KeptUp:
		return "fell behind"
	case s.SourceLimited:
		return "kept up; marketsim could not offer more"
	default:
		return "sustained"
	}
}

// judge applies the definitions of keeping up and of the source limit: the
// detectors processed at least 98% of what was published, and neither
// ingest nor the detectors ended the window more than one second of
// traffic behind (at least 1,000 records).
func (s *Step) judge() {
	second := math.Max(s.Published, 1000)
	s.KeptUp = s.Processed >= 0.98*s.Published && s.Lag["detector"].End <= second && s.Lag["ingest"].End <= second
	s.SourceLimited = s.Published < 0.95*s.TargetRate
}

// Deployment is what the run observed of the stack under test.
type Deployment struct {
	Session           string             `json:"session"`
	DetectorInstances int                `json:"detector_instances"`
	Partitions        int                `json:"market_events_partitions"`
	Symbols           int                `json:"symbols"`
	Injection         map[string]float64 `json:"injection_per_symbol_hour"`
}

// Result is a whole run.
type Result struct {
	Started     time.Time   `json:"started"`
	Finished    time.Time   `json:"finished"`
	Warmup      float64     `json:"warmup_seconds"`
	Measure     float64     `json:"measure_seconds"`
	Environment Environment `json:"environment"`
	Deployment  Deployment  `json:"deployment"`
	Steps       []Step      `json:"steps"`
}

// Sustained returns the step with the highest published rate at which the
// pipeline kept up, and whether there is one.
func (r *Result) Sustained() (Step, bool) {
	var best Step
	found := false
	for _, s := range r.Steps {
		if s.KeptUp && (!found || s.Published > best.Published) {
			best, found = s, true
		}
	}
	return best, found
}

type simStatus struct {
	State     string             `json:"state"`
	SessionID string             `json:"session_id"`
	Pace      string             `json:"pace"`
	Symbols   []string           `json:"symbols"`
	Injection map[string]float64 `json:"injection"`
}

type simClient struct {
	base string
	http *http.Client
}

func (c *simClient) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("marketsim %s %s: HTTP %d: %s", method, path, resp.StatusCode, msg)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *simClient) setRate(ctx context.Context, rate float64) error {
	return c.do(ctx, http.MethodPut, "/api/v1/sim/pace", map[string]any{"mode": "rate", "rate": rate}, nil)
}

type runner struct {
	cfg     Config
	sim     *simClient
	pg      *pgx.Conn
	scraper *scraper
	session string
}

// Run offers each rate in turn and measures the pipeline, stopping after
// the first step at which it fell behind or marketsim could not offer the
// rate. It needs marketsim to be running one long session at a fixed rate,
// as deploy/compose/bench.yml configures it.
func Run(ctx context.Context, cfg Config, env Environment) (*Result, error) {
	r := &runner{cfg: cfg, sim: &simClient{base: cfg.MarketsimURL, http: &http.Client{Timeout: 10 * time.Second}},
		scraper: newScraper(cfg.Targets)}
	var st simStatus
	if err := r.sim.do(ctx, http.MethodGet, "/api/v1/sim/status", nil, &st); err != nil {
		return nil, err
	}
	if st.State != "running" || st.Pace != "rate" {
		return nil, fmt.Errorf("marketsim is %s at %s pace; the benchmark needs a running session at a fixed rate", st.State, st.Pace)
	}
	r.session = st.SessionID
	pg, err := pgx.Connect(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	defer pg.Close(context.Background())
	r.pg = pg

	res := &Result{Started: time.Now().UTC(), Warmup: cfg.Warmup.Seconds(), Measure: cfg.Measure.Seconds(), Environment: env,
		Deployment: Deployment{Session: st.SessionID, Symbols: len(st.Symbols), Injection: st.Injection}}
	for _, rate := range cfg.Rates {
		step, snap, err := r.step(ctx, rate)
		if err != nil {
			return res, err
		}
		if res.Deployment.DetectorInstances == 0 {
			res.Deployment.DetectorInstances = snap.InstanceCount("detector")
			res.Deployment.Partitions = int(snap.Sum("detector", "tradewatch_detector_assigned_partitions", nil))
		}
		res.Steps = append(res.Steps, step)
		cfg.Log.Info("step measured", "target", rate, "published", math.Round(step.Published),
			"processed", math.Round(step.Processed), "alerts", step.Alerts,
			"e2e_p99_ms", float64(step.EndToEnd.P99)*1000, "detector_lag_end", step.Lag["detector"].End, "verdict", step.Verdict())
		if !step.KeptUp || step.SourceLimited {
			break
		}
	}
	res.Finished = time.Now().UTC()
	// Leave marketsim at the gentlest rate so the pipeline drains.
	if err := r.sim.setRate(ctx, cfg.Rates[0]); err != nil {
		cfg.Log.Warn("could not slow marketsim down", "err", err)
	}
	return res, nil
}

// lagJobs maps each consumer group's job to its lag gauge.
var lagJobs = map[string]string{
	"ingest":    "tradewatch_ingest_consumer_lag_records",
	"detector":  "tradewatch_detector_consumer_lag_records",
	"alertsink": "tradewatch_alertsink_consumer_lag_records",
}

func (r *runner) step(ctx context.Context, rate float64) (Step, *Snapshot, error) {
	if err := r.sim.setRate(ctx, rate); err != nil {
		return Step{}, nil, err
	}
	r.cfg.Log.Info("offering a new rate", "events_per_second", rate, "warmup", r.cfg.Warmup)
	if err := sleep(ctx, r.cfg.Warmup); err != nil {
		return Step{}, nil, err
	}
	first, err := r.scraper.scrape(ctx)
	if err != nil {
		return Step{}, nil, err
	}
	samples := map[string][]lagSample{}
	record := func(s *Snapshot) {
		t := s.At.Sub(first.At).Seconds()
		for job, gauge := range lagJobs {
			samples[job] = append(samples[job], lagSample{t, s.Sum(job, gauge, nil)})
		}
	}
	record(first)
	last := first
	deadline := first.At.Add(r.cfg.Measure)
	for time.Now().Before(deadline) {
		if err := sleep(ctx, min(r.cfg.SampleEvery, time.Until(deadline))); err != nil {
			return Step{}, nil, err
		}
		s, err := r.scraper.scrape(ctx)
		if err != nil {
			return Step{}, nil, err
		}
		record(s)
		last = s
	}

	dt := last.At.Sub(first.At).Seconds()
	rateOf := func(job, counter string) float64 {
		return (last.Sum(job, counter, nil) - first.Sum(job, counter, nil)) / dt
	}
	st := Step{
		TargetRate: rate, Seconds: dt,
		Published: rateOf("marketsim", "tradewatch_marketsim_events_published_total"),
		Released:  rateOf("ingest", "tradewatch_ingest_events_released_total"),
		Processed: rateOf("detector", "tradewatch_detector_events_total"),
		Lag:       map[string]LagStats{},
		EventToIngest: HistogramQuantiles(last.Histogram("ingest", "tradewatch_ingest_event_latency_seconds").
			Delta(first.Histogram("ingest", "tradewatch_ingest_event_latency_seconds"))),
		EventToDetector: HistogramQuantiles(last.Histogram("detector", "tradewatch_detector_event_latency_seconds").
			Delta(first.Histogram("detector", "tradewatch_detector_event_latency_seconds"))),
		CPU: map[string]float64{}, RSS: map[string]float64{},
	}
	for job, s := range samples {
		st.Lag[job] = summarizeLag(s)
	}
	for _, t := range r.cfg.Targets {
		st.CPU[t.Job] = rateOf(t.Job, "process_cpu_seconds_total")
		st.RSS[t.Job] = last.Sum(t.Job, "process_resident_memory_bytes", nil)
	}
	latencies, err := r.endToEnd(ctx, first.At, last.At)
	if err != nil {
		return Step{}, nil, err
	}
	st.EndToEnd = ExactQuantiles(latencies)
	st.Alerts = len(latencies)
	st.judge()
	return st, last, nil
}

// endToEnd returns, for every alert of the session stored between from and
// to, the time from the triggering event's publication to the alert's
// insert into Postgres. Both clocks are the host's: every container reads
// the same one.
func (r *runner) endToEnd(ctx context.Context, from, to time.Time) ([]float64, error) {
	rows, err := r.pg.Query(ctx,
		`SELECT payload, first_seen_at FROM alerts WHERE session_id = $1 AND first_seen_at >= $2 AND first_seen_at < $3`,
		r.session, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []float64
	for rows.Next() {
		var payload []byte
		var stored time.Time
		if err := rows.Scan(&payload, &stored); err != nil {
			return nil, err
		}
		var a twpb.Alert
		if err := events.Unmarshal(payload, &a); err != nil {
			return nil, err
		}
		if pub := a.GetTriggerPublishedAtNs(); pub > 0 {
			out = append(out, float64(stored.UnixNano()-pub)/1e9)
		}
	}
	return out, rows.Err()
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
