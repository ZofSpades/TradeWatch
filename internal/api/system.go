package api

import (
	"cmp"
	"context"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

// Links are the URLs of the monitoring tools the dashboard links to, as a
// browser reaches them.
type Links struct {
	// Grafana is Grafana's base URL.
	Grafana string
	// Traces is the Jaeger UI's base URL.
	Traces string
}

// SetMonitoring connects GET /api/v1/system to the Prometheus server at
// promURL and sets the links handed to the dashboard. Without a Prometheus
// URL the endpoint answers 503.
func (s *Server) SetMonitoring(promURL string, links Links) error {
	if promURL != "" {
		c, err := newPromClient(promURL)
		if err != nil {
			return err
		}
		s.prom = c
	}
	s.links = links
	return nil
}

// systemStatus is the body of GET /api/v1/system. Rates and quantiles are
// null when Prometheus has no data for them, for example a latency
// quantile before the first alert.
type systemStatus struct {
	GeneratedAt time.Time         `json:"generated_at"`
	Throughput  systemThroughput  `json:"throughput"`
	ConsumerLag systemLag         `json:"consumer_lag"`
	Latency     systemLatency     `json:"latency"`
	DeadLetters systemDeadLetters `json:"dead_letters"`
	Services    []systemService   `json:"services"`
	History     systemHistory     `json:"history"`
	Links       systemLinks       `json:"links"`
}

type systemThroughput struct {
	Published       *float64 `json:"published"`
	Released        *float64 `json:"released"`
	Processed       *float64 `json:"processed"`
	AlertsPerMinute *float64 `json:"alerts_per_minute"`
}

type systemLag struct {
	Ingest    *float64 `json:"ingest"`
	Detector  *float64 `json:"detector"`
	Alertsink *float64 `json:"alertsink"`
}

type quantiles struct {
	P50 *float64 `json:"p50"`
	P95 *float64 `json:"p95"`
	P99 *float64 `json:"p99"`
}

type systemLatency struct {
	EventToDetector    quantiles `json:"event_to_detector"`
	DetectorProcessing quantiles `json:"detector_processing"`
	EventToStoredAlert quantiles `json:"event_to_stored_alert"`
}

type deadLetterCount struct {
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
	Count  int64  `json:"count"`
}

type systemDeadLetters struct {
	LastHour int64             `json:"last_hour"`
	ByReason []deadLetterCount `json:"by_reason"`
}

type systemService struct {
	Name      string `json:"name"`
	Instances int    `json:"instances"`
	Up        int    `json:"up"`
}

type systemHistory struct {
	Start           time.Time  `json:"start"`
	StepSeconds     int        `json:"step_seconds"`
	Published       []*float64 `json:"published"`
	Processed       []*float64 `json:"processed"`
	DetectorLag     []*float64 `json:"detector_lag"`
	EventToDetector []*float64 `json:"event_to_detector_p95"`
}

type systemLinks struct {
	Grafana string `json:"grafana,omitempty"`
	Traces  string `json:"traces,omitempty"`
}

// services lists the pipeline's services in data-flow order.
var services = []string{"marketsim", "ingest", "detector", "alertsink", "api"}

const (
	historySpan = 30 * time.Minute
	historyStep = 30 * time.Second
	// systemTTL is how long a collected status is served from memory, so
	// that many open dashboards cost Prometheus one set of queries.
	systemTTL = 5 * time.Second
)

func latencyQuery(q, metric, window string) string {
	return "histogram_quantile(" + q + ", sum by (le) (rate(" + metric + "_bucket[" + window + "])))"
}

// systemCache serves a recent status and collapses concurrent collections.
type systemCache struct {
	mu     sync.Mutex
	at     time.Time
	status *systemStatus
	group  singleflight.Group
}

func (s *Server) system(w http.ResponseWriter, r *http.Request) {
	if s.prom == nil {
		problem(w, http.StatusServiceUnavailable, "metrics are not configured (TW_API_PROMETHEUS_URL)")
		return
	}
	s.sysCache.mu.Lock()
	cached, at := s.sysCache.status, s.sysCache.at
	s.sysCache.mu.Unlock()
	if cached != nil && time.Since(at) < systemTTL {
		writeJSON(w, http.StatusOK, cached)
		return
	}
	v, err, _ := s.sysCache.group.Do("status", func() (any, error) {
		// Detached from the request's cancellation, since others may wait
		// for the same result, but inside its trace.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Second)
		defer cancel()
		st, err := s.collectSystem(ctx, time.Now())
		if err == nil {
			s.sysCache.mu.Lock()
			s.sysCache.status, s.sysCache.at = st, time.Now()
			s.sysCache.mu.Unlock()
		}
		return st, err
	})
	if err != nil {
		s.log.Warn("querying Prometheus failed", "err", err)
		problem(w, http.StatusServiceUnavailable, "metrics are unavailable: Prometheus could not be queried")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// collectSystem queries Prometheus for the pipeline's status at now.
func (s *Server) collectSystem(ctx context.Context, now time.Time) (*systemStatus, error) {
	now = now.Truncate(time.Second)
	st := &systemStatus{GeneratedAt: now.UTC(), Links: systemLinks{Grafana: s.dashboardLink(), Traces: s.tracesLink()}}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	instant := func(expr string, into func([]promSample)) {
		g.Go(func() error {
			samples, err := s.prom.query(gctx, expr, now)
			if err == nil {
				into(samples)
			}
			return err
		})
	}
	var mu sync.Mutex // guards st while queries complete
	set := func(dst **float64) func([]promSample) {
		return func(samples []promSample) {
			mu.Lock()
			*dst = scalar(samples)
			mu.Unlock()
		}
	}

	instant("sum(rate(tradewatch_marketsim_events_published_total[1m]))", set(&st.Throughput.Published))
	instant("sum(rate(tradewatch_ingest_events_released_total[1m]))", set(&st.Throughput.Released))
	instant("sum(rate(tradewatch_detector_events_total[1m]))", set(&st.Throughput.Processed))
	instant("sum(rate(tradewatch_alertsink_alerts_raised_total[5m])) * 60", set(&st.Throughput.AlertsPerMinute))
	instant("sum(tradewatch_ingest_consumer_lag_records)", set(&st.ConsumerLag.Ingest))
	instant("sum(tradewatch_detector_consumer_lag_records)", set(&st.ConsumerLag.Detector))
	instant("sum(tradewatch_alertsink_consumer_lag_records)", set(&st.ConsumerLag.Alertsink))
	for _, l := range []struct {
		q    *quantiles
		name string
		win  string
	}{
		{&st.Latency.EventToDetector, "tradewatch_detector_event_latency_seconds", "5m"},
		{&st.Latency.DetectorProcessing, "tradewatch_detector_process_seconds", "5m"},
		// Alerts are rare, so their window is longer.
		{&st.Latency.EventToStoredAlert, "tradewatch_alertsink_pipeline_latency_seconds", "15m"},
	} {
		instant(latencyQuery("0.5", l.name, l.win), set(&l.q.P50))
		instant(latencyQuery("0.95", l.name, l.win), set(&l.q.P95))
		instant(latencyQuery("0.99", l.name, l.win), set(&l.q.P99))
	}
	instant(`sum by (job, reason) (increase(tradewatch_ingest_dead_letters_total[1h]) or increase(tradewatch_alertsink_dead_letters_total[1h]))`,
		func(samples []promSample) {
			mu.Lock()
			defer mu.Unlock()
			st.DeadLetters.ByReason = []deadLetterCount{}
			for _, smp := range samples {
				// increase() extrapolates to the window's edges; counts of
				// records are whole numbers.
				n := int64(math.Round(smp.value))
				if n <= 0 || math.IsNaN(smp.value) {
					continue
				}
				st.DeadLetters.ByReason = append(st.DeadLetters.ByReason,
					deadLetterCount{Stage: smp.labels["job"], Reason: smp.labels["reason"], Count: n})
				st.DeadLetters.LastHour += n
			}
			slices.SortFunc(st.DeadLetters.ByReason, func(a, b deadLetterCount) int {
				return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.Stage, b.Stage), cmp.Compare(a.Reason, b.Reason))
			})
		})
	instant(`up{job=~"`+strings.Join(services, "|")+`"}`, func(samples []promSample) {
		byJob := map[string]*systemService{}
		for _, name := range services {
			byJob[name] = &systemService{Name: name}
		}
		for _, smp := range samples {
			if svc := byJob[smp.labels["job"]]; svc != nil {
				svc.Instances++
				if smp.value == 1 {
					svc.Up++
				}
			}
		}
		mu.Lock()
		defer mu.Unlock()
		for _, name := range services {
			st.Services = append(st.Services, *byJob[name])
		}
	})

	start := now.Add(-historySpan)
	st.History.Start = start.UTC()
	st.History.StepSeconds = int(historyStep / time.Second)
	points := int(historySpan/historyStep) + 1
	ranged := func(expr string, dst *[]*float64) {
		g.Go(func() error {
			series, err := s.prom.queryRange(gctx, expr, start, now, historyStep)
			if err != nil {
				return err
			}
			values := make([]*float64, points)
			if len(series) > 0 {
				for _, p := range series[0].points {
					i := int(math.Round((p.t - float64(start.Unix())) / historyStep.Seconds()))
					if i >= 0 && i < points {
						values[i] = finite(p.v)
					}
				}
			}
			mu.Lock()
			*dst = values
			mu.Unlock()
			return nil
		})
	}
	ranged("sum(rate(tradewatch_marketsim_events_published_total[1m]))", &st.History.Published)
	ranged("sum(rate(tradewatch_detector_events_total[1m]))", &st.History.Processed)
	ranged("sum(tradewatch_detector_consumer_lag_records)", &st.History.DetectorLag)
	ranged(latencyQuery("0.95", "tradewatch_detector_event_latency_seconds", "1m"), &st.History.EventToDetector)

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return st, nil
}

// dashboardLink is the URL of the pipeline dashboard in Grafana.
func (s *Server) dashboardLink() string {
	if s.links.Grafana == "" {
		return ""
	}
	return strings.TrimSuffix(s.links.Grafana, "/") + "/d/tradewatch-pipeline"
}

// tracesLink is the Jaeger UI's search page.
func (s *Server) tracesLink() string {
	if s.links.Traces == "" {
		return ""
	}
	return strings.TrimSuffix(s.links.Traces, "/") + "/search"
}

// traceLink is the Jaeger UI's page for a trace.
func (s *Server) traceLink(traceID string) string {
	if s.links.Traces == "" || traceID == "" {
		return ""
	}
	return strings.TrimSuffix(s.links.Traces, "/") + "/trace/" + traceID
}
