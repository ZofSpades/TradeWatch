package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/getkin/kin-openapi/routers"
	"github.com/prometheus/client_golang/prometheus"
)

// fakeProm answers Prometheus query API calls from canned results.
type fakeProm struct {
	mu      sync.Mutex
	queries []string
	fail    bool
}

// vector returns the instant-query result for expr, as JSON.
func (f *fakeProm) vector(expr string) string {
	one := func(v string) string { return `[{"metric":{},"value":[1700000000,"` + v + `"]}]` }
	quantile := map[string]map[string]string{
		"tradewatch_detector_event_latency_seconds":     {"0.5": "0.004", "0.95": "0.012", "0.99": "0.03"},
		"tradewatch_detector_process_seconds":           {"0.5": "0.000005", "0.95": "0.00002", "0.99": "0.00004"},
		"tradewatch_alertsink_pipeline_latency_seconds": {"0.5": "NaN", "0.95": "NaN", "0.99": "NaN"},
	}
	for metric, qs := range quantile {
		for q, v := range qs {
			if strings.HasPrefix(expr, "histogram_quantile("+q+",") && strings.Contains(expr, metric) {
				return one(v)
			}
		}
	}
	switch {
	case strings.Contains(expr, "marketsim_events_published_total"):
		return one("1500.5")
	case strings.Contains(expr, "ingest_events_released_total"):
		return one("1499.25")
	case strings.Contains(expr, "detector_events_total"):
		return one("1498")
	case strings.Contains(expr, "alerts_raised_total"):
		return one("12")
	case strings.Contains(expr, "ingest_consumer_lag_records"):
		return one("3")
	case strings.Contains(expr, "detector_consumer_lag_records"):
		return one("42")
	case strings.Contains(expr, "alertsink_consumer_lag_records"):
		return `[]` // no data yet
	case strings.Contains(expr, "dead_letters_total"):
		return `[
			{"metric":{"job":"ingest","reason":"decode_error"},"value":[1700000000,"12.4"]},
			{"metric":{"job":"ingest","reason":"invalid_field"},"value":[1700000000,"30.6"]},
			{"metric":{"job":"alertsink","reason":"store_rejected"},"value":[1700000000,"1"]},
			{"metric":{"job":"ingest","reason":"unknown_account"},"value":[1700000000,"0.2"]}]`
	case strings.HasPrefix(expr, "up{"):
		var rs []string
		for i, s := range []struct{ job, up string }{
			{"marketsim", "1"}, {"ingest", "1"}, {"detector", "1"}, {"detector", "0"}, {"alertsink", "1"},
		} {
			rs = append(rs, fmt.Sprintf(`{"metric":{"job":%q,"instance":"10.0.0.%d:80"},"value":[1700000000,%q]}`, s.job, i, s.up))
		}
		return "[" + strings.Join(rs, ",") + "]"
	}
	return `[]`
}

func (f *fakeProm) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	expr := q.Get("query")
	f.mu.Lock()
	f.queries = append(f.queries, r.URL.Path+" "+expr)
	fail := f.fail
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if fail {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"status":"error","errorType":"unavailable","error":"TSDB not ready"}`)
		return
	}
	switch r.URL.Path {
	case "/api/v1/query":
		io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":`+f.vector(expr)+`}}`)
	case "/api/v1/query_range":
		// The published rate has data at the first and third steps only.
		result := `[]`
		if strings.Contains(expr, "marketsim_events_published_total") {
			start, _ := strconv.ParseFloat(q.Get("start"), 64)
			result = fmt.Sprintf(`[{"metric":{},"values":[[%.3f,"1000"],[%.3f,"1100"]]}]`, start, start+60)
		}
		io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":`+result+`}}`)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeProm) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queries)
}

type systemEnv struct {
	t      *testing.T
	base   string
	token  string
	router routers.Router
}

func newSystemEnv(t *testing.T, promURL string) *systemEnv {
	t.Helper()
	srv, err := NewServer(nil, nil, mustAuth(t), "http://127.0.0.1:1", prometheus.NewRegistry(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.SetMonitoring(promURL, Links{Grafana: "http://localhost:3001/"}); err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	e := &systemEnv{t: t, base: hs.URL, router: specRouter(t, hs.URL)}
	body, _ := json.Marshal(map[string]string{"username": "analyst", "password": analystPassword})
	resp, err := http.Post(hs.URL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var login struct {
		Token string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&login); err != nil || login.Token == "" {
		t.Fatalf("login: %v", err)
	}
	e.token = login.Token
	return e
}

// get fetches GET /api/v1/system and checks the response against the spec.
func (e *systemEnv) get() (int, []byte) {
	e.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, e.base+"/api/v1/system", nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	checkResponse(e.t, e.router, req, resp.StatusCode, resp.Header, body)
	return resp.StatusCode, body
}

func num(p *float64) string {
	if p == nil {
		return "null"
	}
	return strconv.FormatFloat(*p, 'g', -1, 64)
}

func TestSystemSummarizesThePipelineFromPrometheus(t *testing.T) {
	prom := &fakeProm{}
	ps := httptest.NewServer(prom)
	defer ps.Close()
	e := newSystemEnv(t, ps.URL)

	code, body := e.get()
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, body)
	}
	var st systemStatus
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		got  *float64
		want string
	}{
		{"published", st.Throughput.Published, "1500.5"},
		{"released", st.Throughput.Released, "1499.25"},
		{"processed", st.Throughput.Processed, "1498"},
		{"alerts per minute", st.Throughput.AlertsPerMinute, "12"},
		{"ingest lag", st.ConsumerLag.Ingest, "3"},
		{"detector lag", st.ConsumerLag.Detector, "42"},
		{"alertsink lag without data", st.ConsumerLag.Alertsink, "null"},
		{"detector p50", st.Latency.EventToDetector.P50, "0.004"},
		{"detector p95", st.Latency.EventToDetector.P95, "0.012"},
		{"detector p99", st.Latency.EventToDetector.P99, "0.03"},
		{"processing p99", st.Latency.DetectorProcessing.P99, "4e-05"},
		{"end to end before any alert", st.Latency.EventToStoredAlert.P95, "null"},
	} {
		if got := num(c.got); got != c.want {
			t.Errorf("%s = %s, want %s", c.name, got, c.want)
		}
	}

	// Counts are rounded, empty reasons dropped, and the largest come first.
	wantDL := []deadLetterCount{{"ingest", "invalid_field", 31}, {"ingest", "decode_error", 12}, {"alertsink", "store_rejected", 1}}
	if fmt.Sprint(st.DeadLetters.ByReason) != fmt.Sprint(wantDL) || st.DeadLetters.LastHour != 44 {
		t.Errorf("dead letters = %d %v, want 44 %v", st.DeadLetters.LastHour, st.DeadLetters.ByReason, wantDL)
	}

	wantSvc := []systemService{{"marketsim", 1, 1}, {"ingest", 1, 1}, {"detector", 2, 1}, {"alertsink", 1, 1}, {"api", 0, 0}}
	if fmt.Sprint(st.Services) != fmt.Sprint(wantSvc) {
		t.Errorf("services = %v, want %v", st.Services, wantSvc)
	}

	h := st.History
	if h.StepSeconds != 30 || len(h.Published) != 61 || len(h.Processed) != 61 || len(h.DetectorLag) != 61 || len(h.EventToDetector) != 61 {
		t.Fatalf("history: step %d, lengths %d %d %d %d", h.StepSeconds, len(h.Published), len(h.Processed), len(h.DetectorLag), len(h.EventToDetector))
	}
	if num(h.Published[0]) != "1000" || num(h.Published[1]) != "null" || num(h.Published[2]) != "1100" || num(h.Processed[0]) != "null" {
		t.Errorf("history points misplaced: published %s %s %s, processed %s",
			num(h.Published[0]), num(h.Published[1]), num(h.Published[2]), num(h.Processed[0]))
	}
	if st.Links.Grafana != "http://localhost:3001/d/tradewatch-pipeline" {
		t.Errorf("grafana link = %q", st.Links.Grafana)
	}

	// A second request within the cache period costs Prometheus nothing.
	queried := prom.count()
	if code, _ := e.get(); code != http.StatusOK || prom.count() != queried {
		t.Errorf("second request: status %d, %d more queries", code, prom.count()-queried)
	}
}

func TestSystemIsUnavailableWithoutPrometheus(t *testing.T) {
	e := newSystemEnv(t, "")
	if code, body := e.get(); code != http.StatusServiceUnavailable || !strings.Contains(string(body), "not configured") {
		t.Errorf("without Prometheus: %d %s", code, body)
	}
}

func TestSystemIsUnavailableWhenPrometheusFails(t *testing.T) {
	ps := httptest.NewServer(&fakeProm{fail: true})
	defer ps.Close()
	e := newSystemEnv(t, ps.URL)
	if code, body := e.get(); code != http.StatusServiceUnavailable || strings.Contains(string(body), "TSDB") {
		t.Errorf("Prometheus failing: %d %s (the cause belongs in the log, not the response)", code, body)
	}
}

func TestSetMonitoringRejectsABadURL(t *testing.T) {
	srv, err := NewServer(nil, nil, mustAuth(t), "http://127.0.0.1:1", prometheus.NewRegistry(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.SetMonitoring("prometheus:9090", Links{}); err == nil {
		t.Error("a URL without a scheme was accepted")
	}
}

func TestPromPointParsesSpecialValues(t *testing.T) {
	var pts []promPoint
	if err := json.Unmarshal([]byte(`[[1.5,"NaN"],[2,"+Inf"],[3,"-Inf"],[4,"0.25"]]`), &pts); err != nil {
		t.Fatal(err)
	}
	if len(pts) != 4 || pts[0].t != 1.5 || finite(pts[0].v) != nil || finite(pts[1].v) != nil || finite(pts[2].v) != nil || num(finite(pts[3].v)) != "0.25" {
		t.Errorf("points = %+v", pts)
	}
	if err := json.Unmarshal([]byte(`[[1,"twelve"]]`), &pts); err == nil {
		t.Error("a non-numeric value was accepted")
	}
}
