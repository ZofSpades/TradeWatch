package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// promClient is a minimal client of the Prometheus HTTP query API, enough
// for the fixed queries behind GET /api/v1/system.
type promClient struct {
	base *url.URL
	http *http.Client
}

func newPromClient(base string) (*promClient, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("prometheus URL %q: scheme must be http or https", base)
	}
	return &promClient{base: u, http: &http.Client{Timeout: 10 * time.Second}}, nil
}

// promSample is one series of an instant query.
type promSample struct {
	labels map[string]string
	value  float64
}

// promSeries is one series of a range query.
type promSeries struct {
	labels map[string]string
	points []promPoint
}

// promPoint is a [unix seconds, "value"] pair as the API encodes it.
type promPoint struct {
	t float64
	v float64
}

func (p *promPoint) UnmarshalJSON(b []byte) error {
	var raw [2]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	var s string
	if err := json.Unmarshal(raw[0], &p.t); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[1], &s); err != nil {
		return err
	}
	v, err := strconv.ParseFloat(s, 64) // also parses NaN and ±Inf
	if err != nil {
		return err
	}
	p.v = v
	return nil
}

type promResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  *promPoint        `json:"value"`
			Values []promPoint       `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func (c *promClient) get(ctx context.Context, path string, params url.Values) (*promResponse, error) {
	u := c.base.JoinPath(path)
	u.RawQuery = params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	var pr promResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("prometheus answered %d with an unreadable body", resp.StatusCode)
	}
	if pr.Status != "success" {
		return nil, fmt.Errorf("prometheus query failed (%s): %s", pr.ErrorType, pr.Error)
	}
	return &pr, nil
}

// query evaluates an instant vector query at t.
func (c *promClient) query(ctx context.Context, expr string, t time.Time) ([]promSample, error) {
	pr, err := c.get(ctx, "/api/v1/query", url.Values{"query": {expr}, "time": {unixSeconds(t)}})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", expr, err)
	}
	if pr.Data.ResultType != "vector" {
		return nil, fmt.Errorf("%s: expected a vector, got %s", expr, pr.Data.ResultType)
	}
	out := make([]promSample, 0, len(pr.Data.Result))
	for _, r := range pr.Data.Result {
		if r.Value != nil {
			out = append(out, promSample{labels: r.Metric, value: r.Value.v})
		}
	}
	return out, nil
}

// queryRange evaluates expr at every step from start to end.
func (c *promClient) queryRange(ctx context.Context, expr string, start, end time.Time, step time.Duration) ([]promSeries, error) {
	pr, err := c.get(ctx, "/api/v1/query_range", url.Values{
		"query": {expr}, "start": {unixSeconds(start)}, "end": {unixSeconds(end)},
		"step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", expr, err)
	}
	if pr.Data.ResultType != "matrix" {
		return nil, fmt.Errorf("%s: expected a matrix, got %s", expr, pr.Data.ResultType)
	}
	out := make([]promSeries, 0, len(pr.Data.Result))
	for _, r := range pr.Data.Result {
		out = append(out, promSeries{labels: r.Metric, points: r.Values})
	}
	return out, nil
}

func unixSeconds(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixMilli())/1000, 'f', 3, 64)
}

// finite returns v, or nil when it is NaN or infinite (for example a
// quantile of a histogram that has no observations), which JSON encodes
// as null.
func finite(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

// scalar returns the value of a query that yields at most one series.
func scalar(samples []promSample) *float64 {
	if len(samples) == 0 {
		return nil
	}
	return finite(samples[0].value)
}
