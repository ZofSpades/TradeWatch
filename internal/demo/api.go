package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/marketsim"
)

// API is the part of the TradeWatch API the demo uses.
type API struct {
	base  string
	http  *http.Client
	token string
}

// NewAPI returns a client for the API at base, for example the dashboard's
// origin, whose proxy forwards /api to the API.
func NewAPI(base string) *API {
	return &API{base: strings.TrimRight(base, "/"), http: &http.Client{Timeout: 15 * time.Second}}
}

// StatusError is a response with a status other than 2xx.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Code, e.Body) }

// Login signs in and uses the access token for every later call.
func (a *API) Login(ctx context.Context, user, password string) error {
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := a.do(ctx, http.MethodPost, "/api/v1/auth/login", map[string]string{"username": user, "password": password}, &out); err != nil {
		return err
	}
	a.token = out.AccessToken
	return nil
}

// SimStatus returns the simulator's state.
func (a *API) SimStatus(ctx context.Context) (marketsim.Status, error) {
	var st marketsim.Status
	err := a.do(ctx, http.MethodGet, "/api/v1/sim/status", nil, &st)
	return st, err
}

// StartSim starts the simulator. It has no effect if it is running.
func (a *API) StartSim(ctx context.Context) error {
	return a.do(ctx, http.MethodPost, "/api/v1/sim/start", nil, nil)
}

// Trigger starts a labelled scenario on a symbol of the running session.
func (a *API) Trigger(ctx context.Context, scenario, symbol string) error {
	return a.do(ctx, http.MethodPost, "/api/v1/sim/scenarios", marketsim.ScenarioRequest{Scenario: scenario, Symbol: symbol}, nil)
}

// Alerts lists the alerts of one abuse type on a symbol in a session.
func (a *API) Alerts(ctx context.Context, session, symbol, abuseType string) ([]live.Summary, error) {
	q := url.Values{"session": {session}, "symbol": {symbol}, "abuse_type": {abuseType}, "limit": {"100"}}
	var page struct {
		Alerts []live.Summary `json:"alerts"`
	}
	err := a.do(ctx, http.MethodGet, "/api/v1/alerts?"+q.Encode(), nil, &page)
	return page.Alerts, err
}

// TraceURL returns the link to an alert's trace, or "" if it has none.
func (a *API) TraceURL(ctx context.Context, key string) (string, error) {
	var detail struct {
		TraceURL string `json:"trace_url"`
	}
	err := a.do(ctx, http.MethodGet, "/api/v1/alerts/"+url.PathEscape(key), nil, &detail)
	return detail.TraceURL, err
}

func (a *API) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
