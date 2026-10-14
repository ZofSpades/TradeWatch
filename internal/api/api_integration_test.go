//go:build integration

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/routers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/store"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

// simCall is a request the fake simulator received.
type simCall struct {
	method, path, body string
	authorization      string
}

type apiEnv struct {
	t      *testing.T
	base   string
	st     *store.Store
	views  *live.Views
	router routers.Router

	mu       sync.Mutex
	simCalls []simCall
}

func setup(t *testing.T) *apiEnv {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	url := testutil.PostgresSchema(t)
	if _, err := store.Migrate(ctx, url, quiet); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	rdb := redis.NewClient(&redis.Options{Addr: testutil.RedisAddr(t)})
	prefix := testutil.UniquePrefix(t)
	t.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), prefix+"*").Result()
		if len(keys) > 0 {
			rdb.Del(context.Background(), keys...)
		}
		rdb.Close()
	})
	e := &apiEnv{t: t, st: st, views: live.New(rdb, prefix, 100)}

	sim := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		e.mu.Lock()
		e.simCalls = append(e.simCalls, simCall{r.Method, r.URL.Path, string(body), r.Header.Get("Authorization")})
		e.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"state":"running","session_id":"live-1","events_published":42,"pace":"realtime",`+
			`"injection":{"wash_trading":1,"spoofing":1,"momentum_ignition":1,"quote_stuffing":1,"marking_the_close":0.5}}`)
	}))
	t.Cleanup(sim.Close)

	srv, err := NewServer(st, e.views, mustAuth(t), sim.URL, prometheus.NewRegistry(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	go srv.hub.run(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for !srv.hub.ready.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the live hub did not subscribe")
		}
		time.Sleep(10 * time.Millisecond)
	}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	e.base = hs.URL
	e.useRouter(t)
	return e
}

// useRouter matches requests against the spec, served at the current base.
func (e *apiEnv) useRouter(t *testing.T) {
	t.Helper()
	e.router = specRouter(t, e.base)
}

// do sends a request and checks the response against the OpenAPI document:
// the status code must be documented for the operation, and the body must
// match its schema.
func (e *apiEnv) do(method, path, token string, body any) (int, []byte, http.Header) {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			e.t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.base+path, rd)
	if err != nil {
		e.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	checkResponse(e.t, e.router, req, resp.StatusCode, resp.Header, data)
	return resp.StatusCode, data, resp.Header
}

func (e *apiEnv) login(user, password string) string {
	e.t.Helper()
	code, body, _ := e.do("POST", "/api/v1/auth/login", "", map[string]string{"username": user, "password": password})
	if code != http.StatusOK {
		e.t.Fatalf("login %s: %d %s", user, code, body)
	}
	var resp struct {
		Token string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		e.t.Fatal(err)
	}
	return resp.Token
}

func expect(t *testing.T, what string, got, want int, body []byte) {
	t.Helper()
	if got != want {
		t.Errorf("%s: status %d, want %d: %s", what, got, want, body)
	}
}

func TestAuthenticationAndRoles(t *testing.T) {
	e := setup(t)
	analyst := e.login("analyst", analystPassword)
	admin := e.login("admin", adminPassword)

	code, body, _ := e.do("POST", "/api/v1/auth/login", "", map[string]string{"username": "analyst", "password": "nope"})
	expect(t, "wrong password", code, 401, body)
	code, body, hdr := e.do("GET", "/api/v1/alerts", "", nil)
	expect(t, "no token", code, 401, body)
	if !strings.HasPrefix(hdr.Get("WWW-Authenticate"), "Bearer") {
		t.Errorf("WWW-Authenticate = %q", hdr.Get("WWW-Authenticate"))
	}
	code, body, _ = e.do("GET", "/api/v1/alerts", "not-a-token", nil)
	expect(t, "garbage token", code, 401, body)
	code, body, hdr = e.do("GET", "/api/v1/me", analyst, nil)
	expect(t, "me", code, 200, body)
	if hdr.Get("Cache-Control") != "no-store" || hdr.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("security headers: %v", hdr)
	}
	if !strings.Contains(string(body), `"role":"analyst"`) {
		t.Errorf("me = %s", body)
	}
	code, body, _ = e.do("GET", "/api/v1/sim/status", analyst, nil)
	expect(t, "analyst controlling the simulator", code, 403, body)
	code, body, _ = e.do("GET", "/api/v1/openapi.yaml", "", nil)
	expect(t, "public spec", code, 200, body)

	code, body, _ = e.do("PUT", "/api/v1/sim/injection", admin, map[string]float64{"spoofing": 4})
	expect(t, "admin sets injection", code, 200, body)
	code, body, _ = e.do("GET", "/api/v1/sim/status", admin, nil)
	expect(t, "admin reads status", code, 200, body)
	e.mu.Lock()
	calls := e.simCalls
	e.mu.Unlock()
	if len(calls) != 2 || calls[0].method != "PUT" || calls[0].path != "/api/v1/sim/injection" ||
		calls[0].body != `{"spoofing":4}` || calls[1].path != "/api/v1/sim/status" {
		t.Fatalf("the simulator received %+v", calls)
	}
	for _, c := range calls {
		if c.authorization != "" {
			t.Errorf("the token was forwarded to the simulator: %q", c.authorization)
		}
	}

	for range 5 {
		e.do("POST", "/api/v1/auth/login", "", map[string]string{"username": "admin", "password": "guessing"})
	}
	code, body, hdr = e.do("POST", "/api/v1/auth/login", "", map[string]string{"username": "admin", "password": adminPassword})
	expect(t, "locked username", code, 429, body)
	if hdr.Get("Retry-After") == "" {
		t.Error("no Retry-After on 429")
	}
	code, body, _ = e.do("GET", "/api/v1/no/such/thing", admin, nil)
	expect(t, "unknown endpoint", code, 404, body)
}

func richAlert(i int) *twpb.Alert {
	return &twpb.Alert{
		AlertKey: fmt.Sprintf("%032x", i), SessionId: "live-1", AbuseType: twpb.AbuseType_ABUSE_TYPE_SPOOFING,
		Detector: "spoofing", RuleId: "TW-SPOOF-001", RuleVersion: 1, RuleDigest: "abc", TaxonomyCode: "MAR-SPOOF",
		Severity: twpb.Severity_SEVERITY_HIGH, Score: float64(60 + i), Symbol: "QZAP", AccountIds: []string{"ACC-1"},
		BeneficialOwnerIds: []string{"BO-1"}, WindowStartNs: 1e18, WindowEndNs: 1e18 + 5e9,
		DetectedAtNs: 1e18 + int64(i)*1e9, EmittedAtNs: time.Now().UnixNano(), Explanation: "Account ACC-1 cancelled 3 large buy orders.",
		DetectorInstance: "det-a", TriggerSeq: 99,
		Evidence: &twpb.Evidence{
			OrderIds: []uint64{1, 2, 3}, TradeIds: []uint64{7},
			Features: []*twpb.Feature{
				{Name: "layers", Value: 3, Threshold: 2, Op: ">=", Breached: true, Unit: "orders"},
				{Name: "opposite_fill_qty", Value: 400, Unit: "shares"},
			},
			Timeline: []*twpb.EvidenceEvent{
				{EventTimeNs: 1e18, Kind: twpb.EvidenceKind_EVIDENCE_KIND_ORDER, OrderId: 1, AccountId: "ACC-1",
					Side: twpb.Side_SIDE_BUY, Price: 1_000_000, Quantity: 5000, Role: "layer"},
				{EventTimeNs: 1e18 + 4e9, Seq: 12, Kind: twpb.EvidenceKind_EVIDENCE_KIND_TRADE, TradeId: 7, AccountId: "ACC-1",
					CounterpartyAccountId: "ACC-2", Side: twpb.Side_SIDE_SELL, Price: 1_000_100, Quantity: 400, Role: "genuine_fill"},
			},
			PriceContext: []*twpb.PricePoint{{EventTimeNs: 1e18, LastPrice: 1_000_000, BestBid: 999_900, BestAsk: 1_000_100, BidSize: 800, AskSize: 700}},
		},
	}
}

func TestAlertWorkflowOverHTTP(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	token := e.login("analyst", analystPassword)
	var batch []*twpb.Alert
	for i := range 3 {
		batch = append(batch, richAlert(i))
	}
	raised, _, err := e.st.UpsertAlerts(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.views.Raised(ctx, raised); err != nil {
		t.Fatal(err)
	}
	key := batch[1].GetAlertKey()

	code, body, _ := e.do("GET", "/api/v1/alerts?limit=2", token, nil)
	expect(t, "list", code, 200, body)
	var page struct {
		Alerts []struct{ Key string } `json:"alerts"`
		Next   string                 `json:"next_cursor"`
	}
	if err := json.Unmarshal(body, &page); err != nil || len(page.Alerts) != 2 || page.Next == "" {
		t.Fatalf("first page: %s", body)
	}
	code, body, _ = e.do("GET", "/api/v1/alerts?limit=2&cursor="+page.Next, token, nil)
	expect(t, "second page", code, 200, body)
	code, body, _ = e.do("GET", "/api/v1/alerts?status=bogus", token, nil)
	expect(t, "bad status filter", code, 400, body)
	code, body, _ = e.do("GET", "/api/v1/alerts?cursor=%21%21", token, nil)
	expect(t, "bad cursor", code, 400, body)

	code, body, _ = e.do("GET", "/api/v1/alerts/"+key, token, nil)
	expect(t, "detail", code, 200, body)
	for _, want := range []string{`"threshold":2`, `"kind":"trade"`, `"side":"sell"`, `"best_bid":999900`, `"trigger_seq":99`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("detail lacks %s: %s", want, body)
		}
	}
	code, body, _ = e.do("GET", "/api/v1/alerts/"+strings.Repeat("0", 31)+"f", token, nil)
	expect(t, "unknown alert", code, 404, body)

	code, body, _ = e.do("POST", "/api/v1/alerts/"+key+"/transitions", token, map[string]string{"to": "escalated", "note": "x"})
	expect(t, "escalate an open alert", code, 409, body)
	code, body, _ = e.do("POST", "/api/v1/alerts/"+key+"/transitions", token, map[string]string{"to": "triaged"})
	expect(t, "triage", code, 200, body)
	code, body, _ = e.do("POST", "/api/v1/alerts/"+key+"/transitions", token, map[string]string{"to": "dismissed"})
	expect(t, "dismiss without a reason", code, 422, body)
	code, body, _ = e.do("POST", "/api/v1/alerts/"+key+"/comments", token, map[string]string{"note": "checked the counterparty"})
	expect(t, "comment", code, 201, body)
	code, body, _ = e.do("POST", "/api/v1/alerts/"+key+"/transitions", token, map[string]string{"to": "dismissed", "note": "hedge"})
	expect(t, "dismiss", code, 200, body)
	code, body, _ = e.do("POST", "/api/v1/alerts/"+key+"/transitions", token, map[string]any{"to": "triaged", "extra": 1})
	expect(t, "unknown field", code, 400, body)

	code, body, _ = e.do("GET", "/api/v1/alerts/"+key+"/audit", token, nil)
	expect(t, "audit", code, 200, body)
	var trail struct {
		Entries []struct{ Actor, Action, Note string } `json:"entries"`
	}
	if err := json.Unmarshal(body, &trail); err != nil || len(trail.Entries) != 4 || trail.Entries[1].Actor != "analyst" ||
		trail.Entries[3].Note != "hedge" {
		t.Fatalf("audit: %s", body)
	}

	code, body, _ = e.do("GET", "/api/v1/stats", token, nil)
	expect(t, "stats", code, 200, body)
	if !strings.Contains(string(body), `"dismissed":1`) || !strings.Contains(string(body), `"total":3`) {
		t.Errorf("stats = %s", body)
	}
	code, body, _ = e.do("GET", "/api/v1/live?limit=10", token, nil)
	expect(t, "live", code, 200, body)
	if !strings.Contains(string(body), `"total":3`) {
		t.Errorf("live = %s", body)
	}

	code, body, _ = e.do("GET", "/api/v1/rules", token, nil)
	expect(t, "rules before any load", code, 404, body)
	if err := e.st.RecordRuleSet(ctx, &twpb.RuleSetSnapshot{RulesetDigest: "set-1", LoadedAtNs: time.Now().UnixNano(),
		LoadedBy: "det-a", Rules: []*twpb.RuleDocument{{RuleId: "TW-SPOOF-001", Version: 1, Digest: "abc",
			Detector: "spoofing", Name: "Spoofing", TaxonomyCode: "MAR-SPOOF", Enabled: true, Source: []byte("spec: {}")}}}); err != nil {
		t.Fatal(err)
	}
	code, body, _ = e.do("GET", "/api/v1/rules", token, nil)
	expect(t, "rules", code, 200, body)
	code, body, _ = e.do("GET", "/api/v1/rules/TW-SPOOF-001", token, nil)
	expect(t, "rule history", code, 200, body)
	if !strings.Contains(string(body), `"source":"spec: {}"`) {
		t.Errorf("history = %s", body)
	}
}

func TestStreamDeliversRaisedAndUpdatedAlerts(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	token := e.login("analyst", analystPassword)
	req, err := http.NewRequest("GET", e.base+"/api/v1/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	events := make(chan string, 16)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		var event string
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				events <- event + " " + strings.TrimPrefix(line, "data: ")
			}
		}
	}()

	raised, _, err := e.st.UpsertAlerts(ctx, []*twpb.Alert{richAlert(7)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.views.Raised(ctx, raised); err != nil {
		t.Fatal(err)
	}
	key := richAlert(7).GetAlertKey()
	if code, body, _ := e.do("POST", "/api/v1/alerts/"+key+"/transitions", token, map[string]string{"to": "triaged"}); code != 200 {
		t.Fatalf("triage: %d %s", code, body)
	}
	for _, want := range []string{"alert.raised", "alert.updated"} {
		select {
		case ev := <-events:
			if !strings.HasPrefix(ev, want+" ") || !strings.Contains(ev, key) {
				t.Fatalf("event %q, want %s of %s", ev, want, key)
			}
			if want == "alert.updated" && !strings.Contains(ev, `"status":"triaged"`) {
				t.Errorf("update event %q", ev)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no %s event", want)
		}
	}
}

func TestUnreachableSimulatorIsABadGateway(t *testing.T) {
	e := setup(t)
	srv, err := NewServer(e.st, e.views, mustAuth(t), "http://127.0.0.1:1", prometheus.NewRegistry(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()
	e.base = hs.URL
	e.useRouter(t)
	token := e.login("admin", adminPassword)
	code, body, _ := e.do("GET", "/api/v1/sim/status", token, nil)
	expect(t, "simulator down", code, 502, body)
}
