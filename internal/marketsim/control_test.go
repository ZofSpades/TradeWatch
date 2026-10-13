package marketsim

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZofSpades/tradewatch/internal/metrics"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

func controlServer(t *testing.T) (*Runner, http.Handler) {
	t.Helper()
	reg := prometheus.NewRegistry()
	r := NewRunner(Config{Pace: PaceFast, Injection: sim.Injection{Spoofing: 1}}, nil, reg, slog.New(slog.DiscardHandler))
	srv := metrics.NewServer(":0", metrics.NewRegistry(), slog.New(slog.DiscardHandler))
	r.Register(srv)
	return r, srv.Handler()
}

func call(t *testing.T, h http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestInjectionEndpoint(t *testing.T) {
	r, h := controlServer(t)
	if code, out := call(t, h, "GET", "/api/v1/sim/injection", ""); code != 200 || out["spoofing"] != 1.0 {
		t.Fatalf("GET injection = %d %v", code, out)
	}
	if code, _ := call(t, h, "PUT", "/api/v1/sim/injection", "{"); code != 400 {
		t.Errorf("malformed body: %d", code)
	}
	if code, _ := call(t, h, "PUT", "/api/v1/sim/injection", `{"wash_trading": -2}`); code != 400 {
		t.Errorf("negative rate: %d", code)
	}
	code, out := call(t, h, "PUT", "/api/v1/sim/injection", `{"wash_trading": 2, "marking_the_close": 0.5}`)
	if code != 200 {
		t.Fatalf("valid update: %d %v", code, out)
	}
	if in := r.Status().Injection; in.WashTrading != 2 || in.MarkingTheClose != 0.5 || in.Spoofing != 0 {
		t.Errorf("injection after update = %+v", in)
	}
}

func TestScenarioEndpoint(t *testing.T) {
	_, h := controlServer(t)
	if code, out := call(t, h, "GET", "/api/v1/sim/scenarios", ""); code != 200 || len(out["scenarios"].([]any)) != len(sim.Scenarios) {
		t.Fatalf("GET scenarios = %d %v", code, out)
	}
	if code, _ := call(t, h, "POST", "/api/v1/sim/scenarios", `{"scenario": "nope"}`); code != 400 {
		t.Errorf("unknown scenario: %d", code)
	}
	if code, _ := call(t, h, "POST", "/api/v1/sim/scenarios", `{"scenario": "spoofing.layering", "symbol": "NOPE"}`); code != 400 {
		t.Errorf("unknown symbol: %d", code)
	}
	if code, out := call(t, h, "POST", "/api/v1/sim/scenarios", `{"scenario": "spoofing.layering", "symbol": "QZAP"}`); code != 409 {
		t.Errorf("trigger without a running session: %d %v, want 409", code, out)
	}
}
