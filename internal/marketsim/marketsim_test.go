package marketsim

import (
	"context"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

func TestPacerRealtimeFollowsSpeed(t *testing.T) {
	p := newPacer(PaceRealtime, 4, 0) // 4 simulated seconds per wall second
	ctx := context.Background()
	start := time.Now()
	base := int64(1_000_000_000_000)
	for i := 0; i <= 20; i++ {
		// 20 steps of 40ms simulated = 800ms simulated = 200ms wall.
		if err := p.wait(ctx, base+int64(i)*int64(40*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	if took := time.Since(start); took < 180*time.Millisecond || took > 600*time.Millisecond {
		t.Errorf("800ms of simulated time at speed 4 took %v, want about 200ms", took)
	}
}

func TestPacerRateLimitsEvents(t *testing.T) {
	p := newPacer(PaceRate, 0, 1_000) // 1,000 events per second
	start := time.Now()
	for i := 0; i < 200; i++ {
		if err := p.wait(context.Background(), 0); err != nil {
			t.Fatal(err)
		}
	}
	if took := time.Since(start); took < 180*time.Millisecond || took > 600*time.Millisecond {
		t.Errorf("200 events at 1000/s took %v, want about 200ms", took)
	}
}

func TestPacerFastNeverWaitsAndStopsOnCancel(t *testing.T) {
	fast := newPacer(PaceFast, 0, 0)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		_ = fast.wait(context.Background(), int64(i)*int64(time.Hour))
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Error("fast pacing slept")
	}

	slow := newPacer(PaceRealtime, 1, 0)
	_ = slow.wait(context.Background(), 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := slow.wait(ctx, int64(time.Hour)); err == nil {
		t.Error("cancelled wait returned no error")
	}
}

func TestLoadConfig(t *testing.T) {
	cfg, err := LoadConfig(config.FromMap(map[string]string{
		"TW_SIM_SEED":             "42",
		"TW_SIM_SESSIONS":         "2",
		"TW_SIM_START":            "2026-03-02T14:30:00Z",
		"TW_SIM_SESSION_DURATION": "30m",
		"TW_SIM_CLOSING_WINDOW":   "3m",
		"TW_SIM_PACE":             "FAST",
		"TW_SIM_SYMBOLS":          "QZAP,TALX",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Seed != 42 || cfg.Sessions != 2 || cfg.Pace != PaceFast || len(cfg.Symbols) != 2 {
		t.Fatalf("cfg = %+v", cfg)
	}
	prefix := cfg.prefix(time.Now())
	if prefix != "seed42" {
		t.Errorf("prefix = %q", prefix)
	}
	s1 := cfg.sessionConfig(1, prefix, time.Now())
	if s1.Seed != 43 || s1.SessionID != "seed42-002" || !s1.Open.Equal(time.Date(2026, 3, 3, 14, 30, 0, 0, time.UTC)) {
		t.Errorf("second session = %+v", s1)
	}
	if s1.Duration != 30*time.Minute || s1.ClosingWindow != 3*time.Minute {
		t.Errorf("session timing = %v / %v", s1.Duration, s1.ClosingWindow)
	}

	for _, bad := range []map[string]string{
		{"TW_SIM_PACE": "warp"},
		{"TW_SIM_PACE": "realtime", "TW_SIM_SPEED": "0"},
		{"TW_SIM_PACE": "rate", "TW_SIM_RATE": "-1"},
		{"TW_SIM_START": "yesterday"},
		{"TW_SIM_SESSIONS": "-1"},
	} {
		if _, err := LoadConfig(config.FromMap(bad)); err == nil {
			t.Errorf("config %v accepted", bad)
		}
	}
}

func TestLivePrefixIsUniquePerStart(t *testing.T) {
	cfg := Config{}
	a := cfg.prefix(time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC))
	b := cfg.prefix(time.Date(2026, 10, 13, 9, 0, 1, 0, time.UTC))
	if a == b || a != "live-20261013T090000Z" {
		t.Errorf("live prefixes %q %q", a, b)
	}
}

func TestRefDataKey(t *testing.T) {
	cases := map[string]*twpb.RefDataRecord{
		"account/ACC-0001": {Record: &twpb.RefDataRecord_Account{Account: &twpb.Account{AccountId: "ACC-0001"}}},
		"instrument/QZAP":  {Record: &twpb.RefDataRecord_Instrument{Instrument: &twpb.Instrument{Symbol: "QZAP"}}},
		"session/S-001":    {Record: &twpb.RefDataRecord_Session{Session: &twpb.TradingSession{SessionId: "S-001"}}},
	}
	for want, rd := range cases {
		if got := RefDataKey(rd); got != want {
			t.Errorf("RefDataKey = %q, want %q", got, want)
		}
	}
}
