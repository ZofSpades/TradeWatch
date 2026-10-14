package detect

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

func loadBaselines(t testing.TB) *rules.Set {
	t.Helper()
	set, err := rules.LoadWithTaxonomy(filepath.Join(repoRules, "baselines"), filepath.Join(repoRules, rules.TaxonomyFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(set); err != nil {
		t.Fatal(err)
	}
	return set
}

func TestBaselineRulesLoad(t *testing.T) {
	set := loadBaselines(t)
	if len(set.Enabled()) != 5 {
		t.Fatalf("%d baseline rules, want 5", len(set.Enabled()))
	}
	for _, r := range set.Rules {
		if r.Taxonomy == nil || r.Taxonomy.AbuseType == "" {
			t.Errorf("%s has no abuse type", r.ID)
		}
	}
}

// Each scenario is a legitimate look-alike that the production rule leaves
// alone and its naive baseline flags: the false positives the production
// detectors' extra evidence exists to avoid.
func TestBaselinesFlagWhatTheProductionRulesLeaveAlone(t *testing.T) {
	cases := []struct {
		name, production, baseline string
		scenario                   func(s *stream)
	}{
		{"incidental same-owner cross", "TW-WASH-001", "TW-BASEWASH-001", func(s *stream) {
			s.owner("A1", "BO-X")
			s.owner("A2", "BO-X")
			s.continuous(px-200, px+200, 500)
			passive := s.limit("A1", sell, px+100, 1_000)
			s.advance(30 * time.Second)
			id := s.ioc("A2", buy, px+100, 400)
			s.fill(buy, "A2", id, "A1", passive, px+100, 400, 0, 600)
		}},
		{"market maker re-quoting", "TW-SPOOF-001", "TW-BASESPOOF-001", func(s *stream) {
			s.continuous(px-100, px+100, 500)
			for range 6 {
				entry := s.now
				id := s.limit("MM", buy, px-100, 500)
				s.advance(2 * time.Second)
				s.cancel("MM", id, buy, px-100, 500, entry)
			}
		}},
		{"trader following a move", "TW-MOMO-001", "TW-BASEMOMO-001", func(s *stream) {
			quietHistory(s, 10*time.Minute)
			before := s.limit("MM1", sell, px+100, 100) // the last trade before the news
			s.cross("MM2", buy, "MM1", before, px+100, 100)
			r := s.limit("MM1", sell, px+10_000, 500)
			s.cross("MM2", buy, "MM1", r, px+10_000, 100)
			s.continuous(px+9_900, px+10_100, 500)
			for i := int64(0); i < 4; i++ {
				p := px + 10_100 + i*100
				r := s.limit("MM1", sell, p, 500)
				s.cross("FOLLOW", buy, "MM1", r, p, 500)
				s.advance(time.Second)
			}
		}},
		{"busy account whose orders trade", "TW-STUFF-001", "TW-BASESTUFF-001", func(s *stream) {
			s.continuous(px-100, px+100, 500)
			for range 120 {
				r := s.limit("MM", sell, px+100, 100)
				s.cross("HFT", buy, "MM", r, px+100, 100)
				s.advance(8 * time.Millisecond)
			}
		}},
		{"execution finishing at the close", "TW-CLOSE-001", "TW-BASECLOSE-001", func(s *stream) {
			s.continuous(px-100, px+100, 500)
			for range 60 {
				r := s.limit("MM", sell, px+100, 1_000)
				s.cross("VWAP", buy, "MM", r, px+100, 500)
				s.advance(time.Minute)
			}
			s.book(twpb.TradingPhase_TRADING_PHASE_CLOSING, levels(px-100, 500), levels(px+100, 500))
			for i := int64(1); i <= 5; i++ {
				p := px + 100 + i*1_000
				r := s.limit("MM", sell, p, 600)
				s.cross("VWAP", buy, "MM", r, p, 600)
				s.advance(5 * time.Second)
			}
			s.book(twpb.TradingPhase_TRADING_PHASE_CLOSED, nil, nil)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prod := newStream(t, onlyRules(t, loadRules(t), c.production))
			c.scenario(prod)
			if n := len(prod.alertsOf(c.production)); n != 0 {
				t.Errorf("%s alerted %d times", c.production, n)
			}
			base := newStream(t, onlyRules(t, loadBaselines(t), c.baseline))
			c.scenario(base)
			alerts := base.alertsOf(c.baseline)
			if len(alerts) == 0 {
				t.Errorf("%s did not alert", c.baseline)
			}
			for _, a := range alerts {
				checkWellFormed(t, a)
			}
		})
	}
}
