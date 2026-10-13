package rules

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// repoRules is the rules directory shipped with the repository.
const repoRules = "../../rules"

// copyRules copies the repository rules into a temporary directory.
func copyRules(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	entries, err := os.ReadDir(repoRules)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(repoRules, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func edit(t *testing.T, dir, file, old, new string) {
	t.Helper()
	p := filepath.Join(dir, file)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), old) {
		t.Fatalf("%s does not contain %q", file, old)
	}
	if err := os.WriteFile(p, []byte(strings.Replace(string(b), old, new, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadRepositoryRules(t *testing.T) {
	set, err := Load(repoRules)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"TW-WASH-001":  "wash_trading",
		"TW-SPOOF-001": "spoofing",
		"TW-MOMO-001":  "momentum_ignition",
		"TW-STUFF-001": "quote_stuffing",
		"TW-CLOSE-001": "marking_the_close",
	}
	if len(set.Rules) != len(want) || len(set.Enabled()) != len(want) {
		t.Fatalf("loaded %d rules (%d enabled), want %d", len(set.Rules), len(set.Enabled()), len(want))
	}
	for id, det := range want {
		r, ok := set.Rule(id)
		if !ok || r.Detector != det {
			t.Fatalf("rule %s: %+v", id, r)
		}
		if r.Taxonomy == nil || r.Taxonomy.Proto() == 0 || len(r.Taxonomy.References) == 0 {
			t.Errorf("rule %s has no resolved taxonomy", id)
		}
		if r.Version < 1 || r.SuppressFor <= 0 || len(r.Digest) != 64 || r.Explanation == nil {
			t.Errorf("rule %s incomplete: %+v", id, r)
		}
	}
	again, err := Load(repoRules)
	if err != nil {
		t.Fatal(err)
	}
	if set.Digest != again.Digest {
		t.Error("set digest is not stable across loads")
	}
	snap := set.Snapshot("test")
	if snap.GetRulesetDigest() != set.Digest || len(snap.GetRules()) != len(want) || len(snap.GetRules()[0].GetSource()) == 0 {
		t.Errorf("snapshot = %v", snap)
	}
}

func TestDecodeParamsIsStrict(t *testing.T) {
	set, err := Load(repoRules)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := set.Rule("TW-WASH-001")
	var p struct {
		Window       Duration `yaml:"window"`
		MaxEntryGap  Duration `yaml:"max_entry_gap"`
		MinSizeMatch float64  `yaml:"min_size_match"`
	}
	if err := r.DecodeParams(&p); err == nil {
		t.Fatal("decoding into a struct missing fields must fail")
	}
	var full struct {
		Window         Duration `yaml:"window"`
		MaxEntryGap    Duration `yaml:"max_entry_gap"`
		MinSizeMatch   float64  `yaml:"min_size_match"`
		MinRepeat      int      `yaml:"min_repeat"`
		MinRingLength  int      `yaml:"min_ring_length"`
		MaxRingLength  int      `yaml:"max_ring_length"`
		MinRingBalance float64  `yaml:"min_ring_balance"`
	}
	if err := r.DecodeParams(&full); err != nil {
		t.Fatal(err)
	}
	if full.Window.D() != 10*time.Minute || full.MaxEntryGap.D() != time.Second || full.MinRingLength != 3 {
		t.Errorf("params = %+v", full)
	}
}

func TestLoadRejectsInvalidRules(t *testing.T) {
	cases := []struct {
		name, file, old, new, want string
	}{
		{"unknown top-level field", "spoofing.yaml", "kind: DetectionRule", "kind: DetectionRule\nextra: 1", "schema"},
		{"bad rule id", "spoofing.yaml", "id: TW-SPOOF-001", "id: spoof-1", "schema"},
		{"unknown detector", "spoofing.yaml", "detector: spoofing", "detector: astrology", "schema"},
		{"unknown param", "spoofing.yaml", "min_layers: 2", "min_layers: 2\n    min_vibes: 3", "schema"},
		{"missing param", "spoofing.yaml", "    min_layers: 2\n", "", "schema"},
		{"ratio above one", "spoofing.yaml", "min_cancel_ratio: 0.8", "min_cancel_ratio: 1.8", "schema"},
		{"bad duration", "spoofing.yaml", "window: 5m", "window: five minutes", "schema"},
		{"zero version", "spoofing.yaml", "version: 1", "version: 0", "schema"},
		{"unknown taxonomy code", "spoofing.yaml", "taxonomy: MAR-SPOOF", "taxonomy: MAR-NOPE", "taxonomy"},
		{"broken template", "spoofing.yaml", "Account {{index .Accounts 0}}", "Account {{index .Accounts 0", "template"},
		{"duplicate rule id", "quote_stuffing.yaml", "id: TW-STUFF-001", "id: TW-SPOOF-001", "also defined"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := copyRules(t)
			edit(t, dir, tc.file, tc.old, tc.new)
			_, err := Load(dir)
			if err == nil {
				t.Fatal("invalid rule set loaded")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestLoadRejectsBadTaxonomy(t *testing.T) {
	dir := copyRules(t)
	edit(t, dir, TaxonomyFile, "abuse_type: spoofing", "abuse_type: rumour")
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "unknown abuse type") {
		t.Fatalf("got %v", err)
	}
}

func TestWatcherReloadsAndKeepsLastGoodSet(t *testing.T) {
	dir := copyRules(t)
	var mu sync.Mutex
	var loaded []*Set
	var failures []error
	w := &Watcher{
		Dir: dir, Interval: 20 * time.Millisecond,
		OnLoad:  func(s *Set) { mu.Lock(); loaded = append(loaded, s); mu.Unlock() },
		OnError: func(err error) { mu.Lock(); failures = append(failures, err); mu.Unlock() },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	time.Sleep(60 * time.Millisecond) // let the watcher take its baseline

	waitFor := func(cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			mu.Lock()
			ok := cond()
			mu.Unlock()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("watcher did not react")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	edit(t, dir, "spoofing.yaml", "version: 1", "version: 2")
	waitFor(func() bool { return len(loaded) == 1 })
	mu.Lock()
	r, _ := loaded[0].Rule("TW-SPOOF-001")
	mu.Unlock()
	if r.Version != 2 {
		t.Fatalf("reloaded version %d", r.Version)
	}

	edit(t, dir, "spoofing.yaml", "min_layers: 2", "min_layers: zero")
	waitFor(func() bool { return len(failures) == 1 })
	mu.Lock()
	n := len(loaded)
	mu.Unlock()
	if n != 1 {
		t.Fatal("an invalid rule set was delivered")
	}
}

func TestDirDigestTracksContent(t *testing.T) {
	dir := copyRules(t)
	a, err := DirDigest(dir)
	if err != nil {
		t.Fatal(err)
	}
	edit(t, dir, "wash_trading.yaml", "min_repeat: 3", "min_repeat: 4")
	b, _ := DirDigest(dir)
	if a == b {
		t.Fatal("digest did not change with content")
	}
}
