package eval

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ZofSpades/tradewatch/internal/rules"
)

// Sweep varies one parameter of one production rule.
type Sweep struct {
	RuleID string
	Param  string
	// Values as they would be written in the rule file, such as "500ms".
	Values []string
}

// DefaultSweeps vary, for each detector, the threshold that trades
// precision against recall most directly.
var DefaultSweeps = []Sweep{
	{"TW-WASH-001", "max_entry_gap", []string{"100ms", "250ms", "500ms", "1s", "2s", "5s", "10s"}},
	{"TW-SPOOF-001", "min_size_vs_depth", []string{"1", "1.5", "2", "3", "4", "6", "8"}},
	{"TW-MOMO-001", "min_move_bps", []string{"10", "15", "20", "30", "40", "60", "80"}},
	{"TW-STUFF-001", "min_rate_multiple", []string{"2", "3", "5", "10", "20", "40"}},
	{"TW-CLOSE-001", "min_volume_share", []string{"0.05", "0.1", "0.15", "0.2", "0.3", "0.4"}},
}

// setLine rewrites the single line "<indent>key: <value>" of a YAML source.
func setLine(src, key, value string) (string, error) {
	re := regexp.MustCompile(`(?m)^(\s+)` + regexp.QuoteMeta(key) + `:[ \t]*\S+[ \t]*$`)
	if n := len(re.FindAllStringIndex(src, -1)); n != 1 {
		return "", fmt.Errorf("eval: %s appears %d times in the rule", key, n)
	}
	return re.ReplaceAllString(src, "${1}"+key+": "+value), nil
}

// currentParam returns a parameter's value in a rule's YAML source.
func currentParam(src, param string) string {
	re := regexp.MustCompile(`(?m)^\s+` + regexp.QuoteMeta(param) + `:[ \t]*(\S+)[ \t]*$`)
	if m := re.FindStringSubmatch(src); m != nil {
		return m[1]
	}
	return ""
}

// Bundle is one rule set holding the production rules, the baselines, and
// a copy of each swept rule per sweep value under an ID of its own
// (TW-MOMO-101, TW-MOMO-102, ...). Rules in one engine share nothing but
// the market view, which is deterministic, so every rule raises in the
// bundle exactly the alerts it would raise alone, while each session needs
// one engine instead of one per variant.
type Bundle struct {
	Set             *rules.Set
	RulesDigest     string
	BaselinesDigest string
	Production      Variant
	Baseline        Variant
	// Sweeps carry one variant per value; their Points are filled by Score.
	Sweeps        []SweepResult
	SweepVariants [][]Variant
}

// NewBundle assembles the bundle in a directory under tmp. The baselines
// use the production taxonomy.
func NewBundle(rulesDir, baselineDir, tmp string, sweeps []Sweep) (*Bundle, error) {
	prod, err := rules.Load(rulesDir)
	if err != nil {
		return nil, err
	}
	base, err := rules.LoadWithTaxonomy(baselineDir, filepath.Join(rulesDir, rules.TaxonomyFile))
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(tmp, "bundle")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	write := func(name string, data []byte) error { return os.WriteFile(filepath.Join(dir, name), data, 0o644) }
	taxonomy, err := os.ReadFile(filepath.Join(rulesDir, rules.TaxonomyFile))
	if err != nil {
		return nil, err
	}
	if err := write(rules.TaxonomyFile, taxonomy); err != nil {
		return nil, err
	}
	b := &Bundle{RulesDigest: prod.Digest, BaselinesDigest: base.Digest,
		Production: Variant{Name: "production"}, Baseline: Variant{Name: "baseline"}}
	for _, r := range prod.Rules {
		b.Production.Rules = append(b.Production.Rules, r.ID)
		if err := write(r.File, r.Source); err != nil {
			return nil, err
		}
	}
	for _, r := range base.Rules {
		b.Baseline.Rules = append(b.Baseline.Rules, r.ID)
		if err := write("baseline-"+r.File, r.Source); err != nil {
			return nil, err
		}
	}
	for _, sw := range sweeps {
		r, ok := prod.Rule(sw.RuleID)
		if !ok {
			return nil, fmt.Errorf("eval: no rule %s in %s", sw.RuleID, rulesDir)
		}
		stem := sw.RuleID[:strings.LastIndexByte(sw.RuleID, '-')]
		var vs []Variant
		for i, v := range sw.Values {
			id := fmt.Sprintf("%s-%d", stem, 101+i)
			src, err := setLine(string(r.Source), sw.Param, v)
			if err == nil {
				src, err = setLine(src, "id", id)
			}
			if err != nil {
				return nil, fmt.Errorf("%s: %w", sw.RuleID, err)
			}
			if err := write(fmt.Sprintf("sweep-%s.yaml", strings.ToLower(id)), []byte(src)); err != nil {
				return nil, err
			}
			vs = append(vs, Variant{Name: fmt.Sprintf("%s %s=%s", sw.RuleID, sw.Param, v), Rules: []string{id}})
		}
		b.Sweeps = append(b.Sweeps, SweepResult{Sweep: sw, Type: r.Taxonomy.AbuseType, Production: currentParam(string(r.Source), sw.Param)})
		b.SweepVariants = append(b.SweepVariants, vs)
	}
	if b.Set, err = rules.Load(dir); err != nil {
		return nil, fmt.Errorf("eval: bundle: %w", err)
	}
	return b, nil
}

// Variants lists every variant of the bundle: production, baseline, then
// the sweeps' variants in order.
func (b *Bundle) Variants() []Variant {
	out := []Variant{b.Production, b.Baseline}
	for _, vs := range b.SweepVariants {
		out = append(out, vs...)
	}
	return out
}

// Report assembles the report from Run's results for b.Variants().
func (b *Bundle) Report(p Plan, symbols int, goVersion string, results []*Result) *Report {
	rep := &Report{Plan: p, Symbols: symbols, RulesDigest: b.RulesDigest, BaselinesDigest: b.BaselinesDigest,
		GoVersion: goVersion, Production: results[0], Baseline: results[1]}
	next := 2
	for i, sw := range b.Sweeps {
		n := len(b.SweepVariants[i])
		sw.Points = results[next : next+n]
		next += n
		rep.Sweeps = append(rep.Sweeps, sw)
	}
	return rep
}
