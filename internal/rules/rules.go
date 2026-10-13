// Package rules loads and validates TradeWatch's versioned detection rules.
//
// A rule is a YAML document (rules/*.yaml) that names a detector, its
// parameters (thresholds and windows), how long to suppress repeat alerts,
// the market-abuse taxonomy code it maps to, and a template for the
// human-readable explanation attached to every alert. Every document is
// validated against an embedded JSON Schema, which holds a strict parameter
// schema per detector, before anything else is read from it. A rule set
// either loads completely or not at all.
package rules

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/template"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

//go:embed rule.schema.json
var schemaJSON []byte

// TaxonomyFile is the name of the taxonomy document inside a rules
// directory.
const TaxonomyFile = "taxonomy.yaml"

// Severity is the severity assigned to a rule's alerts.
type Severity string

const (
	SeverityLow      Severity = "low"
	SeverityMedium   Severity = "medium"
	SeverityHigh     Severity = "high"
	SeverityCritical Severity = "critical"
)

// Proto returns the protobuf severity.
func (s Severity) Proto() twpb.Severity {
	switch s {
	case SeverityLow:
		return twpb.Severity_SEVERITY_LOW
	case SeverityMedium:
		return twpb.Severity_SEVERITY_MEDIUM
	case SeverityHigh:
		return twpb.Severity_SEVERITY_HIGH
	case SeverityCritical:
		return twpb.Severity_SEVERITY_CRITICAL
	default:
		return twpb.Severity_SEVERITY_UNSPECIFIED
	}
}

// Rule is one validated detection rule.
type Rule struct {
	ID          string
	Version     uint32
	Name        string
	Detector    string
	Taxonomy    *TaxonomyCode
	Severity    Severity
	Owner       string
	Description string

	Enabled     bool
	SuppressFor time.Duration
	// EscalateAtScore raises an alert to critical when its score reaches
	// it; zero disables escalation.
	EscalateAtScore float64
	// Explanation renders an alert's human-readable explanation.
	Explanation       *template.Template
	ExplanationSource string

	params yaml.Node
	// Source is the file's content and Digest its SHA-256.
	Source []byte
	Digest string
	File   string
}

// DecodeParams decodes the rule's parameters into v, rejecting unknown
// fields.
func (r *Rule) DecodeParams(v any) error {
	b, err := yaml.Marshal(&r.params)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("rule %s params: %w", r.ID, err)
	}
	return nil
}

// Set is an immutable set of rules loaded together.
type Set struct {
	Rules    []*Rule
	Taxonomy *Taxonomy
	// Digest identifies the set: the SHA-256 over every file's name and
	// digest, taxonomy included.
	Digest   string
	Dir      string
	LoadedAt time.Time
}

// Enabled returns the enabled rules, sorted by ID.
func (s *Set) Enabled() []*Rule {
	var out []*Rule
	for _, r := range s.Rules {
		if r.Enabled {
			out = append(out, r)
		}
	}
	return out
}

// Rule returns a rule by ID.
func (s *Set) Rule(id string) (*Rule, bool) {
	for _, r := range s.Rules {
		if r.ID == id {
			return r, true
		}
	}
	return nil, false
}

// Snapshot describes the set for the rules_audit topic.
func (s *Set) Snapshot(loadedBy string) *twpb.RuleSetSnapshot {
	snap := &twpb.RuleSetSnapshot{RulesetDigest: s.Digest, LoadedAtNs: s.LoadedAt.UnixNano(), LoadedBy: loadedBy}
	for _, r := range s.Rules {
		snap.Rules = append(snap.Rules, &twpb.RuleDocument{
			RuleId: r.ID, Version: r.Version, Digest: r.Digest, Detector: r.Detector, Name: r.Name,
			TaxonomyCode: r.Taxonomy.Code, Enabled: r.Enabled, Source: r.Source,
		})
	}
	return snap
}

// document mirrors the YAML layout of a rule.
type document struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		ID          string `yaml:"id"`
		Version     uint32 `yaml:"version"`
		Name        string `yaml:"name"`
		Detector    string `yaml:"detector"`
		Taxonomy    string `yaml:"taxonomy"`
		Severity    string `yaml:"severity"`
		Owner       string `yaml:"owner"`
		Description string `yaml:"description"`
	} `yaml:"metadata"`
	Spec struct {
		Enabled         bool      `yaml:"enabled"`
		SuppressFor     string    `yaml:"suppress_for"`
		EscalateAtScore float64   `yaml:"escalate_at_score"`
		Params          yaml.Node `yaml:"params"`
		Explanation     string    `yaml:"explanation"`
	} `yaml:"spec"`
}

var compiledSchema = func() *jsonschema.Schema {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaJSON))
	if err != nil {
		panic(fmt.Sprintf("rules: embedded schema is not JSON: %v", err))
	}
	c := jsonschema.NewCompiler()
	const url = "https://tradewatch.local/schema/rule.schema.json"
	if err := c.AddResource(url, doc); err != nil {
		panic(err)
	}
	s, err := c.Compile(url)
	if err != nil {
		panic(fmt.Sprintf("rules: embedded schema does not compile: %v", err))
	}
	return s
}()

// Schema returns the embedded JSON Schema rules are validated against.
func Schema() []byte { return append([]byte(nil), schemaJSON...) }

// validateSchema checks a YAML document against the JSON Schema. YAML is
// converted to JSON first so that the schema sees exactly the JSON data
// model.
func validateSchema(src []byte) error {
	var v any
	if err := yaml.Unmarshal(src, &v); err != nil {
		return err
	}
	j, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("not representable as JSON: %w", err)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(j))
	if err != nil {
		return err
	}
	return compiledSchema.Validate(inst)
}

// Funcs are the helpers available to explanation templates.
var Funcs = template.FuncMap{
	"join": strings.Join,
	"num":  func(v float64) string { return trimFloat(fmt.Sprintf("%.2f", v)) },
	"int":  func(v float64) string { return fmt.Sprintf("%.0f", v) },
	"pct":  func(v float64) string { return trimFloat(fmt.Sprintf("%.1f", 100*v)) + "%" },
	"ms":   func(v float64) string { return fmt.Sprintf("%.0f ms", v) },
	"secs": func(v float64) string { return trimFloat(fmt.Sprintf("%.1f", v)) + " s" },
	"bps":  func(v float64) string { return fmt.Sprintf("%.0f bps", v) },
	// px formats a price in units of 1/10,000.
	"px": func(v float64) string { return fmt.Sprintf("%.2f", v/10_000) },
}

func trimFloat(s string) string {
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

func parseRule(file string, src []byte, tax *Taxonomy) (*Rule, error) {
	if err := validateSchema(src); err != nil {
		return nil, fmt.Errorf("%s: schema: %w", file, err)
	}
	var d document
	dec := yaml.NewDecoder(bytes.NewReader(src))
	dec.KnownFields(true)
	if err := dec.Decode(&d); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	code, ok := tax.Codes[d.Metadata.Taxonomy]
	if !ok {
		return nil, fmt.Errorf("%s: unknown taxonomy code %q", file, d.Metadata.Taxonomy)
	}
	suppress, err := time.ParseDuration(d.Spec.SuppressFor)
	if err != nil {
		return nil, fmt.Errorf("%s: suppress_for: %w", file, err)
	}
	tmpl, err := template.New(d.Metadata.ID).Funcs(Funcs).Option("missingkey=error").Parse(d.Spec.Explanation)
	if err != nil {
		return nil, fmt.Errorf("%s: explanation template: %w", file, err)
	}
	sum := sha256.Sum256(src)
	return &Rule{
		ID: d.Metadata.ID, Version: d.Metadata.Version, Name: d.Metadata.Name,
		Detector: d.Metadata.Detector, Taxonomy: code, Severity: Severity(d.Metadata.Severity),
		Owner: d.Metadata.Owner, Description: strings.TrimSpace(d.Metadata.Description),
		Enabled: d.Spec.Enabled, SuppressFor: suppress, EscalateAtScore: d.Spec.EscalateAtScore,
		Explanation: tmpl, ExplanationSource: d.Spec.Explanation,
		params: d.Spec.Params, Source: src, Digest: hex.EncodeToString(sum[:]), File: file,
	}, nil
}

// ruleFiles lists the rule documents of dir: every *.yaml file except the
// taxonomy, in name order.
func ruleFiles(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	var files []string
	for _, m := range matches {
		if filepath.Base(m) != TaxonomyFile {
			files = append(files, m)
		}
	}
	slices.Sort(files)
	return files, nil
}

// Load reads and validates every rule in dir together with its taxonomy.
func Load(dir string) (*Set, error) {
	taxSrc, err := os.ReadFile(filepath.Join(dir, TaxonomyFile))
	if err != nil {
		return nil, fmt.Errorf("rules: %w", err)
	}
	tax, err := parseTaxonomy(taxSrc)
	if err != nil {
		return nil, fmt.Errorf("rules: %s: %w", TaxonomyFile, err)
	}
	files, err := ruleFiles(dir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("rules: no rule files in %s", dir)
	}
	set := &Set{Taxonomy: tax, Dir: dir, LoadedAt: time.Now()}
	h := sha256.New()
	fmt.Fprintf(h, "%s %x\n", TaxonomyFile, sha256.Sum256(taxSrc))
	var errs []error
	seen := map[string]string{}
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		r, err := parseRule(filepath.Base(f), src, tax)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if prev, dup := seen[r.ID]; dup {
			errs = append(errs, fmt.Errorf("%s: rule ID %s is also defined in %s", r.File, r.ID, prev))
			continue
		}
		seen[r.ID] = r.File
		set.Rules = append(set.Rules, r)
		fmt.Fprintf(h, "%s %s\n", r.File, r.Digest)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	slices.SortFunc(set.Rules, func(a, b *Rule) int { return strings.Compare(a.ID, b.ID) })
	set.Digest = hex.EncodeToString(h.Sum(nil))
	return set, nil
}

// DirDigest hashes the names and contents of a rules directory, so a
// watcher can notice changes without parsing anything.
func DirDigest(dir string) (string, error) {
	files, err := ruleFiles(dir)
	if err != nil {
		return "", err
	}
	files = append(files, filepath.Join(dir, TaxonomyFile))
	h := sha256.New()
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		fmt.Fprintf(h, "%s %x\n", filepath.Base(f), sha256.Sum256(b))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
