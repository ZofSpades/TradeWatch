// Package testreport counts what `make test-report` ran: the events of
// `go test -json`, the test lists of `go test -list` with and without the
// integration build tag, and the dashboard's vitest JSON report. It writes
// docs/results/tests.
package testreport

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Counts are the outcomes of a group of tests.
type Counts struct {
	Passed  int `json:"passed"`
	Failed  int `json:"failed"`
	Skipped int `json:"skipped"`
}

func (c *Counts) add(action string) {
	switch action {
	case "pass":
		c.Passed++
	case "fail":
		c.Failed++
	case "skip":
		c.Skipped++
	}
}

// Package is one Go package's share of the run.
type Package struct {
	Path string `json:"path"`
	// Tests are the top-level test functions, including fuzz targets,
	// which run on their seed corpus; Subtests are those they started.
	Tests    Counts `json:"tests"`
	Subtests Counts `json:"subtests"`
	// Integration counts the test functions compiled only with the
	// integration build tag.
	Integration int      `json:"integration"`
	Property    []string `json:"property,omitempty"`
	Fuzz        []string `json:"fuzz,omitempty"`
	Benchmarks  int      `json:"benchmarks"`
	Seconds     float64  `json:"seconds"`
	Failed      bool     `json:"failed"`
}

// Web is the dashboard's test run.
type Web struct {
	Files int    `json:"files"`
	Tests Counts `json:"tests"`
}

// Report is the outcome of a test-report run.
type Report struct {
	Generated time.Time `json:"generated"`
	Commit    string    `json:"commit,omitempty"`
	GoVersion string    `json:"go_version"`
	// Module is the Go module path that package paths start with.
	Module   string     `json:"module"`
	Packages []*Package `json:"packages"`
	Web      Web        `json:"web"`
	Passed   bool       `json:"passed"`
}

// Totals sums the Go packages.
func (r *Report) Totals() (tests, subtests Counts, integration, property, fuzz, benchmarks int) {
	for _, p := range r.Packages {
		tests.Passed += p.Tests.Passed
		tests.Failed += p.Tests.Failed
		tests.Skipped += p.Tests.Skipped
		subtests.Passed += p.Subtests.Passed
		subtests.Failed += p.Subtests.Failed
		subtests.Skipped += p.Subtests.Skipped
		integration += p.Integration
		property += len(p.Property)
		fuzz += len(p.Fuzz)
		benchmarks += p.Benchmarks
	}
	return
}

type event struct {
	Action      string
	Package     string
	Test        string
	Elapsed     float64
	FailedBuild string
}

// ParseGoTest reads `go test -json` output. It returns the packages that
// have tests, by import path, with each test's final outcome counted.
func ParseGoTest(r io.Reader) (map[string]*Package, error) {
	pkgs := map[string]*Package{}
	final := map[string]map[string]string{} // package -> test -> last outcome
	pkg := func(p string) *Package {
		if pkgs[p] == nil {
			pkgs[p] = &Package{Path: p}
			final[p] = map[string]string{}
		}
		return pkgs[p]
	}
	br := bufio.NewReaderSize(r, 1<<20)
	for {
		line, err := br.ReadBytes('\n')
		if line = bytes.TrimSpace(line); len(line) > 0 && line[0] == '{' {
			var ev event
			if jerr := json.Unmarshal(line, &ev); jerr != nil {
				return nil, fmt.Errorf("go test -json: %w", jerr)
			}
			switch {
			case ev.Package == "" || (ev.Action != "pass" && ev.Action != "fail" && ev.Action != "skip"):
			case ev.Test != "":
				pkg(ev.Package)
				final[ev.Package][ev.Test] = ev.Action
			case ev.Action == "skip":
				// A package without test files.
			default:
				p := pkg(ev.Package)
				p.Seconds = ev.Elapsed
				p.Failed = ev.Action == "fail"
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	for path, tests := range final {
		p := pkgs[path]
		for name, action := range tests {
			if strings.Contains(name, "/") {
				p.Subtests.add(action)
				continue
			}
			p.Tests.add(action)
			if strings.HasPrefix(name, "Fuzz") {
				p.Fuzz = append(p.Fuzz, name)
			}
		}
		slices.Sort(p.Fuzz)
	}
	return pkgs, nil
}

// ParseList reads `go test -list .` output: each package's test,
// benchmark, fuzz and example names, by import path.
func ParseList(r io.Reader) (map[string][]string, error) {
	out := map[string][]string{}
	var names []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		fields := strings.Fields(line)
		switch {
		case len(fields) == 1 && (strings.HasPrefix(line, "Test") || strings.HasPrefix(line, "Benchmark") ||
			strings.HasPrefix(line, "Fuzz") || strings.HasPrefix(line, "Example")):
			names = append(names, line)
		case len(fields) >= 2 && fields[0] == "ok":
			out[fields[1]] = names
			names = nil
		case len(fields) >= 2 && fields[0] == "?":
			names = nil // no test files
		}
	}
	return out, sc.Err()
}

// ParseVitest reads the dashboard's vitest JSON report.
func ParseVitest(r io.Reader) (Web, error) {
	var v struct {
		NumPassedTests  int
		NumFailedTests  int
		NumPendingTests int
		NumTodoTests    int
		TestResults     []json.RawMessage
	}
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		return Web{}, fmt.Errorf("vitest report: %w", err)
	}
	return Web{Files: len(v.TestResults), Tests: Counts{
		Passed: v.NumPassedTests, Failed: v.NumFailedTests, Skipped: v.NumPendingTests + v.NumTodoTests,
	}}, nil
}

// PropertyTests finds the test functions under root that run a
// property-based check (rapid.Check), by package import path.
func PropertyTests(root, module string) (map[string][]string, error) {
	out := map[string][]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); p != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "web" || name == "tmp") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(p))
		if err != nil {
			return err
		}
		pkg := path.Join(module, filepath.ToSlash(rel))
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			if callsRapidCheck(fn.Body) {
				out[pkg] = append(out[pkg], fn.Name.Name)
			}
		}
		return nil
	})
	for _, names := range out {
		slices.Sort(names)
	}
	return out, err
}

func callsRapidCheck(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Check" {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "rapid" {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

// Build combines the parsed inputs. listAll comes from a build with the
// integration tag and listUnit from one without.
func Build(pkgs map[string]*Package, listAll, listUnit, property map[string][]string, web Web) *Report {
	r := &Report{Web: web, Passed: web.Tests.Failed == 0 && web.Files > 0}
	for path, p := range pkgs {
		for _, name := range listAll[path] {
			switch {
			case strings.HasPrefix(name, "Benchmark"):
				p.Benchmarks++
			case strings.HasPrefix(name, "Test") && !slices.Contains(listUnit[path], name):
				p.Integration++
			}
		}
		p.Property = property[path]
		r.Packages = append(r.Packages, p)
	}
	slices.SortFunc(r.Packages, func(a, b *Package) int { return strings.Compare(a.Path, b.Path) })
	for _, p := range r.Packages {
		if p.Failed || p.Tests.Failed > 0 || p.Subtests.Failed > 0 {
			r.Passed = false
		}
	}
	if len(r.Packages) == 0 {
		r.Passed = false
	}
	return r
}

// Write saves the report as results.json and README.md in dir.
func Write(dir string, r *Report) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	js, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "results.json"), append(js, '\n'), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "README.md"), []byte(Markdown(r)), 0o644)
}

// Markdown renders the report.
func Markdown(r *Report) string {
	var b strings.Builder
	p := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	tests, subtests, integration, property, fuzz, benchmarks := r.Totals()
	module := r.Module

	p("# Test suite\n\n")
	p("Generated by `make test-report` on %s UTC", r.Generated.Format("2006-01-02 15:04"))
	if r.Commit != "" {
		p(", at commit `%s`", r.Commit)
	}
	p(", with %s.\n\n", r.GoVersion)
	if r.Passed {
		p("**Every test passed.** ")
	} else {
		p("**Some tests failed.** ")
	}
	p("The run covers the same suites as `make test`: every Go package with the race detector and the `integration` build tag, against Kafka, Postgres and Redis in docker compose, and the dashboard's tests.\n\n")
	p("| Suite | Passed | Failed | Skipped |\n| --- | ---: | ---: | ---: |\n")
	p("| Go test functions, in %d packages | %s | %s | %s |\n", len(r.Packages), n(tests.Passed), n(tests.Failed), n(tests.Skipped))
	p("| Go subtests | %s | %s | %s |\n", n(subtests.Passed), n(subtests.Failed), n(subtests.Skipped))
	p("| Dashboard tests (vitest), in %d files | %s | %s | %s |\n\n", r.Web.Files, n(r.Web.Tests.Passed), n(r.Web.Tests.Failed), n(r.Web.Tests.Skipped))

	p("Of the Go test functions:\n\n")
	p("- %s, compiled only with the `integration` tag and run against the real Kafka, Postgres and Redis.\n",
		isAre(integration, "an integration test", "integration tests"))
	p("- %s, with `pgregory.net/rapid`: %s.\n", isAre(property, "property-based", "property-based"),
		named(r.Packages, module, func(p *Package) []string { return p.Property }))
	p("- %s: %s.\n\n", isAre(fuzz, "a fuzz target, run here on its seed corpus", "fuzz targets, run here on their seed corpus"),
		named(r.Packages, module, func(p *Package) []string { return p.Fuzz }))
	p("The packages also define %s, which `make bench-micro` runs.\n\n", plural(benchmarks, "benchmark"))

	p("## By package\n\n")
	p("| Package | Test functions | Integration | Subtests | Benchmarks | Time |\n| --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, pk := range r.Packages {
		name := strings.TrimPrefix(strings.TrimPrefix(pk.Path, module), "/")
		if pk.Failed || pk.Tests.Failed > 0 || pk.Subtests.Failed > 0 {
			name += " (failed)"
		}
		p("| %s | %s | %s | %s | %s | %.1f s |\n", name, n(pk.Tests.Passed+pk.Tests.Failed+pk.Tests.Skipped),
			n(pk.Integration), n(pk.Subtests.Passed+pk.Subtests.Failed+pk.Subtests.Skipped), n(pk.Benchmarks), pk.Seconds)
	}
	p("\nTimes are each package's wall time in this run; packages run in parallel.\n")
	return b.String()
}

// isAre is "1 is <one>" or "<k> are <many>".
func isAre(k int, one, many string) string {
	if k == 1 {
		return "1 is " + one
	}
	return n(k) + " are " + many
}

func plural(k int, noun string) string {
	if k == 1 {
		return "1 " + noun
	}
	return n(k) + " " + noun + "s"
}

// named lists the selected test names with their packages.
func named(pkgs []*Package, module string, sel func(*Package) []string) string {
	var out []string
	for _, p := range pkgs {
		for _, t := range sel(p) {
			out = append(out, fmt.Sprintf("`%s` (%s)", t, strings.TrimPrefix(strings.TrimPrefix(p.Path, module), "/")))
		}
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, ", ")
}

func n(v int) string {
	s := fmt.Sprint(v)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 && s[0] != '-' {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}
