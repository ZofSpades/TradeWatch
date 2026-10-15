package testreport

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const mod = "example.com/tw"

// goJSON is go test -json output for two packages and one without tests.
const goJSON = `{"Time":"2026-10-15T05:00:00Z","Action":"start","Package":"example.com/tw/internal/a"}
{"Time":"2026-10-15T05:00:00Z","Action":"run","Package":"example.com/tw/internal/a","Test":"TestOne"}
{"Time":"2026-10-15T05:00:00Z","Action":"output","Package":"example.com/tw/internal/a","Test":"TestOne","Output":"=== RUN   TestOne\n"}
{"Time":"2026-10-15T05:00:00Z","Action":"pass","Package":"example.com/tw/internal/a","Test":"TestOne","Elapsed":0.1}
{"Time":"2026-10-15T05:00:00Z","Action":"run","Package":"example.com/tw/internal/a","Test":"TestTable"}
{"Time":"2026-10-15T05:00:00Z","Action":"run","Package":"example.com/tw/internal/a","Test":"TestTable/first"}
{"Time":"2026-10-15T05:00:00Z","Action":"pass","Package":"example.com/tw/internal/a","Test":"TestTable/first","Elapsed":0}
{"Time":"2026-10-15T05:00:00Z","Action":"run","Package":"example.com/tw/internal/a","Test":"TestTable/second"}
{"Time":"2026-10-15T05:00:00Z","Action":"skip","Package":"example.com/tw/internal/a","Test":"TestTable/second","Elapsed":0}
{"Time":"2026-10-15T05:00:00Z","Action":"pass","Package":"example.com/tw/internal/a","Test":"TestTable","Elapsed":0}
{"Time":"2026-10-15T05:00:00Z","Action":"pass","Package":"example.com/tw/internal/a","Test":"FuzzParse","Elapsed":0}
{"Time":"2026-10-15T05:00:00Z","Action":"pass","Package":"example.com/tw/internal/a","Test":"FuzzParse/seed#0","Elapsed":0}
{"Time":"2026-10-15T05:00:01Z","Action":"pass","Package":"example.com/tw/internal/a","Elapsed":1.25}
{"Time":"2026-10-15T05:00:00Z","Action":"run","Package":"example.com/tw/internal/b","Test":"TestStore"}
{"Time":"2026-10-15T05:00:00Z","Action":"fail","Package":"example.com/tw/internal/b","Test":"TestStore","Elapsed":2}
{"Time":"2026-10-15T05:00:02Z","Action":"fail","Package":"example.com/tw/internal/b","Elapsed":2.5}
{"Time":"2026-10-15T05:00:00Z","Action":"skip","Package":"example.com/tw/cmd/c","Elapsed":0}
`

const listAll = `TestOne
TestTable
FuzzParse
BenchmarkParse
BenchmarkBuild
ok  	example.com/tw/internal/a	0.010s
TestStore
ok  	example.com/tw/internal/b	0.020s
?   	example.com/tw/cmd/c	[no test files]
`

// Without the integration tag, internal/b has no tests.
const listUnit = `TestOne
TestTable
FuzzParse
BenchmarkParse
BenchmarkBuild
ok  	example.com/tw/internal/a	0.010s
?   	example.com/tw/internal/b	[no test files]
?   	example.com/tw/cmd/c	[no test files]
`

const vitest = `{"numTotalTestSuites":4,"numPassedTests":5,"numFailedTests":0,"numPendingTests":1,"numTodoTests":0,"success":true,
"testResults":[{"name":"/src/web/src/a.test.ts","status":"passed","assertionResults":[]},{"name":"/src/web/src/b.test.ts","status":"passed","assertionResults":[]}]}`

func build(t *testing.T, goOut string) *Report {
	t.Helper()
	pkgs, err := ParseGoTest(strings.NewReader(goOut))
	if err != nil {
		t.Fatal(err)
	}
	all, err := ParseList(strings.NewReader(listAll))
	if err != nil {
		t.Fatal(err)
	}
	unit, err := ParseList(strings.NewReader(listUnit))
	if err != nil {
		t.Fatal(err)
	}
	web, err := ParseVitest(strings.NewReader(vitest))
	if err != nil {
		t.Fatal(err)
	}
	r := Build(pkgs, all, unit, map[string][]string{mod + "/internal/a": {"TestTable"}}, web)
	r.Module, r.GoVersion, r.Commit = mod, "go1.26.9", "abc1234"
	r.Generated = time.Date(2026, 10, 15, 5, 0, 0, 0, time.UTC)
	return r
}

func TestBuildCountsEachTestsFinalOutcome(t *testing.T) {
	r := build(t, goJSON)
	if len(r.Packages) != 2 {
		t.Fatalf("packages = %d, want 2 (a package without tests is left out)", len(r.Packages))
	}
	a, b := r.Packages[0], r.Packages[1]
	if a.Path != mod+"/internal/a" || b.Path != mod+"/internal/b" {
		t.Fatalf("packages %s, %s", a.Path, b.Path)
	}
	if a.Tests != (Counts{Passed: 3}) || a.Subtests != (Counts{Passed: 2, Skipped: 1}) {
		t.Errorf("a: tests %+v, subtests %+v", a.Tests, a.Subtests)
	}
	if a.Integration != 0 || a.Benchmarks != 2 || !slices.Equal(a.Fuzz, []string{"FuzzParse"}) || a.Seconds != 1.25 {
		t.Errorf("a: integration %d, benchmarks %d, fuzz %v, seconds %v", a.Integration, a.Benchmarks, a.Fuzz, a.Seconds)
	}
	if b.Tests != (Counts{Failed: 1}) || b.Integration != 1 || !b.Failed {
		t.Errorf("b: tests %+v, integration %d, failed %v", b.Tests, b.Integration, b.Failed)
	}
	if r.Passed {
		t.Error("a run with a failed test passed")
	}
	tests, subtests, integration, property, fuzz, benchmarks := r.Totals()
	if tests != (Counts{Passed: 3, Failed: 1}) || subtests.Passed != 2 || integration != 1 || property != 1 || fuzz != 1 || benchmarks != 2 {
		t.Errorf("totals %+v %+v %d %d %d %d", tests, subtests, integration, property, fuzz, benchmarks)
	}
	if r.Web != (Web{Files: 2, Tests: Counts{Passed: 5, Skipped: 1}}) {
		t.Errorf("web = %+v", r.Web)
	}
}

func TestMarkdownReportsTotalsAndPackages(t *testing.T) {
	passing := strings.NewReplacer(`"Action":"fail"`, `"Action":"pass"`).Replace(goJSON)
	r := build(t, passing)
	if !r.Passed {
		t.Fatal("an all-passing run failed")
	}
	md := Markdown(r)
	for _, want := range []string{
		"Generated by `make test-report` on 2026-10-15 05:00 UTC, at commit `abc1234`, with go1.26.9.",
		"**Every test passed.**",
		"| Go test functions, in 2 packages | 4 | 0 | 0 |",
		"| Go subtests | 2 | 0 | 1 |",
		"| Dashboard tests (vitest), in 2 files | 5 | 0 | 1 |",
		"- 1 is an integration test, compiled only with the `integration` tag and run against the real Kafka, Postgres and Redis.",
		"- 1 is property-based, with `pgregory.net/rapid`: `TestTable` (internal/a).",
		"- 1 is a fuzz target, run here on its seed corpus: `FuzzParse` (internal/a).",
		"define 2 benchmarks",
		"| internal/a | 3 | 0 | 3 | 2 | 1.2 s |",
		"| internal/b | 1 | 1 | 0 | 0 | 2.5 s |",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q:\n%s", want, md)
		}
	}
}

func TestMarkdownMarksFailedPackages(t *testing.T) {
	md := Markdown(build(t, goJSON))
	if !strings.Contains(md, "**Some tests failed.**") || !strings.Contains(md, "| internal/b (failed) |") {
		t.Errorf("report:\n%s", md)
	}
}

func TestParseGoTestRejectsCorruptEvents(t *testing.T) {
	if _, err := ParseGoTest(strings.NewReader(`{"Action":"pass",` + "\n")); err == nil {
		t.Error("parsed a truncated event")
	}
}

func TestPropertyTestsFindsRapidChecks(t *testing.T) {
	root := t.TempDir()
	write := func(rel, src string) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("internal/book/model_test.go", `package book
import ("testing"; "pgregory.net/rapid")
func TestModel(t *testing.T) { rapid.Check(t, func(rt *rapid.T) {}) }
func TestPlain(t *testing.T) {}
func helper(t *testing.T) { rapid.Check(t, func(rt *rapid.T) {}) }
`)
	write("internal/book/book.go", "package book\n\nfunc check() { rapid.Check(nil, nil) }\n")
	write("web/node_modules/x/x_test.go", "package x\n\nfunc TestIgnored(t *testing.T) { rapid.Check(t, nil) }\n")

	got, err := PropertyTests(root, mod)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !slices.Equal(got[mod+"/internal/book"], []string{"TestModel"}) {
		t.Errorf("property tests = %v, want TestModel in internal/book only", got)
	}
}
