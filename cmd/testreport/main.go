// Command testreport supports `make test-report`. It counts the test runs
// the target saved under -dir and writes the report to -out:
//
//	go.json        go test -race -tags=integration -json ./...
//	list-all.txt   go test -tags=integration -list . ./...
//	list-unit.txt  go test -list . ./...
//	web.json       vitest run --reporter=json
//
// It exits non-zero if any test failed.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/ZofSpades/tradewatch/internal/testreport"
)

func main() {
	dir := flag.String("dir", "tmp/test-report", "directory holding the saved test runs")
	out := flag.String("out", "docs/results/tests", "directory the report is written to")
	commit := flag.String("commit", "", "the commit under test")
	flag.Parse()
	if err := run(*dir, *out, *commit); err != nil {
		fmt.Fprintln(os.Stderr, "testreport:", err)
		os.Exit(1)
	}
}

func run(dir, out, commit string) error {
	module, err := modulePath("go.mod")
	if err != nil {
		return err
	}
	read := func(name string, parse func(*os.File) error) error {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		defer f.Close()
		return parse(f)
	}
	var (
		pkgs              map[string]*testreport.Package
		listAll, listUnit map[string][]string
		web               testreport.Web
	)
	for _, step := range []struct {
		name  string
		parse func(*os.File) error
	}{
		{"go.json", func(f *os.File) (err error) { pkgs, err = testreport.ParseGoTest(f); return }},
		{"list-all.txt", func(f *os.File) (err error) { listAll, err = testreport.ParseList(f); return }},
		{"list-unit.txt", func(f *os.File) (err error) { listUnit, err = testreport.ParseList(f); return }},
		{"web.json", func(f *os.File) (err error) { web, err = testreport.ParseVitest(f); return }},
	} {
		if err := read(step.name, step.parse); err != nil {
			return err
		}
	}
	property, err := testreport.PropertyTests(".", module)
	if err != nil {
		return err
	}
	r := testreport.Build(pkgs, listAll, listUnit, property, web)
	r.Generated, r.Commit, r.GoVersion, r.Module = time.Now().UTC(), commit, runtime.Version(), module
	if err := testreport.Write(out, r); err != nil {
		return err
	}
	tests, subtests, _, _, _, _ := r.Totals()
	fmt.Printf("Go: %d test functions and %d subtests passed, %d failed. Dashboard: %d tests passed, %d failed. Report written to %s\n",
		tests.Passed, subtests.Passed, tests.Failed+subtests.Failed, web.Tests.Passed, web.Tests.Failed, out)
	if !r.Passed {
		return fmt.Errorf("some tests failed: see %s/README.md", out)
	}
	return nil
}

// modulePath reads the module path from a go.mod file.
func modulePath(gomod string) (string, error) {
	f, err := os.Open(gomod)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if m, ok := strings.CutPrefix(strings.TrimSpace(sc.Text()), "module "); ok {
			return strings.Trim(strings.TrimSpace(m), `"`), nil
		}
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%s has no module line", gomod)
}
