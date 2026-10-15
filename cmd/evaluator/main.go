// Command evaluator measures how well the detection rules find the abuse
// the market simulator injects, compares them with naive baselines, sweeps
// one threshold per rule, and writes a report (README.md, metrics.json and
// one chart per sweep) to -out. The run is deterministic: the same flags
// and rules produce the same files. With -postgres it also records the run,
// with the production rules' figures, in the evaluation_runs table.
// `make eval` runs it with the defaults.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/eval"
	"github.com/ZofSpades/tradewatch/internal/logging"
	"github.com/ZofSpades/tradewatch/internal/sim"
	"github.com/ZofSpades/tradewatch/internal/store"
)

func main() {
	log := logging.New("evaluator")
	if err := run(log); err != nil {
		log.Error("evaluation failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	rulesDir := flag.String("rules", "rules", "production rules directory")
	baselineDir := flag.String("baselines", "rules/baselines", "baseline rules directory (uses the production taxonomy)")
	out := flag.String("out", "docs/results/eval", "output directory")
	postgres := flag.String("postgres", "", "record the run in this Postgres database (empty: write the report only)")
	plan := eval.Plan{}
	flag.Uint64Var(&plan.FirstSeed, "seed", 1000, "seed of the first session")
	flag.IntVar(&plan.Sessions, "sessions", 20, "sessions with injected abuse")
	flag.IntVar(&plan.Clean, "clean", 5, "sessions without injected abuse")
	flag.DurationVar(&plan.Duration, "duration", 2*time.Hour, "continuous trading per session")
	flag.DurationVar(&plan.ClosingWindow, "closing", 10*time.Minute, "closing phase per session")
	flag.IntVar(&plan.Workers, "workers", 0, "sessions simulated at once (0: number of CPUs, at most 4)")
	sweeps := flag.Bool("sweeps", true, "include the parameter sweeps")
	flag.Float64Var(&plan.Injection.WashTrading, "wash", 1, "wash trading episodes per symbol-hour")
	flag.Float64Var(&plan.Injection.Spoofing, "spoofing", 1, "spoofing episodes per symbol-hour")
	flag.Float64Var(&plan.Injection.MomentumIgnition, "momentum", 1, "momentum ignition episodes per symbol-hour")
	flag.Float64Var(&plan.Injection.QuoteStuffing, "stuffing", 1, "quote stuffing episodes per symbol-hour")
	flag.Float64Var(&plan.Injection.MarkingTheClose, "close", 0.6, "probability of marking the close per symbol and session")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tmp, err := os.MkdirTemp("", "tradewatch-eval-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	var sweepList []eval.Sweep
	if *sweeps {
		sweepList = eval.DefaultSweeps
	}
	bundle, err := eval.NewBundle(*rulesDir, *baselineDir, tmp, sweepList)
	if err != nil {
		return err
	}
	if err := detect.Validate(bundle.Set); err != nil {
		return err
	}

	var rec *recorder
	if *postgres != "" {
		if rec, err = startRecording(ctx, *postgres, plan, bundle.RulesDigest, log); err != nil {
			return fmt.Errorf("record the run: %w", err)
		}
		defer rec.close()
	}
	rep, err := evaluate(ctx, plan, bundle, *out, log)
	if rerr := rec.finish(rep, err); rerr != nil {
		log.Warn("the run could not be recorded", "err", rerr)
	}
	if err != nil {
		return err
	}
	for _, t := range eval.TypeNames() {
		p, b := rep.Production.ByType[t], rep.Baseline.ByType[t]
		if p == nil || b == nil {
			continue
		}
		fmt.Printf("%-18s recall %.2f precision %.2f | baseline recall %.2f precision %.2f\n",
			t, p.Recall().Value, p.Precision().Value, b.Recall().Value, b.Precision().Value)
	}
	return nil
}

// evaluate runs the plan and writes the report to out.
func evaluate(ctx context.Context, plan eval.Plan, bundle *eval.Bundle, out string, log *slog.Logger) (*eval.Report, error) {
	variants := bundle.Variants()
	start := time.Now()
	log.Info("evaluating", "sessions", plan.Sessions, "clean_sessions", plan.Clean, "rules", len(bundle.Set.Rules),
		"variants", len(variants))
	results, err := eval.Run(ctx, plan, bundle.Set, variants)
	if err != nil {
		return nil, err
	}
	rep := bundle.Report(plan, len(sim.DefaultUniverse().Instruments), runtime.Version(), results)
	if err := os.MkdirAll(out, 0o755); err != nil {
		return nil, err
	}
	js, err := rep.JSON()
	if err != nil {
		return nil, err
	}
	files := map[string][]byte{"README.md": []byte(rep.Markdown()), "metrics.json": append(js, '\n')}
	for _, sw := range rep.Sweeps {
		files[sw.PlotFile()] = []byte(sw.SVG())
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(out, name), data, 0o644); err != nil {
			return nil, err
		}
	}
	log.Info("report written", "dir", out, "files", len(files), "elapsed", time.Since(start).Round(time.Second).String())
	return rep, nil
}

// recorder keeps the run in the evaluation_runs table. A nil recorder
// records nothing.
type recorder struct {
	st  *store.Store
	run *store.EvaluationRun
}

func startRecording(ctx context.Context, url string, p eval.Plan, rulesDigest string, log *slog.Logger) (*recorder, error) {
	if _, err := store.Migrate(ctx, url, log); err != nil {
		return nil, err
	}
	st, err := store.Open(ctx, url)
	if err != nil {
		return nil, err
	}
	last := p.FirstSeed + uint64(p.Sessions+p.Clean) - 1
	source := fmt.Sprintf("%d sessions with injected abuse and %d without, seeds %d to %d, %s each",
		p.Sessions, p.Clean, p.FirstSeed, last, p.Duration+p.ClosingWindow)
	run, err := st.StartEvaluation(ctx, store.EvalKindEval, source, "evaluator", rulesDigest)
	if err != nil {
		st.Close()
		return nil, err
	}
	log.Info("recording the run", "id", run.ID)
	return &recorder{st: st, run: run}, nil
}

// finish stores the production rules' figures, or the error that ended the
// run. It uses a context of its own, so an interrupted run is still marked
// as failed.
func (r *recorder) finish(rep *eval.Report, runErr error) error {
	if r == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if runErr != nil {
		return r.st.FinishEvaluation(ctx, r.run.ID, "", []eval.TypeSummary{}, runErr)
	}
	var episodes, alerts, clean int
	for _, s := range rep.Production.ByType {
		episodes, alerts, clean = episodes+s.Labels, alerts+s.Alerts, clean+s.CleanAlerts
	}
	summary := fmt.Sprintf("%d labelled episodes; %d alerts on the sessions with abuse and %d on those without", episodes, alerts, clean)
	return r.st.FinishEvaluation(ctx, r.run.ID, summary, rep.Production.Summary(), nil)
}

func (r *recorder) close() {
	if r != nil {
		r.st.Close()
	}
}
