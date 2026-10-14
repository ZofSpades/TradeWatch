// Command replay reruns detection over a session recording, offline, and
// scores the alerts against the recording's ground-truth labels.
//
//	replay [-rules dir] [-compare dir] [-json] session.twrec.gz
//
// With -compare it also replays the recording under a second rules
// directory and lists the alerts only one of the two raises, which
// backtests a rule change on recorded history. With -json the alerts of
// the first rules directory are written to stdout as JSON lines and the
// summary goes to stderr. `make replay` runs it on the compose stack's
// recordings.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/eval"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/replay"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "replay:", err)
		os.Exit(1)
	}
}

func load(dir string) (*rules.Set, error) {
	set, err := rules.Load(dir)
	if err != nil {
		return nil, err
	}
	return set, detect.Validate(set)
}

func run() error {
	rulesDir := flag.String("rules", "rules", "rules directory")
	compareDir := flag.String("compare", "", "a second rules directory to compare with")
	asJSON := flag.Bool("json", false, "write the alerts as JSON lines to stdout and the summary to stderr")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "usage: replay [-rules dir] [-compare dir] [-json] session.twrec.gz")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		return fmt.Errorf("expected one recording")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dirs := []string{*rulesDir}
	if *compareDir != "" {
		dirs = append(dirs, *compareDir)
	}
	var sets []*rules.Set
	for _, d := range dirs {
		set, err := load(d)
		if err != nil {
			return err
		}
		sets = append(sets, set)
	}
	out, err := replay.Run(ctx, flag.Arg(0), sets)
	if err != nil {
		return err
	}

	summary := io.Writer(os.Stdout)
	if *asJSON {
		summary = os.Stderr
		enc := protojson.MarshalOptions{UseProtoNames: true}
		for _, a := range out.Alerts[0] {
			b, err := enc.Marshal(a)
			if err != nil {
				return err
			}
			fmt.Println(string(b))
		}
	}
	fmt.Fprintf(summary, "recording %s: %d symbols, %d events, %d labelled episodes\n\n",
		out.Header.GetSessionId(), out.Symbols, out.Events, len(out.Labels))
	for i, set := range sets {
		res := eval.ScoreAlerts(dirs[i], out.Alerts[i], out.Labels)
		fmt.Fprintf(summary, "rules %s (digest %.12s): %d alerts\n", dirs[i], set.Digest, len(out.Alerts[i]))
		fmt.Fprintf(summary, "  %-18s %8s %8s %8s %8s\n", "abuse type", "episodes", "detected", "alerts", "matched")
		for _, t := range eval.TypeNames() {
			if s := res.ByType[t]; s != nil {
				fmt.Fprintf(summary, "  %-18s %8d %8d %8d %8d\n", t, s.Labels, s.Detected, s.Alerts, s.Matched)
			}
		}
		fmt.Fprintln(summary)
	}
	if len(sets) == 2 {
		d := replay.Compare(out.Alerts[0], out.Alerts[1])
		fmt.Fprintf(summary, "%d alerts raised by both; %d only by %s; %d only by %s\n", d.Both, len(d.OnlyA), dirs[0], len(d.OnlyB), dirs[1])
		list(summary, "only by "+dirs[0], d.OnlyA)
		list(summary, "only by "+dirs[1], d.OnlyB)
	}
	return nil
}

func list(w io.Writer, title string, alerts []*twpb.Alert) {
	if len(alerts) == 0 {
		return
	}
	fmt.Fprintf(w, "\n%s:\n", title)
	for _, a := range alerts {
		fmt.Fprintf(w, "  %s %s %s [%s] %s\n", a.GetAlertKey(), a.GetRuleId(), a.GetSymbol(),
			strings.Join(a.GetAccountIds(), ","), a.GetExplanation())
	}
}
