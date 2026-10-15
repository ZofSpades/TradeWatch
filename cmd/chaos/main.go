// Command chaos supports `make chaos`, which kills, restarts and rescales
// the pipeline's consumers mid-stream and checks that no alert is lost or
// duplicated.
//
//	chaos duplicate -session S [-tail N]
//	    re-publishes exact copies of the last N records of every partition
//	    of the raw feeds and market_events, and of every alert of session S
//	chaos verify -session S -recording F -faults F [-out dir]
//	    waits for the consumer groups to drain, compares the alerts Postgres
//	    stored for S with an offline replay of its recording, and writes the
//	    report; it exits non-zero if the comparison fails
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/chaos"
	"github.com/ZofSpades/tradewatch/internal/kafka"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(1)
	}
}

func usage() error {
	return fmt.Errorf("usage: chaos duplicate|verify [flags]")
}

func run() error {
	if len(os.Args) < 2 {
		return usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	session := fs.String("session", "", "the session under test")
	brokers := fs.String("brokers", os.Getenv("TW_IT_KAFKA_BROKERS"), "Kafka brokers, comma-separated")
	kcfg := func() kafka.Config {
		return kafka.Config{Brokers: strings.Split(*brokers, ","), ClientID: "chaos", Partitions: 6, Replication: 1, LogLevel: kgo.LogLevelWarn}
	}
	switch os.Args[1] {
	case "duplicate":
		tail := fs.Int64("tail", 5000, "records to copy from the end of every partition")
		fs.Parse(os.Args[2:])
		if *session == "" {
			return fmt.Errorf("-session is required")
		}
		topics := kcfg().Topics()
		counts, err := chaos.Duplicate(ctx, kcfg(), log, append(topics.Raw(), topics.MarketEvents), *tail)
		if err != nil {
			return err
		}
		alerts, err := chaos.DuplicateSession(ctx, kcfg(), log, topics.Alerts, *session)
		if err != nil {
			return err
		}
		raw := 0
		for _, t := range topics.Raw() {
			raw += counts[t]
		}
		fmt.Printf("re-published copies of %d raw records, %d market_events records and all %d alert records of the session\n",
			raw, counts[topics.MarketEvents], alerts)
		return nil
	case "verify":
		recording := fs.String("recording", "", "the session's recording")
		faults := fs.String("faults", "", "the chaos script's fault log")
		out := fs.String("out", "docs/results/chaos", "directory the report is written to")
		rulesDir := fs.String("rules", "rules", "rules directory the detectors run")
		postgres := fs.String("postgres", os.Getenv("TW_IT_POSTGRES_URL"), "Postgres URL of the alert store")
		prom := fs.String("prometheus", "http://prometheus:9090", "Prometheus, for the recovery counters (empty to skip)")
		commit := fs.String("commit", "", "the commit under test")
		drain := fs.Duration("drain", 5*time.Minute, "how long to wait for the consumer groups to drain")
		fs.Parse(os.Args[2:])
		if *session == "" || *recording == "" || *faults == "" {
			return fmt.Errorf("-session, -recording and -faults are required")
		}
		f, err := os.Open(*faults)
		if err != nil {
			return err
		}
		fl, err := chaos.ParseFaults(f)
		f.Close()
		if err != nil {
			return err
		}
		res, err := chaos.Verify(ctx, chaos.Config{
			Session: *session, Recording: *recording, RulesDir: *rulesDir, PostgresURL: *postgres, Kafka: kcfg(),
			PrometheusURL: *prom, Faults: fl, Groups: []string{"ingest", "detector", "alertsink"}, Drain: *drain,
			Commit: *commit, Log: log,
		})
		if err != nil {
			return err
		}
		if err := chaos.Write(*out, res); err != nil {
			return err
		}
		c := res.Comparison
		fmt.Printf("expected %d alerts, stored %d: %d missing, %d unexpected, %d differing, %d raised twice; %d deliveries on the alerts topic\n",
			c.Expected, c.Stored, len(c.Missing), len(c.Unexpected), len(c.Differing), len(res.RaisedTwice), res.AlertRecords)
		if !res.Passed {
			return fmt.Errorf("FAILED: see %s/README.md", *out)
		}
		fmt.Printf("PASSED: report written to %s\n", *out)
		return nil
	default:
		return usage()
	}
}
