// Command bench load-tests the running pipeline at increasing event rates
// and writes a report: throughput at each stage, consumer lag, latency from
// an event's publication to its stored alert, and the resources each
// service used. `make bench` runs it in the toolbox container against the
// compose stack, with marketsim configured by deploy/compose/bench.yml.
//
//	bench [-rates 1000,2000,...] [-warmup 20s] [-measure 60s] [-out dir]
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ZofSpades/tradewatch/internal/bench"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run() error {
	rates := flag.String("rates", "1000,2000,5000,10000,20000,30000,40000,50000,60000,80000,100000",
		"event rates to offer, in events per second, in order; the run stops at the first one the pipeline does not sustain")
	warmup := flag.Duration("warmup", 20*time.Second, "how long each rate runs before it is measured")
	measure := flag.Duration("measure", 60*time.Second, "length of each measurement window")
	out := flag.String("out", "docs/results/bench", "directory the report is written to")
	marketsim := flag.String("marketsim", "http://marketsim:8090", "marketsim's admin server")
	postgres := flag.String("postgres", os.Getenv("TW_IT_POSTGRES_URL"), "Postgres URL of the alert store")
	docker := flag.String("docker", "", "Docker's version, as the host reports it")
	commit := flag.String("commit", "", "the commit under test")
	flag.Parse()

	var rs []float64
	for _, f := range strings.Split(*rates, ",") {
		r, err := strconv.ParseFloat(strings.TrimSpace(f), 64)
		if err != nil || r <= 0 {
			return fmt.Errorf("bad rate %q", f)
		}
		rs = append(rs, r)
	}
	if *postgres == "" {
		return fmt.Errorf("no Postgres URL: set -postgres or TW_IT_POSTGRES_URL")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	res, err := bench.Run(ctx, bench.Config{
		Rates: rs, Warmup: *warmup, Measure: *measure, SampleEvery: 2 * time.Second,
		MarketsimURL: *marketsim, PostgresURL: *postgres, Targets: bench.DefaultTargets, Log: log,
	}, bench.DetectEnvironment(*docker, *commit))
	if err != nil {
		return err // a partial run is not reported: it would claim too much
	}
	if err := bench.Write(*out, res); err != nil {
		return err
	}
	log.Info("report written", "dir", *out)
	return nil
}
