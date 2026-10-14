// Command detector runs TradeWatch's market-abuse detectors over the
// market_events stream and publishes alerts.
//
// Configuration is read from TW_* environment variables; see
// internal/detector.LoadConfig. `detector healthcheck` probes the admin
// server; `detector validate-rules [dir]` checks a rules directory and exits.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/detector"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/logging"
	"github.com/ZofSpades/tradewatch/internal/metrics"
	"github.com/ZofSpades/tradewatch/internal/rules"
	"github.com/ZofSpades/tradewatch/internal/tracing"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "healthcheck":
			os.Exit(metrics.Probe(config.FromOS().String("TW_HTTP_ADDR", ":8092")))
		case "validate-rules":
			dir := "rules"
			if len(os.Args) > 2 {
				dir = os.Args[2]
			}
			os.Exit(validateRules(dir))
		}
	}
	log := logging.New("detector")
	if err := run(log); err != nil {
		log.Error("detector failed", "err", err)
		os.Exit(1)
	}
}

func validateRules(dir string) int {
	set, err := rules.Load(dir)
	if err == nil {
		err = detect.Validate(set)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	for _, r := range set.Rules {
		fmt.Printf("%-14s v%-3d %-20s enabled=%-5v %s\n", r.ID, r.Version, r.Detector, r.Enabled, r.Taxonomy.Code)
	}
	fmt.Printf("rule set %s is valid\n", set.Digest[:12])
	return 0
}

func run(log *slog.Logger) error {
	cfg, err := detector.LoadConfig(config.FromOS())
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	_, flush, err := tracing.Init(ctx, config.FromOS(), "detector", log)
	if err != nil {
		return err
	}
	defer flush()

	reg := metrics.NewRegistry()
	svc, err := detector.NewService(cfg, reg, log)
	if err != nil {
		return err
	}
	cl, err := kafka.NewClient(cfg.Kafka, log, svc.ClientOptions()...)
	if err != nil {
		return err
	}
	defer cl.Close()
	svc.Attach(cl)

	srv := metrics.NewServer(cfg.HTTPAddr, reg, log)
	srv.SetReady(true)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return srv.Run(gctx) })
	g.Go(func() error {
		defer stop()
		return svc.Run(gctx)
	})
	return g.Wait()
}
