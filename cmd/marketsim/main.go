// Command marketsim simulates an exchange and publishes its order, cancel,
// trade and book-snapshot feeds to Kafka. All data is synthetic.
//
// Configuration is read from TW_* environment variables; see
// internal/marketsim.LoadConfig. `marketsim healthcheck` probes the admin
// server for container health checks.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/logging"
	"github.com/ZofSpades/tradewatch/internal/marketsim"
	"github.com/ZofSpades/tradewatch/internal/metrics"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(metrics.Probe(config.FromOS().String("TW_HTTP_ADDR", ":8090")))
	}
	log := logging.New("marketsim")
	if err := run(log); err != nil {
		log.Error("marketsim failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := marketsim.LoadConfig(config.FromOS())
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cl, err := kafka.NewClient(cfg.Kafka, log,
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.MaxBufferedRecords(100_000),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	reg := metrics.NewRegistry()
	runner := marketsim.NewRunner(cfg, cl, reg, log)
	srv := metrics.NewServer(cfg.HTTPAddr, reg, log)
	runner.Register(srv)
	srv.SetReady(true)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return srv.Run(gctx) })
	g.Go(func() error {
		err := runner.Run(gctx)
		if err == nil && cfg.Sessions > 0 {
			log.Info("all sessions completed")
			stop() // a bounded run exits once its sessions are done
		}
		return err
	})
	return g.Wait()
}
