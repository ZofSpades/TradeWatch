// Command ingest validates, normalizes and enriches the raw exchange feeds,
// sends rejected records to the dead-letter topic, and publishes the
// per-symbol sequenced market_events stream.
//
// Configuration is read from TW_* environment variables; see
// internal/ingest.LoadConfig. `ingest healthcheck` probes the admin server.
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
	"github.com/ZofSpades/tradewatch/internal/ingest"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/logging"
	"github.com/ZofSpades/tradewatch/internal/metrics"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(metrics.Probe(config.FromOS().String("TW_HTTP_ADDR", ":8091")))
	}
	log := logging.New("ingest")
	if err := run(log); err != nil {
		log.Error("ingest failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := ingest.LoadConfig(config.FromOS())
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := metrics.NewRegistry()
	svc := ingest.NewService(cfg, reg, log)

	cl, err := kafka.NewClient(cfg.Kafka, log, svc.ClientOptions()...)
	if err != nil {
		return err
	}
	defer cl.Close()

	refCfg := cfg.Kafka
	refCfg.ClientID += "-refdata"
	refCl, err := kafka.NewClient(refCfg, log,
		kgo.ConsumeTopics(cfg.Kafka.Topics().RefData),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer refCl.Close()

	svc.Attach(cl, ingest.NewRefDataTail(refCl, cfg.Kafka.Topics().RefData, log))
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
