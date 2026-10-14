// Command alertsink stores the detectors' alerts in Postgres, records the
// rule sets they loaded, and keeps the live views in Redis.
//
// Configuration is read from TW_* environment variables; see
// internal/alertsink.LoadConfig. `alertsink healthcheck` probes the admin
// server; `alertsink migrate` applies the database migrations and exits.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/ZofSpades/tradewatch/internal/alertsink"
	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/logging"
	"github.com/ZofSpades/tradewatch/internal/metrics"
	"github.com/ZofSpades/tradewatch/internal/store"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "healthcheck":
			os.Exit(metrics.Probe(config.FromOS().String("TW_HTTP_ADDR", ":8093")))
		case "migrate":
			log := logging.New("migrate")
			if err := migrate(log); err != nil {
				log.Error("migration failed", "err", err)
				os.Exit(1)
			}
			return
		}
	}
	log := logging.New("alertsink")
	if err := run(log); err != nil {
		log.Error("alertsink failed", "err", err)
		os.Exit(1)
	}
}

func migrate(log *slog.Logger) error {
	env := config.FromOS()
	url := env.Required("TW_POSTGRES_URL")
	if err := env.Err(); err != nil {
		return err
	}
	applied, err := store.Migrate(context.Background(), url, log)
	if err != nil {
		return err
	}
	log.Info("database is up to date", "applied", applied)
	return nil
}

func run(log *slog.Logger) error {
	cfg, err := alertsink.LoadConfig(config.FromOS())
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer st.Close()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	reg := metrics.NewRegistry()
	svc := alertsink.NewService(cfg, st, live.New(rdb, cfg.RedisPrefix, cfg.RecentAlerts), reg, log)
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
