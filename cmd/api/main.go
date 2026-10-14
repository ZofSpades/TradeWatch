// Command api serves TradeWatch's REST and server-sent-events API.
//
// Configuration is read from TW_* environment variables; see
// internal/api.LoadConfig. The signing secret and the two account
// passwords come from the environment (`make env` generates them into
// .env). `api healthcheck` probes the admin server.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/ZofSpades/tradewatch/internal/api"
	"github.com/ZofSpades/tradewatch/internal/auth"
	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/logging"
	"github.com/ZofSpades/tradewatch/internal/metrics"
	"github.com/ZofSpades/tradewatch/internal/store"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(metrics.Probe(config.FromOS().String("TW_HTTP_ADDR", ":8094")))
	}
	log := logging.New("api")
	if err := run(log); err != nil {
		log.Error("api failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := api.LoadConfig(config.FromOS())
	if err != nil {
		return err
	}
	users, err := cfg.Users()
	if err != nil {
		return err
	}
	authn, err := auth.New(auth.Config{
		Secret: []byte(cfg.JWTSecret), TTL: cfg.TokenTTL, Issuer: "tradewatch", Audience: "tradewatch-api", Users: users,
	})
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
	srv, err := api.NewServer(st, live.New(rdb, cfg.RedisPrefix, 500), authn, cfg.SimURL, reg, log)
	if err != nil {
		return err
	}
	admin := metrics.NewServer(cfg.HTTPAddr, reg, log)
	admin.SetReady(true)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return admin.Run(gctx) })
	g.Go(func() error {
		defer stop()
		return srv.Run(gctx, cfg.Addr)
	})
	return g.Wait()
}
