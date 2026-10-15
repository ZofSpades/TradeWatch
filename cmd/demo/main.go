// Command demo supports `make demo`. With the stack running, it injects
// labelled abuse scenarios into the simulated market through the API and
// shows, for each one, its ground truth and the alert the detectors raised.
//
//	demo [-scenarios a,b,c] [-attempts N]
//
// It signs in as admin with TW_API_ADMIN_PASSWORD and reads the labels from
// the groundtruth topic on TW_KAFKA_BROKERS. It exits non-zero if a scenario
// raised no matching alert in any attempt.
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

	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/demo"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

func run() error {
	api := flag.String("api", "http://web:8080", "the dashboard's origin: the demo calls the API through its proxy, as the dashboard does")
	dashboard := flag.String("dashboard", "http://localhost:3000", "the dashboard's URL as a browser reaches it, for links")
	scenarios := flag.String("scenarios", strings.Join(demo.DefaultScenarios, ","),
		"scenarios to inject, comma-separated, from "+strings.Join(sim.Scenarios, ", "))
	attempts := flag.Int("attempts", 3, "injections of a scenario before the demo gives up on it")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	env := config.FromOS()
	password := env.Required("TW_API_ADMIN_PASSWORD")
	kcfg := kafka.LoadConfig(env, "demo")
	if err := env.Err(); err != nil {
		return err
	}

	// Follow the labels before the first trigger, so none is missed.
	labels, err := demo.FollowLabels(ctx, kcfg, log)
	if err != nil {
		return err
	}
	defer labels.Close()
	client := demo.NewAPI(*api)
	if err := client.Login(ctx, "admin", password); err != nil {
		return fmt.Errorf("sign in as admin: %w", err)
	}
	return demo.Run(ctx, demo.Config{
		API: client, Labels: labels, Scenarios: strings.Split(*scenarios, ","), Attempts: max(1, *attempts),
		// A wash ring can run for about seven minutes of simulated time:
		// up to 12 legs, up to 28 s apart, and up to 2 minutes waiting for
		// room inside the spread. A matching alert's evidence may start up
		// to a minute after the episode ends (eval.Grace).
		LabelTimeout: 8 * time.Minute, AlertTimeout: 90 * time.Second, MinSessionLeft: 5 * time.Minute,
		Poll: time.Second, Dashboard: *dashboard, Out: os.Stdout,
	})
}
