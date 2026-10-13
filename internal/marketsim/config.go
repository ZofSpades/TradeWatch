// Package marketsim runs simulated trading sessions (internal/sim) and
// publishes their events to Kafka, paced against the wall clock.
package marketsim

import (
	"fmt"
	"strings"
	"time"

	"github.com/ZofSpades/tradewatch/internal/config"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

// PaceMode controls how fast simulated time runs relative to the wall clock.
type PaceMode string

const (
	// PaceRealtime runs Speed simulated seconds per wall-clock second.
	PaceRealtime PaceMode = "realtime"
	// PaceFast runs as fast as the pipeline accepts events.
	PaceFast PaceMode = "fast"
	// PaceRate publishes a fixed number of events per wall-clock second,
	// used by load tests.
	PaceRate PaceMode = "rate"
)

// Config configures the marketsim service.
type Config struct {
	Kafka kafka.Config

	Seed uint64
	// Sessions is the number of sessions to run; 0 runs forever.
	Sessions int
	// SessionPrefix prefixes session IDs. Empty derives it from the start
	// mode: "live-<start time>" for wall-clock sessions, "seed<seed>"
	// otherwise.
	SessionPrefix string
	// Start is the opening time of the first session. Zero opens every
	// session at the wall-clock time it starts, which is what the live demo
	// uses. A fixed Start makes event times, and therefore the whole run,
	// reproducible.
	Start            time.Time
	Duration         time.Duration
	ClosingWindow    time.Duration
	SnapshotInterval time.Duration
	Activity         float64
	Symbols          []string

	Pace  PaceMode
	Speed float64
	Rate  float64

	// Injection sets the rates of injected market abuse (see sim.Injection).
	Injection sim.Injection

	// MalformedRate is the fraction of published records deliberately
	// corrupted to exercise validation and the dead-letter topic. Recordings
	// always hold the clean events.
	MalformedRate float64

	// RecordDir, when set, receives one recording file per session.
	RecordDir string
	// AutoStart starts the first session immediately; otherwise the
	// service waits for POST /api/v1/sim/start.
	AutoStart bool
	HTTPAddr  string
}

// LoadConfig reads the TW_SIM_* environment variables.
func LoadConfig(env *config.Env) (Config, error) {
	def := sim.DefaultConfig()
	cfg := Config{
		Kafka:            kafka.LoadConfig(env, "marketsim"),
		Seed:             env.Uint64("TW_SIM_SEED", 1),
		Sessions:         env.Int("TW_SIM_SESSIONS", 0),
		SessionPrefix:    env.String("TW_SIM_SESSION_PREFIX", ""),
		Duration:         env.Duration("TW_SIM_SESSION_DURATION", def.Duration),
		ClosingWindow:    env.Duration("TW_SIM_CLOSING_WINDOW", def.ClosingWindow),
		SnapshotInterval: env.Duration("TW_SIM_SNAPSHOT_INTERVAL", def.SnapshotInterval),
		Activity:         env.Float("TW_SIM_ACTIVITY", 1),
		Symbols:          env.List("TW_SIM_SYMBOLS", nil),
		Pace:             PaceMode(strings.ToLower(env.String("TW_SIM_PACE", string(PaceRealtime)))),
		Speed:            env.Float("TW_SIM_SPEED", 1),
		Rate:             env.Float("TW_SIM_RATE", 10_000),
		RecordDir:        env.String("TW_SIM_RECORD_DIR", ""),
		MalformedRate:    env.Float("TW_SIM_MALFORMED_RATE", 0),
		Injection: sim.Injection{
			WashTrading:      env.Float("TW_SIM_INJECT_WASH_TRADING", 0),
			Spoofing:         env.Float("TW_SIM_INJECT_SPOOFING", 0),
			MomentumIgnition: env.Float("TW_SIM_INJECT_MOMENTUM_IGNITION", 0),
			QuoteStuffing:    env.Float("TW_SIM_INJECT_QUOTE_STUFFING", 0),
			MarkingTheClose:  env.Float("TW_SIM_INJECT_MARKING_THE_CLOSE", 0),
		},
		AutoStart: env.Bool("TW_SIM_AUTOSTART", true),
		HTTPAddr:  env.String("TW_HTTP_ADDR", ":8090"),
	}
	if s := env.String("TW_SIM_START", ""); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return cfg, fmt.Errorf("TW_SIM_START: %w", err)
		}
		cfg.Start = t
	}
	if err := env.Err(); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

// Validate reports configuration errors.
func (c Config) Validate() error {
	switch c.Pace {
	case PaceRealtime:
		if c.Speed <= 0 {
			return fmt.Errorf("marketsim: speed must be positive, got %v", c.Speed)
		}
	case PaceRate:
		if c.Rate <= 0 {
			return fmt.Errorf("marketsim: rate must be positive, got %v", c.Rate)
		}
	case PaceFast:
	default:
		return fmt.Errorf("marketsim: unknown pace mode %q", c.Pace)
	}
	if c.Sessions < 0 {
		return fmt.Errorf("marketsim: sessions must be >= 0")
	}
	if err := c.Injection.Validate(); err != nil {
		return err
	}
	if c.MalformedRate < 0 || c.MalformedRate > 1 {
		return fmt.Errorf("marketsim: malformed rate must be within [0, 1], got %v", c.MalformedRate)
	}
	return c.Kafka.Validate()
}

// sessionConfig returns the simulator configuration of session n (0-based).
// prefix is the resolved session ID prefix.
func (c Config) sessionConfig(n int, prefix string, wallNow time.Time) sim.Config {
	sc := sim.DefaultConfig()
	sc.Seed = c.Seed + uint64(n)
	sc.SessionID = fmt.Sprintf("%s-%03d", prefix, n+1)
	sc.Open = wallNow.UTC().Truncate(time.Millisecond)
	if !c.Start.IsZero() {
		sc.Open = c.Start.Add(time.Duration(n) * 24 * time.Hour)
	}
	sc.Duration = c.Duration
	sc.ClosingWindow = c.ClosingWindow
	sc.SnapshotInterval = c.SnapshotInterval
	sc.Activity = c.Activity
	sc.Symbols = c.Symbols
	sc.Injection = c.Injection
	return sc
}

// prefix resolves the session ID prefix for a run that started at started.
func (c Config) prefix(started time.Time) string {
	switch {
	case c.SessionPrefix != "":
		return c.SessionPrefix
	case c.Start.IsZero():
		return "live-" + started.UTC().Format("20060102T150405Z")
	default:
		return fmt.Sprintf("seed%d", c.Seed)
	}
}
