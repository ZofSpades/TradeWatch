package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/config"
)

// Config is the connection and topology configuration shared by services.
type Config struct {
	Brokers  []string
	ClientID string
	// Prefix is prepended to every topic name.
	Prefix string
	// Partitions is the partition count of the symbol-keyed topics.
	Partitions int32
	// Replication is the replication factor used when creating topics.
	Replication int16
	// LogLevel is the franz-go client log level.
	LogLevel kgo.LogLevel
}

// LoadConfig reads TW_KAFKA_* variables.
func LoadConfig(env *config.Env, clientID string) Config {
	cfg := Config{
		Brokers:     env.List("TW_KAFKA_BROKERS", []string{"localhost:29092"}),
		ClientID:    env.String("TW_KAFKA_CLIENT_ID", clientID),
		Prefix:      env.String("TW_KAFKA_TOPIC_PREFIX", ""),
		Partitions:  int32(env.Int("TW_KAFKA_PARTITIONS", 6)),
		Replication: int16(env.Int("TW_KAFKA_REPLICATION", 1)),
		LogLevel:    parseLogLevel(env.String("TW_KAFKA_LOG_LEVEL", "warn")),
	}
	return cfg
}

// Topics returns the topic names for this configuration.
func (c Config) Topics() Topics { return NewTopics(c.Prefix) }

// Validate reports configuration errors.
func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("kafka: no brokers configured")
	}
	if c.Partitions < 1 {
		return fmt.Errorf("kafka: partitions must be >= 1, got %d", c.Partitions)
	}
	if c.Replication < 1 {
		return fmt.Errorf("kafka: replication must be >= 1, got %d", c.Replication)
	}
	return nil
}

// NewClient builds a franz-go client with the shared base options followed
// by opts.
func NewClient(cfg Config, log *slog.Logger, opts ...kgo.Opt) (*kgo.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	base := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.WithLogger(&slogLogger{log: log.With("component", "kgo"), level: cfg.LogLevel}),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return cl, nil
}

func parseLogLevel(s string) kgo.LogLevel {
	switch strings.ToLower(s) {
	case "debug":
		return kgo.LogLevelDebug
	case "info":
		return kgo.LogLevelInfo
	case "error":
		return kgo.LogLevelError
	case "none", "off":
		return kgo.LogLevelNone
	default:
		return kgo.LogLevelWarn
	}
}

// slogLogger adapts slog to the kgo.Logger interface.
type slogLogger struct {
	log   *slog.Logger
	level kgo.LogLevel
}

func (s *slogLogger) Level() kgo.LogLevel { return s.level }

func (s *slogLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var l slog.Level
	switch level {
	case kgo.LogLevelError:
		l = slog.LevelError
	case kgo.LogLevelWarn:
		l = slog.LevelWarn
	case kgo.LogLevelInfo:
		l = slog.LevelInfo
	default:
		l = slog.LevelDebug
	}
	s.log.Log(context.Background(), l, msg, keyvals...)
}
