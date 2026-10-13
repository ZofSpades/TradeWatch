// Package logging builds the structured logger shared by all services.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON slog.Logger tagged with the service name. The level is
// read from TW_LOG_LEVEL (debug, info, warn, error; default info).
func New(service string) *slog.Logger {
	return NewWithWriter(os.Stdout, service, os.Getenv("TW_LOG_LEVEL"))
}

// NewWithWriter is New with an explicit destination and level.
func NewWithWriter(w io.Writer, service, level string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: ParseLevel(level)})
	return slog.New(h).With("service", service)
}

// ParseLevel maps a level name to a slog.Level, defaulting to info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
