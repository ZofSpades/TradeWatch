// Package config reads service configuration from environment variables.
//
// Every TradeWatch service is configured exclusively through TW_* environment
// variables so that the same binary runs unchanged under docker compose,
// Kubernetes and tests. Parse errors are accumulated rather than returned one
// at a time, so a misconfigured service reports every problem at once.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env reads typed values from an environment and collects parse errors.
type Env struct {
	lookup func(string) (string, bool)
	errs   []error
}

// FromOS returns an Env backed by the process environment.
func FromOS() *Env { return &Env{lookup: os.LookupEnv} }

// FromMap returns an Env backed by a map, for tests.
func FromMap(m map[string]string) *Env {
	return &Env{lookup: func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}}
}

func (e *Env) get(key string) (string, bool) {
	v, ok := e.lookup(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	return v, v != ""
}

func (e *Env) fail(key string, err error) {
	e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
}

// String returns the value of key, or def when it is unset or empty.
func (e *Env) String(key, def string) string {
	if v, ok := e.get(key); ok {
		return v
	}
	return def
}

// Required returns the value of key and records an error when it is unset.
func (e *Env) Required(key string) string {
	v, ok := e.get(key)
	if !ok {
		e.fail(key, errors.New("required but not set"))
	}
	return v
}

// Int returns key parsed as a base-10 integer.
func (e *Env) Int(key string, def int) int {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.fail(key, fmt.Errorf("not an integer: %q", v))
		return def
	}
	return n
}

// Uint64 returns key parsed as an unsigned base-10 integer.
func (e *Env) Uint64(key string, def uint64) uint64 {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		e.fail(key, fmt.Errorf("not an unsigned integer: %q", v))
		return def
	}
	return n
}

// Float returns key parsed as a float64.
func (e *Env) Float(key string, def float64) float64 {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		e.fail(key, fmt.Errorf("not a number: %q", v))
		return def
	}
	return f
}

// Bool returns key parsed with strconv.ParseBool.
func (e *Env) Bool(key string, def bool) bool {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.fail(key, fmt.Errorf("not a boolean: %q", v))
		return def
	}
	return b
}

// Duration returns key parsed with time.ParseDuration.
func (e *Env) Duration(key string, def time.Duration) time.Duration {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.fail(key, fmt.Errorf("not a duration: %q", v))
		return def
	}
	return d
}

// List returns key split on commas, with surrounding whitespace and empty
// elements removed.
func (e *Env) List(key string, def []string) []string {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// Err returns all accumulated errors joined, or nil.
func (e *Env) Err() error { return errors.Join(e.errs...) }
