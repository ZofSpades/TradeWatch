// Package testutil holds helpers shared by tests.
//
// Integration tests are compiled only with the "integration" build tag and
// expect the infrastructure started by `make test` (docker compose). The
// connection details come from TW_IT_* environment variables; a missing
// variable fails the test instead of skipping it, so a misconfigured CI run
// cannot pass silently.
package testutil

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"regexp"
	"strings"
	"testing"
)

func required(t testing.TB, key string) string {
	t.Helper()
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		t.Fatalf("%s is not set; run integration tests with `make test`", key)
	}
	return v
}

// KafkaBrokers returns the bootstrap brokers of the test Kafka cluster.
func KafkaBrokers(t testing.TB) []string {
	return strings.Split(required(t, "TW_IT_KAFKA_BROKERS"), ",")
}

// PostgresURL returns the connection URL of the test database.
func PostgresURL(t testing.TB) string { return required(t, "TW_IT_POSTGRES_URL") }

// RedisAddr returns the address of the test Redis server.
func RedisAddr(t testing.TB) string { return required(t, "TW_IT_REDIS_ADDR") }

var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

// UniquePrefix returns a resource-name prefix unique to this test run, such
// as "it-ensuretopics-3fa9c2-", so tests never share topics or consumer
// groups with each other or with a running stack.
func UniquePrefix(t testing.TB) string {
	t.Helper()
	var b [3]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	name := nonAlnum.ReplaceAllString(strings.ToLower(t.Name()), "")
	if len(name) > 24 {
		name = name[:24]
	}
	return "it-" + name + "-" + hex.EncodeToString(b[:]) + "-"
}
