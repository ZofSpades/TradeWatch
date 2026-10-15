package auth

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// FailureLog counts each username's failed logins for the lockout.
type FailureLog interface {
	// Failures returns how many failures username had within window
	// before now.
	Failures(ctx context.Context, username string, now time.Time, window time.Duration) (int, error)
	// Fail records a failure at now.
	Fail(ctx context.Context, username string, now time.Time, window time.Duration) error
	// Forget drops the username's failures, after a successful login.
	Forget(ctx context.Context, username string) error
}

// memoryFailures is the failure log of a single API instance.
type memoryFailures struct {
	mu       sync.Mutex
	failures map[string][]time.Time
}

func newMemoryFailures() *memoryFailures { return &memoryFailures{failures: map[string][]time.Time{}} }

func (m *memoryFailures) Failures(_ context.Context, username string, now time.Time, window time.Duration) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.prune(username, now, window)), nil
}

func (m *memoryFailures) Fail(_ context.Context, username string, now time.Time, window time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[username] = append(m.prune(username, now, window), now)
	return nil
}

func (m *memoryFailures) Forget(_ context.Context, username string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.failures, username)
	return nil
}

// prune drops failures at least window old; the caller holds mu.
func (m *memoryFailures) prune(username string, now time.Time, window time.Duration) []time.Time {
	fs := m.failures[username]
	i := 0
	for i < len(fs) && now.Sub(fs[i]) >= window {
		i++
	}
	fs = fs[i:]
	if len(fs) == 0 {
		delete(m.failures, username)
	} else {
		m.failures[username] = fs
	}
	return fs
}

// RedisFailures keeps failed logins in Redis, so that every API instance
// enforces the same lockout. Each username has a sorted set of its failures,
// scored by time in microseconds, which expires a window after its latest
// failure.
type RedisFailures struct {
	rdb    redis.UniversalClient
	prefix string
}

// NewRedisFailures returns a failure log under the key prefix, for example
// "tw:".
func NewRedisFailures(rdb redis.UniversalClient, prefix string) *RedisFailures {
	return &RedisFailures{rdb: rdb, prefix: prefix}
}

func (f *RedisFailures) key(username string) string { return f.prefix + "auth:failures:" + username }

func (f *RedisFailures) Failures(ctx context.Context, username string, now time.Time, window time.Duration) (int, error) {
	key := f.key(username)
	var n *redis.IntCmd
	_, err := f.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(now.Add(-window).UnixMicro(), 10))
		n = p.ZCard(ctx, key)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int(n.Val()), nil
}

func (f *RedisFailures) Fail(ctx context.Context, username string, now time.Time, window time.Duration) error {
	var id [6]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	key := f.key(username)
	_, err := f.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZAdd(ctx, key, redis.Z{Score: float64(now.UnixMicro()), Member: fmt.Sprintf("%d-%x", now.UnixMicro(), id)})
		p.PExpire(ctx, key, window)
		return nil
	})
	return err
}

func (f *RedisFailures) Forget(ctx context.Context, username string) error {
	return f.rdb.Del(ctx, f.key(username)).Err()
}
