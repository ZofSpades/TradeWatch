//go:build integration

package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ZofSpades/tradewatch/internal/testutil"
)

func TestRedisFailuresLockTheUsernameAcrossInstances(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: testutil.RedisAddr(t)})
	t.Cleanup(func() { rdb.Close() })
	prefix := testutil.UniquePrefix(t)
	t.Cleanup(func() { rdb.Del(context.Background(), prefix+"auth:failures:ana") })
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	instance := func() *Authenticator {
		a, err := New(Config{Secret: secret, TTL: time.Hour, Issuer: "tradewatch", Audience: "tradewatch-api",
			Users: []User{mustUser(t, "ana", RoleAnalyst, "correct horse battery")}, Failures: NewRedisFailures(rdb, prefix)})
		if err != nil {
			t.Fatal(err)
		}
		a.now = func() time.Time { return now }
		return a
	}
	a, b := instance(), instance()
	ctx := context.Background()
	for i := range 5 {
		inst := a
		if i%2 == 1 {
			inst = b
		}
		if _, _, err := inst.Login(ctx, "ana", "guess"); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
	for name, inst := range map[string]*Authenticator{"a": a, "b": b} {
		if _, _, err := inst.Login(ctx, "ana", "correct horse battery"); !errors.Is(err, ErrTooManyAttempts) {
			t.Errorf("instance %s after five failures across both: %v", name, err)
		}
	}
	if ttl := rdb.PTTL(ctx, prefix+"auth:failures:ana").Val(); ttl <= 0 || ttl > 15*time.Minute {
		t.Errorf("failures expire in %v, want within the window", ttl)
	}

	// The first failure leaves the window 15 minutes after it happened.
	now = now.Add(15*time.Minute - 5*time.Second)
	if _, _, err := b.Login(ctx, "ana", "correct horse battery"); err != nil {
		t.Fatalf("once the oldest failure left the window: %v", err)
	}
	if n, err := a.failures.Failures(ctx, "ana", now, 15*time.Minute); err != nil || n != 0 {
		t.Errorf("%d failures left after a successful login (err %v)", n, err)
	}
}
