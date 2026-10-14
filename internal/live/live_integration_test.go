//go:build integration

package live

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/store"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

func setup(t *testing.T, keep int) (*Views, *store.Store, redis.UniversalClient) {
	t.Helper()
	ctx := context.Background()
	url := testutil.PostgresSchema(t)
	if _, err := store.Migrate(ctx, url, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	rdb := redis.NewClient(&redis.Options{Addr: testutil.RedisAddr(t)})
	t.Cleanup(func() { rdb.Close() })
	prefix := testutil.UniquePrefix(t)
	t.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), prefix+"*").Result()
		if len(keys) > 0 {
			rdb.Del(context.Background(), keys...)
		}
	})
	return New(rdb, prefix, keep), s, rdb
}

func alert(i int, typ twpb.AbuseType, symbol string) *twpb.Alert {
	return &twpb.Alert{
		AlertKey: fmt.Sprintf("k%02d", i), SessionId: "S1", AbuseType: typ, Detector: "d", RuleId: "R", RuleVersion: 1,
		Severity: twpb.Severity_SEVERITY_HIGH, Score: 60, Symbol: symbol, AccountIds: []string{"A"},
		DetectedAtNs: int64(i) * int64(time.Second), Explanation: "e",
	}
}

func TestRaisedUpdatedAndRebuild(t *testing.T) {
	v, s, _ := setup(t, 3)
	ctx := context.Background()
	events, err := v.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var batch []*twpb.Alert
	for i := range 5 {
		typ := twpb.AbuseType_ABUSE_TYPE_SPOOFING
		if i%2 == 0 {
			typ = twpb.AbuseType_ABUSE_TYPE_WASH_TRADING
		}
		batch = append(batch, alert(i, typ, "QZAP"))
	}
	raised, _, err := s.UpsertAlerts(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Raised(ctx, raised); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		select {
		case ev := <-events:
			if ev.Type != EventRaised || ev.Alert.Key != fmt.Sprintf("k%02d", i) {
				t.Fatalf("event %d = %+v", i, ev)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("event %d not received", i)
		}
	}
	counts, err := v.Counts(ctx)
	if err != nil || counts.Total != 5 || counts.ByType["wash_trading"] != 3 || counts.BySymbol["QZAP"] != 5 {
		t.Fatalf("counts %+v, err %v", counts, err)
	}
	recent, err := v.Recent(ctx, 10)
	if err != nil || len(recent) != 3 {
		t.Fatalf("recent keeps %d alerts (err %v), want the 3 newest", len(recent), err)
	}

	a, err := s.Transition(ctx, "k04", store.StatusTriaged, "ana", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Updated(ctx, a); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-events:
		if ev.Type != EventUpdated || ev.Alert.Key != "k04" || ev.Alert.Status != "triaged" {
			t.Fatalf("update event %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("update event not received")
	}

	// Simulate a sink that stored alerts but died before updating Redis,
	// and Redis losing a counter: a rebuild restores both from Postgres.
	more, _, err := s.UpsertAlerts(ctx, []*twpb.Alert{alert(5, twpb.AbuseType_ABUSE_TYPE_QUOTE_STUFFING, "MOSS")})
	if err != nil || len(more) != 1 {
		t.Fatal(err)
	}
	if err := v.rdb.HDel(ctx, v.countsKey(), "type:spoofing").Err(); err != nil {
		t.Fatal(err)
	}
	if err := v.Rebuild(ctx, s); err != nil {
		t.Fatal(err)
	}
	counts, err = v.Counts(ctx)
	if err != nil || counts.Total != 6 || counts.ByType["spoofing"] != 2 || counts.ByType["quote_stuffing"] != 1 ||
		counts.BySymbol["MOSS"] != 1 {
		t.Fatalf("counts after rebuild %+v, err %v", counts, err)
	}
	recent, err = v.Recent(ctx, 10)
	if err != nil || len(recent) != 3 || recent[0].Key != "k05" {
		t.Fatalf("recent after rebuild %+v, err %v", recent, err)
	}
	for _, r := range recent {
		if r.Key == "k04" && r.Status != "triaged" {
			t.Errorf("rebuilt summary of k04 has status %s", r.Status)
		}
	}
}

func TestRebuildOfAnEmptyStore(t *testing.T) {
	v, s, _ := setup(t, 10)
	ctx := context.Background()
	if err := v.Rebuild(ctx, s); err != nil {
		t.Fatal(err)
	}
	counts, err := v.Counts(ctx)
	if err != nil || counts.Total != 0 {
		t.Fatalf("counts %+v, err %v", counts, err)
	}
	if recent, err := v.Recent(ctx, 10); err != nil || len(recent) != 0 {
		t.Fatalf("recent %+v, err %v", recent, err)
	}
}
