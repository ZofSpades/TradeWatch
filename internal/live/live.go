// Package live keeps TradeWatch's real-time alert views in Redis and fans
// alert events out to every API instance over Redis pub/sub.
//
// Everything here is derived from Postgres and can be rebuilt from it at
// any time (Rebuild), which is what makes the views safe despite Redis
// running without persistence and with LRU eviction. The alert sink adds
// an alert to the views only after Postgres has stored it for the first
// time, so a redelivered alert is never counted twice. An alert stored by
// a sink that died before updating Redis is restored by the Rebuild every
// sink performs when it starts.
package live

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ZofSpades/tradewatch/internal/store"
)

// Event types published on the live channel.
const (
	EventRaised  = "alert.raised"
	EventUpdated = "alert.updated"
)

// Summary is an alert as the live views and the API's alert lists show it.
type Summary struct {
	Key          string    `json:"key"`
	SessionID    string    `json:"session_id"`
	Symbol       string    `json:"symbol"`
	AbuseType    string    `json:"abuse_type"`
	Severity     string    `json:"severity"`
	Score        float64   `json:"score"`
	RuleID       string    `json:"rule_id"`
	RuleVersion  int       `json:"rule_version"`
	TaxonomyCode string    `json:"taxonomy_code"`
	AccountIDs   []string  `json:"account_ids"`
	OwnerIDs     []string  `json:"beneficial_owner_ids"`
	WindowStart  time.Time `json:"window_start"`
	WindowEnd    time.Time `json:"window_end"`
	DetectedAt   time.Time `json:"detected_at"`
	Explanation  string    `json:"explanation"`
	Status       string    `json:"status"`
	Deliveries   int       `json:"deliveries"`
	FirstSeenAt  time.Time `json:"first_seen_at"`
	StatusAt     time.Time `json:"status_changed_at"`
}

// Summarize converts a stored alert.
func Summarize(a *store.Alert) Summary {
	ns := func(v int64) time.Time { return time.Unix(0, v).UTC() }
	list := func(xs []string) []string {
		if xs == nil {
			return []string{}
		}
		return xs
	}
	return Summary{
		Key: a.Key, SessionID: a.SessionID, Symbol: a.Symbol, AbuseType: a.AbuseType, Severity: a.Severity,
		Score: a.Score, RuleID: a.RuleID, RuleVersion: a.RuleVersion, TaxonomyCode: a.TaxonomyCode,
		AccountIDs: list(a.AccountIDs), OwnerIDs: list(a.OwnerIDs), WindowStart: ns(a.WindowStartNs), WindowEnd: ns(a.WindowEndNs),
		DetectedAt: ns(a.DetectedAtNs), Explanation: a.Explanation, Status: string(a.Status), Deliveries: a.Deliveries,
		FirstSeenAt: a.FirstSeenAt.UTC(), StatusAt: a.StatusChangedAt.UTC(),
	}
}

// Event is one message on the live channel. Trace is the W3C traceparent
// of the span that stored a traced alert, so that the API instances
// streaming it continue its trace; clients never see it.
type Event struct {
	Type  string  `json:"type"`
	Alert Summary `json:"alert"`
	Trace string  `json:"trace,omitempty"`
}

// Counts are the live alert totals.
type Counts struct {
	Total    int64            `json:"total"`
	ByType   map[string]int64 `json:"by_abuse_type"`
	BySymbol map[string]int64 `json:"by_symbol"`
}

// Views reads and writes the live views under a key prefix.
type Views struct {
	rdb    redis.UniversalClient
	prefix string
	keep   int
	ttl    time.Duration
}

// New returns views stored under prefix (for example "tw:"), keeping the
// most recent keep alerts in the recent list.
func New(rdb redis.UniversalClient, prefix string, keep int) *Views {
	return &Views{rdb: rdb, prefix: prefix, keep: keep, ttl: 24 * time.Hour}
}

func (v *Views) recentKey() string          { return v.prefix + "live:recent" }
func (v *Views) countsKey() string          { return v.prefix + "live:counts" }
func (v *Views) alertKey(key string) string { return v.prefix + "live:alert:" + key }

// Channel is the pub/sub channel of live events.
func (v *Views) Channel() string { return v.prefix + "live:events" }

func score(a *store.Alert) float64 { return float64(a.FirstSeenAt.UnixMilli()) }

// Raised adds newly stored alerts to the views and publishes them. The
// caller guarantees each alert is passed once, after Postgres stored it
// for the first time. traces maps an alert key to the W3C traceparent of
// the span that stored it, for alerts that are traced; it may be nil.
func (v *Views) Raised(ctx context.Context, alerts []*store.Alert, traces map[string]string) error {
	if len(alerts) == 0 {
		return nil
	}
	_, err := v.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for _, a := range alerts {
			sum := Summarize(a)
			b, err := json.Marshal(sum)
			if err != nil {
				return err
			}
			ev, err := json.Marshal(Event{Type: EventRaised, Alert: sum, Trace: traces[a.Key]})
			if err != nil {
				return err
			}
			p.Set(ctx, v.alertKey(a.Key), b, v.ttl)
			p.ZAdd(ctx, v.recentKey(), redis.Z{Score: score(a), Member: a.Key})
			p.HIncrBy(ctx, v.countsKey(), "total", 1)
			p.HIncrBy(ctx, v.countsKey(), "type:"+a.AbuseType, 1)
			p.HIncrBy(ctx, v.countsKey(), "symbol:"+a.Symbol, 1)
			p.Publish(ctx, v.Channel(), ev)
		}
		p.ZRemRangeByRank(ctx, v.recentKey(), 0, int64(-v.keep-1))
		return nil
	})
	return err
}

// Updated refreshes an alert whose status changed and publishes the change.
func (v *Views) Updated(ctx context.Context, a *store.Alert) error {
	sum := Summarize(a)
	b, err := json.Marshal(sum)
	if err != nil {
		return err
	}
	ev, err := json.Marshal(Event{Type: EventUpdated, Alert: sum})
	if err != nil {
		return err
	}
	_, err = v.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.SetXX(ctx, v.alertKey(a.Key), b, v.ttl)
		p.Publish(ctx, v.Channel(), ev)
		return nil
	})
	return err
}

// Rebuild replaces the views with what Postgres holds: the totals, and the
// most recent alerts.
func (v *Views) Rebuild(ctx context.Context, s *store.Store) error {
	counts, err := s.Count(ctx)
	if err != nil {
		return err
	}
	recent, err := s.Recent(ctx, v.keep)
	if err != nil {
		return err
	}
	tmpRecent, tmpCounts := v.recentKey()+":rebuild", v.countsKey()+":rebuild"
	_, err = v.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, tmpRecent, tmpCounts)
		fields := map[string]any{"total": counts.Total}
		for t, n := range counts.ByType {
			fields["type:"+t] = n
		}
		for sym, n := range counts.BySymbol {
			fields["symbol:"+sym] = n
		}
		p.HSet(ctx, tmpCounts, fields)
		for _, a := range recent {
			b, err := json.Marshal(Summarize(a))
			if err != nil {
				return err
			}
			p.Set(ctx, v.alertKey(a.Key), b, v.ttl)
			p.ZAdd(ctx, tmpRecent, redis.Z{Score: score(a), Member: a.Key})
		}
		p.Rename(ctx, tmpCounts, v.countsKey())
		if len(recent) > 0 {
			p.Rename(ctx, tmpRecent, v.recentKey())
		} else {
			p.Del(ctx, v.recentKey())
		}
		return nil
	})
	return err
}

// Recent returns up to n of the most recently stored alerts, newest first.
// Alerts whose summary has expired are skipped.
func (v *Views) Recent(ctx context.Context, n int) ([]Summary, error) {
	keys, err := v.rdb.ZRevRange(ctx, v.recentKey(), 0, int64(n-1)).Result()
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = v.alertKey(k)
	}
	vals, err := v.rdb.MGet(ctx, full...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Summary, 0, len(vals))
	for _, val := range vals {
		s, ok := val.(string)
		if !ok {
			continue
		}
		var sum Summary
		if err := json.Unmarshal([]byte(s), &sum); err != nil {
			return nil, err
		}
		out = append(out, sum)
	}
	return out, nil
}

// Counts returns the live totals.
func (v *Views) Counts(ctx context.Context) (Counts, error) {
	c := Counts{ByType: map[string]int64{}, BySymbol: map[string]int64{}}
	fields, err := v.rdb.HGetAll(ctx, v.countsKey()).Result()
	if err != nil {
		return c, err
	}
	for f, s := range fields {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return c, fmt.Errorf("live: counter %s: %w", f, err)
		}
		switch {
		case f == "total":
			c.Total = n
		case strings.HasPrefix(f, "type:"):
			c.ByType[strings.TrimPrefix(f, "type:")] = n
		case strings.HasPrefix(f, "symbol:"):
			c.BySymbol[strings.TrimPrefix(f, "symbol:")] = n
		}
	}
	return c, nil
}

// Subscribe delivers live events until ctx ends. Malformed messages are
// skipped. The channel is closed when the subscription ends.
func (v *Views) Subscribe(ctx context.Context) (<-chan Event, error) {
	ps := v.rdb.Subscribe(ctx, v.Channel())
	if _, err := ps.Receive(ctx); err != nil { // wait for the subscription to be confirmed
		ps.Close()
		return nil, err
	}
	out := make(chan Event, 256)
	go func() {
		defer close(out)
		defer ps.Close()
		msgs := ps.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-msgs:
				if !ok {
					return
				}
				var ev Event
				if json.Unmarshal([]byte(m.Payload), &ev) != nil {
					continue
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
