package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/kafka"
)

// readRange returns the records of each partition of topics from max(start,
// end-n) up to the end offset as of the call; n < 0 reads from the start.
func readRange(ctx context.Context, cfg kafka.Config, log *slog.Logger, topics []string, n int64) ([]*kgo.Record, error) {
	admCl, err := kafka.NewClient(cfg, log)
	if err != nil {
		return nil, err
	}
	defer admCl.Close()
	adm := kadm.NewClient(admCl)
	starts, err := adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return nil, err
	}
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, err
	}
	from := map[string]map[int32]kgo.Offset{}
	until := map[string]map[int32]int64{}
	pending := 0
	ends.Each(func(o kadm.ListedOffset) {
		start := int64(0)
		if s, ok := starts.Lookup(o.Topic, o.Partition); ok {
			start = s.Offset
		}
		first := start
		if n >= 0 {
			first = max(start, o.Offset-n)
		}
		if first >= o.Offset {
			return // nothing to read
		}
		if from[o.Topic] == nil {
			from[o.Topic], until[o.Topic] = map[int32]kgo.Offset{}, map[int32]int64{}
		}
		from[o.Topic][o.Partition] = kgo.NewOffset().At(first)
		until[o.Topic][o.Partition] = o.Offset
		pending++
	})
	if pending == 0 {
		return nil, nil
	}
	cl, err := kafka.NewClient(cfg, log, kgo.ConsumePartitions(from))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var out []*kgo.Record
	for pending > 0 {
		pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		fetches := cl.PollFetches(pctx)
		cancel()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("read %s/%d: %w", errs[0].Topic, errs[0].Partition, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			end := until[r.Topic][r.Partition]
			if r.Offset >= end {
				return
			}
			out = append(out, r)
			if r.Offset == end-1 {
				pending--
			}
		})
	}
	return out, nil
}

// Duplicate re-publishes exact copies (key, value, headers) of the last n
// records of every partition of topics, each to its original partition, as
// a producer that retried without idempotence would. It returns how many
// records it copied, by topic.
func Duplicate(ctx context.Context, cfg kafka.Config, log *slog.Logger, topics []string, n int64) (map[string]int, error) {
	recs, err := readRange(ctx, cfg, log, topics, n)
	if err != nil {
		return nil, err
	}
	return produceCopies(ctx, cfg, log, recs)
}

// DuplicateSession re-publishes an exact copy of every record of topic that
// belongs to session (by its tw-session header).
func DuplicateSession(ctx context.Context, cfg kafka.Config, log *slog.Logger, topic, session string) (int, error) {
	recs, err := readRange(ctx, cfg, log, []string{topic}, -1)
	if err != nil {
		return 0, err
	}
	var mine []*kgo.Record
	for _, r := range recs {
		if s, _ := kafka.HeaderValue(r.Headers, kafka.HeaderSession); s == session {
			mine = append(mine, r)
		}
	}
	n, err := produceCopies(ctx, cfg, log, mine)
	return n[topic], err
}

func produceCopies(ctx context.Context, cfg kafka.Config, log *slog.Logger, recs []*kgo.Record) (map[string]int, error) {
	counts := map[string]int{}
	if len(recs) == 0 {
		return counts, nil
	}
	cl, err := kafka.NewClient(cfg, log, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.MaxBufferedRecords(len(recs)+1))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	copies := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		copies = append(copies, &kgo.Record{Topic: r.Topic, Partition: r.Partition, Key: r.Key, Value: r.Value,
			Headers: append([]kgo.RecordHeader(nil), r.Headers...)})
		counts[r.Topic]++
	}
	if err := cl.ProduceSync(ctx, copies...).FirstErr(); err != nil {
		return nil, err
	}
	return counts, nil
}

// SessionRecords counts the records of topic that belong to session, and
// their distinct keys.
func SessionRecords(ctx context.Context, cfg kafka.Config, log *slog.Logger, topic, session string) (records, keys int, err error) {
	recs, err := readRange(ctx, cfg, log, []string{topic}, -1)
	if err != nil {
		return 0, 0, err
	}
	seen := map[string]bool{}
	for _, r := range recs {
		if s, _ := kafka.HeaderValue(r.Headers, kafka.HeaderSession); s == session {
			records++
			seen[string(r.Key)] = true
		}
	}
	return records, len(seen), nil
}

// WaitDrained waits until every group has committed up to the end of all
// its partitions, three times in a row two seconds apart.
func WaitDrained(ctx context.Context, cfg kafka.Config, log *slog.Logger, groups []string, timeout time.Duration) error {
	cl, err := kafka.NewClient(cfg, log)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	deadline := time.Now().Add(timeout)
	calm := 0
	for {
		lags, err := adm.Lag(ctx, groups...)
		total := int64(0)
		if err == nil {
			for _, g := range groups {
				l, ok := lags[g]
				if !ok || l.Error() != nil {
					total = -1
					break
				}
				total += l.Lag.Total()
			}
		}
		if err == nil && total == 0 {
			calm++
			if calm == 3 {
				return nil
			}
		} else {
			calm = 0
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the consumer groups did not drain within %s (lag %d, err %v)", timeout, total, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
