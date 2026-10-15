package demo

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
)

// Labels follows the groundtruth topic from where it ended when Labels was
// created, so it sees the label of every episode that finishes afterwards.
// The simulator publishes an episode's label when the episode finishes.
type Labels struct {
	cl      *kgo.Client
	pending []*twpb.GroundTruthLabel
}

// FollowLabels starts following the groundtruth topic at its current end.
func FollowLabels(ctx context.Context, cfg kafka.Config, log *slog.Logger) (*Labels, error) {
	topic := cfg.Topics().GroundTruth
	admin, err := kafka.NewClient(cfg, log)
	if err != nil {
		return nil, err
	}
	ends, err := kadm.NewClient(admin).ListEndOffsets(ctx, topic)
	admin.Close()
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return nil, fmt.Errorf("end offsets of %s: %w", topic, err)
	}
	from := map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) { from[o.Partition] = kgo.NewOffset().At(o.Offset) })
	if len(from) == 0 {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}
	cl, err := kafka.NewClient(cfg, log, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: from}))
	if err != nil {
		return nil, err
	}
	return &Labels{cl: cl}, nil
}

// Next returns the next label, waiting until one is published.
func (l *Labels) Next(ctx context.Context) (*twpb.GroundTruthLabel, error) {
	for len(l.pending) == 0 {
		fs := l.cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fs.Err(); err != nil {
			return nil, err
		}
		var decodeErr error
		fs.EachRecord(func(r *kgo.Record) {
			lb := &twpb.GroundTruthLabel{}
			if err := events.Unmarshal(r.Value, lb); err != nil {
				decodeErr = fmt.Errorf("label at offset %d: %w", r.Offset, err)
				return
			}
			l.pending = append(l.pending, lb)
		})
		if decodeErr != nil {
			return nil, decodeErr
		}
	}
	lb := l.pending[0]
	l.pending = l.pending[1:]
	return lb, nil
}

// Close stops following the topic.
func (l *Labels) Close() { l.cl.Close() }
