package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// EnsureTopics creates every topic in specs that does not exist yet, waits
// until all of them are visible in cluster metadata, and then verifies that
// co-partitioned topics have the expected partition count. A topic created
// concurrently by another service (TopicAlreadyExists) is verified the same
// way, so a mismatch is reported no matter which service created the topic.
func EnsureTopics(ctx context.Context, adm *kadm.Client, specs []TopicSpec, replication int16) error {
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Name
	}
	existing, err := adm.ListTopics(ctx, names...)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}

	var errs []error
	for _, s := range specs {
		if d, ok := existing[s.Name]; ok && d.Err == nil {
			continue
		}
		configs := make(map[string]*string, len(s.Configs))
		for k, v := range s.Configs {
			configs[k] = &v
		}
		if _, err := adm.CreateTopic(ctx, s.Partitions, replication, configs, s.Name); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			errs = append(errs, fmt.Errorf("create topic %q: %w", s.Name, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	details, err := waitForTopics(ctx, adm, names)
	if err != nil {
		return err
	}
	for _, s := range specs {
		if got := int32(len(details[s.Name].Partitions)); s.CoPartitioned && got != s.Partitions {
			errs = append(errs, fmt.Errorf("topic %q has %d partitions, want %d: co-partitioned topics must have identical partition counts", s.Name, got, s.Partitions))
		}
	}
	return errors.Join(errs...)
}

// waitForTopics polls metadata until every topic reports all partitions with
// a leader, so producers started right after EnsureTopics do not race topic
// creation.
func waitForTopics(ctx context.Context, adm *kadm.Client, names []string) (kadm.TopicDetails, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		details, err := adm.ListTopics(ctx, names...)
		if err == nil && topicsReady(details, names) {
			return details, nil
		}
		if time.Now().After(deadline) {
			if err == nil {
				err = errors.New("partitions without leaders")
			}
			return nil, fmt.Errorf("topics not ready after 30s: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func topicsReady(details kadm.TopicDetails, names []string) bool {
	for _, name := range names {
		d, ok := details[name]
		if !ok || d.Err != nil || len(d.Partitions) == 0 {
			return false
		}
		for _, p := range d.Partitions {
			if p.Err != nil || p.Leader < 0 {
				return false
			}
		}
	}
	return true
}
