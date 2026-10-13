package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CommitOffset is an offset to commit together with an opaque metadata
// string. Stateful consumers store a checkpoint of their state there, so
// whoever owns the partition next can resume exactly where the offset
// points.
type CommitOffset struct {
	Offset   int64
	Metadata string
}

// ErrNotInGroup is returned when committing while the client is not an
// active group member.
var ErrNotInGroup = errors.New("kafka: client is not a member of the group")

// CommitWithMetadata commits offsets and metadata for the client's consumer
// group in a single OffsetCommit request.
//
// franz-go's own commit path always stores the member ID as metadata, so
// this sends the request directly. It uses the member ID and generation the
// client currently holds, so the coordinator rejects the commit if this
// member has been fenced by a rebalance. Topics are identified by both name
// and ID, which keeps the request valid whichever protocol version
// (pre- or post-v10) is negotiated.
func CommitWithMetadata(ctx context.Context, cl *kgo.Client, group string, offsets map[string]map[int32]CommitOffset, topicIDs map[string]kadm.TopicID) error {
	member, gen := cl.GroupMetadata()
	if gen < 0 {
		return ErrNotInGroup
	}
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = group
	req.Generation = gen
	req.MemberID = member
	for topic, parts := range offsets {
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = topic
		rt.TopicID = topicIDs[topic]
		for p, o := range parts {
			rp := kmsg.NewOffsetCommitRequestTopicPartition()
			rp.Partition = p
			rp.Offset = o.Offset
			rp.LeaderEpoch = -1
			md := o.Metadata
			rp.Metadata = &md
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return fmt.Errorf("offset commit: %w", err)
	}
	var errs []error
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				errs = append(errs, fmt.Errorf("offset commit %s[%d]: %w", t.Topic, p.Partition, err))
			}
		}
	}
	return errors.Join(errs...)
}

// TopicIDs returns the IDs of the given topics, for CommitWithMetadata.
func TopicIDs(ctx context.Context, adm *kadm.Client, topics ...string) (map[string]kadm.TopicID, error) {
	details, err := adm.ListTopics(ctx, topics...)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]kadm.TopicID, len(topics))
	for _, t := range topics {
		d, ok := details[t]
		if !ok || d.Err != nil {
			return nil, fmt.Errorf("topic %q: %v", t, d.Err)
		}
		ids[t] = d.ID
	}
	return ids, nil
}
