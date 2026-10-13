//go:build integration

package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

func newTestClient(t *testing.T, opts ...kgo.Opt) (*kgo.Client, *kadm.Client) {
	t.Helper()
	cfg := Config{
		Brokers:     testutil.KafkaBrokers(t),
		ClientID:    "integration-test",
		Partitions:  6,
		Replication: 1,
		LogLevel:    kgo.LogLevelWarn,
	}
	cl, err := NewClient(cfg, slog.New(slog.DiscardHandler), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl, kadm.NewClient(cl)
}

func deleteTopicsOnCleanup(t *testing.T, adm *kadm.Client, specs []TopicSpec) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		names := make([]string, len(specs))
		for i, s := range specs {
			names[i] = s.Name
		}
		if _, err := adm.DeleteTopics(ctx, names...); err != nil {
			t.Logf("delete topics: %v", err)
		}
	})
}

func TestEnsureTopicsCreatesTopologyIdempotently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, adm := newTestClient(t)

	topics := NewTopics(testutil.UniquePrefix(t))
	specs := topics.Specs(6)
	deleteTopicsOnCleanup(t, adm, specs)

	if err := EnsureTopics(ctx, adm, specs, 1); err != nil {
		t.Fatalf("first EnsureTopics: %v", err)
	}
	if err := EnsureTopics(ctx, adm, specs, 1); err != nil {
		t.Fatalf("second EnsureTopics must be a no-op: %v", err)
	}

	details, err := adm.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range specs {
		d, ok := details[s.Name]
		if !ok {
			t.Fatalf("topic %s was not created", s.Name)
		}
		if got := int32(len(d.Partitions)); got != s.Partitions {
			t.Errorf("%s: %d partitions, want %d", s.Name, got, s.Partitions)
		}
	}

	cfgs, err := adm.DescribeTopicConfigs(ctx, topics.RefData)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := cfgs.On(topics.RefData, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range rc.Configs {
		if c.Key == "cleanup.policy" && c.MaybeValue() != "compact" {
			t.Errorf("refdata cleanup.policy = %q, want compact", c.MaybeValue())
		}
	}
}

func TestEnsureTopicsRejectsPartitionMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, adm := newTestClient(t)

	topics := NewTopics(testutil.UniquePrefix(t))
	specs := topics.Specs(6)
	deleteTopicsOnCleanup(t, adm, specs)

	if _, err := adm.CreateTopic(ctx, 3, 1, nil, topics.Trades); err != nil {
		t.Fatal(err)
	}
	err := EnsureTopics(ctx, adm, specs, 1)
	if err == nil || !strings.Contains(err.Error(), topics.Trades) {
		t.Fatalf("expected a partition-count error for %s, got %v", topics.Trades, err)
	}
}

// TestSymbolKeysAreCoPartitioned verifies the property the ingest sequencer
// relies on: with the default (murmur2) key partitioner and equal partition
// counts, a symbol maps to the same partition number in every raw topic.
func TestSymbolKeysAreCoPartitioned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, adm := newTestClient(t, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))

	topics := NewTopics(testutil.UniquePrefix(t))
	specs := topics.Specs(6)
	deleteTopicsOnCleanup(t, adm, specs)
	if err := EnsureTopics(ctx, adm, specs, 1); err != nil {
		t.Fatal(err)
	}

	symbols := []string{"QZAP", "VNTR", "ORBT", "KRYL", "MOSS", "TALX", "WREN", "YUMA"}
	partitionOf := map[string]map[string]int32{}
	for _, topic := range append(topics.Raw(), topics.MarketEvents) {
		partitionOf[topic] = map[string]int32{}
		for _, sym := range symbols {
			res := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(sym), Value: []byte("x")})
			r, err := res.First()
			if err != nil {
				t.Fatalf("produce %s/%s: %v", topic, sym, err)
			}
			partitionOf[topic][sym] = r.Partition
		}
	}
	for _, sym := range symbols {
		want := partitionOf[topics.Orders][sym]
		for topic, parts := range partitionOf {
			if parts[sym] != want {
				t.Errorf("%s: partition %d in %s but %d in %s", sym, parts[sym], topic, want, topics.Orders)
			}
		}
	}
}

// TestCommitWithMetadataRoundTrip joins a consumer group, commits offsets
// with custom metadata through the raw OffsetCommit path and reads them back
// with kadm.
func TestCommitWithMetadataRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	prod, adm := newTestClient(t)

	topics := NewTopics(testutil.UniquePrefix(t))
	specs := topics.Specs(6)
	deleteTopicsOnCleanup(t, adm, specs)
	if err := EnsureTopics(ctx, adm, specs, 1); err != nil {
		t.Fatal(err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topics.Orders, Key: []byte("QZAP"), Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ids, err := TopicIDs(ctx, adm, topics.Orders)
	if err != nil {
		t.Fatal(err)
	}

	group := "it-" + topics.Orders + "-group"
	member, _ := newTestClient(t,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics.Orders),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err := CommitWithMetadata(ctx, member, group, nil, ids); err != ErrNotInGroup {
		t.Fatalf("commit before joining: %v, want ErrNotInGroup", err)
	}
	// Polling joins the group.
	for {
		member.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatal("never joined the group")
		}
		if _, gen := member.GroupMetadata(); gen >= 0 {
			break
		}
	}
	offsets := map[string]map[int32]CommitOffset{topics.Orders: {}}
	for p := int32(0); p < 6; p++ {
		offsets[topics.Orders][p] = CommitOffset{Offset: int64(10 + p), Metadata: fmt.Sprintf(`{"v":1,"p":%d}`, p)}
	}
	if err := CommitWithMetadata(ctx, member, group, offsets, ids); err != nil {
		t.Fatal(err)
	}

	got, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	for p := int32(0); p < 6; p++ {
		o, ok := got.Lookup(topics.Orders, p)
		if !ok || o.Err != nil {
			t.Fatalf("partition %d: no committed offset (%v)", p, o.Err)
		}
		if o.At != int64(10+p) || o.Metadata != fmt.Sprintf(`{"v":1,"p":%d}`, p) {
			t.Errorf("partition %d: committed %d %q", p, o.At, o.Metadata)
		}
	}
}

func TestProduceConsumeWithEventMeta(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	prod, adm := newTestClient(t)

	topics := NewTopics(testutil.UniquePrefix(t))
	specs := topics.Specs(6)
	deleteTopicsOnCleanup(t, adm, specs)
	if err := EnsureTopics(ctx, adm, specs, 1); err != nil {
		t.Fatal(err)
	}

	order := &twpb.OrderEvent{
		Header:   &twpb.EventHeader{SessionId: "S1", Symbol: "QZAP", Seq: 9, EventTimeNs: 1},
		Action:   twpb.OrderAction_ORDER_ACTION_NEW,
		OrderId:  42,
		Side:     twpb.Side_SIDE_BUY,
		Price:    1_000_000,
		Quantity: 100,
	}
	value, err := events.Marshal(order)
	if err != nil {
		t.Fatal(err)
	}
	meta := EventMeta{Schema: events.SchemaOrder, Session: "S1", Seq: 9, HasSeq: true}
	rec := &kgo.Record{Topic: topics.Orders, Key: []byte("QZAP"), Value: value, Headers: meta.Headers()}
	if err := prod.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatal(err)
	}

	cons, _ := newTestClient(t,
		kgo.ConsumeTopics(topics.Orders),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	for {
		fetches := cons.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatal("timed out waiting for the record")
		}
		fetches.EachError(func(topic string, p int32, err error) { t.Fatalf("fetch %s/%d: %v", topic, p, err) })
		var got *kgo.Record
		fetches.EachRecord(func(r *kgo.Record) { got = r })
		if got == nil {
			continue
		}
		if m := ParseEventMeta(got.Headers); m != meta {
			t.Errorf("meta = %+v, want %+v", m, meta)
		}
		var decoded twpb.OrderEvent
		if err := events.Unmarshal(got.Value, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.GetOrderId() != 42 || decoded.GetHeader().GetSeq() != 9 {
			t.Errorf("decoded %v", &decoded)
		}
		return
	}
}
