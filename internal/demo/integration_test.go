//go:build integration

package demo

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

func TestFollowLabelsStartsAtTheEndOfTheTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	log := slog.New(slog.DiscardHandler)
	cfg := kafka.Config{
		Brokers: testutil.KafkaBrokers(t), ClientID: "demo-test", Prefix: testutil.UniquePrefix(t),
		Partitions: 6, Replication: 1, LogLevel: kgo.LogLevelWarn,
	}
	topic := cfg.Topics().GroundTruth
	cl, err := kafka.NewClient(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if err := kafka.EnsureTopics(ctx, adm, []kafka.TopicSpec{{Name: topic, Partitions: 1}}, 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := adm.DeleteTopics(ctx, topic); err != nil {
			t.Logf("delete topic: %v", err)
		}
	})
	publish := func(id string) {
		t.Helper()
		value, err := events.Marshal(&twpb.GroundTruthLabel{LabelId: id, SessionId: "s", Scenario: "wash.ring"})
		if err != nil {
			t.Fatal(err)
		}
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("s"), Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	publish("before")
	labels, err := FollowLabels(ctx, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	defer labels.Close()
	publish("after-1")
	publish("after-2")
	for _, want := range []string{"after-1", "after-2"} {
		l, err := labels.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if l.GetLabelId() != want {
			t.Fatalf("label %q, want %q", l.GetLabelId(), want)
		}
	}
}

func TestFollowLabelsFailsWithoutTheTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := kafka.Config{
		Brokers: testutil.KafkaBrokers(t), ClientID: "demo-test", Prefix: testutil.UniquePrefix(t),
		Partitions: 6, Replication: 1, LogLevel: kgo.LogLevelWarn,
	}
	if _, err := FollowLabels(ctx, cfg, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("FollowLabels succeeded on a topic that does not exist")
	}
}
