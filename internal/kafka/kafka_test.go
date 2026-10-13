package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/config"
)

func TestTopicSpecs(t *testing.T) {
	topics := NewTopics("it-")
	specs := topics.Specs(6)

	seen := map[string]bool{}
	for _, s := range specs {
		if seen[s.Name] {
			t.Errorf("duplicate topic %q", s.Name)
		}
		seen[s.Name] = true
		if s.Name[:3] != "it-" {
			t.Errorf("topic %q is missing the prefix", s.Name)
		}
	}
	// Every symbol-keyed topic must be co-partitioned with the same count.
	for _, name := range append(topics.Raw(), topics.MarketEvents) {
		var found bool
		for _, s := range specs {
			if s.Name == name {
				found = true
				if !s.CoPartitioned || s.Partitions != 6 {
					t.Errorf("%s: co-partitioned=%v partitions=%d", name, s.CoPartitioned, s.Partitions)
				}
			}
		}
		if !found {
			t.Errorf("%s missing from specs", name)
		}
	}
	for _, s := range specs {
		if s.Name == topics.RefData && s.Configs["cleanup.policy"] != "compact" {
			t.Error("refdata must be compacted")
		}
	}
}

func TestEventMetaRoundTrip(t *testing.T) {
	m := EventMeta{Schema: "tradewatch.v1.OrderEvent", Session: "S1", Seq: 42, HasSeq: true}
	got := ParseEventMeta(m.Headers())
	if got != m {
		t.Fatalf("got %+v, want %+v", got, m)
	}

	bad := ParseEventMeta([]kgo.RecordHeader{{Key: HeaderSeq, Value: []byte("not-a-number")}})
	if bad.HasSeq {
		t.Error("malformed seq header must not set HasSeq")
	}
	if v, ok := HeaderValue(m.Headers(), HeaderSession); !ok || v != "S1" {
		t.Errorf("HeaderValue = %q, %v", v, ok)
	}
}

func TestLoadConfig(t *testing.T) {
	env := config.FromMap(map[string]string{
		"TW_KAFKA_BROKERS":      "a:9092, b:9092",
		"TW_KAFKA_TOPIC_PREFIX": "bench-",
		"TW_KAFKA_PARTITIONS":   "12",
	})
	cfg := LoadConfig(env, "svc")
	if err := env.Err(); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Brokers) != 2 || cfg.Brokers[1] != "b:9092" {
		t.Errorf("brokers = %v", cfg.Brokers)
	}
	if cfg.ClientID != "svc" || cfg.Partitions != 12 || cfg.Replication != 1 {
		t.Errorf("cfg = %+v", cfg)
	}
	if cfg.Topics().Orders != "bench-orders" {
		t.Errorf("orders topic = %q", cfg.Topics().Orders)
	}
	if err := (Config{Brokers: []string{"x"}, Partitions: 0, Replication: 1}).Validate(); err == nil {
		t.Error("zero partitions should be invalid")
	}
}
