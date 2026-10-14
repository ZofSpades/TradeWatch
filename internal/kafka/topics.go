// Package kafka holds the TradeWatch Kafka topology and the franz-go helpers
// shared by every service.
package kafka

// Topics names every topic in the pipeline. All names share an optional
// prefix so that several isolated pipelines (for example integration tests
// and benchmarks) can run against one cluster.
type Topics struct {
	// Raw exchange feeds published by marketsim. These four topics are
	// co-partitioned: they have identical partition counts and are keyed by
	// symbol, so all events of a symbol land on the same partition number in
	// every topic (ADR-0003).
	Orders        string
	Cancels       string
	Trades        string
	BookSnapshots string

	// Normalized, enriched, per-symbol-sequenced stream produced by ingest.
	MarketEvents string

	// Reference data (accounts, instruments, sessions); compacted.
	RefData string
	// Labels for injected abuse, for scoring only; detectors never read them.
	GroundTruth string
	// Detector output.
	Alerts string
	// Records rejected by a pipeline stage, wrapped with the reason.
	DeadLetter string
	// Rule sets loaded by detectors; compacted.
	RulesAudit string
}

// NewTopics returns the topic names with the given prefix.
func NewTopics(prefix string) Topics {
	return Topics{
		Orders:        prefix + "orders",
		Cancels:       prefix + "cancels",
		Trades:        prefix + "trades",
		BookSnapshots: prefix + "book_snapshots",
		MarketEvents:  prefix + "market_events",
		RefData:       prefix + "refdata",
		GroundTruth:   prefix + "groundtruth",
		Alerts:        prefix + "alerts",
		DeadLetter:    prefix + "dlq",
		RulesAudit:    prefix + "rules_audit",
	}
}

// Raw returns the four raw exchange topics in a fixed order.
func (t Topics) Raw() []string {
	return []string{t.Orders, t.Cancels, t.Trades, t.BookSnapshots}
}

// TopicSpec describes how a topic is created.
type TopicSpec struct {
	Name       string
	Partitions int32
	// CoPartitioned topics must have exactly Partitions partitions; a
	// mismatch on an existing topic is a configuration error because it
	// breaks per-symbol co-location.
	CoPartitioned bool
	Configs       map[string]string
}

const (
	retentionDay  = "86400000"
	retentionWeek = "604800000"
)

// Specs returns the full topology. marketPartitions is the partition count
// shared by all symbol-keyed topics.
func (t Topics) Specs(marketPartitions int32) []TopicSpec {
	market := func(name string) TopicSpec {
		return TopicSpec{
			Name:          name,
			Partitions:    marketPartitions,
			CoPartitioned: true,
			Configs:       map[string]string{"retention.ms": retentionDay},
		}
	}
	return []TopicSpec{
		market(t.Orders),
		market(t.Cancels),
		market(t.Trades),
		market(t.BookSnapshots),
		market(t.MarketEvents),
		{Name: t.RefData, Partitions: 1, Configs: map[string]string{"cleanup.policy": "compact"}},
		{Name: t.GroundTruth, Partitions: 1, Configs: map[string]string{"retention.ms": retentionWeek}},
		{Name: t.Alerts, Partitions: marketPartitions, Configs: map[string]string{"retention.ms": retentionWeek}},
		{Name: t.DeadLetter, Partitions: 1, Configs: map[string]string{"retention.ms": retentionWeek}},
		{Name: t.RulesAudit, Partitions: 1, Configs: map[string]string{"cleanup.policy": "compact"}},
	}
}
