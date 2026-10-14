# Architecture decision records

| ADR | Decision |
| --- | --- |
| [0001](0001-kafka-client.md) | Use franz-go as the Kafka client |
| [0002](0002-event-schema-and-evolution.md) | Protobuf event schemas and how they evolve |
| [0003](0003-partitioning-and-ordering.md) | Partition by symbol and restore cross-topic order with sequence numbers |
| [0004](0004-checkpoints-in-commit-metadata.md) | Stateful consumers checkpoint in offset-commit metadata |
| [0005](0005-detector-state-by-replay.md) | Detector state is rebuilt by deterministic replay |
