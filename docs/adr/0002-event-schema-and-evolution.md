# ADR-0002: Protobuf event schemas and how they evolve

- Status: Accepted
- Date: 2026-10-13

## Context

Six services exchange events over Kafka. They also share recordings of
sessions on disk, which the replay tool and the evaluator read. We need:

- A typed, compact wire format with generated Go code.
- Rules for changing schemas without breaking running consumers or old
  recordings.
- A way to tell what a record contains without decoding it. This matters for
  dead-lettering and for filling sequence gaps when a payload is corrupt.

## Decision

1. **Protocol Buffers (proto3)**, package `tradewatch.v1`, sources in
   `proto/tradewatch/v1/`. Code is generated into `internal/events/twpb`
   and checked in, so building the services needs neither `protoc` nor
   `buf`.
2. **buf** handles linting (`STANDARD` rules), formatting and generation
   (`make proto`). It also runs the breaking-change check (`FILE` rules),
   which CI applies to every pull request against its base branch.
3. **Deterministic marshaling** (`proto.MarshalOptions{Deterministic: true}`)
   everywhere. Identical events then produce identical bytes, so recordings
   and replays can be compared byte for byte.
4. **Routing metadata in record headers**, duplicated from the payload:
   - `tw-schema`: the fully-qualified message name, such as
     `tradewatch.v1.OrderEvent`.
   - `tw-session`: the session ID.
   - `tw-seq`: the per-symbol sequence number.

   The record key is the symbol.

## Evolution rules

Within `tradewatch.v1`, changes are additive only:

- New fields get new field numbers. Field numbers are never reused or
  renumbered. A removed field's number and name are listed in `reserved`.
- Enums keep `*_UNSPECIFIED = 0` as their default, and new values are only
  appended. Consumers must handle values they do not know, for example by
  dead-lettering them with a reason or ignoring them, and must never
  crash. Unknown fields are preserved by the protobuf runtime.
- A field's type or meaning never changes. If semantics change, add a new
  field and deprecate the old one.
- An incompatible change creates `tradewatch.v2` on new topics. Producers
  dual-publish to both versions until every consumer has migrated. The
  `tw-schema` header lets a consumer read either version during the
  transition.

`buf breaking` enforces the mechanical part of these rules in CI. Code review
covers semantic changes.

## Alternatives considered

- **JSON with JSON Schema.** Human-readable and simple to debug, but larger
  on the wire and slower to encode and decode. It has no generated types,
  and compatibility checking would need extra tooling. JSON is still used at
  the edges: the REST API and the evidence JSON stored in Postgres.
- **Avro with a schema registry.** A strong choice for a multi-team
  platform. Here it would add another always-on service (the registry) for a
  single-repository system whose schemas are versioned with the code. A
  production deployment could register these same protobuf schemas in a
  registry. The `tw-schema` header already identifies the schema.

## Consequences

- The wire format is compact and fast, with typed Go structs and
  compatibility checked in CI.
- Generated code is committed. CI regenerates it and fails on any diff, so
  it cannot drift from the `.proto` sources.
- Prices are fixed-point `int64` values in units of 1/10,000. This avoids
  floating-point drift in the matching engine and in feature calculations.
