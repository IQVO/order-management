---
id: 0027-kafka-integration-publisher-partition-key
slug: /adr/0027-kafka-integration-publisher-partition-key
title: 27. Partition key (OrderId) on the integration publisher's Kafka messages
sidebar_label: 27. Kafka partition key on integration events
description: "ADR 0027 — order-management's integration publisher (warehouse.order-management.events) never set a Kafka message Key, so messages round-robinned across partitions. Safe by accident at 1 partition; broke per-order event ordering the moment warehouse-infra's Phase 3 partition scaleup took every business topic to 8 partitions. Fix: key every message by the event's OrderId (mirroring the existing AnalyticsPublisher), and switch the writer's Balancer from LeastBytes (which ignores Key for routing) to Hash (FNV-1a over Key) so same-order messages actually land on the same partition."
---

# 27. Partition key (OrderId) on the integration publisher's Kafka messages

## Status

Accepted — implemented in the same change that introduces this record.

## Context

`warehouse-infra` PR #42 (Phase 3 of the fleet's Kafka scaleup, already
merged into `develop`) took every business topic — including this
service's own integration topic, `warehouse.order-management.events` —
from 1 partition to 8, to raise consumer-side throughput headroom fleet-
wide.

`internal/adapters/outbound/kafka/publisher.go`'s `Publisher` (the
adapter forwarding `OrderAllocated`, `OrderPartiallyAllocated`, and
`OrderRepromised` onto that topic, per the package doc comment and ADR
0014 §5 / ADR 0018) never set `kafka-go`'s `Message.Key` on any message
it wrote — every `kafkago.Message{...}` literal in `Encode`/`Publish`
carried only `Value`/`Headers`. With exactly one partition this was
accidentally correct: every message for every order landed on the same
(only) partition, in publish order, and any single consumer reading that
one partition observed a total order across all orders, which trivially
includes a correct order per aggregate too.

At 8 partitions, a nil `Message.Key` means `kafka-go`'s writer distributes
messages across partitions independently of any business key — a
consumer group with more than one member can now receive
`OrderAllocated` for order X on one partition and a *later* `OrderRepromised`
for that SAME order X on a different partition, processed by a different
consumer instance with no ordering relationship between the two
partitions' consumption. wes-work-planning and any other downstream
consumer of this topic can therefore observe an order's own event history
out of sequence — e.g. seeing `OrderRepromised` applied, then later seeing
the earlier `OrderAllocated` "arrive" and overwrite it — even though
nothing on the publish side raced; the only thing that changed was the
topic's partition count.

This service's own `AnalyticsPublisher` (a separate adapter, same package,
publishing to `warehouse.order-management.analytics`) had already solved
this correctly since it was written: `marshalData` returns the event's
`OrderId` as `key` for every event type, and `Encode`/`write` both set
`kafkago.Message{Key: []byte(key), ...}`. `Publisher` was the one adapter
in this package that never got that treatment, because the gap was latent
and invisible at 1 partition — nothing exercised it until the partition
count actually changed.

### A second, non-obvious finding: setting `Key` alone is not sufficient

Verifying the fix against a real multi-partition Kafka broker (Testcontainers,
not a fake writer) surfaced that `kafka-go`'s `Writer.Balancer` — not the
mere presence of a `Message.Key` — decides partition placement. Every
writer in this package (`Publisher.NewWriterForTopic`, `AnalyticsPublisher`'s
own writer, and the outbox relay's `RelaySink`) was configured with
`&kafkago.LeastBytes{}`, whose `Balance` method distributes purely by
cumulative bytes written per partition and reads `Message.Key`/`Value`
only to add their lengths to that running total — it never hashes or
otherwise routes on the key's *content*. Stamping `OrderId` onto every
message without also changing the balancer would have shipped a fix that
passes a fake-writer unit test (which only asserts the `Key` field is
populated) while remaining completely inert against a real broker,
because `LeastBytes` would still spread same-key messages across whichever
partition currently has the least cumulative bytes. Confirmed concretely:
a real-Kafka integration test asserting all 3 messages for one order land
on the same partition failed (1 of 3) under `LeastBytes` even after `Key`
was populated, and passed (3 of 3) only after switching to `&kafkago.Hash{}`
(FNV-1a over `Message.Key`, `kafka-go`'s key-based balancer — the same
algorithm Sarama's hash partitioner uses, per the kafka-go documentation).

## Decision

1. **`encodeEnvelope` (the shared encode path behind both `Publish` and
   `Encode`) now returns the event's `shared.OrderId`, serialized via its
   existing `.String()` method, as the message key** — for all three
   event types this publisher forwards (`OrderAllocated`,
   `OrderPartiallyAllocated`, `OrderRepromised`). This mirrors
   `AnalyticsPublisher.marshalData`'s existing choice exactly: same field
   (`shared.OrderId`), same serialization (`.String()`), so the two
   publishers key identically for the same event even though they are
   independent adapters over independent topics.
2. **`Publisher.Publish` and `Publisher.Encode` (and therefore the
   Postgres outbox relay path, which calls `Encode` inside the use case's
   transaction) both set `kafkago.Message.Key`/`kafka.Encoded.Key` from
   that value.** No behavior differs between the direct-publish path
   (`EVENT_PUBLISHER=kafka`, no `DATABASE_URL`) and the outbox path —
   both produce byte-identical keys for the same event.
3. **Every `*kafkago.Writer` this package constructs
   (`Publisher.NewWriterForTopic`, `AnalyticsPublisher`'s writer,
   `RelaySink`'s writer) switches its `Balancer` from `&kafkago.LeastBytes{}`
   to `&kafkago.Hash{}`.** This is the balancer that actually keys
   partition placement off `Message.Key`'s bytes (FNV-1a hash mod
   partition count) — the previous `LeastBytes` choice silently discarded
   any key that was ever set, on this topic or the analytics one, for as
   long as this package has existed. `AnalyticsPublisher` had been
   setting `Key` correctly since it was written, but — until this change —
   was *also* not actually getting same-order-same-partition placement
   from a real broker, because its writer had the same `LeastBytes`
   balancer. This ADR closes that latent gap for both publishers at once,
   not just the newly-keyed `Publisher`.
4. No custom `Balancer` implementation, no producer-side partition
   pinning, and no consumer-side reordering buffer were introduced —
   `kafka-go`'s stock `Hash` balancer plus a non-nil `Key` is Kafka's
   standard idiom for "route by key," and it is sufficient here: this
   service does not need a specific partition number, only that the SAME
   order's events always land on the SAME partition as each other,
   regardless of how many partitions the topic has.

## Consequences

- Every event this publisher forwards for the same `OrderId` — across
  `OrderAllocated`, `OrderPartiallyAllocated`, and `OrderRepromised`,
  published at different times by different code paths (`ReceiveOrder`,
  `AllocateOrder`, `RepromiseOrder`) — now deterministically lands on the
  same partition of `warehouse.order-management.events`, for any
  partition count. A consumer reading that topic observes a correct
  relative order for any single order's own event history even when
  reading with multiple consumer instances across many partitions.
  Ordering ACROSS different orders is still not guaranteed and was never
  a requirement — this fixes only the per-aggregate guarantee that the
  1-partition topic used to provide by accident.
- `AnalyticsPublisher`'s partition-affinity guarantee, which looked
  correct by inspection (it already set `Key`) but was silently defeated
  by `LeastBytes`, is now also genuinely enforced. No caller-visible
  change to `AnalyticsPublisher`'s public API or wire payload — only its
  writer's `Balancer` field changed.
- A downstream consumer that happened to rely on this topic's total
  publish-time order across *different* orders (nothing in this
  codebase's own consumers does, verified against wes-work-planning's
  documented consumer contract in `CLAUDE.md`) would need to re-evaluate
  that assumption — but that guarantee was already gone the moment
  warehouse-infra's PR #42 changed the partition count; this ADR does not
  introduce it, it only stops silently pretending per-order ordering
  still held.
- `kafka-go`'s `Hash` balancer hashes only `Message.Key`; it falls back to
  round-robin for a nil key. Every message this package's `Publisher`/
  `AnalyticsPublisher` produce always carries a non-nil key (a domain
  event outside either publisher's contract is skipped before a message
  is ever constructed, per each encoder's existing `ok=false` convention),
  so the fallback path is not expected to be exercised in production —
  documented here so a future new event type added to either publisher's
  switch is not accidentally added without also supplying a key.
- No wire-format change: `Message.Key` is Kafka metadata, not part of the
  JSON envelope/payload a consumer decodes. wes-work-planning's consumer
  needs zero changes.
