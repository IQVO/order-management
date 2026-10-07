---
id: 0036-product-classification-local-copy
slug: /adr/0036-product-classification-local-copy
title: "0036. Product classification from a local copy of product-master events"
sidebar_label: "36. Product classification local copy"
sidebar_position: 36
description: "ADR 0036 — ports.ProductClassificationLookup is now answered from a local Postgres copy fed by product-master's ProductClassified events on warehouse.product-master.events, replacing the live HTTP lookup against inventory-storage. PRODUCT_CLASSIFICATION_MODE becomes kafka|permissive; http is removed and rejected at boot. Unknown-SKU semantics are unchanged."
---

# 0036. Product classification from a local copy of product-master events

## Status

Accepted — implemented by the change that introduces this record.

Supersedes the **live-lookup part** of [ADR 0016](./0016-eligibility-driven-process-path-selection.md)
(the `productclassification` HTTP client against inventory-storage's
`GET /products/{sku}/classification`, `PRODUCT_CLASSIFICATION_MODE=http`)
and the product-classification breaker/retry of
[ADR 0025](./0025-resilience-circuit-breakers-retry-dlq-shutdown.md). The
port `ports.ProductClassificationLookup`, the eligibility evaluation in
`order.PathSelectionPolicy` and `ReceiveOrder`'s fail-open handling stay
exactly as ADR 0016 / ADR 0021 decided.

Companion decisions: product-master ADR 0001 (product-master owns
`ProductClassification`) and ADR 0003 stage D (downstream readers move to
local copies); inventory-storage ADR 0034 (hand-over of classification
ownership).

## Context

Since ADR 0016, `ReceiveOrder` asked inventory-storage, synchronously and
once per line at intake, for a SKU's handling tags
(`GET /products/{sku}/classification`), so `PathSelectionPolicy` could
evaluate them against a candidate path's `Eligibility`. The call sat behind
a circuit breaker and a jittered retry (ADR 0025) and failed open: a 404, a
transport error or an open breaker all meant "no derived attributes".

The fleet moved the source of truth. `ProductClassification` now belongs to
the new **product-master** bounded context (its ADR 0001), which publishes
`com.warehouse.wms.product-master.product.ProductClassified` on
`warehouse.product-master.events` (key and CloudEvents subject = the SKU)
through a transactional outbox. Every payload is the FULL classification
plus the aggregate `version`; consumers keep a local copy per SKU and apply
a message only when its `version` is greater than the stored one.
inventory-storage stops being an authority for classification (its ADR
0034) and its `GET` endpoint is deprecated until product-master's stage E
removes it. Keeping a synchronous call into a context that is no longer the
owner — or moving the same call to product-master — would keep intake
latency and availability coupled to another service for a soft enrichment
input; product-master's ADR 0003 stage D asks every reader for a local copy
instead.

## Decision

### 1. The port stays; the adapter is swapped

`ports.ProductClassificationLookup` and `ports.ProductClassification` are
unchanged, and so are `ReceiveOrder`, `PathSelectionPolicy` and the rest of
the domain. The live HTTP client (`internal/adapters/outbound/productclassification`:
`Client`, `BreakerClient`, their tests and the
`product-classification` breaker) is deleted.
`internal/adapters/outbound/productclassificationcopy` implements the same
port by reading a local table:

- `PostgresStore` reads `product_classification_copy` (migration 0011:
  `sku` PK, `handling_tags TEXT[]`, `temperature_class`, `dot_hazard_class`,
  `version BIGINT`, `updated_at`).
- `MemoryStore` is the same behaviour in memory, used when `DATABASE_URL`
  is unset (the repo's existing no-Postgres run mode) and in tests.
- `PermissiveLookup` moved here unchanged (always `Known=false`).

The copy maps to exactly what the HTTP client returned: a stored row is
`{SKU, HandlingTags, Known: true}`; an absent SKU is `Known=false`; a read
error is also `Known=false` with a nil error (logged at WARN). That is the
same fail-open behaviour the client had for a 404 and for a transport
error, so `ReceiveOrder` sees no difference. `temperature_class` and
`dot_hazard_class` are stored (they are part of the payload and cost
nothing) but the port still exposes only the handling tags.

### 2. A consumer of `warehouse.product-master.events` writes the copy

`inbound/kafka.ProductClassificationConsumer` mirrors
`PlannedCapacityConsumer` (ADR 0031):

- STABLE shared consumer group from env
  `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` (never a literal, no default).
  This is a process-and-commit consumer, not a per-process replay cache:
  the copy is durable in Postgres, so a restarted pod resumes from the
  committed offset.
- CloudEvents 1.0 only via `internal/adapters/kafka/cloudevents.Decode`.
  Dispatch on the FULL type
  `com.warehouse.wms.product-master.product.ProductClassified`; every other
  type on the topic (`ProductRegistered`, `ProductDescriptionChanged`,
  `ProductDimensionsDeclared`, `ProductMeasured`, anything new) is
  committed past untouched.
- A message that is not a valid CloudEvent, or whose payload cannot be
  applied (undecodable data, empty `sku`, `version` < 1, a `subject` that
  disagrees with `data.sku`), is logged at WARN with topic/partition/offset
  and committed past. Unlike ADR 0031 there is no dead-letter topic: the
  payload is a full-state replacement, so the next `ProductClassified` for
  that SKU repairs the copy anyway.
- A use case, `usecases.ApplyProductClassification`, claims the
  CloudEvents `id` (`ports.ProductClassificationProcessedEvents`, table
  `product_classification_processed_events`) and runs the version-guarded
  upsert (`ports.ProductClassificationCopy`) in ONE `UnitOfWork`. The
  upsert is a single `INSERT ... ON CONFLICT (sku) DO UPDATE ... WHERE
  stored.version < incoming.version`: a stale or equal version is a no-op,
  but its id is still claimed. The dedupe mechanism is the repo's existing
  one (a `MarkProcessed` port per consumer plus `atomically`); the table is
  new because every OLTP consumer here owns its own processed-events
  table, so ids from different topics never share a key space.
- A failure to apply (Postgres down, deadlock) rolls back both the claim
  and the upsert and is retried on the SAME message with backoff until it
  succeeds or the process stops; the offset is committed only after
  success. Nothing is skipped on a transient error.

There is no "unclassify" event in v1, so a row is never deleted.

### 3. Modes: `kafka|permissive`; `http` is rejected at boot

`PRODUCT_CLASSIFICATION_MODE`:

| Value | Behaviour |
| --- | --- |
| `permissive` (default) | `PermissiveLookup`, no consumer — unchanged. |
| `kafka` | the copy adapter answers the port and the consumer runs. Requires `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` and `KAFKA_BROKERS`; uses Postgres when `DATABASE_URL` is set, the in-memory copy otherwise. |
| `http` | **boot error** naming this ADR. A deployment still configured for the old live lookup fails loudly instead of silently degrading to permissive. |
| anything else | boot error. |

`cmd/order` is the only binary that builds the lookup. `cmd/mcp` serves
read-only tools that never evaluate eligibility, and the analytics binaries
never did; none of them builds the lookup or starts the consumer, so no
second consumer joins the group. The consumer is stopped in `cmd/order`'s
existing shutdown order, alongside the planned-capacity consumer: after
the HTTP drain and before the database pool closes.

`INVENTORY_STORAGE_BASE_URL` stays: allocation still calls inventory-storage
(`POST`/`DELETE /reservations`). It is no longer used for classification.

### 4. Eventual consistency

The copy is as fresh as the consumer's lag. Between a classification being
set in product-master and the copy applying it (normally well under a
second), intake evaluates the previous classification, or none for a SKU
the copy has never seen. That is the same outcome intake already accepted
when the live lookup failed open, so no new failure mode reaches the
domain. On a first deployment the copy starts empty and fills from the
topic (product-master's ADR 0003 runbook backfills it); until then unknown
SKUs keep the fail-open "no derived attributes" behaviour.

## Consequences

- Intake no longer makes a network call for classification: no breaker, no
  retry, no `product-classification` series on the `circuit_breaker.state`
  gauge, and no inventory-storage outage can slow intake through this path.
- One new table pair (migration 0011), one consumer, one use case, one
  consumed CloudEvents type in `apis/asyncapi.yaml` and in ADR 0030's
  consumed-types table.
- Deploy order: warehouse-infra must switch order-management from
  `PRODUCT_CLASSIFICATION_MODE=http` to `kafka` and set
  `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` (chart value
  `productClassification.consumerGroup`) in the same rollout, or the pod
  refuses to start.
- product-master's stage E can delete inventory-storage's deprecated
  `GET /products/{sku}/classification` once every reader has made this
  switch; order-management no longer depends on it.

## Alternatives considered

- **Call product-master's REST API live.** Rejected: it keeps intake's
  latency and availability coupled to another context for a soft input,
  which is what product-master ADR 0003 stage D removes fleet-wide.
- **Per-process full-replay in-memory cache (like `kafkacatalog`).**
  Rejected: the topic grows with every product change, so boot replay would
  grow without bound, and readiness would have to wait for it. A durable
  copy with a stable group resumes in constant time.
- **Reuse `planned_capacity_processed_events` for dedupe.** Rejected: each
  consumer in this repo owns its own processed-events table so one
  consumer's retention or replay can never hide another's messages.
- **Keep `http` as a deprecated alias for `kafka`.** Rejected: a silent
  alias hides a stale deployment; product-master ADR 0003 asks for a boot
  failure.
