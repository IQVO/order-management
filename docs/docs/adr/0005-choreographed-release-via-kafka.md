---
id: 0005-choreographed-release-via-kafka
slug: /adr/0005-choreographed-release-via-kafka
title: 0005. Choreographed release via Kafka, folded allocate-then-release, and pathId goes internal-only
sidebar_label: 0005. Choreographed release via Kafka
description: ADR 0005 — replace the public /allocate and /release REST verbs and the synchronous wes-work-planning HTTP call with event choreography over Kafka, fold allocation and release into one flow, and make pathId an internal-only default.
---

# 0005. Choreographed release via Kafka, folded allocate-then-release, and pathId goes internal-only

## Status

> **Partially superseded (envelope only) by ADR-0030** (the flat `event_id`/`event_type`/`occurred_at`/`source`/`data` integration envelope described here). Every Kafka message is now a
> CloudEvents 1.0 event — see
> [ADR 0030](./0030-cloudevents-mandatory-event-envelope.md). The rest of
> this record stands.

Accepted. This is a real architecture redesign of an already-shipped v1,
driven by direct review feedback on that v1 (see the Context section
below) — not a hypothetical exercise. It amends the release-orchestration
portion of [ADR 0002](./0002-http-consumer-of-inventory-and-wes-not-shared-code.md):
allocation (this service → inventory-storage) stays exactly as ADR 0002
described it, a synchronous HTTP Customer/Supplier call — but release
(this service → wes-work-planning) is superseded by the Kafka
choreography this record describes. `internal/adapters/outbound/weswork/`
and `ports.WorkReleaseClient` are deleted; ADR 0002's characterisation of
wes-work-planning as an HTTP Supplier this service calls synchronously no
longer holds for release, only for the description of the bounded-context
boundary itself, which is otherwise unchanged (this service still does not
import wes-work-planning's Go packages or touch its database — it just no
longer calls its REST API either).

## Context

The v1 build shipped with three choices that, once built and reviewed
against the fleet's actual conventions, turned out to be wrong:

1. **`POST /orders/{id}/allocate` and `POST /orders/{id}/release` exposed
   internal saga-step mechanics as public REST verbs.** A caller of this
   service wants to express ONE intent — "place an order" — not learn that
   Order Management internally models a two-step (later three-step, once
   retry-allocation is counted) saga and must drive each step by hand.
   Every other service in this fleet that has a multi-step internal
   process (inventory-storage's reserve → confirm-pick, for instance)
   keeps that orchestration behind a single public command; this service's
   v1 did not, and that was an oversight, not a considered choice.

2. **Zero real integration events despite being designed for
   cross-service integration.** `wes-work-planning` was always meant to
   learn about released work from this service — the CLAUDE.md language
   even says "release of allocated work (via wes-work-planning)" — but v1
   built that as a synchronous `POST /paths/{pathId}/work-units` call, the
   exact weak coupling strategy `inventory-storage`'s own
   [ADR 0004](https://github.com/claudioed/inventory-storage — see that
   repo's `docs/docs/adr/0004-kafka-integration-events.md`) already
   rejected for the identical problem shape (Work Planning needing to know
   about a WMS-side fact). Order Management should have followed that
   precedent from day one.

3. **The public intake DTO exposed `pathId`, wes-work-planning's internal
   process-path/routing vocabulary, as a caller-settable field.** A
   customer placing an order has no business knowing that
   wes-work-planning organizes its work into named "paths" — that is
   purely this platform's internal routing concern. Exposing it on
   `POST /orders`'s request body leaked an implementation detail of a
   downstream Supplier through this service's public contract.

Fixing (2) with the SAME synchronous-HTTP shape ADR 0002 chose for
allocation was considered and rejected: `inventory-storage`'s ADR 0004
context section already makes the case against synchronous polling/direct
calls for this exact kind of "another bounded context needs to react to a
fact this one owns" problem, and there is no reason Order Management's
version of that problem should get a different answer. Kafka is already
the fleet's answer, with a shared broker
(`/Users/claudioed/warehouse-systems/docker-compose.kafka.yml`) and an
established envelope/adapter shape.

## Decision

**We will (a) delete the `/allocate` and `/release` REST endpoints and
fold their internal logic into `ReceiveOrder` and `RetryAllocation` as one
allocate-then-release flow, (b) publish the resulting allocation/release
fact as an enriched `OrderAllocated`/`OrderPartiallyAllocated` Kafka
integration event carrying a `lines[]` payload instead of calling
wes-work-planning synchronously, and (c) make `pathId` an internal-only
default, never caller-supplied.**

### (a) One flow, not three public commands

- `POST /orders` (`ReceiveOrder`) still validates lines, mints an
  `OrderId`, persists the order, and publishes `OrderReceived`
  unconditionally — exactly as before. Immediately afterward, in the SAME
  call, it attempts allocation-then-release as a best-effort next step. A
  hard (non-business) failure in that best-effort step is logged/visible
  via the existing `OrderAllocationPartiallyFailed` event but is never
  surfaced as `ReceiveOrder` itself failing — the order genuinely was
  received.
- `POST /orders/{id}/retry-allocation` (`RetryAllocation`) keeps its
  existing contract and error-return behaviour (a hard failure DOES
  propagate to its caller — it is an explicit, on-purpose recovery
  action), but now ALSO attempts release in the same call on success.
- The shared logic lives in one new function,
  `allocateAndRelease(ctx, deps, o, lines, retry)`, in
  `internal/application/usecases/allocation.go` — following the exact
  pattern the codebase already used to share `allocateLines` between the
  old `AllocateOrder` and `RetryAllocation`.
- `AllocateOrder` and `ReleaseOrder` are deleted as public use case types.
  Their pure domain-transition logic (`Order.Allocate`, `Order.Release`,
  `Order.EnsureReleasable`) is UNCHANGED — this is an application-layer
  and adapter-layer redesign, not a domain-layer one.

### (b) Choreographed release over Kafka

- A new outbound adapter, `internal/adapters/outbound/kafka`, mirrors
  `inventory-storage`'s `internal/adapters/outbound/kafka/publisher.go`
  convention exactly: the same envelope fields
  (`event_id`/`event_type`/`occurred_at`/`source`/`data`), the same
  `Writer` interface for testability, the same OTel producer-span pattern.
- New topic: `warehouse.order-management.events`, matching the platform's
  `warehouse.<context>.events` naming (see inventory-storage's
  `warehouse.inventory.events`).
- Only two of this service's eight domain events are forwarded —
  `OrderAllocated` and `OrderPartiallyAllocated` — mirroring
  inventory-storage's own precedent of forwarding only two of its several
  domain events (`StockReserved`, `ReservationRevoked`). Everything else
  (`OrderReceived`, `OrderLineAllocated`, `OrderLineBackordered`,
  `OrderReleased`, `OrderCancelled`, `OrderAllocationPartiallyFailed`)
  stays local-only.
- Those two events are ENRICHED with a `Lines []ReleasedLine` field (new
  domain type: `LineNo`, `SKU`, `PathID`, `GiftWrap`) carrying exactly the
  lines released in that pass — this is the mechanism that replaces the
  synchronous `POST /paths/{pathId}/work-units` call: instead of this
  service telling wes-work-planning "enqueue this work" over HTTP,
  wes-work-planning's own Kafka consumer reacts to the fact "these lines
  were released" and enqueues its own work independently.
- `EVENT_PUBLISHER=kafka|log` (env, case-insensitive), defaulting to
  `log` — the SAME toggle name and default inventory-storage uses, wired
  in `cmd/order/main.go`'s `buildRepoAdapters` exactly mirroring
  inventory-storage's `cmd/inventory/main.go` `buildAdapters` structure.
  `KAFKA_BROKERS` (comma-separated, default `localhost:9092`) selects the
  broker addresses.
- **The deterministic work-unit id convention is preserved as a frozen,
  independently-reconstructed contract, not a wire field.** `WorkUnitID(orderID,
  lineNo) = "{orderID}-line-{lineNo}"` still exists in
  `internal/application/usecases/allocation.go`, documented as the exact
  formula wes-work-planning's consumer reconstructs from
  `(order_id, lines[].line_no)` on the Kafka payload — it MUST match
  byte-for-byte or idempotent redelivery breaks. It is no longer
  transmitted over any wire; both sides derive it independently.
- `internal/adapters/outbound/weswork/` (both the real HTTP client and its
  permissive counterpart) is deleted entirely, along with
  `ports.WorkReleaseClient`, `ports.WorkUnitRequest`, and
  `ports.WorkUnitResult`. `WES_WORK_PLANNING_MODE`/`WES_WORK_PLANNING_BASE_URL`
  are removed from `cmd/order/main.go`.
- This repo gets its OWN `docker-compose.kafka.yml`, faithfully duplicating
  the shared compose file's `apache/kafka:3.8.0` KRaft single-node service
  definition shape with a distinguishing `container_name`
  (`order-management-kafka`) and its own volume — reusing the same
  image/config, not referencing the sibling repo's file across
  repositories.
- **v1 deliberately ships fire-and-forget, with NO release-confirmation
  reply event from wes-work-planning.** This service publishes
  `OrderAllocated`/`OrderPartiallyAllocated` and moves on; it never learns
  whether wes-work-planning's consumer actually processed the event or
  successfully enqueued its own work. This is the same honest,
  deliberately-scoped-down choice inventory-storage's own ADR 0004 made
  ("Publish failures fail the request... a transactional outbox would
  decouple them and is not built") — a confirmation-loop pattern (e.g. a
  `WorkEnqueued` reply event this service subscribes to) is a real,
  documented v1 gap, not an oversight. See the README's "Deferred (v1)"
  section.

### (c) pathId becomes internal-only

- `receiveOrderLineRequest.PathID` (the inbound DTO field) is deleted.
  Every line unconditionally gets `shared.NewPathIdOrDefault("")`, which
  always resolves to `shared.DefaultPathId`.
- The response DTO's `pathId` field is UNCHANGED in shape — callers can
  still see the assigned path on every line — its doc comment is updated
  to clarify the value is internally assigned, never caller-controlled.
- The domain (`OrderLine.pathID`) and application layers
  (`usecases.NewLine.PathID`) keep modelling `PathId` exactly as before;
  this is purely an inbound-adapter-level change.

## Consequences

### Easier

- **The public API finally matches the actual intent a caller has.** One
  command — `POST /orders` — expresses "place an order"; internal saga
  orchestration is no longer a caller's concern.
- **wes-work-planning integration finally exists as real integration
  events**, closing a gap this service was designed for but never built.
- **No synchronous coupling to wes-work-planning's availability at
  release time.** A wes-work-planning outage no longer blocks this
  service's release path the way `ReleaseOrder`'s hard failure used to.
- **The public intake contract no longer leaks wes-work-planning's
  internal vocabulary.** `pathId` selection becomes a pure internal
  concern this service (and, later, a real domain policy) owns.
- **Less code, one flow to reason about instead of two.** `allocateLines`
  and `publishOrderAllocationOutcome` were already shared between the old
  `AllocateOrder` and `RetryAllocation`; this redesign extends that
  sharing to cover release too, rather than adding a third parallel
  implementation.

### Harder

- **At-least-once delivery, and no confirmation loop, is now
  wes-work-planning's problem to solve on its side** (deduplicate by
  `event_id`, as inventory-storage's own consumers already do) — and this
  service has no visibility into whether that consumption succeeded. See
  the fire-and-forget note above.
- **No ordering guarantee across events on the topic** originally (no
  partition key), matching inventory-storage's own documented limitation
  for the identical reason — **resolved for the integration topic by
  [ADR 0027](./0027-kafka-integration-publisher-partition-key.md)**,
  which keys the integration publisher's messages on `OrderId` so all
  events for one order land on one partition, in order.
- **The frozen `WorkUnitID` formula is now a coordination point with zero
  wire-level enforcement.** Nothing catches a drift between this service's
  formula and wes-work-planning's independently-reconstructed one except
  manual review and cross-repo test discipline.
- **A ship-complete order's whole lifecycle (received, allocated,
  released) can now complete inside a single `POST /orders` call with no
  intermediate caller action**, which is the intended behaviour but does
  mean a caller who WANTED fine-grained control over each step (e.g. to
  inspect allocation before authorising release) has lost that lever —
  no v1 caller exercised it, and none was ever the point of the original
  public verbs, but it is a real behavioural change worth naming.
- **Two envelope generations coexisted across the fleet** until
  [ADR 0030](./0030-cloudevents-mandatory-event-envelope.md) made
  CloudEvents 1.0 the single mandatory envelope; this adapter emitted the
  established flat envelope
  (`event_id`/`event_type`/`occurred_at`/`source`/`data`) at the time,
  consistent with the sibling service then. The banner above covers the
  migration.

Downstream consumers also receive more than this record originally
described: the topic now carries `OrderRepromised` (forwarded here since
[ADR 0018](./0018-repromise-order-consumer-and-order-repromised.md)),
not just the release events this ADR introduced.

## Verification

Unit-tested against an in-memory `Writer` fake
(`internal/adapters/outbound/kafka/publisher_test.go`), mirroring
inventory-storage's `publisher_test.go` structure — envelope shape, the
enriched `lines[]` payload, ignoring non-forwarded events, and the OTel
trace-context injection. The folded allocate-then-release flow is covered
by `internal/application/usecases/{receive_order,retry_allocation}_test.go`:
ship-complete-all-allocated-releases-everything,
partial-shipment-releases-what-it-can,
ship-complete-still-backordered-releases-nothing, and
hard-failure-during-ReceiveOrder's-implicit-allocation-does-not-fail-ReceiveOrder
are each explicit test cases. `go test -race -coverprofile=... ./...`
against `./internal/domain/...,./internal/application/...` reports total
coverage well above the 90% gate at the time this ADR was written.
