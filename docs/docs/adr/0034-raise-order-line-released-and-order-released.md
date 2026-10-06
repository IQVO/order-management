---
id: 0034-raise-order-line-released-and-order-released
slug: /adr/0034-raise-order-line-released-and-order-released
title: "0034. Raise OrderLineReleased and OrderReleased at the release transition"
sidebar_label: "34. Raise release facts"
sidebar_position: 34
description: "ADR 0034 — OrderLineReleased and OrderReleased were declared (domain events, AsyncAPI, analytics projector) but no use case ever published them, so the Order Funnel's linesReleased/ordersReleased were always 0. allocateAndRelease now raises them at the real Allocated -> Released transition, analytics topic only, through the outbox."
---

# 0034. Raise OrderLineReleased and OrderReleased at the release transition

## Status

Accepted.

## Context

`shared.OrderLineReleased` and `shared.OrderReleased` were declared in
`internal/domain/shared/events.go`, listed in `apis/asyncapi.yaml`
(`OrderLineReleasedAnalytics`, `OrderReleasedAnalytics`), encoded by the
`AnalyticsPublisher`, consumed by the `AnalyticsConsumer` and projected into
`funnel_rollup.lines_released` / `orders_released` (ADR 0006). Nothing in
`internal/application/usecases` ever published either event: since
[ADR 0005](./0005-choreographed-release-via-kafka.md) the release is a pure
domain transition (`Order.Release`) inside `allocateAndRelease`, announced
only through the enriched `OrderAllocated` / `OrderPartiallyAllocated`
integration events. The two release facts were left over from the pre-ADR-0005
design in which a synchronous wes-work-planning call produced them. The
consequence was a published data product with two columns that were always
`0`, documented as a known gap in the funnel report and the DDD pack.

## Decision

`allocateAndRelease` raises the two facts at the one place where lines really
move `Allocated` -> `Released` (`publishReleaseFacts`, called right after the
aggregate `Save` and the `OrderAllocated`/`OrderPartiallyAllocated`
publish, inside the same `atomically(UnitOfWork)` scope):

- **`OrderLineReleased`** — one per line released **in that pass**, carrying
  the line's own `PathId` and the frozen work-unit id
  (`usecases.WorkUnitID`, `{orderID}-line-{lineNo}`). A line released in an
  earlier pass is never announced again.
- **`OrderReleased`** — once, when that pass leaves **every** line of the
  order `Released` (`Order.Status() == Released`). A partial-shipment order
  therefore raises `OrderLineReleased` per pass and `OrderReleased` only when
  its last backordered line is retried and released.

Nothing is raised when the release leg releases nothing: a ship-complete order
blocked by BR3 ([ADR 0003](./0003-ship-complete-default-and-fail-closed-allocation.md)),
a held order ([ADR 0020](./0020-network-originated-demand-hold-and-deadline-feasibility.md))
before `ReleaseHeldOrder`, a held order whose reconfirm lost a reservation, or
a repeated `ReleaseHeldOrder`.

Both events are **analytics-only**. The integration topic
`warehouse.order-management.events` keeps its three-event contract
(`OrderAllocated`, `OrderPartiallyAllocated`, `OrderRepromised`); wes-work-planning
continues to learn about released lines only from `OrderAllocated.lines[]`.
They are CloudEvents 1.0 on `warehouse.order-management.analytics`, structured
mode, built by `internal/adapters/kafka/cloudevents` from the event-name
constants:

- `com.warehouse.wes.order-management.order.OrderLineReleased`
  (`dataschema` `urn:warehouse:order-management:analytics:OrderLineReleased:v1`)
- `com.warehouse.wes.order-management.order.OrderReleased`
  (`dataschema` `urn:warehouse:order-management:analytics:OrderReleased:v1`)

Payloads (`order_id`, `line_no`, `path_id`, `work_unit_id` / `order_id`,
`path_id`) are exactly what `apis/asyncapi.yaml` already declared; no schema,
type or topic changes. Because they go through the same `UnitOfWork`, the
transactional outbox ([ADR 0022](./0022-transactional-outbox.md)) writes their
rows in the same transaction as the aggregate save.

## Consequences

- **Contract change, additive.** Two analytics event types that were declared
  but never on the wire now appear on `warehouse.order-management.analytics`.
  Consumers dispatch on the full `type` and ignore unknown types
  ([ADR 0030](./0030-cloudevents-mandatory-event-envelope.md)); the only
  in-repo consumer, `cmd/order-projector`, already handles them. No existing
  event, payload or REST response changes.
- The Order Funnel's `linesReleased` and `ordersReleased` become real. They
  count releases from the deploy onwards; orders released earlier emitted no
  events and are not back-filled.
- Per released order the analytics topic and outbox carry `lineCount + 1`
  more rows, relayed like every other event.
- Event ordering within a pass, keyed by `OrderId` (ADR 0027):
  `OrderLineAllocated…`, `OrderAllocated`/`OrderPartiallyAllocated`,
  `OrderLineReleased…`, then `OrderReleased`.

## Alternatives considered

- **Delete the two declared events.** Rejected: the projector, the funnel
  columns, the AsyncAPI contract and the report all depend on them; raising
  them is smaller than removing them from four places and a published data
  product.
- **Derive the counts from `OrderAllocated.lines[]` in the projector.**
  Rejected: it would make the projector re-implement what "released" means and
  leave two event types permanently dead.
- **Also forward them on the integration topic.** Rejected: nothing consumes
  them there, and ADR 0005 deliberately keeps that topic to the minimal subset
  wes-work-planning needs.

## Verification

Unit: `internal/application/usecases/release_events_test.go` (ship-complete
release raises N line facts + one order fact with the work-unit id and path;
partial shipment raises no `OrderReleased` until the last line is retried and
does not re-announce earlier lines; BR3-blocked, held, and
lost-reservation-at-reconfirm raise nothing; a repeated held release raises
nothing). Integration (Testcontainers Postgres):
`TestOutbox_ReceiveOrder_ReleaseFactsCommitWithTheAggregate` proves the rows
land on the analytics topic only, with the CloudEvents `type` above, committed
with the aggregate persisted `Released`.
