---
title: Use Cases
sidebar_label: Use Cases
sidebar_position: 1
description: The nine application-layer use cases, their HTTP, MCP and Kafka triggers, the events they raise and their failure modes.
---

# Use Cases

Nine use cases, one struct each, in `internal/application/usecases`. Each
depends only on the domain and on `application/ports` — never on an
adapter. Dependencies are plain struct fields, wired in `cmd/order`
(`main.go`, `wiring.go`, `planned_capacity.go`) and, for `GetOrder` over
MCP, in `cmd/mcp`.

Per [ADR 0005](/docs/adr/0005-choreographed-release-via-kafka), allocation
and release are not public use cases. They are folded into one shared
function, `allocateAndRelease` (`allocation.go`), that `ReceiveOrder`,
`RetryAllocation` and `ReleaseHeldOrder` all call — a caller expresses one
intent ("place an order", "retry a stuck one", "release a held one") and
this service attempts allocation and then release as one flow.

| # | Use case | Trigger | Raises |
| --- | --- | --- | --- |
| 1 | `ReceiveOrder` | `POST /orders` | `OrderReceived`, then (best-effort) the allocation-pass events |
| 2 | `RetryAllocation` | `POST /orders/{id}/retry-allocation` | `OrderLineAllocated`, `OrderLineBackordered`, `OrderAllocated`/`OrderPartiallyAllocated`, `OrderAllocationPartiallyFailed` |
| 3 | `ReleaseHeldOrder` | `POST /orders/{id}/release` | `OrderLineBackordered` (lost reservation), `OrderAllocated`/`OrderPartiallyAllocated` for the released lines |
| 4 | `CancelOrder` | `DELETE /orders/{id}` | `OrderCancelled` |
| 5 | `GetOrder` | `GET /orders/{id}`, MCP `get_order` | — (query) |
| 6 | `RepromiseOrder` | Kafka `TaskCPTMissed`/`PackageManifested` on `warehouse.fulfillment.events` | `OrderRepromised` |
| 7 | `ApplyPlannedCapacity` | Kafka `CapacityPlan*`/`CapacityShortageDetected` on `warehouse.warehouse-planning.events` (opt-in) | — (read model write) |
| 8 | `GetPlannedCapacity` | `GET /planned-capacity?site=&from=` (registered only when the read model is wired) | — (query) |
| 9 | `OrderCapacityConstraints` | every order response, when the read model is wired | — (query) |

Step-by-step diagrams for every row are on
[Sequence Diagrams](./sequence-diagrams.md).

## 1. ReceiveOrder(lines[], allowPartialShipment, releaseOnAllocation, requiredShipBy)

Intake. Validates the intake intent (a held order must be ship-complete),
then for every requested line looks up the SKU's product classification
(fail-open), resolves its process path with `PathSelectionPolicy` over the
active catalogue paths (shortest known `CycleTimeP95` among eligible paths,
ties to the lower `PathId`, `pick` when no catalogue is wired — ADR 0021),
and rejects an inactive path (`400` `unknown-process-path`) or a line no
active path admits (`422` `line-ineligible-for-resolved-path`) before
anything is persisted. It then mints an `OrderId`, persists the order in
`Received` status, and publishes `OrderReceived` **unconditionally**.

With Postgres, the call sits behind the `Idempotency-Key` middleware
(ADR 0023): the header is required (`400` `idempotency-key-required`), a
replay returns the stored response, and the same key with a different body
is `422` `idempotency-key-reused`.

**Request:**

```json
{
  "allowPartialShipment": false,
  "releaseOnAllocation": true,
  "lines": [
    {"sku": "SKU-1", "quantity": 2},
    {"sku": "SKU-2", "quantity": 1, "giftWrap": true}
  ]
}
```

`pathId` is never part of the request — the path is resolved internally.
Optional fields from
[ADR 0020](/docs/adr/0020-network-originated-demand-hold-and-deadline-feasibility):
`releaseOnAllocation` (default `true`; `false` holds the order after
allocation — it must then be ship-complete, else `422`
`held-order-must-be-ship-complete`) and `requiredShipBy` (the promise is
constrained to a CPT window at or before that instant; if none exists the
order is still received and allocated, but the response carries no
`promiseDate`).

In the same call, `ReceiveOrder` runs `allocateAndRelease` as a
best-effort next step. **A hard (non-business) failure in that step never
fails `ReceiveOrder` itself** — the order really was received. The failure
is visible through `OrderAllocationPartiallyFailed` whenever some line was
allocated first.

**Response — `201 Created`, `Location: /orders/{id}`** — reflects whatever
the allocation pass achieved:

```json
{
  "id": "ord-a1b2c3d4-0000-0000-0000-000000000001",
  "status": "Released",
  "allowPartialShipment": false,
  "releaseOnAllocation": true,
  "promiseDate": "2026-08-27T12:00:00Z",
  "lines": [
    {"lineNo": 1, "sku": "SKU-1", "quantity": 2, "pathId": "pick", "giftWrap": false, "status": "Released", "reservationId": "res-a1b2c3d4-0000-0000-0000-000000000009"},
    {"lineNo": 2, "sku": "SKU-2", "quantity": 1, "pathId": "pick", "giftWrap": true, "status": "Released", "reservationId": "res-a1b2c3d4-0000-0000-0000-000000000010"}
  ]
}
```

**Fails when:** a line's SKU is empty, the order has no lines, the body is
malformed or the resolved path is not active (`400`); a quantity is not
greater than zero, a line is ineligible, or a held order allows partial
shipment (`422`); or the order could not be persisted (`500`) — never
because the implicit allocation pass hit a hard failure.

## 2. `allocateAndRelease` — the shared allocate-then-release flow

Not a public use case (`internal/application/usecases/allocation.go`); it
runs inside one `UnitOfWork` transaction.

1. Calls inventory-storage's `POST /reservations` once per line to
   allocate, with this order's id as `demandRef` and a deterministic
   `Idempotency-Key` (ADR 0028). **BR2:** a `409` is the business fact
   "not enough usable stock" — that line becomes `Backordered` and the pass
   continues. Anything else is ambiguous: the pass hard-fails, keeps what
   genuinely allocated and raises `OrderAllocationPartiallyFailed`.
2. Computes the promise — `PromisePolicy.PromiseGroups` (capability-derived
   CPT window per shipment group, lead time as fallback — ADR 0014/0017),
   or `FeasibleBy` when `requiredShipBy` is set (ADR 0020).
3. If the order is held, saves and publishes the outcome with no released
   lines. Otherwise it **reconfirms** every line allocated in an earlier
   pass (a lapsed reservation now `409`s and the line goes back to
   `Backordered` via `LoseReservation`), checks `EnsureReleasable` (**BR3**:
   a ship-complete order releases nothing while any line is unallocated)
   and releases every `Allocated` line with the pure domain transition
   `Order.Release`.
4. Saves the order once (version-guarded, ADR 0024) and publishes
   `OrderAllocated` or `OrderPartiallyAllocated` with a `lines[]` payload of
   exactly what was released this pass.

A BR3 block is not an error: the order is saved `Backordered` and the HTTP
caller sees a success code.

## 3. RetryAllocation(orderId)

Re-attempts allocation for every `Backordered` line only — the single
route from `Backordered` back to `Allocated` — then releases in the same
call via `allocateAndRelease`. Unlike `ReceiveOrder`, this is an explicit
recovery action: a hard failure **does** propagate to the caller.

**Fails when:** order unknown (`404`), no backordered line (`409`
`no-backordered-lines`), a concurrent writer won (`409`
`concurrent-modification`), inventory-storage fails non-409 (`503`
`downstream-unavailable`) or is not configured (`503`
`downstream-not-configured`).

## 4. ReleaseHeldOrder(orderId)

The second half of the ADR 0020 hold. Reuses `allocateAndRelease`'s
release leg with no lines to allocate, so reconfirmation, BR3, the promise
recompute and the outcome events are those of every other release.
Idempotent for a held order whose lines are already released (`200`, no
change, no event); an order that was never held answers `409`
`order-not-held`. The hold flag is never cleared.

**Fails when:** order unknown (`404`), not held (`409`), concurrent writer
(`409` `concurrent-modification`), inventory-storage fails during
reconfirm (`503`). A ship-complete held order that lost a reservation is
**not** an error: nothing is released and the call returns `200` with the
order `Backordered` — `ship-complete-blocked` is mapped in `errors.go` but
unreachable over HTTP.

## 5. CancelOrder(orderId)

Checks `EnsureCancellable` first (**BR6**: `409` `order-already-released`
if any line is `Released`, before anything is revoked), then revokes every
allocated line's reservation via `DELETE /reservations/{id}`, cancels every
line and saves.

**Response:** `204 No Content`.

**Fails when:** order unknown (`404`), any line released (`409`), a
concurrent writer (`409` `concurrent-modification`), a revoke fails
(`503`) — earlier revocations stay in place and the caller retries.

## 6. GetOrder(orderId)

Read-only. Returns the current `Order`, with the order-level `status`
derived from line statuses at read time. Also backs the MCP tool
`get_order` (`cmd/mcp`, ADR 0010).

**Fails when:** order unknown (`404`).

## 7. RepromiseOrder (Kafka-driven)

Not reachable over HTTP. `RepromiseConsumer` (`internal/adapters/inbound/kafka`,
group `order-management-repromise`) decodes `TaskCPTMissed` /
`PackageManifested` from `warehouse.fulfillment.events`, maps the
work-unit id back to an order line (`ParseWorkUnitID`), and
`RepromiseOrder` recomputes that line's shipment-group promise, publishing
`OrderRepromised` only if it moved
([ADR 0018](/docs/adr/0018-repromise-order-consumer-and-order-repromised)).
Each inbound CloudEvents `id` is processed at most once
(`ports.RepromiseProcessedEvents`); after 3 failed attempts the message is
dead-lettered to `warehouse.fulfillment.events.dlq`.

## 8. ApplyPlannedCapacity (Kafka-driven, opt-in)

Runs only when `PLANNED_CAPACITY_CONSUMER_GROUP` is set
([ADR 0031](/docs/adr/0031-consume-warehouse-planning-capacity-plans)).
`PlannedCapacityConsumer` maps `CapacityPlanCreated` to a `DRAFT` window
and `CapacityPlanPublished`/`CapacityShortageDetected` to a `PUBLISHED`
one; the use case validates the window, claims the event id
(`ports.PlannedCapacityProcessedEvents`) and upserts it into
`planned_capacity_windows` (last writer wins, a DRAFT never overwrites a
PUBLISHED plan). No order changes and no event is raised.

## 9. GetPlannedCapacity and OrderCapacityConstraints (queries)

`GetPlannedCapacity` serves `GET /planned-capacity?site=&from=` — the
windows for one site that end after `from` (default now).
`OrderCapacityConstraints` is called while building every order response:
an order with a promise gets a `capacityConstraint` annotation listing the
published shortage windows its promise overlaps, for the single configured
site (`PLANNED_CAPACITY_SITE_ID`, defaulting to `DEFAULT_SITE_ID`). Neither
changes a promise.

## REST endpoint summary

| Method | Path | Use case |
| --- | --- | --- |
| `POST` | `/orders` | ReceiveOrder (allocates + releases automatically) |
| `GET` | `/orders/{id}` | GetOrder |
| `POST` | `/orders/{id}/retry-allocation` | RetryAllocation |
| `POST` | `/orders/{id}/release` | ReleaseHeldOrder (held orders only, ADR 0020) |
| `DELETE` | `/orders/{id}` | CancelOrder |
| `GET` | `/planned-capacity` | GetPlannedCapacity (ADR 0031) |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe (200 `ready`, 503 `not_ready` once shutdown starts) |

`POST /orders/{id}/allocate` no longer exists, and the pre-ADR-0005
general-purpose release endpoint is gone. The current `/release` path is
ADR 0020's held-order release only. The full contract, generated from
`apis/openapi.yaml`, is on the [API Reference](/docs/api-reference) pages.
