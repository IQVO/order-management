---
paths:
  - "internal/domain/**"
  - "internal/application/**"
  - "features/**"
---
# Domain model

## Ubiquitous Language (use these exact names)

- **Order** — the aggregate root. `OrderId`, `OrderLine[]`,
  `AllowPartialShipment bool`, `Status`, the promise (`PromiseDate`,
  `PromiseCptId`, `PromiseBasis`, and per-shipment-group
  `PromiseGroups []PromiseGroup` — ADR-0014/0017), plus ADR-0020's
  `ReleaseOnAllocation()` (stored inversely as `heldAtIntake`) and
  optional `RequiredShipBy *time.Time`.
- **OrderLine** — `SKU`, `Quantity`, `PathId` (internal-only, resolved by
  `PathSelectionPolicy` since ADR-0005/0021 — never caller-settable, see below), `GiftWrap
  bool`, `LineStatus` (`Pending`/`Allocated`/`Backordered`/`Released`/
  `Cancelled`), `ReservationId *string` (set once allocated; needed to
  cancel).
- **Status** (order-level, ALWAYS derived from line statuses — never a
  redundant field that can drift out of sync): `Received` →
  `Allocated` | `PartiallyAllocated` | `Backordered` → `Released` |
  `PartiallyReleased` → `Cancelled` (only reachable from a pre-release
  state).
- **Allocation** — reserving stock for one line via inventory-storage's
  `POST /reservations`. This service does NOT model a local Reservation
  aggregate — it only stores the `ReservationId` reference.
  inventory-storage remains the sole owner/source-of-truth for reservation
  state.
- **Release** — since ADR-0005, announcing an allocated line as released
  work via a Kafka integration event, NOT a synchronous call.
  `wes-work-planning`'s own consumer reacts independently.
- **Promise** — computed at allocation time by `order.PromisePolicy`
  (ADR-0014): a CPT window derived from process-path capability
  (cycle time, eligibility, site CPT schedule) and wes-work-planning path
  capacity, `PromiseBasis=Capability`. When those caches are cold or
  `PATH_CATALOGUE_SOURCE=none`, it falls back to `LeadTimePolicy`
  (configurable per-path lead time, `PromiseBasis=LeadTime`). With
  `AllowPartialShipment=true` lines are promised per shipment group
  (ADR-0017). An order with `RequiredShipBy` uses
  `PromisePolicy.FeasibleBy` instead — the LATEST window at or before the
  deadline, `PromiseBasis=Network`, and NO promise (never a lead-time
  fallback) when the deadline cannot be met (ADR-0020).
- **Planned capacity** (ADR-0031) — a LOCAL read model of
  warehouse-planning's CapacityPlans (`order.PlannedCapacityWindow`: plan id,
  `location` = site code, `[start, end)`, `shortage`, `status`
  DRAFT|PUBLISHED), fed only by its CloudEvents, last-writer-wins per plan id.
  A PUBLISHED shortage overlapping an order's `[now, promise cutoff)` at the
  configured site ANNOTATES the order (`capacityConstraint`, derived at read
  time). It never moves the promise, rejects an order, or touches allocation.
- **Hold** (ADR-0020) — `releaseOnAllocation=false` at intake: the order
  allocates and stops ("allocated, not released" — deliberately NOT a new
  status), until `ReleaseHeldOrder` commits it. A held order must be
  ship-complete (`ErrHeldOrderMustBeShipComplete`).
- **Backordered** — a line-level state set when inventory-storage's
  `POST /reservations` returns 409 (insufficient usable stock). This is a
  BUSINESS FACT, distinct from a transport/5xx error, which is NOT a
  business fact and must fail the call outright rather than silently
  marking a line backordered (fail-closed on ambiguity — BR2).
- **FulfillmentClass** (ADR-0008) — a derived value object classifying an
  order's demand shape: `Single` / `SameSKUMulti` / `MultiLineMulti`. A
  demand-composition fact, computed on every call (never stored, same
  derive-don't-store discipline as `Status`), never a process-path name.
  Propagated additively on the frozen `OrderAllocated`/
  `OrderPartiallyAllocated` Kafka payload's `lines[].fulfillment_class`.
  Has zero effect on any domain transition (`Allocate`/`Release`/
  `EnsureReleasable`) — it is a downstream planning hint only.

## Aggregates & invariants (enforce in domain, unit-tested — each needs a failing-path test)

- **Order**: cannot allocate the same line twice; cannot release a line
  that isn't `Allocated`; cannot cancel once ANY line is `Released`
  (`ErrOrderAlreadyReleased`); order-level `Status` is always computed from
  line statuses, never stored redundantly.
- **OrderLine**: `Quantity` must be > 0; `SKU` must be non-empty; a
  `Backordered` line may transition back to `Allocated` ONLY via
  `RetryAllocation` — no other path. The only `Allocated` -> `Backordered`
  transition is `Order.LoseReservation`, used when the reconfirm-before-
  release step (`reconfirmAllocatedLines` in `allocation.go`) gets a 409
  for a line allocated in an earlier pass.
- **BR2 (fail closed on ambiguity)**: a `409` from inventory-storage's
  `POST /reservations` is the business fact "no usable stock" and
  backorders that one line. A transport failure, a 5xx, or any other
  non-2xx is NOT a business fact: the whole allocation pass fails and
  nothing is silently marked backordered.
- **BR3 (ship-complete default)**: `AllowPartialShipment=false` (the
  default): if ANY line ends up `Backordered` during allocation, the WHOLE
  order's status is `Backordered` (no line proceeds to release) until a
  human/caller issues `RetryAllocation`. `AllowPartialShipment=true`:
  allocated lines are independently eligible for release; order status
  becomes `PartiallyAllocated`.
- **BR6 (cancellation boundary)**: `CancelOrder` is legal ONLY while no
  line has reached `Released`. The boundary is checked BEFORE any
  reservation is revoked, so a rejected cancellation leaves
  inventory-storage untouched. Legal cancellation revokes every allocated
  line's reservation via `DELETE /reservations/{id}`. Once ANY line is
  `Released`, v1 does NOT claw back released work — a documented,
  deliberate known gap (ADR-0004), not an oversight.

## Domain events (past tense, 10 total — `internal/domain/shared/events.go`)

`ports.EventPublisher` has two real implementations selected by
`EVENT_PUBLISHER` (env, default `log`):

- **`log`**: every event logged as JSON, in-process only.
- **`kafka`**: same local behavior PLUS `OrderAllocated`,
  `OrderPartiallyAllocated` and `OrderRepromised` forwarded to the
  integration topic
  `warehouse.order-management.events`, and the full analytics-relevant
  event set fanned to `warehouse.order-management.analytics` (ADR-0006).

| Event | Raised when | Forwarded to Kafka integration topic? |
| --- | --- | --- |
| `OrderReceived` | `ReceiveOrder` accepts a new order into `Received` | No |
| `OrderLineAllocated` | inventory-storage's `POST /reservations` succeeds for a line | No |
| `OrderLineBackordered` | inventory-storage returns 409 for a line | No |
| `OrderAllocated` | Every line `Allocated`, eligible lines released in the same pass | **Yes** — enriched `lines[]` |
| `OrderPartiallyAllocated` | Some lines allocated/released, some backordered, `AllowPartialShipment=true` | **Yes** — enriched `lines[]` |
| `OrderAllocationPartiallyFailed` | Hard (non-409) failure mid-allocation; already-succeeded lines kept | No — operational visibility only |
| `OrderLineReleased` | Declared, never raised today (no use case publishes it; see `deferred-and-known-gaps.md`) | No |
| `OrderReleased` | Declared, never raised today | No |
| `OrderCancelled` | `CancelOrder` succeeds | No |
| `OrderRepromised` | `RepromiseOrder` (ADR-0018) finds a shipment group's promise moved after an inbound `TaskCPTMissed`/`PackageManifested` | **Yes** — `{cpt_id_old, cpt_id_new, reason}` |

Only 3 of the 10 are integration events, mirroring `inventory-storage`'s own
precedent of forwarding a minimal subset. Every event that IS raised is
fanned to the analytics topic when `EVENT_PUBLISHER=kafka` (see
`api-contracts.md`).

## Use cases (application layer) — folded flow since ADR-0005

1. **`ReceiveOrder(lines[], allowPartialShipment, releaseOnAllocation,
   requiredShipBy)`** → validates lines, resolves each line's path
   (`PathSelectionPolicy`, ADR-0013/0016 — unknown path 400, ineligible
   line 422, both before anything persists), mints `OrderId`, persists
   `Received`, publishes `OrderReceived` unconditionally. **Immediately afterward, in the SAME call**, attempts
   allocation-then-release as a best-effort next step (shared
   `allocateAndRelease` function in `internal/application/usecases/allocation.go`).
   A hard failure in that best-effort step does NOT fail `ReceiveOrder`
   itself — the order genuinely was received; the returned body reflects
   whatever the pass actually achieved. With `releaseOnAllocation=false`
   the pass stops after allocation (ADR-0020).
2. **`RetryAllocation(orderId)`** → re-attempts allocation for `Backordered`
   lines only, then ALSO attempts release in the same call on success. This
   is an explicit, on-purpose recovery action: unlike `ReceiveOrder`'s
   implicit attempt, a hard failure here DOES propagate to the caller.
3. **`CancelOrder(orderId)`** → revokes every allocated line's reservation
   via `DELETE /reservations/{id}`; rejects with `ErrOrderAlreadyReleased`
   if any line is already `Released` (BR6, checked before any revoke call).
4. **`GetOrder(orderId)`** → current Order state (read).
5. **`ReleaseHeldOrder(orderId)`** (ADR-0020) → releases a HELD order's
   allocated lines via `allocateAndRelease`'s release leg (same BR3 gate,
   same promise recompute, same events). Idempotent for a held order;
   `ErrOrderNotHeld` (409) for one that was never held.
6. **`RepromiseOrder`** (ADR-0018) → driven by the inbound Kafka consumer,
   not HTTP: recomputes the affected line's shipment-group promise and
   publishes `OrderRepromised` if it moved; idempotent on the inbound
   CloudEvents `id` (`ports.RepromiseProcessedEvents`).

`AllocateOrder` and `ReleaseOrder` no longer exist as public use case
types — their pure domain-transition logic (`Order.Allocate`,
`Order.Release`, `Order.EnsureReleasable`) is unchanged, this was an
application/adapter-layer redesign, not a domain-layer one.

## pathId is internal-only (ADR-0005)

The inbound `POST /orders` DTO has no `pathId` field. Each line's path is
resolved by `order.PathSelectionPolicy.Select` (ADR-0013/0016/0021): it
lists every active path from `ports.ProcessPathCatalogue.ListActive`,
keeps the ones whose declared `Eligibility` admits the line (max units,
required/excluded product attributes, gift wrap as the `"giftWrap"`
attribute), and picks the shortest KNOWN `CycleTimeP95`, ties broken on
the lower `PathId`. A nil catalogue (`PATH_CATALOGUE_SOURCE=none`) or an
empty `ListActive()` fails OPEN to `shared.DefaultPathId` (`"pick"`); no
eligible candidate is `shared.ErrLineIneligibleForResolvedPath` (422). The
response DTO still shows `pathId` on every line — callers can see it,
never set it. A caller placing an order has no business supplying
`wes-work-planning`'s internal routing vocabulary.
