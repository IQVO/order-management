---
id: 0031-consume-warehouse-planning-capacity-plans
slug: /adr/0031-consume-warehouse-planning-capacity-plans
title: 31. Consume warehouse-planning's capacity plans into a local planned-capacity read model
sidebar_label: 31. Planned capacity from warehouse-planning
description: "ADR 0031 — order-management consumes warehouse-planning's CapacityPlan CloudEvents into a LOCAL, Postgres-backed read model of planned capacity windows (last-writer-wins per plan id) and annotates, at read time, the orders whose promise window overlaps a published shortage at the configured site. It only annotates: no promise is moved, no order is rejected, no reservation is touched."
---

# 31. Consume warehouse-planning's capacity plans into a local planned-capacity read model

## Status

Accepted — implemented by the change that introduces this record. Builds on
[ADR 0014](./0014-promise-derived-from-fulfillment-capability.md) (the promise
is a CPT window), [ADR 0015](./0015-wes-work-planning-path-capacity-changed-wired.md)
(the existing Kafka-fed capacity input), [ADR 0017](./0017-per-shipment-group-promising.md)
(per-group promises; fill-or-kill is unchanged),
[ADR 0018](./0018-repromise-order-consumer-and-order-repromised.md) (the
at-least-once consumer shape reused here),
[ADR 0022](./0022-transactional-outbox.md) (the `UnitOfWork` reused here) and
[ADR 0030](./0030-cloudevents-mandatory-event-envelope.md) (CloudEvents only).

## Context

`warehouse-planning` is a new bounded context that answers *"can this
warehouse process the demand assigned to it?"*. An operator creates a
`CapacityPlan` for a future window and publishes it; the service raises four
CloudEvents 1.0 on `warehouse.warehouse-planning.events` (key and `subject` =
the plan id, through a transactional outbox, so redelivery repeats the `id`):
`CapacityPlanCreated` (a DRAFT), `CapacityPlanPublished`, and — only when the
plan has a shortage — `CapacityShortageDetected` and `BottleneckDetected`. The
payload of `CapacityShortageDetected` is `plan_id`, `warehouse_id`, `location`,
`path_id`, `window_start`, `window_end` (exclusive), `assigned_demand`,
`capacity_over_window`, `shortage`, `bottleneck_step` — read from
warehouse-planning's own `apis/asyncapi.yaml`, not guessed. `location` is the
site/building code (e.g. `SIM1`); `warehouse_id` is the planner's own label.

A plan is a **planned statement about a future window**: "in this window, at
this site, 4000 orders of demand cannot be processed". It is published by a
person, it is not live execution state, and it says nothing about *when* the
backlog clears.

order-management already reacts to WES capacity in one way (ADR 0015): the
promise policy consults a Kafka-fed in-memory cache of wes-work-planning's
`PathCapacityChanged` at promise time. That input is **live remaining
capacity per (path, cutoff)** and feeds the CPT-window search. A planned
shortage is a different kind of fact — a window at a site, a quantity, a
status, authored ahead of time — so it needs its own model, and the question
is how far it should be allowed to reach into promising.

### What the spike found

- **How promise dates are computed.** `PromisePolicy` (ADR 0014/0017) picks
  the earliest CPT window every allocated line can make, from process-path
  capability, the site's CPT schedule and `PathCapacityChanged`, with
  `LeadTimePolicy` as the tagged fallback. The result is stored on the order
  as `promiseDate` plus per-group `PromiseGroups`. `SiteId` is **one
  configured value** (`DEFAULT_SITE_ID`, default `site-1`).
- **The order's fulfillment site cannot be determined from order data.**
  `Order`/`OrderLine` carry no site; nothing in intake, allocation (a
  reservation id only) or the promise records one. ADR 0014 documents the
  single configured site as a known simplification. This is **the gap**, see
  *What this does NOT solve*.
- **Allocation and fill-or-kill are out of scope.** ADR 0003/0017's
  ship-complete / fail-closed allocation and ADR 0004/0005's commit-before-work
  boundary must not change: a planned shortage must never reject an order,
  backorder a line, or touch a reservation.
- **The existing re-promise mechanism is not the right lever.** `RepromiseOrder`
  (ADR 0018) re-runs `PromisePolicy.PromiseGroups` when fulfillment-execution
  reports a *missed* CPT or a *manifested* package, and emits `OrderRepromised`
  when the promise moves. A planned shortage changes none of the inputs that
  function reads, and a shortage is a **quantity** ("4000 orders short"), not a
  **delay** — there is no honest number of hours to push a promise by. Pushing
  the date would invent one.
- **There is a `UnitOfWork` and tx-in-context mechanism** (`ports.UnitOfWork`,
  `internal/pgtx`, `postgres.UnitOfWork`). `RepromiseOrder` already runs its
  idempotency claim and its writes inside it; ADR 0018's "this repo has no
  UnitOfWork" sentence predates ADR 0022.

## Decision

### 1. A local read model, fed only by events

`order.PlannedCapacityWindow` (domain value object, `planned_capacity.go`) is
one row per CapacityPlan: plan id, warehouse id, `location`, path label,
`[start, end)`, assigned demand, capacity over window, `shortage`,
`bottleneck_step`, `status` (`DRAFT` | `PUBLISHED`) and `AsOf` (the CloudEvents
`time` of the event that wrote it). Two ports in `ports.go`:
`PlannedCapacityRepo` (`Upsert`, `ListByLocation`) and
`PlannedCapacityProcessedEvents` (`MarkProcessed`). Adapters: Postgres
(`planned_capacity_windows`, `planned_capacity_processed_events`, migration
`0010`, purely additive) and in-memory (no `DATABASE_URL`).

**There is no call to warehouse-planning, REST or MCP, anywhere.** The model
mirrors what that service published; a plan it never published does not exist
here.

**Last writer wins by plan id**, decided in one place
(`PlannedCapacityWindow.Supersedes`): a write with a *later or equal* `AsOf`
replaces the row — including its window, so a re-planned window overwrites
rather than accumulates — an older one is a no-op, and a `DRAFT` never replaces
a `PUBLISHED` row (a plan is published at most once and is never un-published).
Postgres reads the row `FOR UPDATE` and applies that same function, so the SQL
and in-memory adapters cannot disagree.

### 2. The consumer: the repromise consumer's shape, `PlannedCapacityConsumer`

`internal/adapters/inbound/kafka/planned_capacity_consumer.go`, mirroring
`RepromiseConsumer` (ADR 0018/0025) and reusing its DLQ writer, retry bounds,
backoff and tracing helpers:

- **CloudEvents only**, decoded through `internal/adapters/kafka/cloudevents`;
  **dispatch on the FULL `type`**; unknown types — and `BottleneckDetected`,
  whose `bottleneck_step` `CapacityShortageDetected` already carries — are
  **ignored and committed**, never dead-lettered; the CloudEvents `id` is the
  idempotency key. `CapacityPlanCreated` → `DRAFT`;
  `CapacityPlanPublished` and `CapacityShortageDetected` → `PUBLISHED`
  (both are applied; they carry the same facts and the same `time`).
- **Atomic with the idempotency mark.** `usecases.ApplyPlannedCapacity` runs
  `MarkProcessed` and `Upsert` inside **one `ports.UnitOfWork.Execute`** — the
  existing `postgres.UnitOfWork` carrying a pgx transaction in the context,
  which both repos join (`querierFrom`/`beginOrJoin`). A failure after the
  claim rolls the claim back, so the redelivery is processed instead of being
  skipped as "already handled". Pure payload validation
  (`PlannedCapacityWindow.Validate`) runs *before* the transaction opens, so a
  bad message never touches the database.
- **Offset committed only after success.** `FetchMessage` + `CommitMessages`
  (never `ReadMessage`); the offset is committed after the use case returned
  nil, or after the message was parked on the dead-letter topic.
- **Never crash, never block the partition.** A message that is not a CloudEvent
  (including the retired flat envelope), has an undecodable or invalid payload
  (missing plan id/location/window, inverted window, negative quantity,
  `subject` ≠ `data.plan_id`) goes straight to `<topic>.dlq` (raw bytes
  unchanged, error context in headers) and is committed. A transient
  infrastructure failure is retried in-process up to `maxHandlerAttempts` (3)
  with jittered backoff — each attempt rolls back fully — and only then
  dead-lettered, exactly ADR 0025's contract. A commit or DLQ-publish failure
  is retried forever without committing (nothing is lost).
- **Consumer group from configuration.** `PLANNED_CAPACITY_CONSUMER_GROUP`; the
  constructor takes the id as an argument and there is **no default**, so a
  locally run process can never join the live cluster's group by accident
  (`TestKafkaConsumerGroupNeverHardcodedInline`). A **stable shared** group, not
  a per-process-unique one: this is a normal process-and-commit consumer (ADR
  0018 §5), not a full-replay cache like ADR 0015's.
- **The env var is also the feature's off switch.** Unset ⇒ no consumer, no
  `/planned-capacity` route, no order annotation, no extra query: the service
  behaves *exactly* as before. The group id is set in warehouse-infra's
  `helm-values/order-management.yaml` through a dedicated chart value.

### 3. The effect on promising: annotate, derive at read time, change nothing else

When an order's promise overlaps a **published** shortage window at the
configured site, the order response carries a new, optional
`capacityConstraint` object — `{constrained: true, site, windows: [{planId,
windowStart, windowEnd, shortage, bottleneckStep}]}` — and is otherwise
byte-identical. Absent when there is no such window, so with no events consumed
every response is what it was.

The rule (`order.CapacityConstraints`, pure domain): the order's work window is
`[now, promise cutoff)`; it is constrained when that interval and a window's
`[start, end)` — **both half-open** — share at least one instant. A window
starting exactly at the cutoff does not overlap; one second of shared time does;
an empty or inverted interval overlaps nothing. With per-shipment-group
promises (ADR 0017) every group's cutoff is checked, else the legacy
`promiseDate`. Only `PUBLISHED` windows with `shortage > 0` count: a `DRAFT` is
visible on `GET /planned-capacity` but is never a statement an operator
committed to. A cancelled order, or one with no promise yet, is never
constrained.

It is **derived at read time** (`OrderCapacityConstraints.For`, called by the
HTTP adapter for `POST /orders`, `GET /orders/{id}`, `retry-allocation` and
`release`), not stored — the repo's own derive-don't-store discipline
(`Status`, `FulfillmentClass`). Consequently a plan published *after* an order
was placed annotates it immediately, with no order write, no
`ErrConcurrentModification` against the order's version column, and no
re-promise job that would have to scan orders by promise window (the order
repository has no such query). The annotation is fail-soft: a failing read
model is logged and omitted — an order read never fails because of an advisory
field.

It **does not**: move `promiseDate` or any `PromiseGroup`; publish
`OrderRepromised` (the promise did not move, so reusing that event would lie);
reject, backorder or hold an order; change `Status`; or touch allocation,
reservations or release. `PromisePolicy`, `Order`, every existing use case and
every existing test are untouched.

### 4. `GET /planned-capacity?site=SIM1[&from=…]`

A read endpoint over the local model (windows of every status ending after
`from`, default now, ordered by start) so an operator can see what order
annotations are derived from. `site` is required (400 otherwise), `from` must
be RFC 3339 (400 otherwise, including an empty `from=`). Registered only when the
integration is enabled (otherwise the router's 404, which the OpenAPI documents);
`scripts/contract-test.sh` sets `PLANNED_CAPACITY_CONSUMER_GROUP` so Schemathesis
contract-tests the route for real.

### 5. Site matching: the one honest simplification

`order.CapacityConstraints` matches `location == siteID` (exact, case-sensitive)
where `siteID` is `PLANNED_CAPACITY_SITE_ID`, defaulting to `DEFAULT_SITE_ID` —
the single site the promise itself is computed for. **This does not determine the
order's fulfillment site; it reuses the one-site assumption ADR 0014 already
documents.** See below.

## Consequences

**Easier**

- A published shortage becomes visible, with its window, size and bottleneck, on
  exactly the orders whose promise it overlaps — conservative (annotate-only)
  and explainable (the response names the plan).
- No new transaction pattern, no new envelope, no new event type, no new auth,
  no allocation change; one additive migration, one optional response field,
  one optional route, one optional env var.
- `PlannedCapacityWindow.Supersedes` and `.Overlaps` are small, pure, and pinned
  by exact-boundary tests (start == end, a one-second overlap, no overlap) and
  by the mutation gate.

**Harder / what this does NOT solve (read before extending)**

- **THE GAP: orders have no fulfillment site.** The annotation is only as right as
  the assumption that every order is fulfilled at the one configured site. If
  `PLANNED_CAPACITY_SITE_ID`/`DEFAULT_SITE_ID` does not equal the `location`
  warehouse-planning uses (the live fleet uses `SIM1`), **no order is ever
  annotated** — silently, by design (fail-open). Proposal: the order (or its
  reservation) should carry a `siteId` set at intake — from the allocating
  inventory-storage reservation's site, or from `network-fulfillment`'s intake
  call — and `OrderCapacityConstraints` should match on that instead of the
  configured one; `PromisePolicy.SiteId` would stop being a global at the same
  time. That is an order-model change with its own ADR, deliberately not smuggled
  into this one.
- **Path vocabularies differ.** warehouse-planning's `path_id` (e.g.
  `pick-rebin-pack`) is its own label, not an order-management process path id
  (`pick`). A window therefore constrains by **site and time only**, not by path;
  `pathId` and `bottleneckStep` are shown so a reader can judge relevance. A
  shortage on one path flags every order at the site promised across it.
- **A shortage is a quantity, not a delay.** The promise date is not pushed
  because nothing says by how much. If warehouse-planning later publishes a
  "capacity restored at" instant, moving the promise through
  `PromisePolicy`/`RepromiseOrder` (and `OrderRepromised`) becomes well-defined
  and should be its own ADR.
- **The annotation is time-relative.** It covers `[now, cutoff)`: once a shortage
  window has fully elapsed it no longer annotates an order that was exposed to
  it earlier. It describes the exposure ahead, not history.
- **No event is raised on annotation.** Downstream consumers see no
  `OrderCapacityConstrained`; the fact is on the read model and REST response
  only. Notification is a later, separate concern.
- **A deterministic poison message is dead-lettered, not retried; a persistent
  infrastructure failure is dead-lettered after three attempts** (ADR 0025). The
  DLQ copy is byte-identical and replayable; its claim is *not* recorded, so a
  replay is processed. A missed event leaves the model without that shortage —
  fail-open: orders are simply not annotated.
- The MCP `get_order` tool is unchanged (no annotation); only REST shows it.

## Alternatives considered

- **Push the promise date / re-promise orders when a plan arrives.** Rejected: a
  shortage is not a duration, so any push would be invented; and finding the
  affected orders needs a by-promise-window query the repository does not have,
  plus an order write (and version bump) per order per plan.
- **Persist a `capacity_constrained` flag on the order.** Rejected: it would go
  stale the moment a plan is re-published or a window elapses, and writing it
  means loading and saving every affected order. Derive-don't-store is this
  repo's established discipline for exactly this reason.
- **Reject or hold orders that overlap a shortage.** Rejected: fill-or-kill and
  fail-closed allocation (ADR 0003/0017) are explicitly out of scope; a planned
  statement must not become an intake gate.
- **Feed the shortage into `PathCapacity.Remaining` (ADR 0015's cache).**
  Rejected: that port is remaining units per `(path, cutoff)` for the window
  search; a site/window shortage in orders has neither a path nor an exact
  cutoff, and conflating them would corrupt the promise.
- **Call warehouse-planning's REST/MCP at read time.** Rejected: a hard
  constraint — contexts integrate through published events only.
- **In-memory cache with a per-process-unique group and full replay (ADR 0015's
  pattern).** Rejected: the model must be queryable from every replica without a
  warm-up replay and must dedupe on `id` atomically; ADR 0018's stable-group,
  Postgres-backed shape is the right one for state that is *applied*, not
  *cached*.
- **Handle the site by guessing it from the order** (a SKU prefix, the reservation
  id). Rejected as invention; see the gap above.

## Verification

- Unit: exact-boundary tests for `Overlaps` (start == end on either side, a
  one-second overlap, no overlap, empty/inverted interval), `Supersedes`,
  `Validate`, `CapacityConstraints` (groups, legacy date, cancelled, site,
  draft, zero shortage, ordering); handler tests (valid event updates the model,
  replayed id is a no-op, unknown type ignored, legacy-flat/garbage dead-lettered
  without error, transient repository failure returns an error leaving nothing
  written and the claim un-recorded).
- godog scenarios through the real HTTP surface (`features/planned_capacity.feature`).
- `-tags=integration` against testcontainers Kafka **and** Postgres: a real
  `CapacityShortageDetected` is consumed into the Postgres model, the real HTTP
  order response shows the annotation, a poison message reaches the real
  dead-letter topic without blocking, a replayed id is a no-op, and a failure
  inside the unit of work rolls back the claim together with the write.
- `gremlins` on `internal/domain/order`: every mutant in `planned_capacity.go` is
  killed; thresholds ratcheted in `.gremlins.yaml`.
