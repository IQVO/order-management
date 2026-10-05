---
id: 0014-promise-derived-from-fulfillment-capability
slug: /adr/0014-promise-derived-from-fulfillment-capability
title: 14. The delivery promise is a CPT window derived from fulfillment capability, not a configured lead time
sidebar_label: 14. Promise derived from fulfillment capability
description: "ADR 0014 — replace now+leadTime with a PromisePolicy that selects a discrete CPT window per shipment group from process-path capability (cycle time, eligibility, CPT schedule) and remaining path capacity, keeps LeadTimePolicy as a tagged fallback, and closes the loop with OrderRepromised when the building misses."
---

# 14. The delivery promise is a CPT window derived from fulfillment capability, not a configured lead time

## Status

Accepted. Companion to process-path-management ADR 0010 (fulfillment
capability contract), accepted together, which supplies the data this
decision consumes. Depends on a future wes-work-planning ADR for
remaining capacity per CPT; this ADR is written so it degrades
correctly while that does not exist.

## Context

The promise this service makes at intake is the fleet's only customer-
facing commitment, and it is the input `wes-work-planning` uses as the
Critical Pull Time that orders every downstream queue (WES
`WorkPool.ReleaseNext` and fulfillment-execution `claim-next` are both
earliest-CPT-first). Today that promise is computed by
`order.LeadTimePolicy.PromiseDate`: `now` plus the slowest configured
lead time among allocated lines, with lead times read from the
`PROMISE_PATH_LEAD_TIMES` environment variable and a 48h default. The
policy's own doc comment calls this "the honest v1 model", and it was:
there was nothing in the fleet to derive a promise from.

That is no longer true, and the gap between how this service promises
and how a real fulfillment centre promises is now the largest domain-
fidelity gap in the fleet:

1. **The promise ignores path feasibility.** No signal about how long a
   path takes, what it may carry, or whether it is saturated reaches
   this service. `wes-work-planning` publishes `BacklogThresholdBreached`
   and `PathThrottled`; nothing here consumes them. A throttled path is
   promised at full speed.
2. **The promise is a timestamp, not a departure.** A real promise is
   "makes the 18:00 truck". Ours is "now + 24h", so WES receives a
   different CPT for every order and cannot group work onto the
   departure it actually competes for. Its per-CPT `ChargeForecast`
   buckets never coincide with the CPTs on real work units.
3. **Path selection is a stub.** ADR 0013 made `PathSelectionPolicy` a
   real domain object with one rule (`PICK`), explicitly deferring
   attribute-driven routing until real capability data existed. It now
   will (PPM ADR 0010).
4. **Nothing flows back.** `fulfillment-execution` publishes only
   `TaskCompleted`; this service's only inbound consumer is the path
   catalogue. A missed CPT is invisible to the order and to whoever
   placed it. There is no concept of re-promising.
5. **Nothing measures it.** None of on-time-to-CPT, re-promise rate,
   split-shipment rate or promise-to-CPT gap exists in any analytics
   read model.

The products this service has used as its bar (Manhattan Active OM,
Salesforce OMS, a major e-commerce retailer's order platform) all treat promise calculation
as first-class domain logic fed by fulfillment capability. ADR 0013
already recorded that standard; this ADR applies it to the promise
itself.

## Decision

### 1. The promise is a CPT window

`Order` gains a promise expressed as a value object:

```
Promise
  cptId        stable id from the site's CPTSchedule (e.g. "sp1-1800")
  cutoffAt     the concrete departure instant this promise targets
  basis        Capability | LeadTime
```

`promiseDate` (a bare `time.Time`) is kept on the wire and in the
database as `cutoffAt` for backward compatibility, and `OrderAllocated`
/ `OrderPartiallyAllocated` gain `promise_cpt_id` and `promise_basis`
as additive fields. `wes-work-planning` continues to build its CPT from
`promise_date`; once it also reads `promise_cpt_id`, work for the same
departure shares one CPT identity. That WES change is small and is
sequenced in the rollout below, not decided here.

### 2. `PromisePolicy` replaces `LeadTimePolicy` as the primary policy

A new domain service `order.PromisePolicy` computes the promise at
allocation time from three inputs, all supplied through outbound ports
so the domain stays free of Kafka and HTTP:

- `ports.ProcessPathCatalogue` widened from `IsActive(pathId)` to also
  return `CycleTimeP95` and `Eligibility` per path (the fields PPM ADR
  0010 adds; ADR 0013 explicitly left this widening for this phase).
- `ports.CPTScheduleCache` — `NextCutoffs(siteId, from, n)`, backed by
  the same Kafka-fed local cache, decoding `CPTScheduleChanged`. (The
  port shipped as `CPTScheduleCache`, not the `CPTSchedule` name sketched
  here, to read as what it is — a cache — beside `ProcessPathCatalogue`.)
- `ports.PathCapacity` — `Remaining(pathId, cptId) (units, known bool)`.
  Until wes-work-planning publishes capacity this port has one
  implementation, `unknown`, which always reports `known=false`.

The rule, per shipment group (see §3), is a major e-commerce retailer's rule stated
plainly: **the earliest cutoff that every line in the group can make.**
A line can make a cutoff when (a) its path is in the cutoff's
`eligiblePathIds`, (b) `now + cycleTimeP95 <= cutoffAt`, and (c) either
remaining capacity for `(path, cpt)` is unknown or `>= quantity`. If no
cutoff within the schedule horizon qualifies, the policy returns
`ErrNoFeasibleCPT` and the order is placed with a `LeadTime`-basis
promise (below), never rejected: a building that cannot say when is
still a building that will ship.

`LeadTimePolicy` is kept, unchanged, as the fallback when any input is
missing: no schedule for the site, a path with no cycle time (cannot
happen after PPM's migration, but the decoder must tolerate it), or
`ErrNoFeasibleCPT`. Every promise records which policy produced it in
`basis`, so the analytics can separate real promises from fallbacks
instead of averaging them together.

### 3. Split shipments are promised per group, not per order

`Order.AllowPartialShipment` already exists (ADR 0003). Its meaning is
extended from "may release lines separately" to "may promise lines
separately": when true, lines are grouped by the cutoff they can make
and each group gets its own `Promise`; the order's `PromiseDate()`
becomes the latest of them (so no existing reader sees an earlier date
than before). When false the whole order is one group and the slowest
line governs, exactly as today. This is the checkout behaviour of
showing two dates for a book and a bag of dog food; it is a domain rule,
not a presentation choice, and it lives on the aggregate.

### 4. `PathSelectionPolicy` becomes eligibility-driven

The ADR 0013 stub is replaced by evaluation of each Active path's
declared `Eligibility` against the line: quantity against
`maxUnitsPerLine`, `giftWrap`, and product attributes (`hazmat`,
`fragile`) obtained through the fleet's existing product-classification
sync edge (`PRODUCT_CLASSIFICATION_MODE=http|permissive`, the same
pattern fulfillment-execution and wes-work-planning already use). Among
eligible paths the one with the shortest `cycleTimeP95` wins. If none is
eligible, `PICK` remains the default (it is the only path with a
permissive eligibility after migration), so today's behaviour is the
floor, never a regression.

### 5. The loop closes: `OrderRepromised`

A new inbound Kafka consumer subscribes to
`warehouse.fulfillment.events` for two events fulfillment-execution will
add under its own ADR: `TaskCPTMissed` (a task still open past its CPT)
and `PackageManifested` (SLAM pass). A new use case `RepromiseOrder`
recomputes the promise for the affected shipment group from the current
capability/capacity inputs and, if it moves, raises
`OrderRepromised{orderId, cptId_old, cptId_new, reason}` on
`warehouse.order-management.events` and updates the aggregate. This
event is the fleet's "your delivery is delayed" trigger. It is
additive; nothing existing changes shape.

`RepromiseOrder` is idempotent on `(orderId, sourceEventId)`, using the
same inbox convention the fleet's other Kafka consumers use, because
fulfillment-execution's missed-CPT sweep will re-emit on every pass.

### 6. Promise KPIs become part of the analytical data product

ADR 0006's Order Funnel report gains: promise basis distribution,
re-promise rate, split-shipment rate, and promise-to-cutoff gap
(`cutoffAt - allocatedAt`). On-time-to-CPT itself is measured where the
evidence is, in fulfillment-execution's analytics (`cpt` vs
`manifested_at`), not here. Both are exposed as read-only MCP tools per
ADR 0010 so `warehouse-ops-agent` can reason about promise health
without a REST detour.

### Alternatives considered

- **Widen `PROMISE_PATH_LEAD_TIMES` and add a per-path saturation
  multiplier.** Cheaper, but the promise stays a guess with no departure
  identity, and nothing can be measured against it. Rejected as the
  status quo with more knobs.
- **Let `wes-work-planning` compute the promise and reply.** WES has the
  capacity data, but the promise is a commitment to the customer made by
  the order's owner; moving it downstream inverts the Customer/Supplier
  relationship ADR 0002 established and re-introduces the synchronous
  call ADR 0005 removed. Rejected.
- **A dedicated `delivery-orchestration` bounded context.** This is the
  genuine production shape (a promise/sourcing engine separate from
  order intake), and it is where this logic goes if this service ever
  splits. Rejected for now because it would be a service whose only
  aggregate is `Promise`, coupled to `Order` at every step; the policy
  is written as a pure domain service so it lifts out unchanged.
- **Reject orders that cannot make any CPT.** A real promise engine
  never refuses to sell for lack of a date; it promises later.
  Rejected; the lead-time fallback with `basis=LeadTime` is the honest
  behaviour.

## Consequences

**Easier**

- The promise becomes derivable from the same facts the building runs
  on; when it is wrong there is a specific field (`cycleTimeP95`, a
  schedule, a capacity signal) to correct, not an env var to widen.
- Orders competing for the same truck carry the same CPT identity
  downstream, making WES's per-CPT charge planning and FE's earliest-CPT
  dispatch operate on real departures.
- Missed CPTs become a domain event this service reacts to and a KPI
  the ops-agent can read, instead of a silent loss.
- `LeadTimePolicy`, `AllowPartialShipment` and `PathSelectionPolicy`
  are all extended rather than replaced; their existing tests remain
  valid and their behaviour is the guaranteed floor.

**Harder**

- Two more Kafka-fed local caches (schedule, capacity) with the same
  startup-readiness trade-off ADR 0013 accepted: a long Kafka outage
  blocks startup. The alternative, serving promises from a possibly
  stale cache, was rejected for the same reason ADR 0013 rejected it.
- Product attributes must be looked up at intake, adding a synchronous
  edge to whichever context owns product classification. It is the
  existing fleet pattern, permissive by default, but it is a new
  runtime dependency on the order-intake hot path.
- The aggregate grows: multiple promises per order when partial
  shipment is allowed is a real increase in `Order`'s state and
  invariants (a group may never be promised earlier than a line it
  contains can be released). The coverage and mutation gates on
  `internal/domain/order` will need real new tests, not a threshold
  adjustment.
- Until wes-work-planning publishes capacity, condition (c) is always
  satisfied and a saturated path is still promised optimistically. The
  `basis` field does not distinguish "capability without capacity" from
  "capability with capacity"; that is deliberate to keep the enum
  small, and the rollout order below makes the window short.
- `OrderRepromised` tells downstream that the promise moved but does
  not itself contact a customer; there is no notification context in
  this fleet. It is the trigger, not the email.

## Rollout

Each step is its own feature branch and PR into `develop`, CI green
before merge, in this order:

1. **process-path-management ADR 0010 ships first** (fields, schedule,
   events). This service's `kafkacatalog` decoder is proven tolerant of
   the new event type and fields before that merges.
2. **This service, step A:** widen `ProcessPathCatalogue`, add the
   `CPTScheduleCache` and the `unknown` capacity adapter, introduce
   `Promise` and `PromisePolicy`, tag `basis`, additive wire fields.
   Behaviour change is visible only where a schedule exists.
3. **wes-work-planning:** read `promise_cpt_id` when present; publish
   `PathCapacityChanged` (its own ADR). This service then swaps the
   `unknown` capacity adapter for a Kafka-fed one.
4. **This service, step B:** eligibility-driven `PathSelectionPolicy`
   and per-group promising.
5. **fulfillment-execution:** `TaskCPTMissed` sweep and
   `PackageManifested` on the outbox (its own ADR). **This service, step
   C:** `RepromiseOrder` consumer and `OrderRepromised`.
6. **Analytics + MCP tools + e2e-tests:** two discriminating scenarios
   in `e2e-tests` — "a saturated path pushes the promise to the next
   cutoff" (same order, different `promise_cpt_id` before and after
   saturating the path) and "a task open past its CPT produces
   `OrderRepromised`". Neither can pass by accident.
