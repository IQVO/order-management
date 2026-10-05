---
id: 0020-network-originated-demand-hold-and-deadline-feasibility
slug: /adr/0020-network-originated-demand-hold-and-deadline-feasibility
title: 20. Network-originated demand — release-on-allocation as an intake choice, deadline feasibility, and the Network promise basis
sidebar_label: 20. Network-originated demand
description: "ADR 0020 — order-management's half of the network-fulfillment boundary: an additive releaseOnAllocation intake flag so demand can be allocated without being committed to the floor, a FeasibleBy(deadline) dual of the existing PromisePolicy.Promise selection, and a third PromiseBasis value Network for a promise dictated by an external deadline rather than chosen by this service. Companion to network-fulfillment ADR 0001."
---

# 20. Network-originated demand — release-on-allocation as an intake choice, deadline feasibility, and the Network promise basis

## Status

Accepted (2026-09-23). Companion to `network-fulfillment` ADR 0001 (the bounded
context that speaks a major e-commerce retailer's Selling Partner API and owns the
acknowledgement clock). Neither is meaningful without the other: this
record adds three narrow capabilities to this service that have no
caller until that context exists, and that context cannot honour a
24-hour fill-or-kill acknowledgement deadline without them. Raised
together so the boundary can be accepted or rejected as one decision.

This ADR deliberately adds **no** knowledge of a major e-commerce retailer, purchase orders,
ASINs or acknowledgement codes to this service. If a reader finds any of
that vocabulary in `internal/domain/order` after this ships, the
boundary has been violated — see §5.

## Context

[ADR 0014](/docs/adr/0014-promise-derived-from-fulfillment-capability)
replaced `now + leadTime` with a promise derived from real fulfillment
capability, and Phases 0–6 shipped it end to end
(ADRs 0015–0019). The promise this service makes is now a discrete CPT
window it *selects*: `PromisePolicy.Promise` answers **"what is the
earliest cutoff every allocated line can make?"**, tagging the result
`Capability` or `LeadTime` via `order.PromiseBasis`.

Every order this service has ever received has arrived through
`POST /orders` from a caller with no opinion about when it ships. The
promise is therefore always a free choice, and the intake flow reflects
that: since ADR 0005 the use case is a folded, best-effort saga —
receive, allocate, release — completing inside the one call. The
OpenAPI description states it plainly: *"allocates and releases
automatically"*.

A second class of demand is now in scope. In a
**network-originated** fulfilment relationship — a retail network
sending demand to a warehouse it does not own — three facts are true
that have never been true here:

1. **The deadline arrives with the demand.** The order carries a
   required ship-by instant set by the network. The promise is not
   selected; it is *given*, and the only question this service can
   answer is whether the building can meet it.
2. **Commitment precedes work.** The network requires an explicit
   accept/reject decision, and measures the accepting party on the gap
   between what was accepted and what actually shipped. Work must not
   start before that decision is made and sent.
3. **It is whole-order or nothing.** The accept/reject decision covers
   every line together. There is no "we'll send you three of the five".

Fact 1 is a small addition: the same three inputs `PromisePolicy`
already consults (`CapabilitySource`, `ScheduleSource`,
`CapacitySource`) answer "is any cutoff at-or-before *T* feasible?" as
readily as they answer "which cutoff is earliest?". It is the dual of a
question this service already answers well, and that it can be asked at
all is a good sign the ADR 0014 model was cut at the right joint.

Facts 2 and 3 collide with shipped behaviour, and those collisions are
the substance of this record.

### Collision A: there is no state between allocated and released

`ReceiveOrder` releases every eligible allocated line in the same call.
For network demand that is backwards — it commits work to the floor
before this fleet has told the network whether it accepts the order.
The only recovery available today is to release and then cancel, and
[ADR 0004](/docs/adr/0004-cancellation-boundary-at-release) is explicit
that **release is the cancellation boundary**: past it, this service
cannot claw work back, a gap that record documents honestly rather than
papering over. Optimistic release would therefore make every rejection
an unrecoverable one, and would do so by deliberately crossing a line an
earlier ADR drew on purpose.

The aggregate already distinguishes `Allocated` from `Released` — the
states needed exist and are tested. What is missing is a way for a
caller to *stop between them*.

### Collision B: per-shipment-group promising is exactly wrong here

[ADR 0017](/docs/adr/0017-per-shipment-group-promising) extended
`AllowPartialShipment` from "may release lines separately" to "may
promise lines separately", grouping lines by the cutoff each can make
and giving each group its own `Promise` (`order_promise_groups`, and
the additive per-line `promise_cpt_id`/`promise_basis`/
`promise_cutoff_at` fields on `ReleasedLine`). That is the right domain
rule for a customer buying a book and a bag of dog food.

Under fact 3 it is precisely the wrong rule: a whole-order accept/reject
decision cannot be expressed as several promises, and an order split
across two cutoffs has no single answer to give. Nothing in the code
prevents a network order from being received with
`allowPartialShipment: true` today, and the failure would be silent and
external — accepted, then partially shipped, then penalised by the
network. This needs to be an invariant, not a convention.

### What this service must NOT absorb

The pull is to model the network relationship here, since this is where
orders live. That would be wrong on the fleet's own established
grounds:

- A major e-commerce retailer's Selling Partner API is a **Conformist** upstream: this fleet
  has exactly zero influence over its contract. Its vocabulary
  (`purchaseOrderNumber`, `itemSequenceNumber`, `buyerProductIdentifier`,
  `acknowledgementStatus` codes, `sellingParty`/`shipFromParty`) is not
  ours and must be translated, not adopted.
- Network orders carry **customer PII** — ship-to name, address, phone —
  behind restricted-data authorisation. No context in this fleet holds
  PII today. Introducing it into the aggregate that four other contexts
  already read would be the single largest blast-radius decision
  available, made for the narrowest reason.
- The 24-hour acknowledgement clock is an **external SLA**. This service
  has no SLA-bearing responsibilities and should not acquire one.

So: the network relationship is a bounded context of its own
(`network-fulfillment`, its ADR 0001), and this record is the minimal,
additive set of capabilities it needs from here.

## Decision

### 1. `releaseOnAllocation` — an additive intake flag, default `true`

`ReceiveOrder` gains an optional request field:

```
releaseOnAllocation   boolean, default true
```

When `true` (the default, and therefore every existing caller and every
existing test) behaviour is **byte-identical to today**: receive,
allocate, release, publish `OrderAllocated`/`OrderPartiallyAllocated`.
The folded saga of ADR 0005 is untouched.

When `false`, the use case stops after allocation. Lines reach
`Allocated` and stop there; `OrderAllocated` is still published — the
allocation genuinely happened and inventory-storage genuinely holds
reservations — but no line is released, so `wes-work-planning` sees no
work. A promise **is** computed and attached at allocation time exactly
as today; a caller holding an unreleased order can therefore read the
promise and decide.

A second, small use case completes the pair:

```
ReleaseHeldOrder(orderId) — releases the already-allocated lines of an
                            order received with releaseOnAllocation=false
```

It reuses the existing release leg of `allocation.go` rather than
reimplementing it, and is a no-op returning success for an order whose
lines are already released (idempotent — the caller may be retrying
after a network failure and must not be punished for it).

Because the hold state is "allocated but not released", **cancellation
before release keeps working unchanged**, and ADR 0004's boundary is
respected rather than circumvented: a rejected network order is
cancelled on the correct side of the line, reservations are revoked
through the existing path, and no work ever reached the floor.

This is deliberately a domain-neutral capability. It is described in
terms of allocation and release — concepts this service already owns —
and is useful to any future caller that must decide before committing.
Nothing about it names a network.

#### Amendment (2026-09-23, during implementation): the hold intent is persisted

This record originally claimed the hold needed no storage, because
"allocated but not released" is expressible in the line statuses the
aggregate already owns. **That is wrong for the partially-allocated
case, and implementing it surfaced the hole.**

A held order can have a backordered line. When stock arrives,
`RetryAllocation` allocates it — and a held order whose lines are
`Allocated`-but-not-`Released` is *indistinguishable, by line status
alone*, from an ordinary ship-complete order that BR3 held back while it
waited for that same stock. Without stored intent, retry releases the
order, putting work on the floor for demand nobody has committed to:
precisely the failure the hold exists to prevent, arriving by the one
path nobody was watching.

So `orders.release_on_allocation BOOLEAN NOT NULL DEFAULT TRUE` is added
(migration `0005`), and callers pass `o.ReleaseOnAllocation()` into the
allocation flow rather than a literal. The intent is read from the
aggregate, which is what makes a *later* pass — triggered by a different
caller, minutes or hours afterwards — behave correctly.

Three details worth keeping:

- The aggregate field is stored as its **inverse** (`heldAtIntake`).
  Go's zero value for a bool is `false`, and every existing constructor,
  test literal and `Rehydrate` call site leaves the field unset; storing
  `releaseOnAllocation` directly would make all of them silently produce
  *held* orders — failing in the worst direction, as work that never
  reaches the floor.
- `release_on_allocation` is deliberately **absent from the `INSERT …
  ON CONFLICT DO UPDATE SET` list**. The hold is an intake fact; a later
  `Save` (e.g. after release) must never rewrite it.
- `ReleaseHeldOrder` does **not** clear the flag. It records what the
  order was *received as*, not where it is now — line statuses already
  carry current state. Clearing it would erase the order's history and
  destroy exactly the retry-safety it was added for.

`DEFAULT TRUE` keeps this additive: every pre-existing row, and every
caller that never sends the field, is unchanged.

### 2. `PromisePolicy.FeasibleBy(deadline)` — the dual of `Promise`

`PromisePolicy` gains one method alongside `Promise`:

```go
// FeasibleBy reports whether some CPT window at or before deadline can
// be made by every allocated line, and which window that is.
func (p PromisePolicy) FeasibleBy(now time.Time, o *Order, deadline time.Time) (Promise, bool)
```

It consults the same `CapabilitySource`, `ScheduleSource` and
`CapacitySource`, applies the same three per-line conditions ADR 0014 §2
states (path in the cutoff's `eligiblePathIds`;
`now + cycleTimeP95 <= cutoffAt`; capacity unknown or sufficient), and
returns the **latest** qualifying window at or before `deadline` — the
one that leaves the floor the most slack while still meeting the
commitment, which is the correct choice when the date is fixed and
cannot be improved by shipping earlier.

Where `Promise` falls back to `LeadTimePolicy`, `FeasibleBy` returns
`ok=false`. This asymmetry is intentional and is the single most
important line in this record: **"we could not determine feasibility"
and "we can meet your deadline" must never be the same answer.**
`Promise` falls back because a building that cannot say *when* is still
a building that will ship (ADR 0014's own words). `FeasibleBy` cannot
borrow that generosity — a guess here becomes an external commitment
this fleet is measured against. Missing schedule, unknown cycle time,
empty horizon: all `false`.

`Promise` is not modified. Its tests remain valid.

#### Amendment (2026-09-23, phase 4): the deadline reaches intake

Phase 2 added `FeasibleBy` and stopped there, which left this record
describing a method **nothing in the service ever called**. `POST /orders`
accepted no deadline, so the only way to reach it was a domain unit test.
`network-fulfillment` discovered this the moment it tried: its ADR 0001
§7 says feasibility is *asked* of `order-management`, and there was no
way to ask.

Intake therefore accepts an optional `requiredShipBy`:

- `orders.required_ship_by TIMESTAMPTZ NULL` (migration `0006`),
  nullable with no default, because "no deadline" is the overwhelming
  majority and must stay distinguishable from "a deadline at the zero
  instant" — the latter would make every ordinary order look infeasible.
- `Order.requiredShipBy` is a `*time.Time` for the same reason, set via
  a `SetRequiredShipBy` mutator rather than a fourth `Rehydrate`
  parameter (the chain is already `Rehydrate` / `RehydrateWithGroups` /
  `RehydrateHeld`; widening it again costs every call site an argument
  it does not care about).
- `allocationDeps.setPromiseDate` routes on it: a deadline present means
  `FeasibleBy`, absent means `PromiseGroups` exactly as before.

**An infeasible order is left with no promise at all**, and is still
created, still allocated, still returned `201`. "We cannot make your
date" is an answer, not a rejection — and an optimistic promise we
already know breaks the deadline would tell the caller we can do
something we cannot, when finding that out is their entire reason for
sending a deadline.

The deadline is persisted rather than passed transiently for the same
reason the hold is (§1's own amendment): a later `RetryAllocation`
re-promises, and a deadline that did not survive the round trip would
silently drop back to the ordinary earliest-window promise and break the
external commitment with no trace.

#### Amendment (2026-10, ADR-conformance audit): re-promise honours the deadline too

The routing above was applied at intake and in `RetryAllocation`, but
`RepromiseOrder` (ADR 0018) called `PromiseGroups` unconditionally. A
`Network`-basis order hit by `TaskCPTMissed`/`PackageManifested` was
therefore re-promised on the Capability/LeadTime basis — possibly past
the deadline, and silently flipping `promiseBasis` away from `Network`.
`RepromiseOrder` now routes exactly like `setPromiseDate`: an order with
a persisted `requiredShipBy` is recomputed by `FeasibleBy` (single
ship-complete group, `Network` basis, latest qualifying window at or
before the deadline). When the deadline is no longer feasible
(`FeasibleBy` → `false`), the existing promise is **kept** and no
`OrderRepromised` is published — the same "no promise we know breaks the
deadline" outcome as intake.

### 3. `PromiseBasis` gains a third value: `Network`

```go
BasisNetwork PromiseBasis = "Network"
```

A `Network`-basis promise is one whose `CutoffAt` was **dictated by an
external deadline** rather than selected by this service. `CptId` is
populated — the window `FeasibleBy` selected is a real departure from
the site's schedule — but the promise itself was not this service's free
choice.

This is the same discipline ADR 0014 already applies in distinguishing
`Capability` from `LeadTime`, extended one step: ADR 0019's promise-KPI
read model must not average a dictated promise together with a derived
one, or the promise-to-cutoff-gap and basis-distribution metrics become
meaningless the day network demand starts flowing. The basis is
recorded on the existing `promise_basis` wire field and needs no schema
change beyond accepting the new value.

### 4. Network-originated orders are ship-complete, enforced

An order received with `releaseOnAllocation=false` **and**
`allowPartialShipment=true` is rejected at intake with a 422. The two
are contradictory: a caller that needs to decide before committing is
by definition making one decision for the whole order, and per-group
promising (ADR 0017) would give it several answers where it can only
send one.

Stating it as an aggregate-level invariant rather than a convention in
the caller is what makes it hold. The alternative — trusting
`network-fulfillment` to always pass `false` — fails the first time
someone "improves" network orders by enabling split shipments, and
fails externally, where this fleet is penalised for it.

### 5. The boundary, stated so it can be checked

This service, after this ADR:

- knows an order may arrive with a deadline and may be held before
  release;
- does **not** know what a purchase order, an ASIN, an acknowledgement
  code, a selling party or a marketplace is;
- stores **no** customer PII. Network orders reach `POST /orders` with
  SKUs and quantities. The ship-to identity stays in
  `network-fulfillment` and reaches the carrier label from there.

The existing `arch-go` fitness test
(`internal/architecture/architecture_test.go`) already forbids adapter
and infrastructure types in the domain. It does not, and cannot, forbid
a *vocabulary* leak. That check is a human one, and this section is the
statement it checks against.

### Alternatives considered

- **Release optimistically, cancel on rejection.** No new intake flag;
  reuse `cancelOrder`. Rejected: ADR 0004 makes release the
  cancellation boundary and documents the no-clawback gap as a known
  limitation. This would convert that documented gap into a routine
  operational event, and would do it to satisfy an external party's
  deadline — the worst possible reason to cross a boundary an earlier
  decision drew deliberately.
- **Let `network-fulfillment` compute feasibility itself.** It has the
  deadline, and could consume the same three Kafka-fed caches.
  Rejected: that duplicates ADR 0014's promise math in a second repo,
  where it would drift. The promise is this service's responsibility —
  the same reasoning ADR 0014 used to reject moving promise computation
  into `wes-work-planning`, which likewise had the data.
- **A `Held` order status.** More explicit than "allocated and not
  released". Rejected: it adds a state to an aggregate four contexts
  read, and to every consumer's exhaustive status handling, to express
  something the existing `Allocated`/`Released` pair already expresses
  exactly. The status enum should describe where the work is, not why.
- **Reuse `BasisCapability` for deadline-driven promises.** Smaller
  enum. Rejected: it silently corrupts ADR 0019's KPIs, which exist
  precisely to keep different kinds of promise separable.
- **A `delivery-orchestration` bounded context now.** ADR 0014 named
  this as the genuine production shape and deferred it. Still deferred:
  this record adds two methods and a flag, which does not justify a
  service whose only aggregate is `Promise`. If this service ever
  splits, `PromisePolicy` and `FeasibleBy` lift out together, unchanged
  — which is the property worth preserving.

## Consequences

**Easier**

- Demand can be allocated without being committed, which any external
  channel needs and which this fleet has never been able to express.
- Feasibility against a fixed date is answerable from capability data
  the fleet already publishes, with no new synchronous edge and no new
  promise math.
- A dictated promise stays analytically distinguishable from a chosen
  one, so promise KPIs survive the arrival of network demand.
- Rejection is clean: cancel before release, reservations revoked
  through the existing path, no work on the floor, ADR 0004's boundary
  intact.

**Harder**

- `ReceiveOrder` gains a second outcome shape. Its tests must now cover
  the held path, and "allocated but not released" becomes a state that
  can persist indefinitely if a caller crashes between allocation and
  its decision. A held order holds real inventory reservations —
  `network-fulfillment` owns the acknowledgement clock that bounds this,
  but an orphaned hold is a new way for this service to sit on stock,
  and nothing here sweeps it. This is a real gap, recorded as one.
- `FeasibleBy`'s no-fallback rule means it returns `false` whenever the
  CPT schedule cache is cold — including during the startup window ADR
  0014 already accepted for the catalogue caches. Early in a restart,
  network orders are rejected rather than mispromised. That is the
  correct trade and it will still look like a bug to whoever sees it
  first.
- A third `PromiseBasis` value means every exhaustive switch on basis —
  in the projector, the report, the MCP tools — must handle it. The
  compiler will not catch a missing case on a string-typed enum.
- The 422 on `releaseOnAllocation=false` + `allowPartialShipment=true`
  is a new intake rejection, and its error body is a contract.

**Deliberately out of scope**

- Everything that speaks to a network: polling, acknowledgement codes,
  transaction-status reconciliation, advertised availability, shipment
  confirmation. All of it is `network-fulfillment` ADR 0001.
- Sweeping orphaned holds. Named above as a real gap; the natural owner
  is the context holding the SLA clock.
- Site modelling. ADR 0014 §"SiteId is a known simplification" still
  stands — every promise is computed against one configured site, and
  `FeasibleBy` inherits that unchanged.

> **Accepted deferral (2026-10 ADR-conformance audit).** The audit
> re-flagged the missing orphaned-hold sweeper. It is **deliberately not
> implemented in order-management**: a hold's lifetime is bounded by the
> acknowledgement clock `network-fulfillment` owns, so the sweeper's
> correct trigger and timeout live there, and a second, independent TTL
> here would race it. Until a sweeper exists, an orphaned hold keeps its
> inventory reservations until someone calls `DELETE /orders/{id}`
> (BR6-legal while nothing is released) or `POST /orders/{id}/release`.
> Tracking: open a follow-up in `network-fulfillment` ("sweep orphaned
> holds past the acknowledgement window by calling order-management's
> cancel") — this is an accepted limitation, not an unowned one.

## Rollout

Each step is its own feature branch and PR into `develop`, CI green
before merge, in this order:

1. **This ADR and its companion**, accepted together (docs only).
2. **This service:** `BasisNetwork`, then
   `PromisePolicy.FeasibleBy` with its own table-driven tests including
   the no-fallback cases. Pure domain, no wire change, no caller.
3. **This service:** `releaseOnAllocation` + `ReleaseHeldOrder` + the
   ship-complete invariant, with the OpenAPI regeneration that any
   `apis/openapi.yaml` change requires.
4. **`network-fulfillment`** builds against the shipped shape (its own
   ADR 0001's rollout).
5. **`e2e-tests`:** a scenario proving a held order reaches `Allocated`
   and produces no work in `wes-work-planning`, and one proving a
   deadline earlier than the earliest feasible cutoff yields
   `FeasibleBy` false rather than a fallback promise. Neither can pass
   by accident.
