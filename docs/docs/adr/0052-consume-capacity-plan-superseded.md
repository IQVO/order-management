---
id: 0052-consume-capacity-plan-superseded
slug: /adr/0052-consume-capacity-plan-superseded
title: "0052. Consume CapacityPlanSuperseded: a superseded plan stops constraining orders"
sidebar_label: "52. CapacityPlanSuperseded; terminal SUPERSEDED"
sidebar_position: 52
description: "ADR 0052 — order-management consumes warehouse-planning's CapacityPlanSuperseded. The older plan's planned-capacity row moves to a terminal read-model status SUPERSEDED that never constrains an order; a Superseded for a plan never seen inserts a SUPERSEDED tombstone, so a late or redelivered Created/Published/ShortageDetected can never revive it. superseded_by is stored. Lands before warehouse-planning publishes the type."
---

# 0052. Consume CapacityPlanSuperseded: a superseded plan stops constraining orders

## Status

Accepted — implemented by the change that introduces this record. Extends
[ADR 0031](./0031-consume-warehouse-planning-capacity-plans.md) §1 (the
last-writer-wins rule of the planned-capacity read model) and §2 (the
consumer's dispatch table). Required by warehouse-planning ADR 0024
decision 5, and merged BEFORE warehouse-planning publishes the new type.

## Context

warehouse-planning ADR 0024 decision 3: publishing a capacity plan when
another PUBLISHED plan already exists for the same
`(site_id, location, path_id, window)` moves the OLDER plan to a terminal
status `SUPERSEDED`. In the same unit of work it publishes
`com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanSuperseded`
on `warehouse.warehouse-planning.events` (and on its analytics topic):

- `data`: `plan_id` (the OLDER plan), `superseded_by` (the newer plan's
  id), `site_id`, `path_id`, `window_start`, `window_end`.
- Kafka key and CloudEvents `subject` = the older plan's id.
- On one publish the events come in this order: Superseded (of the older
  plan), Published (of the newer one), then ShortageDetected and
  BottleneckDetected when the newer plan is short.

ADR 0024 decision 5 requires every consumer to keep only the NEWEST
published plan of a window. order-management, as it stands, does not:

- `PlannedCapacityConsumer` keys the read model `planned_capacity_windows`
  by `plan_id` and ignores every type it does not know — the new one
  included.
- `order.CapacityConstraints` lets EVERY published window with
  `shortage > 0` at the order's site constrain the order.

So once warehouse-planning publishes a replacement plan, the older plan
would keep annotating orders as capacity-constrained forever, next to the
newer one. ADR 0024 decision 5 makes this consumer change its own
decision, and it must merge before the producer starts publishing.

## Decision

1. **A new read-model status, `SUPERSEDED`, that never constrains.**
   `order.PlannedCapacitySuperseded` joins `DRAFT` and `PUBLISHED`;
   `Validate` accepts it. `Constraining()` is unchanged —
   `PUBLISHED && shortage > 0` — so a SUPERSEDED row never constrains an
   order, by construction rather than by a new exception.

2. **`SUPERSEDED` is terminal; it beats every status regardless of time.**
   `PlannedCapacityWindow.Supersedes(prev)`, still the one rule both
   adapters apply:
   - a SUPERSEDED `prev` is replaced by NOTHING — not a later Created,
     Published or ShortageDetected (a redelivery, a DLQ replay, an
     out-of-order delivery), and not a second Superseded (the first
     `superseded_by` stands);
   - a SUPERSEDED write replaces any non-SUPERSEDED `prev` whatever the
     two `time`s say — the plan is terminal at the producer, so its
     ordering against the plan's own earlier events does not matter;
   - between DRAFT and PUBLISHED the ADR 0031 rules are unchanged: a
     DRAFT never replaces a PUBLISHED row, otherwise a later-or-equal
     `time` wins.

3. **Superseding a known row keeps that row's facts.** The Superseded
   event carries no `warehouse_id`, quantities or bottleneck.
   When it lands on a row the model already holds, the stored row keeps
   everything it had (location, window, demand, capacity, shortage,
   bottleneck) and changes only `status` → `SUPERSEDED`,
   `superseded_by` and `AsOf` (the Superseded event's `time`). The merge
   is one pure domain function
   (`PlannedCapacityWindow.ReplacementOf(prev)`), so the Postgres and
   in-memory adapters cannot disagree, exactly as for `Supersedes`.

4. **A Superseded for a plan never seen inserts a tombstone.** The
   events of the older plan may not have reached this consumer (a
   partition replay, a DLQ replay, an older plan published before this
   consumer existed). The Superseded event then inserts a SUPERSEDED row
   with only the fields the event carries: `plan_id`, `location` = the
   event's optional `location` (the field the plan's other events carry),
   else its `site_id`, `path_id`, the window,
   `superseded_by`, the event `time`; `warehouse_id` empty and every
   quantity zero. Because of rule 2, a Published or ShortageDetected for
   that plan arriving later is a no-op: it can never make the plan
   constraining.

5. **`superseded_by` is stored.** A nullable `superseded_by` column
   (migration `0015`, additive) holds the newer plan's id, for
   `GET /planned-capacity` and for the logs. The same migration widens
   the status CHECK to `DRAFT | PUBLISHED | SUPERSEDED` and adds a CHECK
   that a SUPERSEDED row names its successor. `Validate` enforces the
   same: a SUPERSEDED window needs a `superseded_by` that differs from its
   own `plan_id`; a payload that breaks it is dead-lettered like any
   other invalid payload (ADR 0031 §2).

6. **The consumer adds one case, same shape.** `PlannedCapacityConsumer`
   dispatches the new FULL type to a SUPERSEDED write. It decodes
   `plan_id`, `superseded_by`, `site_id`, `path_id`, `window_start`,
   `window_end`; the CloudEvents `subject` must equal `data.plan_id`, as
   for the other types. Idempotency (CloudEvents `id`, one UnitOfWork),
   DLQ and retry are ADR 0031's, untouched. `BottleneckDetected` and
   every unknown type stay ignored and committed.

7. **The newer plan needs nothing new.** It arrives as its own
   CapacityPlanPublished (and CapacityShortageDetected when short) under
   its own `plan_id`, and constrains exactly as today.

8. **The read side shows the status; the annotation cannot.**
   `GET /planned-capacity` lists SUPERSEDED rows like DRAFT ones (every
   status is listed, ADR 0031 §4), now with an optional `supersededBy`;
   the OpenAPI `status` enum gains `SUPERSEDED` (a response enum value
   added — a warning, not an error, in the spec-breaking gate). The
   order's `capacityConstraint` is built only from constraining rows, so
   it never names a superseded plan.

## Consequences

**Easier**

- A replaced plan stops annotating orders the moment its Superseded is
  applied, and nothing that arrives afterwards can bring it back. The
  read model converges on "newest published plan of a window" without
  any cross-plan query or window matching in this repo: the producer
  names the older plan, and the row is keyed by that name.
- The decision still lives in two pure domain functions (`Supersedes`,
  `ReplacementOf`) pinned by a table test over every (prev, next) status
  pair and by the mutation gate.

**Harder / what this does NOT solve**

- **Lands before the producer.** warehouse-planning's pinned spec does
  not document `CapacityPlanSuperseded` yet. The consumer contract test
  therefore reads a LOCAL example
  (`internal/adapters/inbound/kafka/testdata/capacity-plan-superseded.example.json`)
  built from ADR 0024's field list, not the pinned producer spec. The
  follow-up, once warehouse-planning's change merges: refresh the pin
  and add a pinned-example assertion (E1/E2 against the producer's own
  example), then delete the local example.
- **A tombstone is listed under the plan's location.** Its `location` is
  the event's `location` when present, else its `site_id`, and its
  quantities are zero: `GET /planned-capacity`
  shows it as a SUPERSEDED row with no shortage. That is the truth this
  context knows about a plan it never saw published.
- **A down migration forgets superseded plans.** Restoring the old CHECK
  needs the SUPERSEDED rows gone, so `0015`'s down deletes them; a
  rolled-back binary would then accept a redelivered Published for such
  a plan as before this ADR. A rollback is a deliberate operator act;
  the trade-off is recorded here.
- **The rule is per plan id, not per window.** This context never decides
  which of two published plans of a window is newer; it trusts the
  producer's Superseded. A producer that replaced a plan without
  publishing it would leave both constraining, as before this ADR.
- **No new outbound event.** Superseding a plan raises nothing on this
  context's own topics; the change is visible on the read model and the
  REST read only.
