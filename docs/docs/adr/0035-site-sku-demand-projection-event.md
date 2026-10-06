---
id: 0035-site-sku-demand-projection-event
slug: /adr/0035-site-sku-demand-projection-event
title: "0035. SiteSkuDemandChanged — an additive, PII-free site/SKU demand projection event"
sidebar_label: "35. SiteSkuDemandChanged projection event"
sidebar_position: 35
description: "ADR 0035 — publish one PII-free SiteSkuDemandChanged integration event per source order line on warehouse.order-management.events, derived from Order aggregate line state in the same order-save/outbox transaction, with the site resolved from a versioned static demand-site scope in application configuration (never a fake Order field, no dynamic assignment)."
---

# 0035. SiteSkuDemandChanged — an additive, PII-free site/SKU demand projection event

## Status

Accepted — implemented by the change that introduces this record.

## Context

Site-level planning (which site will see how many units of which SKU, by
when) currently has no signal from this context. Everything published on
`warehouse.order-management.events` is order-shaped:
`OrderAllocated`/`OrderPartiallyAllocated` carry the lines released in a
pass — keyed by the **order** id — and `OrderRepromised` carries a
promise move. A consumer that wants "demand for SKU S at site X due by
T" has to reconstruct it from order lifecycle events that were never
designed for that grain, and that carry fields (`gift_wrap`, path
attribution, work-unit derivation) a site planner has no use for.

Two constraints shape the solution. First, the Order aggregate owns no
fulfillment site — a documented ADR-0031 deferral ("a fulfillment site
on the order"), and one this decision deliberately does not reverse:
inventing a fake `Order.SiteID` just to have something to publish would
pollute the aggregate with a projection concern. Second, the frozen
`OrderAllocated`/`OrderPartiallyAllocated` contracts shared with
wes-work-planning must not change (a hard repo rule); the demand signal
has to be a NEW, additive event.

The trigger for deciding now is network-originated demand (ADR 0020):
its orders arrive with externally-dictated deadlines, and site capacity
planning is the natural next consumer of a per-line demand fact.

## Decision

1. **A new domain event, `shared.SiteSkuDemandChanged`** — one
   occurrence per source order line whose demand at a site changed.
   Payload is deliberately minimal and PII-free:
   `{source_order_id, line_no, site_id, sku, demanded_units, due_at,
   state, assignment_version}` with `state ∈ {ACTIVE, REMOVED}`. No
   customer data, no product attributes, no path or work-unit
   vocabulary.

2. **CloudEvents 1.0 shape (ADR 0030), line-scoped identity.** `source`
   `/warehouse/order-management`, `type`
   `com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged`
   — a NEW `siteskudemand` entity segment, because this event is raised
   about a line's demand, not by the Order aggregate's lifecycle —
   `dataschema`
   `urn:warehouse:order-management:events:SiteSkuDemandChanged:v1`.
   `subject` and the Kafka key are both
   `<source_order_id>/line/<line_no>`: the LINE is the stream identity,
   so one line's demand history is totally ordered on its own partition
   under the existing `Hash` balancer. This is a deliberate departure
   from every Order* event's bare-order-id key, and the one a demand
   consumer actually needs.

3. **Derived from Order aggregate line state, transactionally.** The
   event is emitted by the use cases that change line state —
   `allocateAndRelease` (intake allocation, retry re-allocation, and
   the held-order release path's allocation leg), `RepromiseOrder`'s
   moved group, and `CancelOrder` — always AFTER the aggregate `Save`
   but INSIDE the same `atomically(UnitOfWork)` scope, so the order
   mutation and its outbox rows commit together or not at all
   (ADR 0022). ACTIVE is emitted for lines that allocate (including
   partial-progress salvages, which hold real reservations) and for the
   lines of a re-promised group with the FRESH cutoff; REMOVED for
   allocated lines a legal cancellation actually removes. Lines that
   never allocated emit nothing, and `due_at` is the line's OWN
   promise-group cutoff (ADR 0017) where one exists — a split-shipment
   order's lines are due at different instants — falling back to the
   order-level promise date. An order with no promise at all (an
   infeasible ADR-0020 deadline) emits nothing: there is no due date
   to report.

4. **The site comes from a versioned static scope in application
   configuration — the seam.** `usecases.DemandProjectionPolicy
   {SiteID, AssignmentVersion}` is application-layer configuration
   threaded through the use cases, resolved in `cmd/order` from
   `DEMAND_PROJECTION_SITE_ID` with the fixed
   `usecases.StaticDemandAssignmentVersion = "static-site-v1"`. The
   zero value (env unset) disables the whole projection — no event is
   ever emitted, byte-identical to before. The site is never an Order
   field, and Phase-1 performs no dynamic assignment: every line of
   every order projects to the one configured site. Dynamic assignment
   (Phase 5 of the planning rollout) must introduce a NEW
   `assignment_version` value, never change `static-site-v1`'s
   meaning — the version travels on the wire precisely so consumers
   can tell regimes apart.

5. **Additive everywhere.** `OrderAllocated`/
   `OrderPartiallyAllocated`/`OrderRepromised` wire contracts are
   untouched; `kafka.Publisher`'s existing encoders are unchanged; the
   event is NOT forwarded to the analytics topic (the projector's
   funnel is not a demand consumer — its `marshalData` default-skip
   already handles that for free). Existing consumers that dispatch on
   the full `type` ignore the new event by the fleet's standing rule.

## Consequences

- A site planner can build a per-(site, SKU, due-date) demand view from
  one keyed stream, idempotent on the CloudEvents `id`, without
  decoding any order-lifecycle vocabulary.
- The line-scoped key means one ORDER's demand events land on up to N
  partitions (one per line) — acceptable because the demand stream's
  ordering unit is the line, and the order's own events keep their
  existing per-order ordering on the same topic.
- Every use case that changes line state now carries one more optional
  dependency (`DemandProjectionPolicy`); forgetting to thread it into
  a future use case silently drops that use case's demand facts — the
  seam is explicit configuration, not a discovered service.
- `static-site-v1` states one site for the whole deployment: a
  multi-site deployment must wait for a versioned dynamic-assignment
  phase, which this ADR deliberately does not build.
- Cancellation of an order whose lines were already Released emits
  nothing (BR6/ADR 0004's no-clawback gap carries over to the demand
  projection: released work's demand is wes-work-planning's to
  reconcile).
