---
id: 0047-promise-at-the-orders-own-fulfillment-site
slug: /adr/0047-promise-at-the-orders-own-fulfillment-site
title: "0047. An order is promised against its own fulfillment site"
sidebar_label: "47. Promise at the order's own site"
sidebar_position: 47
description: "ADR 0047 — amends ADR 0044, ADR 0014 step A and ADR 0031 §5. The CPT promise (Promise, PromiseGroups, FeasibleBy — intake, retry, held release and every re-promise) reads the CPT schedule of the order's own caller-assigned fulfillmentSiteId, falling back to DEFAULT_SITE_ID for an order without one; the planned-capacity annotation matches the same site, falling back to PLANNED_CAPACITY_SITE_ID. Path capacity and the process-path catalogue stay site-less because their upstream contracts carry no site. No new env var, no wire change, frozen events untouched."
---

# 0047. An order is promised against its own fulfillment site

## Status

Accepted — implemented by the change that introduces this record.
**Amends** [ADR 0044](./0044-caller-assigned-fulfillment-site-on-the-order.md)
(the order's site was used for demand projection only),
[ADR 0014](./0014-promise-derived-from-fulfillment-capability.md) step A (every promise
used one configured site, `DEFAULT_SITE_ID`) and
[ADR 0031](./0031-consume-warehouse-planning-capacity-plans.md) §5 (the annotation
matched one configured site). ADR 0044's rule is kept unchanged: the site is
**assigned by the caller, never inferred or looked up**.

## Context

ADR 0014 step A computed every promise against ONE site: `order.PromisePolicy.SiteId`,
wired from `DEFAULT_SITE_ID` (default `site-1`) in `cmd/order/outbound.go`, passed to
every `ScheduleSource.NextCutoffs` call in `promise_policy.go` (`Promise`,
`FeasibleBy`, `promiseForLines`). ADR 0031's planned-capacity annotation reused the
same one-site assumption through `PLANNED_CAPACITY_SITE_ID` (defaulting to
`DEFAULT_SITE_ID`). Both ADRs named the missing order site as the gap.

ADR 0044 then gave the order an optional, immutable, caller-assigned
`fulfillmentSiteId` — but only the site/SKU demand projection read it.

That left a real collision in the running fleet. `site-1` is the network-connected
building the storefront sells from. The warehouse-day simulator (e2e-tests) runs its
own building, `SIM1`, with compressed UTC cutoffs; because every order was promised
against `site-1`, the simulator had to PUT its compressed CPT schedule onto `site-1`
to get capability-based promises at all, and every simulator run overwrote the
storefront's real cutoffs and promises.

Alternatives considered:

- **A second order-management deployment per site.** Rejected: one service already
  holds every site's CPT schedule (the `kafkacptschedule` cache is keyed by
  `site_id`); duplicating the deployment to change one string per order is cost
  without a decision.
- **Infer the site from the reservation or the SKU.** Rejected by ADR 0044 §2: no
  sourcing logic belongs here.
- **Make the promise site a request parameter separate from `fulfillmentSiteId`.**
  Rejected: two site fields on one order could disagree, and the promise and the
  demand projection would then name different buildings for the same order.

## Decision

1. **The promise reads the order's own site.** One domain rule,
   `order.PromiseSiteFor(o, defaultSite)`: the order's `FulfillmentSiteID()` when set,
   else `defaultSite`. `PromisePolicy` resolves it **per call, from the order it is
   given** (`siteFor(o)`), so every entry point is covered without any caller passing
   a site: `Promise`, `PromiseGroups` (ship-complete and per-line partial shipment),
   and `FeasibleBy`. The field `PromisePolicy.SiteId` is renamed `DefaultSiteId` and
   is ONLY the fallback; `cmd/order` still wires it from `DEFAULT_SITE_ID`.

2. **Every promise path inherits it.** All of them go through those three methods
   with the persisted order, whose site is immutable (ADR 0044 §4): intake and
   retry (`allocationDeps.setPromiseDate`, for `ReceiveOrder`, `RetryAllocation` and
   `ReleaseHeldOrder`), the deadline-constrained hold (ADR 0020, `FeasibleBy`), and
   the re-promise consumer (`RepromiseOrder.freshPromiseGroups`, triggered by
   fulfillment-execution's `TaskCPTMissed` sweep and `PackageManifested`) whose
   `OrderRepromised` therefore carries old/new CPT ids of the order's own site.

3. **Every site-keyed read on the promise path, decided explicitly:**

   | Read | Keyed by site today? | Decision |
   |---|---|---|
   | CPT schedule — `ScheduleSource.NextCutoffs(siteId, …)`, `kafkacptschedule` cache (`schedules[site_id]`) | yes | **Per order**: `PromiseSiteFor(o, DEFAULT_SITE_ID)`. An order at a site with no schedule yet falls back to `LeadTimePolicy` (or, for `FeasibleBy`, answers *not feasible*) — it never borrows another site's schedule. |
   | Planned capacity — `OrderCapacityConstraints` → `PlannedCapacityRepo.ListByLocation(location, …)` and `order.CapacityConstraints(…, site, …)` | yes (planning `location`) | **Per order**, same rule, with `PLANNED_CAPACITY_SITE_ID` (itself defaulting to `DEFAULT_SITE_ID`) as the fallback. `capacityConstraint.site` names the site actually matched. Still advisory only (ADR 0031): the annotation never moves a promise. |
   | Path capacity — `CapacitySource.Remaining(pathId, cptId, cutoffAt)`, `kafkapathcapacity` keyed `(PathId, CutoffAt)` | **no** | **Kept site-less.** wes-work-planning's `PathCapacityChanged` carries no site (`path_id`, `cutoff_at`, `remaining_units`, `known`): it is single-site today. No multi-site capacity data is invented; see Consequences. |
   | Capability — `CapabilitySource.CycleTimeP95(pathId)` and path eligibility, `kafkacatalog` | **no** | **Kept site-less.** process-path-management's path catalogue events carry no site; a path's cycle time is one fleet-wide value. Window eligibility (`EligiblePathIds`) already comes from the per-site CPT schedule above. |
   | Demand projection — `DemandProjectionPolicy.resolve` | yes | Unchanged since ADR 0044 (per order, `DEMAND_PROJECTION_SITE_ID` fallback). |
   | `LeadTimePolicy` fallback | no | Unchanged; it is not site-aware. |

4. **Unchanged for every order without a site.** `PromiseSiteFor` returns
   `DEFAULT_SITE_ID` (promise) / `PLANNED_CAPACITY_SITE_ID` (annotation), exactly the
   values used before this ADR, so behaviour, responses and events are byte-identical
   for every existing caller.

5. **No contract change.** The frozen `OrderAllocated` / `OrderPartiallyAllocated` /
   `OrderRepromised` payloads do not change by a byte and still do not carry the
   site (ADR 0044 §7). No new env var, no new chart value (INFRA VALUES REQUIRED:
   none). The OpenAPI change is descriptive only: `fulfillmentSiteId` and
   `CapacityConstraint.site` now say which site the promise and annotation use.

## Consequences

- A caller that names a site gets that building's cutoffs. The warehouse-day
  simulator can put `fulfillmentSiteId: SIM1` on its orders and PUT its compressed
  schedule on `SIM1`, leaving `site-1` (the storefront's building) untouched; the
  storefront's orders, which carry no site, stay on `DEFAULT_SITE_ID`.
- A caller can name a site whose CPT schedule this service has never seen. That
  order is promised on the `LeadTime` basis (`basis=LeadTime` in the response),
  never on another site's windows — honest, and visible.
- **Known gap — path capacity is per path, not per site.** Because
  `PathCapacityChanged` has no site, two sites whose CPT windows share the same
  `(pathId, cutoffAt)` read the same remaining-units figure. The exact `CutoffAt`
  match (ADR 0015) keeps this rare; fixing it needs a site on wes-work-planning's
  event first (a `.v2`, its decision), then a site key here.
- **Known gap — cycle time is per path, not per site.** A building that is slower
  on the same path cannot say so until process-path-management's contract does.
- The site value object (`^[A-Z0-9]+$`, ADR 0044 §3) cannot express a lower-case
  code such as `site-1`, so an order cannot explicitly name the `site-1` building;
  it reaches it by naming no site. This is consistent with ADR 0044 (the default
  is the deployment's, not the caller's).
- Verified by domain tests (an order at `SIM1` promised on `SIM1`'s schedule while
  `site-1`'s differs; no site → `DEFAULT_SITE_ID`; re-promise stays on the order's
  site; `FeasibleBy` judged on the order's site only), use-case and HTTP tests, and
  a testcontainers integration test with a real Kafka CPT cache holding two sites
  and orders received and re-promised through real Postgres.
