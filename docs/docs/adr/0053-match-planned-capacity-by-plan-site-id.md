---
id: 0053-match-planned-capacity-by-plan-site-id
slug: /adr/0053-match-planned-capacity-by-plan-site-id
title: "0053. Match planned capacity to an order's site by the plan's site_id"
sidebar_label: "53. Match plans by site_id"
sidebar_position: 53
description: "ADR 0053 — amends ADR 0031 §5. The planned-capacity read model keeps the plan's canonical site_id (warehouse-planning ADR 0012). A window matches an order's site by its site_id when it has one, else by its location (the legacy rule). A write without a site_id never erases a stored one; the merge is one pure domain function the Postgres and in-memory adapters share."
---

# 0053. Match planned capacity to an order's site by the plan's site_id

## Status

Accepted — implemented by the change that introduces this record. **Amends
[ADR 0031](./0031-consume-warehouse-planning-capacity-plans.md) §5** (site
matching) and extends §1 (the read-model row and its merge). ADR 0031 is an
accepted ADR and, by the rule in [About ADRs](./about.md), is not edited to change its
decision: this record is the amendment, and ADR 0031's *Status* line points here, as it
already does for [ADR 0047](./0047-promise-at-the-orders-own-fulfillment-site.md) and
[ADR 0052](./0052-consume-capacity-plan-superseded.md). A new record rather than a
rewrite of ADR 0031 is required precisely because the decision changes.

## Context

ADR 0031 §5 matches a window to an order's site when `window.location == site`, where
`site` is the order's `fulfillmentSiteId` or `PLANNED_CAPACITY_SITE_ID` (ADR 0047). It
documented the weakness: warehouse-planning's `location` is the **ProcessCapacity
location the plan evaluates**, not necessarily a site code, so a deployment whose site
code differs from a plan's `location` annotates no order, silently.

warehouse-planning's own ADR 0012 added the canonical `site_id` for exactly this
reason: a facility-layout site code, additive and optional on the event (`omitempty`;
a plan stored before its migration 0008 carries none). Only `CapacityPlanPublished`
and `CapacityPlanSuperseded` carry it. The events that follow a Published on the same
plan do not:

- `CapacityShortageDetected` is queued AFTER `CapacityPlanPublished` and carries
  `location` only — and it is the event that carries the shortage.
- A late `CapacityPlanCreated` carries `location` only.

The read model upserts by `plan_id`, last writer wins by `AsOf`. A naive upsert
therefore overwrites the stored `site_id` with an empty one the moment the shortage
event lands.

## Decision

1. **The row keeps `site_id`.** `order.PlannedCapacityWindow` gains `SiteID`.
   `planned_capacity_windows` gains `site_id TEXT NOT NULL DEFAULT ''` (migration
   `0016`, additive: existing rows are not touched and read as "no site_id", with a
   partial index on `(site_id, window_end)` for the rows that have one). The consumer
   reads `data.site_id` on every type that may carry it.

2. **Matching: `site_id` when the plan has one, else `location`.**
   `PlannedCapacityWindow.MatchesSite(site)` is the one rule: a non-empty `SiteID`
   matches only when it equals the site, whatever the location (so a plan for another
   site whose `location` happens to equal this site does NOT annotate); an empty
   `SiteID` matches when `Location` equals the site, which is the legacy behaviour
   for every plan that never carried one. Both are exact and case-sensitive, as in
   ADR 0031. `CapacityConstraints` uses `MatchesSite`.

3. **A write without a `site_id` never erases a stored one.** The merge lives in the
   domain function both adapters already share, `PlannedCapacityWindow.ReplacementOf`:
   - a Created, Published or ShortageDetected write replaces the row whole (ADR 0031
     §1), except that an empty incoming `site_id` keeps the stored one, and a
     non-empty one replaces it;
   - a SUPERSEDED write keeps the stored facts (ADR 0052), including a stored
     `site_id`, and fills an empty one from the event.
   The Postgres adapter reads the row `FOR UPDATE` and calls `Supersedes` then
   `ReplacementOf`, the in-memory one the same two functions, so they cannot disagree.
   `Supersedes` is unchanged. A write that `Supersedes` rejects (stale, a downgrade, or
   onto a SUPERSEDED row) is dropped whole and teaches nothing, as before.

4. **The annotation lists by site; the REST listing does not change.** A new port
   method `PlannedCapacityRepo.ListForSite(site, endingAfter)` returns the rows
   `MatchesSite(site)` accepts (the SQL predicate mirrors the domain rule; the domain
   re-applies it). `OrderCapacityConstraints.For` uses it. `GET /planned-capacity?site=`
   still filters on `location` exactly as before and `ListByLocation` is untouched.
   The only change to that response is an additive, optional `siteId` on each window
   (`omitempty`, absent for a plan without one): a response property added to an
   existing schema, not a breaking change. The order's `capacityConstraint.site` is
   still the site that was matched.

5. **Operator guidance.** A deployment no longer needs `PLANNED_CAPACITY_SITE_ID` to
   equal a plan's `location`: it needs to equal the plan's `site_id`. For a plan with no
   `site_id` the old rule stands, so no deployment that annotated before stops doing so.

## Consequences

**Easier**

- A plan whose `location` is a zone annotates the orders of its site, and a plan of
  another site no longer annotates an order by coincidence of a location string.
- The `site_id` survives the Published → ShortageDetected → Superseded sequence,
  pinned by a table over (stored, incoming) × (equal, differs, empty) and by a
  consumer test that replays that sequence.

**Harder / what this does NOT solve**

- **A plan stored before this change has no `site_id` in this read model**, even if
  warehouse-planning's event carried one: the migration does not backfill. Such a plan
  keeps matching by `location` until a Published, or a Superseded, for it is redelivered
  (a DLQ or partition replay). That is the legacy rule, not a regression.
- **Two plans of one window but different `site_id` do not supersede one another here.**
  This context still trusts warehouse-planning's `CapacityPlanSuperseded` (ADR 0052).
- **`site_id` is not validated.** Like `location`, it is a producer-owned code taken as
  published; a plan carrying a code that matches no order simply annotates none.
- **The in-flight window of a deploy.** The migration is additive and the column is
  read and written only by this binary, so a rolling update where an old pod upserts a
  row without `site_id` (the column default) leaves that row on the legacy rule until a
  new-version write for the plan arrives.
- **Mutation score.** The new decisions live in `MatchesSite`, `ReplacementOf` and
  `firstNonEmpty` in `planned_capacity.go`, inside the `gremlins` gate on
  `internal/domain/order`.
