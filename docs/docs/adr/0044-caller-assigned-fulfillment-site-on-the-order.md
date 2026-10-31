---
id: 0044-caller-assigned-fulfillment-site-on-the-order
slug: /adr/0044-caller-assigned-fulfillment-site-on-the-order
title: "0044. An order may carry an optional, caller-assigned fulfillment site"
sidebar_label: "44. Caller-assigned fulfillment site"
sidebar_position: 44
description: "ADR 0044 — amends ADR 0035 §4 and ADR 0031's deferral ('a fulfillment site on the order'). POST /orders (ordinary orders and the ADR-0020 network-originated hold alike) accepts an optional fulfillmentSiteId, assigned by the caller and never inferred or looked up; the Order aggregate stores it immutably; SiteSkuDemandChanged projects to it with assignment_version order-site-v1 and falls back to the static DEMAND_PROJECTION_SITE_ID seam (static-site-v1) for orders without one. The frozen OrderAllocated/OrderPartiallyAllocated/OrderRepromised contracts are untouched."
---

# 0044. An order may carry an optional, caller-assigned fulfillment site

## Status

Accepted — implemented by the change that introduces this record. **Amends**
[ADR 0035](/docs/adr/0035-site-sku-demand-projection-event) §4 (the site of a
`SiteSkuDemandChanged`) and lifts the deferral recorded in
[ADR 0031](/docs/adr/0031-consume-warehouse-planning-capacity-plans) ("a fulfillment
site on the order"). The owner took the decision; this record writes it down.
**Amended by [ADR 0047](./0047-promise-at-the-orders-own-fulfillment-site.md):** the
site is no longer read by the demand projection only — the CPT promise (intake and
every re-promise) and the planned-capacity annotation use it too, falling back to
`DEFAULT_SITE_ID` / `PLANNED_CAPACITY_SITE_ID`. The "assigned, never inferred" rule
below is unchanged.

## Context

ADR 0035 projects every `SiteSkuDemandChanged` to ONE static site,
`DEMAND_PROJECTION_SITE_ID`, through `usecases.DemandProjectionPolicy{SiteID,
AssignmentVersion}` with `StaticDemandAssignmentVersion = "static-site-v1"`. It
deliberately kept the site off the Order aggregate, and ADR 0031 deferred "a
fulfillment site on the order" as an order-model change that would get its own ADR.

That seam is now the bottleneck. Because orders carry no site, every demand fact goes
to the single configured site, so:

- `network-inventory-planning`, which needs demand facts at BOTH the origin and the
  destination site of a transfer, can never see demand at a second site, and a real
  transfer approval cannot happen outside injected test data;
- `slotting` likewise only ever sees one site.

The information is not ours to produce. Which site should fulfil an order is a
sourcing decision made by whoever places the order — `customer-ordering` for a retail
order, `network-fulfillment` for network-originated demand (ADR 0020) — and this
bounded context has no sourcing logic and must not grow any.

## Decision

1. **The order MAY carry a fulfillment site, assigned by the caller.**
   `POST /orders` accepts an optional `fulfillmentSiteId`. It applies to ordinary
   orders and to the ADR-0020 network-originated hold alike: the hold is not a
   separate request, it is the same `ReceiveOrderRequest` with
   `releaseOnAllocation: false` (and usually `requiredShipBy`), so one field covers
   both. The order and hold read responses (`GET /orders/{id}`, the `201`, the
   retry/release `200`s) echo `fulfillmentSiteId`, and omit it when the caller named
   none.

2. **Assigned, never inferred.** This service does not look a site up, default one,
   derive one from an SKU, a reservation, the promise, or `DEFAULT_SITE_ID`, and
   makes no call to `facility-layout`. It carries what the caller sent. No sourcing
   logic belongs here: if a future requirement is "pick the best site", that is
   `network-fulfillment`'s or `customer-ordering`'s context, and arrives here as a
   `fulfillmentSiteId`.

3. **A value object with the fleet's site-code shape, mirrored not called.**
   `shared.FulfillmentSiteID` accepts exactly facility-layout's pattern
   `^[A-Z0-9]+$`: non-empty, uppercase ASCII letters and digits only (`WH1`, `ABE8`,
   `123` are valid; `""`, `wh1`, `WH-1`, `WH 1` are not). facility-layout imposes no
   maximum length, so none is invented here. Existence is NOT checked — a well-formed
   code for a site we have never heard of is accepted; the rule is copied, there is
   no cross-service call. A malformed value — including an explicit `""` or JSON
   `null` — is rejected with **400** `application/problem+json`, problem type
   `invalid-fulfillment-site-id` (an explicit `null` is the existing
   `malformed-request-body`). Nothing is persisted and no event is raised for a
   rejected request.

4. **Immutable.** `Order.AssignFulfillmentSite` sets it once, on the freshly
   constructed order at intake (a second call returns
   `ErrFulfillmentSiteAlreadyAssigned`). Allocation, retry, release, re-promise and
   cancel never touch it. In storage it is a nullable `orders.fulfillment_site_id`
   (migration `0013`, with a CHECK mirroring the pattern), written only by the
   `INSERT` of a new order — the version-guarded `UPDATE` does not set it, so no
   later save can rewrite it. Rows written before the migration read `NULL` = no site.

5. **Projection (amends ADR 0035 §4).** The site of a `SiteSkuDemandChanged` is
   resolved per order:

   | Order has `FulfillmentSiteID` | Static policy configured | site_id | assignment_version |
   |---|---|---|---|
   | yes | either | the order's site | `order-site-v1` |
   | no | yes (`DEMAND_PROJECTION_SITE_ID`) | the static site | `static-site-v1` |
   | no | no | — nothing is emitted | — |

   The order's site wins even when no static site is configured, so a deployment can
   serve multiple sites with `DEMAND_PROJECTION_SITE_ID` unset. `static-site-v1`'s
   meaning is unchanged and `order-site-v1` is a NEW value, as ADR 0035 requires of
   any new assignment regime. The decision lives in one place,
   `DemandProjectionPolicy.resolve`, which every projecting use case already goes
   through (`allocateAndRelease`, `RepromiseOrder`, `CancelOrder`), so ACTIVE and
   REMOVED facts for one order always agree on the site.

6. **The seam stays.** `DemandProjectionPolicy` and `DEMAND_PROJECTION_SITE_ID` remain
   for orders without a site (every existing caller). No new environment variable and
   no new chart value are introduced (INFRA VALUES REQUIRED: none).

7. **Frozen contracts are byte-identical.** `OrderAllocated`,
   `OrderPartiallyAllocated` and `OrderRepromised` (shared with wes-work-planning) do
   not carry the site and do not change by a byte; a use-case test asserts the
   non-demand events of an order with a site are `DeepEqual` to those of the same
   order without one, and a Postgres + Kafka integration test compares the wire
   payload shape. The analytics topic is untouched. `SiteSkuDemandChanged`'s payload
   shape and `dataschema` (`…SiteSkuDemandChanged:v1`) are unchanged: only the
   `assignment_version` enum gains `order-site-v1` — additive for a consumer that
   already treats it as an opaque regime tag.

## Consequences

- `network-inventory-planning` and `slotting` can receive demand facts at more than
  one site as soon as the callers send a site; `assignment_version` tells them whether
  the site was the caller's (`order-site-v1`) or the deployment's (`static-site-v1`).
- Behaviour, events and tests are unchanged for every order without a site.
- **Callers must follow up** for any of this to matter in a running fleet:
  `customer-ordering` and `network-fulfillment` each have to send `fulfillmentSiteId`
  on `POST /orders` (network-fulfillment on the hold request). Until they do, every
  order projects to the static site as before.
- A caller can name a site that does not exist; downstream consumers own that
  judgment. We accept this rather than couple intake to `facility-layout`'s
  availability.
- **Not changed:** the planned-capacity annotation of ADR 0031 still matches on
  `PLANNED_CAPACITY_SITE_ID`, not on the order's site. Matching on the order's site is
  now possible and is a deliberate follow-up with its own decision, because moving the
  annotation would alter the read-time behaviour that ADR 0031 pinned as advisory.
- The same-site rule is strict by construction: a split-shipment order's lines all
  project to the one site on the order. Per-line sites are out of scope.
- The `fulfillmentSiteId` is a caller-visible input on the public intake DTO; it is a
  site code, not PII, and does not appear on the analytics topic.
