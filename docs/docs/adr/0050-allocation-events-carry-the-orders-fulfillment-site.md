---
id: 0050-allocation-events-carry-the-orders-fulfillment-site
slug: /adr/0050-allocation-events-carry-the-orders-fulfillment-site
title: "0050. OrderAllocated/OrderPartiallyAllocated carry the order's fulfillment site"
sidebar_label: "50. Allocation events carry the site"
sidebar_position: 50
description: "ADR 0050 — additively supersedes ADR 0044 §7's freeze: the integration payloads for OrderAllocated and OrderPartiallyAllocated gain an optional top-level site_id (string, omitempty), sourced from the order's caller-assigned fulfillment site (ADR 0044). No breaking change: an order without a site publishes bytes byte-identical to the pre-ADR-0050 payload, and consumers that ignore unknown fields are unaffected. Per-line sites stay Phase 2; the site is order-level."
---

# 0050. OrderAllocated/OrderPartiallyAllocated carry the order's fulfillment site

## Status

Accepted — implemented by the change that introduces this record. **Additively
supersedes** ADR [0044](./0044-caller-assigned-fulfillment-site-on-the-order.md) §7
("frozen contracts are byte-identical … the site … does not change by a byte")
and its two guard tests, in the narrow sense that the two allocation events now
MAY carry the site as an optional member. Everything else §7 froze — field
names, shapes, the `data.lines[]` entry shape, `dataschema` still `…:v1` — is
unchanged, and an order without a site still publishes exactly the pre-0050
bytes. Not amended: `OrderRepromised` (carries no site — a re-promise is not a
siting fact) and the analytics topic (untouched).

## Context

ADR 0044 let a caller assign the order's fulfillment site, and ADR 0047 made
promising read it — but the site never crosses the context boundary on the
allocation events. The pick-batching prerequisites spike (2026-11-05) measured
the downstream effect: 100% of order-driven PICK work reaches
wes-work-planning with no site, because `OrderAllocated` /
`OrderPartiallyAllocated` carry none and no other consumed event or REST field
can source it. Downstream, a work unit without a site has no `Placement()`, so
the task is never announced, task-interleaving never sees it, and order work
can never be sited, batched or walked by site.

The site already exists at the source: `Order.fulfillmentSite` is set once at
intake (`ReceiveOrder` → `AssignFulfillmentSite`, before allocation ever runs)
and is immutable afterwards, so it is available at publish time —
`publishOrderAllocationOutcome` holds the aggregate when it raises the events.

ADR 0044 §7 froze these payloads deliberately, to prove the site feature could
not leak into the frozen contracts. The freeze has since done its job: the
need is now an explicit, consumer-coordinated decision, not a leak.

## Decision

1. **Both allocation events carry the order's site as an optional top-level
   `site_id`.** The `allocationData` payload of `OrderAllocated` and
   `OrderPartiallyAllocated` gains `site_id` (string, `omitempty`). The value
   is the order's caller-assigned `fulfillmentSite` (ADR 0044's
   `shared.FulfillmentSiteID`, `^[A-Z0-9]+$`), stamped on the domain events by
   the raise site (`WithSite`), never re-derived, defaulted or looked up:
   "assigned, never inferred" is inherited unchanged.

2. **Additive only — no version bump, no breaking change.** `dataschema`
   stays `…:v1`; the CloudEvents `type`s stay. `site_id` is omitted when the
   order has no site, so the published bytes for such orders are
   byte-identical to the pre-ADR-0050 payload — pinned by golden encoder
   tests that compare the raw `data` member bytes against the literals the
   pre-existing goldens established. Consumers that ignore unknown fields
   (wes-work-planning's decoder today) are unaffected; the concurrent
   wes-work-planning change builds against this pinned shape
   (`orderAllocatedData.SiteId → EnqueueWorkUnitRequest.SiteId`).

3. **Order-level, not per-line.** One order has one fulfillment site, so the
   member is top-level, mirroring `promise_date`. Multi-site splits of one
   order (per-line sites) are Phase 2 and need their own decision; if they
   ever land, they arrive additively on `lines[]` the same way
   `fulfillment_class` did (ADR 0008).

4. **The ADR 0044 §7 guards are re-pinned, not deleted.** The use-case test
   that asserted sited and unsited orders produce `DeepEqual` non-demand
   events now asserts the ONLY difference is the allocation events'
   `SiteID`; the Postgres + Kafka integration test now asserts the with-site
   payload is the without-site payload plus exactly one `site_id` member
   (absent, `omitempty`, for the order without one).

## Consequences

- wes-work-planning can site order work: an `OrderAllocated` with a
  `site_id` yields work units that release with `site_id` on `WorkReleased`,
  which unblocks pick placement and interleaving for order picks. Until it
  consumes the field, nothing changes for it.
- Every order without a caller-assigned site behaves exactly as before —
  same events, same bytes (golden-pinned), same downstream behaviour.
- The freeze language in ADR 0044 §7 and in the AsyncAPI description now
  reads "frozen, additive members excepted" — `fulfillment_class`
  (ADR 0008), the promise members (ADR 0014/0017) and now `site_id` all
  follow the same additive-members pattern on a v1 payload.
- A split-shipment order's lines all carry the one site on the order; a
  future multi-site split is Phase 2 and will need its own ADR.
- `apis/asyncapi.yaml`'s shared `AllocationData` schema gains the optional
  `site_id` with examples on both messages; no required member changes.
