---
id: 0051-consume-shipment-confirmed-and-package-missedload
slug: /adr/0051-consume-shipment-confirmed-and-package-missedload
title: "0051. Consume ShipmentConfirmed/PackageMissedLoad: the Order ships"
sidebar_label: "51. Consume ship-confirm; Order gains Shipped"
sidebar_position: 51
description: "ADR 0051 — order-management consumes outbound-shipping's ShipmentConfirmed and PackageMissedLoad. The Order aggregate gains line Shipped and derived order Shipped/PartiallyShipped; PackageMissedLoad drives the existing re-promise machinery order-wide. Idempotent on the CloudEvents id; unknown orders and non-shippable states are skip-and-log, never crashes."
---

# 0051. Consume ShipmentConfirmed/PackageMissedLoad: the Order ships

## Status

Accepted — implemented by the change that introduces this record.

## Context

Until now the Order aggregate's lifecycle ended at `Released`
(`internal/domain/order/status.go`): once wes-work-planning accepted the
work, order-management knew nothing more. The physical truth — the
package left the site on a trailer — lived nowhere this context could
read, so an order that shipped yesterday still queried as `Released`.

outbound-shipping (its merged `apis/asyncapi.yaml` on develop, the
producer contract this ADR pins) now publishes two facts on
`warehouse.outbound-shipping.events`, CloudEvents 1.0, full types
`com.warehouse.wms.outbound-shipping.<entity>.<EventName>`:

- **`outboundload.ShipmentConfirmed`** — a closed load physically
  departed. Its `data.orders[]` carries the DISTINCT
  `source_order_id` of the shipped packages (each entry `{order_id}`).
- **`shippingpackage.PackageMissedLoad`** — a package was not loaded by
  its cutoff: `source_order_id` (a bare OrderId — the producer strips
  any `-line-N` suffix its upstream `PackageManifested` correlation
  carried) and `cutoff`.

The wave-1 spike (`spike-correlation.md`, Q7) confirmed this repo's
consumers decode fixed structs with no `DisallowUnknownFields`, so the
producers' additive fields are ignored by construction.

## Decision

1. **Line `Shipped`; order `Shipped`/`PartiallyShipped` (derived).** A
   `Released` line transitions to `Shipped` via `Order.Ship` /
   `Order.ShipAll` — the only new domain transitions, terminal like
   `Cancelled`. The order-level statuses stay DERIVED from line
   statuses (`Order.Status`): all lines `Shipped` → `Shipped`; some
   `Shipped`, some not → `PartiallyShipped`. Nothing is stored, so the
   summary cannot drift. Error names: `ErrLineNotReleased` (Ship on a
   non-Released, non-Shipped line), `ErrOrderNotShippable` (no line in
   `Released`/`Shipped`).

2. **`ConfirmShipment` applies one ShipmentConfirmed atomically.** The
   consumer decodes `{load_id, orders[].order_id}` (every other member
   ignored) and one use case call handles the WHOLE event in ONE
   UnitOfWork: claim the CloudEvents id in the new
   `shipment_processed_events` gate, then for every named order ship
   its Released lines, save, and raise `OrderLineShipped` per moved
   line plus one order-level `OrderShipped`/`OrderPartiallyShipped`
   carrying the `load_id` correlation. The idempotency key is the RAW
   CloudEvents id with no per-order scoping — one event names many
   orders and the claim covers them all at once (the deliberate
   difference from ADR 0018's single-order events). Below the gate, the
   aggregate itself is idempotent: a re-delivery under a NEW id that
   names an already-shipped order is a `no_change` outcome, never a
   second transition.

3. **Skip-and-log, never crash.** A ShipmentConfirmed naming an UNKNOWN
   order, or an order with no Released line (still
   Received/Allocated/Backordered, or cancelled —
   `ErrOrderNotShippable`), is logged at WARN and committed past; the
   other orders in the same event still ship. Only a message that can
   never be applied (not a CloudEvent, undecodable `data`) is
   dead-lettered to `<topic>.dlq`; infrastructure failures retry
   in-process (ADR 0025 bounds) before dead-lettering. Unknown event
   types are committed past (forward compatibility).

4. **PackageMissedLoad re-promises through the EXISTING machinery.** No
   new saga: `RepromiseOrder.ExecuteForMissedLoad` is the same use
   case, same `PromisePolicy` recompute (honouring `requiredShipBy`
   exactly as ADR 0020 does), same `OrderRepromised` event with reason
   `PackageMissedLoad`, same `repromise_processed_events` gate — driven
   at ORDER granularity (`source_order_id` is a bare OrderId, not the
   WorkUnitId shape `TaskCPTMissed`/`PackageManifested` carry), so
   EVERY group the order holds is recomputed and each group whose
   promise moved publishes one `OrderRepromised`. The event's `cutoff`
   is logged, never used as the new promise — the recompute is from
   current inputs, the machinery's established convention.

5. **One consumer, stable shared group, ships dark.**
   `OutboundShippingConsumer` on `warehouse.outbound-shipping.events`
   dispatches both types on the FULL type string and ignores the rest.
   It follows the ADR 0043 launch shape: `OUTBOUND_SHIPPING_CONSUMER_MODE=off`
   (default) | `kafka`; kafka mode requires
   `OUTBOUND_SHIPPING_CONSUMER_GROUP` (no default, so a local run can
   never join a live group), `KAFKA_BROKERS` and `DATABASE_URL`.

6. **No outbound contract change.** The three new domain events
   (`OrderLineShipped`, `OrderShipped`, `OrderPartiallyShipped`) are
   raised through the events port and are LOCAL concerns this PR: the
   integration publisher's contract (ADR 0018/0035) and the analytics
   contract are untouched; both publishers skip them by their existing
   outside-the-contract rule. This repo's `apis/asyncapi.yaml` gains
   ONLY the two consumed-message declarations (the repo's convention
   for consumed contracts), and the rules digest's consumed-type list
   gains the two full types.

## Known gaps (recorded, deliberate)

- **Per-package shipping is additive later.** `orders[]` carries order
  ids only — no per-package, per-line counts — so one ShipmentConfirmed
  ships the order's whole Released remainder. When outbound-shipping
  adds per-package detail, `ConfirmShipment` narrows to the lines whose
  packages were on the load; nothing else changes.
- **What the re-promise cannot yet do.** The machinery re-promises
  groups holding `Allocated`/`Released` lines (the policy's
  `allocatedLines` eligibility). A missed load for a FULLY SHIPPED
  order is a late fact about a shipment that already left — fail-soft
  skip, correct. But a missed load for an order with NO recorded
  promise groups (a pre-ADR-0014 legacy row) also skips, keeping its
  legacy single-valued promise untouched; re-promising those rows needs
  a legacy-row recompute this repo does not have, and is not invented
  here.
- **Analytics does not yet see shipped.** Until the shipped events join
  the analytics contract (an additive change with the projector), the
  analytics data product keeps ending at Released; the OLTP read side
  (GetOrder) is the source of truth for shipped state today.

## Consequences

- The Order lifecycle is complete: Received → Allocated → Released →
  Shipped (or PartiallyShipped along the way), each step driven by a
  fact another context owns.
- `Shipped` is terminal: `Allocate`/`Release`/`MarkBackordered` all
  reject a Shipped line by their existing error names, so no late
  duplicate upstream fact can walk a shipped line backwards.
- `shipment_processed_events` (migration 0014) is the third per-consumer
  idempotency gate, after `repromise_processed_events` (0004) and
  `release_confirmation_processed_events` (0012).
- The consumer is OFF by default: enabling it is a deployment decision
  (set `OUTBOUND_SHIPPING_CONSUMER_MODE=kafka` plus the group env), the
  same dark-launch posture ADR 0043 took.
