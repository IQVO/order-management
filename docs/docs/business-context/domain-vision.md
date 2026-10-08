---
title: Domain Vision
sidebar_label: Domain Vision
description: Why the Order Management bounded context exists, and the business problem it solves.
---

# Domain Vision

## The gap this context closes

Before Order Management existed, **"an order" was not a modelled thing
anywhere in this platform** — it was just an unowned, unvalidated string,
independently reinvented three different ways:

- `inventory-storage` knows it as `demandRef` on a `Reservation`,
- `wes-work-planning` knows it as `reference` on a `WorkUnit`,
- `fulfillment-execution` knows it as a `Reference` on a `Task`.

Nothing validated those strings, nothing owned them, and nothing could
answer "what is the state of order X" without joining three services by
string equality. Order Management makes `OrderId` a real identity and
becomes the upstream context that supplies it to the others.

## This context's slice of the platform vision

Order Management owns **Order** and **OrderLine** as first-class,
validated aggregates: intake, per-line stock allocation (via
inventory-storage), a capability-derived delivery promise, release of
allocated work (announced to wes-work-planning over Kafka), and
cancellation up to the release boundary.

It is the **missing upstream Open Host Service** for the fleet — every
other service answers "what is happening in the warehouse right now";
this one answers "what did the customer ask for, and how far along is it."

## Why fail-closed matters (BR2)

The allocation pass (inside `ReceiveOrder`/`RetryAllocation`) calls
inventory-storage's `POST /reservations` once per
line. A `409` response is a **business fact**: inventory-storage has
authoritatively decided there is not enough usable stock, and the line
becomes `Backordered`. Anything else — a transport failure, a timeout, a
5xx — is **not** a business fact. It is an absence of information, and
treating it as a backorder would silently tell an operator "we are out of
stock" for goods that may be sitting on the shelf. So the whole
allocation pass fails instead, loudly, and nothing is silently marked
backordered.

## Why ship-complete is the default (BR3)

`allowPartialShipment` defaults to `false`. With it false, any backordered
line puts the WHOLE order in `Backordered` and no line proceeds to release
until a human or caller issues `RetryAllocation` — the only sanctioned
route from `Backordered` back to `Allocated`. With it `true`, allocated
lines are independently eligible for release and the order reads
`PartiallyAllocated`. This is a business decision, not a technical one: it
is the difference between never surprising a customer with a partial
delivery they did not ask for, and shipping faster for a customer who
opted in.

## Why the order-level status is derived, never stored

`Order.Status` is always **computed** from the line statuses on every
read — there is no `status` column that some write path could forget to
update. A stored, independently-mutated status field would eventually
disagree with the lines it claims to summarise, most likely at exactly the
moment someone is debugging why an order did not ship. Deriving it is what
makes BR3 impossible to bypass: the check lives in one place nothing can
route around.

## Why cancellation stops at release (BR6)

Cancelling an order means undoing whatever has already been committed on
the customer's behalf. Before allocation, that is nothing — cancelling is
a local state change. After allocation, inventory-storage is holding
reservations that must be handed back via `DELETE /reservations/{id}`.
After release, `wes-work-planning` has accepted work units and, downstream
of that, physical work may already be underway — the order has left this
context's control entirely. `CancelOrder` is therefore legal only while no
line has reached `Released`; once any line is released, v1 does not
attempt to recall it. This is a documented, deliberate known gap (see
[ADR 0004](/docs/adr/0004-cancellation-boundary-at-release)), in the same
honesty pattern as inventory-storage's ADR-0003 "no expiry sweeper"
section — not an oversight to silently paper over.

## Why this context calls, rather than shares code with, its Suppliers

Order Management is a **pure HTTP consumer** of inventory-storage's
already-published, already-stable REST API (reservations). Since
[ADR 0005](/docs/adr/0005-choreographed-release-via-kafka) it no longer
calls wes-work-planning — release is a published Kafka event — and since
[ADR 0036](/docs/adr/0036-product-classification-local-copy) it reads
product classification from a local copy of **product-master**'s
`ProductClassified` events instead of asking inventory-storage. It is a
separate Go module in a separate repository — it imports no Go package
from any sibling repo, and it gets no write access to their internal
aggregates (`Reservation`, `WorkPool`, `WorkUnit`), only to their published
HTTP and Kafka contracts. Order Management is the **Customer**;
inventory-storage is the **Supplier / Open Host Service** — the same
directional Customer/Supplier relationship the fleet's DDD reference docs
already use for WMS → WES. See
[ADR 0002](/docs/adr/0002-http-consumer-of-inventory-and-wes-not-shared-code)
for the full reasoning.
