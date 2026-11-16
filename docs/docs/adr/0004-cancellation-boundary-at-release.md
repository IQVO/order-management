---
id: 0004-cancellation-boundary-at-release
slug: /adr/0004-cancellation-boundary-at-release
title: 0004. The cancellation boundary is release
sidebar_label: 0004. Cancellation boundary
description: ADR 0004 — BR6 — cancellation is legal only before any line is released, and v1 does not claw back released work.
---

# 0004. The cancellation boundary is release

## Status

Accepted, with a documented known gap (below). Established with the initial
implementation of this bounded context.

## Context

Cancelling an order means undoing whatever this context has already committed
on the customer's behalf. What has been committed depends entirely on how far
the order has travelled:

- **Before allocation** — nothing. Cancelling is a local state change.
- **After allocation** — `inventory-storage` is holding reservations against
  this order's `demandRef`. Those must be handed back, or the stock stays
  invisibly bound to an order that no longer exists. inventory-storage
  publishes exactly the operation needed: `DELETE /reservations/{id}`, and its
  reservations are revocable by design.
- **After release** — `wes-work-planning` has accepted work units, and
  downstream of *that*, `fulfillment-execution` may already have created
  tasks and an associate may already be walking to a bin. The order has left
  this context's control and entered the physical warehouse.

That last step is a genuine boundary, not a technicality. Once work is
released, "cancelling" is no longer a state change — it is a warehouse
operation: recall the work, return picked goods to stock, reconcile the
inventory. None of that can be expressed through the contracts this context
consumes, and inventing a half-version of it would be worse than not having
it, because it would *look* like the order was cleanly cancelled while pickers
kept working.

There is also an ordering question with real consequences. `CancelOrder`
revokes reservations upstream *and* mutates the aggregate. If it mutates
first and the revoke then fails, the order reads `Cancelled` while
inventory-storage still holds its stock — and nothing left in this context
records the reservation ids needed to try again.

## Decision

**BR6: `CancelOrder` is legal ONLY while no line has reached `Released`.**

- The boundary is enforced on the aggregate (`Order.EnsureCancellable` and
  `Order.Cancel`, both returning `ErrOrderAlreadyReleased`), so it holds for
  any caller, not just the use case.
- The check happens **before any reservation is revoked**. A rejected
  cancellation leaves `inventory-storage` completely untouched — no partial
  revocation, nothing to reconcile.
- A legal cancellation revokes **every** allocated line's reservation via
  `DELETE /reservations/{id}`, then cancels every line and publishes
  `OrderCancelled`.
- **A failed revoke fails the whole cancellation.** The order is NOT marked
  cancelled, the lines are untouched, and the reservation ids are still
  recorded — so retrying is safe and converges. Fail closed: it is better to
  leave an order visibly un-cancelled than to lose the only reference that can
  return the stock.
- A `404` from `DELETE /reservations/{id}` counts as success. The desired end
  state — that reservation no longer holding stock — is true either way, and
  treating it as a failure would deadlock a retry after a partial revoke.

This is also why the aggregate stores the `reservationId` at all. It is the
single piece of Supplier state this context keeps a reference to, and
cancellation is the reason (see
[ADR 0002](./0002-http-consumer-of-inventory-and-wes-not-shared-code.md)).

## Known gap (v1): released work is not clawed back

**Once ANY line is `Released`, v1 does not attempt to recall the work.** The
order simply cannot be cancelled through this API; the caller gets a `409`
with an `order-already-released` problem body.

This is a deliberate, stated limitation, recorded here in the same spirit as
`inventory-storage`'s ADR-0003 "no expiry sweeper" section — a known gap that
a reader should find written down rather than discover in production.

What it means concretely:

- An order released by mistake stays released. Stopping it is a manual
  warehouse process today, coordinated outside this system.
- There is no compensating command on `wes-work-planning`'s published contract
  for this context to call, and inventing one would mean changing that service
  — which [ADR 0002](./0002-http-consumer-of-inventory-and-wes-not-shared-code.md)
  explicitly rules out for this build.

What closing it would take, when the business asks for it:

- A cancellation/withdrawal operation on `wes-work-planning` for a `Pending`
  work unit, plus a decision about `Released` ones (which may already be
  assigned).
- A compensating flow for goods already picked — returning them to stock
  through `inventory-storage`, which is a stow, not a revoke.
- An order-level `Cancelling` state, because that flow is asynchronous and can
  partially fail, unlike today's synchronous all-or-nothing cancellation.

None of that is v1 scope. The boundary is drawn where this context can still
keep its promise.

## Consequences

### Easier

- **Cancellation is all-or-nothing and retry-safe.** Either every reservation
  came back and the order is cancelled, or nothing changed anywhere.
- **The rule is a property of the aggregate,** so it is unit-tested directly
  and cannot be skipped by a future caller.
- **No stranded stock.** The only reservations this context stops tracking are
  ones it has successfully revoked or ones that moved on to released work.
- **The limitation is legible.** A `409` with a specific problem `type` tells
  the caller precisely why, rather than failing vaguely or pretending to
  succeed.

### Harder

- **A partially released order cannot be cancelled at all** — not even its
  still-allocated lines. That is the strict reading of BR6, and it is
  deliberate: a "partial cancellation" would leave an order in a state this
  model has no name for. An order that allows partial shipment is the case
  where this bites hardest, and it is the first thing to revisit if the
  business asks for finer-grained cancellation.
- **Callers must cancel early.** The window closes at release, which in a busy
  warehouse can be soon after allocation.
- **The gap needs an operational answer, not just a documented one.** Until
  work recall exists, "stop that order" is a phone call to the floor.

## Addendum 2026-11-15: a Shipped line closes the boundary too

ADR 0051 added `Shipped` after this ADR was accepted. It is past `Released`
and terminal, like `Cancelled`, so the rule above reads: `CancelOrder` is
legal only while every line is `Pending`, `Backordered` or `Allocated`.
`Order.EnsureCancellable` (and therefore `Order.Cancel`) now rejects a
`Released` **or** `Shipped` line with `ErrOrderAlreadyReleased`, and
`DELETE /orders/{id}` answers the same `409 order-already-released`. Before
this fix the check named only `Released`, so a fully shipped order answered
`204` and its lines were rewritten to `Cancelled`. The decision, the problem
slug and the "no claw-back" gap above are unchanged.
