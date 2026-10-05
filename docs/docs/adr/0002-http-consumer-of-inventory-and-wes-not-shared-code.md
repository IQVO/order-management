---
id: 0002-http-consumer-of-inventory-and-wes-not-shared-code
slug: /adr/0002-http-consumer-of-inventory-and-wes-not-shared-code
title: 0002. HTTP consumer of inventory-storage and wes-work-planning, not shared code
sidebar_label: 0002. HTTP consumer, not shared code
description: ADR 0002 — consume the Suppliers' published REST contracts only; no shared Go module, no shared database.
---

# 0002. HTTP consumer of inventory-storage and wes-work-planning, not shared code

## Status

Accepted. Established with the initial implementation of this bounded
context. **Partially superseded by
[ADR 0005](./0005-choreographed-release-via-kafka.md):** the
release-orchestration portion of this decision (this service calling
`wes-work-planning`'s `POST /paths/{pathId}/work-units` synchronously) no
longer holds — release is now announced via a Kafka integration event.
The allocation portion (this service calling `inventory-storage`'s
`POST /reservations` synchronously) is UNCHANGED and this record's
reasoning for it still applies in full, as does everything below about the
bounded-context boundary itself (no shared Go module, no shared database,
no imported packages from either Supplier).

## Context

Order Management exists to close a specific gap. Before it, "an order" was not
a modelled thing anywhere in this platform — it was an unowned, unvalidated
string, independently reinvented three times:

- `inventory-storage` knows it as `demandRef` on a `Reservation`,
- `wes-work-planning` knows it as `reference` on a `WorkUnit`,
- `fulfillment-execution` knows it as a `Reference` on a `Task`.

Nothing validated those strings, nothing owned them, and nothing could answer
"what is the state of order X" without joining three services by string
equality. This context makes `OrderId` a real identity and becomes the
upstream that supplies it to the others.

Doing that requires calling both `inventory-storage` (to reserve stock) and
`wes-work-planning` (to enqueue work). There are several ways to arrange that,
and the convenient ones are the dangerous ones:

- **Extract a shared Go module** with the reservation and work-unit types in
  it. Tempting: no duplicated structs, compile-time safety across services.
  But it converts three independently deployable contexts into one distributed
  monolith with a shared release cadence, and it makes every internal type
  change in a Supplier a potential break here.
- **Import the Suppliers' packages directly** (they are on the same machine,
  after all). This gives this context reach into `Reservation`, `WorkPool` and
  `WorkUnit` — aggregates it has no business mutating — and quietly makes
  their *internal* structure part of this service's contract.
- **Read (or write) their databases.** The fastest path to a query, and the
  fastest path to invariants being bypassed: `inventory-storage`'s
  "reserved never exceeds usable" rule lives in its aggregates, not in its
  schema.
- **Change the Suppliers to suit this context.** Also tempting, and also the
  end of their autonomy.

The forces pointing the other way are strong. Both Suppliers already publish
stable, versioned REST contracts that do exactly what is needed — including
`sku` and `giftWrap` as already-optional fields on
`POST /paths/{pathId}/work-units`. And the platform's DDD reference documents
already classify WMS → WES as a **Customer/Supplier** relationship. This
context is simply another Customer.

## Decision

**Order Management is a pure HTTP Customer of `inventory-storage` and
`wes-work-planning`, consuming only their already-published REST contracts.**

Specifically:

- This service is a **separate Go module in a separate repository**. It
  imports **no Go package** from either Supplier — not a type, not a
  constant, not a test helper.
- It has **no database access** to either Supplier's schema.
- It gets **no write access to their internal aggregates** (`Reservation`,
  `WorkPool`, `WorkUnit`). It holds only the *references* it genuinely needs:
  the `reservationId` required to revoke on cancellation, and nothing else.
- The contracts it consumes are exactly:
  - `POST /reservations` and `DELETE /reservations/{id}` on inventory-storage,
  - `POST /paths/{pathId}/work-units` on wes-work-planning — until
    [ADR 0005](./0005-choreographed-release-via-kafka.md) replaced that
    synchronous call with the `OrderReleased` integration event.
- The request and response shapes for those calls are **local mirrors** inside
  the outbound adapters (`internal/adapters/outbound/inventorystorage`; the
  wes-work mirror lived in `internal/adapters/outbound/weswork` until ADR
  0005 deleted it) — unexported structs that exist purely
  to marshal that Supplier's JSON. Duplicating those few fields is the
  *price of autonomy*, deliberately paid.
- **This build is 100% additive from the Suppliers' point of view.** Neither
  repository is modified, at all, for this context to exist.

Both outbound adapters follow the env-selected `MODE=http|permissive` pattern
already used three times in this platform (inventory-storage → facility-layout,
wes-work-planning → inventory-storage, fulfillment-execution →
inventory-storage), defaulting to `permissive` so no unit test ever reaches the
network — with one deliberate difference described in
[ADR 0003](./0003-ship-complete-default-and-fail-closed-allocation.md): those
permissive adapters fail *open*, and these fail *loudly*.

## Consequences

### Easier

- **Every service keeps its own release cadence.** A change inside
  `inventory-storage`'s `Reservation` aggregate cannot break this build; only
  a change to its published HTTP contract can, which is exactly the coupling
  that was intended.
- **Supplier invariants stay enforced by their owners.** This context cannot
  reach past `POST /reservations` to touch usable-vs-reserved arithmetic, so it
  cannot corrupt it.
- **Testing is honest and cheap.** The use cases run against a scripted port,
  and the adapters run against an `httptest` server that asserts the exact
  wire shape — method, path, and every field name — so a drifted contract
  fails a fast local test rather than a deployment.
- **Substituting a Supplier is an adapter change.** Nothing above
  `internal/adapters/outbound/` knows either service exists.

### Harder

- **The mirrored request/response structs are duplicated by design.** They
  will drift if a Supplier changes its contract without telling anyone. That
  is a real cost, mitigated by the wire-shape adapter tests and, in a later
  round, by consuming the Suppliers' OpenAPI specs in CI.
- **No compile-time safety across the boundary.** A renamed JSON field is a
  runtime failure, not a build failure.
- **No cross-service transaction.** `AllocateOrder` can reserve line 1 and
  then fail on line 2. The use case handles this explicitly by persisting the
  reservations that genuinely happened before returning the error, so nothing
  is stranded upstream and re-issuing resumes — but "explicitly handled" is
  more work than "impossible".
- **Latency and failure modes are now this context's problem.** Every
  allocation is N HTTP calls, each of which can time out. Bounded timeouts and
  the fail-closed rule in ADR 0003 are the answer, not retries-by-default.
