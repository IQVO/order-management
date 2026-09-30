---
id: 0028-inventory-storage-reservations-idempotency-key
slug: /adr/0028-inventory-storage-reservations-idempotency-key
title: 28. Send Idempotency-Key on POST /reservations, restoring the inventory-storage contract PR #98 changed
sidebar_label: 28. Idempotency-Key on POST /reservations
description: "ADR 0028 — inventory-storage PR #98 made POST /reservations require a caller-supplied Idempotency-Key header, but order-management's outbound Client.Reserve was never updated to send one. Every real allocation attempt in the live cluster failed with 400 idempotency-key-required, leaving every order line stuck Pending and tripping the inventory-storage circuit breaker permanently open. Fix: Client.Reserve derives and sends a stable, deterministic key from (OrderId, LineNo, Order.Version) via a new ports.ReservationRequest.LineNo/.Attempt pair, mirroring the replay contract inventory-storage's own RequireIdempotencyKey middleware (this repo's ADR 0023, adopted verbatim) actually enforces."
---

# 28. Send `Idempotency-Key` on `POST /reservations`, restoring the inventory-storage contract PR #98 changed

## Status

Accepted — implemented in the same change that introduces this record.
Production-blocking bug, found during Phase 4 live-cluster resilience
validation (Experiment 2): reproduced with **zero fault injection** —
`POST /orders` against a fully healthy `inventory-storage` still left
every line stuck `Pending`.

## Context

`inventory-storage` PR #98 (merged 2026-09-27, commit `fbd8acd`) wired
its own `RequireIdempotencyKey` middleware — the exact pattern this
repo's own ADR 0023 established for `POST /orders` — onto its `POST
/reservations` route:

```go
if s.IdempotencyPool != nil {
    r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/reservations", s.handleReserveStock)
} else {
    r.Post("/reservations", s.handleReserveStock)
}
```

(and, separately, onto `POST /stock/receive` — no caller in this fleet
issues that call except `inventory-storage` itself; confirmed by
grepping every repo under `~/warehouse-systems` for `stock/receive`.)

`order-management`'s outbound client for this exact route,
`internal/adapters/outbound/inventorystorage/client.go`'s
`Client.Reserve`, set only `Content-Type`/`Accept` on the outgoing
request — no `Idempotency-Key`. The result, against a live
`INVENTORY_STORAGE_BASE_URL` with `IdempotencyPool` configured (i.e. the
real cluster, not the in-memory dev default): **every** `Reserve` call
returned `400 application/problem+json`
(`idempotency-key-required`), which `Client.Reserve`'s existing status
switch correctly treats as `ErrUnexpectedStatus` — a hard, non-business
failure, exactly right per this repo's own 409-vs-everything-else rule
(see `allocateLines`'s doc comment). But a hard failure on literally
every allocation attempt meant:

- every order line stayed `Pending` forever — `allocateAndRelease` never
  got a single successful `Reserve` to transition a line out of
  `Pending`,
- `resilience.ReadyToTrip` (ADR 0025) saw an unbroken run of real
  failures and tripped the `inventory-storage` circuit breaker **open**,
  which then made `BreakerClient.Reserve` fall back to
  `PermissiveClient` — fail-loud, but now masking the ORIGINAL
  contract-mismatch error behind a generic "breaker open" outcome,
- this reproduced with **no chaos/fault injection at all**: a plain
  `curl -X POST .../orders` against a fully healthy `inventory-storage`
  demonstrated the bug, because the break was in the request contract
  itself, not in availability.

`Client.RevokeReservation` (`DELETE /reservations/{id}`) was checked
against the same question and does **not** need this header:
`inventory-storage`'s route table
(`internal/adapters/inbound/http/server.go`) wires
`RequireIdempotencyKey` onto exactly two routes — `POST /stock/receive`
and `POST /reservations` — and registers `DELETE
/reservations/{id}` plain, with no `r.With(RequireIdempotencyKey(...))`
wrapper. This is the correct design on inventory-storage's side (mirrors
this repo's own ADR 0023 §"Context": a caller-supplied `{id}` DELETE is
already idempotent by ordinary HTTP semantics, no additional protection
needed) — `RevokeReservation`'s existing 404-is-success handling already
covers its own idempotence, unrelated to this header.

### What inventory-storage's replay contract actually requires (read before choosing a key)

Read against `inventory-storage/internal/adapters/inbound/http/idempotency.go`
(byte-for-byte the same design this repo's own ADR 0023 documents —
inventory-storage adopted it verbatim, per that middleware's own doc
comment: "Ported from order-management PR #105 (ADR 0023)") and its
`idempotency_integration_test.go`:

- A **fresh** key + body → the real handler runs once, `201` with a new
  `Reservation`, and the outcome is cached against that key.
- The **same** key + the **same** body (a genuine retry) → the cached
  response is replayed VERBATIM, the real handler never runs again, and
  — critically — `countReservationRows` stays at exactly 1
  (`TestIdempotency_Reserve_Replay_ReturnsIdenticalResponseNoDuplicate`).
  This is the property a caller needs a retry to have: no duplicate
  reservation.
- The **same** key + a **different** body → `422
  idempotency-key-reused`, and the original reservation is untouched.
- **No** header → `400 idempotency-key-required`, exactly the failure
  mode this ADR fixes.

So the key this client sends must be **stable across a retry of the
SAME logical reservation attempt** (so a retry replays instead of
double-reserving or getting rejected as a body mismatch) and
**different for a genuinely new attempt** (so a real, intentional
second reservation — e.g. a `RetryAllocation` pass after a prior
`Backordered` outcome — is not silently swallowed as a replay of a
stale cached response).

## Decision

### 1. `ports.ReservationRequest` gains `LineNo int` and `Attempt int`

```go
type ReservationRequest struct {
    SKU       shared.SKU
    Quantity  int
    DemandRef shared.OrderId
    LineNo    int
    Attempt   int
}
```

Both fields are transport-adapter concerns only — inventory-storage's
own request body (`reserveRequest{SKU, Quantity, DemandRef}`) is
UNCHANGED; neither field is marshalled into it. They exist solely so an
`InventoryReservationClient` implementation has enough information to
derive a deterministic key, without `allocateLines` (the one caller,
in `internal/application/usecases/allocation.go`) needing to know
anything about HTTP headers.

`allocateLines` now passes `LineNo: line.LineNo()` (already available —
each `OrderLine` addresses itself by this) and `Attempt: o.Version()` —
the Order aggregate's OWN optimistic-concurrency version field, already
present since ADR 0024, reused here rather than inventing a new nonce
mechanism.

### 2. Why `Order.Version()` is exactly the right "attempt" signal, not a new counter

ADR 0024 already established `Order.Version()` (`orders.version`,
bumped by exactly one on every successful `OrderRepo.Save` that updates
an existing row) as inert optimistic-concurrency metadata riding along
on the aggregate. It has a property this problem needs for free:

- **Stable across every `Reserve` call within ONE allocation pass.**
  `allocateAndRelease`'s whole flow — `allocateLines` for every line in
  this pass, `Order.Save` — is ONE atomic transaction (`atomically(...)`,
  ADR 0022's outbox scope); `o.Version()` cannot change again until that
  transaction's `Save` actually commits, which happens strictly AFTER
  every `Reserve` call in the pass has already returned. So every line
  reserved in the same pass, and any transport-level retry of the exact
  same `Reserve` call before that `Save` commits, observes the identical
  `Attempt` value.
- **Strictly different for a genuinely later, independent pass.** A
  later `RetryAllocation` call re-reads the order from the repository
  AFTER the prior pass's `Save` committed and bumped the stored version
  — so its own `o.Version()` is provably greater than the value the
  earlier pass used. Two different allocation passes for the same line
  (e.g. `Backordered` then `RetryAllocation`) therefore always derive
  two different keys, and a genuinely new reservation attempt is never
  mistaken for a replay of a stale one.

This is precisely "same across a retry of THIS attempt, different for a
new attempt" — the exact shape the header contract requires — with zero
new state, no UUID/nonce generation, and no new failure mode to test:
the version field's own correctness is already proven by ADR 0024's
existing test suite.

### 3. `idempotencyKeyFor` — deterministic derivation, no randomness

```go
func idempotencyKeyFor(req ports.ReservationRequest) string {
    return fmt.Sprintf("res-%s-line-%d-att-%d", req.DemandRef.String(), req.LineNo, req.Attempt)
}
```

`res-<orderId>-line-<lineNo>-att-<version>` — human-readable (useful
when reading inventory-storage's `idempotency_keys` table directly),
globally unique per (order, line, attempt) since `OrderId` is already
generated as `ord-<uuid>` (see `OrderRepo.NextID`), and — unlike a
randomly-generated nonce — provably reproducible: calling it twice with
an identical `ReservationRequest` always yields the identical string, by
construction, which is the property the "same retry -> same key" side of
the contract actually needs (a UUID minted fresh per call would defeat
the entire point: every retry would look like a brand-new attempt to
inventory-storage).

`Client.Reserve` sets this on every request via the new
`IdempotencyKeyHeader` constant (`"Idempotency-Key"`, matching
inventory-storage's own header name, byte for byte).

### 4. `Client.RevokeReservation` — explicitly, no header

Documented in place (`client.go`'s updated doc comment on
`RevokeReservation`) rather than silently left alone, so a future reader
does not have to re-derive from scratch that this was a deliberate
decision, not an oversight: `DELETE /reservations/{id}` is not wrapped
by `RequireIdempotencyKey` on the inventory-storage side, confirmed by
reading its route table directly.

## Consequences

- Every real `POST /reservations` call from `order-management` now
  succeeds against a live `inventory-storage` with
  `IdempotencyPool` configured — the exact production-blocking failure
  this ADR closes. Reproduced fixed by a passing regression test
  (`TestReserveSendsInventoryStoragesPublishedRequestShape` now also
  asserts the header is present) and a dedicated key-derivation test
  (`TestReserveIdempotencyKeyIsStableAcrossRetryButDiffersAcrossLineOrAttempt`)
  proving the stable-for-a-retry / different-for-a-new-attempt property
  against inventory-storage's actual documented replay semantics.
- No wire-format change to the request BODY inventory-storage decodes —
  `reserveRequest{SKU, Quantity, DemandRef}` is byte-for-byte unchanged.
  Only a new header is added.
- `ports.InventoryReservationClient`'s Go interface signature is
  unchanged (`Reserve(ctx, ReservationRequest) (ReservationResult,
  error)`); only `ReservationRequest`'s field set widened additively.
  Every existing caller that constructs a `ReservationRequest` literal
  without `LineNo`/`Attempt` still compiles — those fields default to
  the zero value, which only matters for `Client`'s own key derivation
  (test fixtures and `PermissiveClient`, which never makes an HTTP call
  at all, are unaffected).
- `BreakerClient` (ADR 0025) needed **zero changes**: it calls
  `c.inner.Reserve(callCtx, req)` and passes `req` straight through, so
  the widened struct and the new header flow through the breaker/
  fallback wrapper transparently.
- `RevokeReservation` is unaffected — confirmed inventory-storage never
  gates that route, so no header was ever missing there. This ADR does
  not touch `RevokeReservation`'s request beyond documenting why it
  deliberately sends no key.
- Fleet-wide check performed as part of this fix: `warehouse-ops-agent`
  is confirmed read-only/GET-only against inventory-storage (its own
  zero-write-capability ADR) and therefore never calls `POST
  /reservations` or `POST /stock/receive` at all. No other repo in
  `~/warehouse-systems` calls `POST /stock/receive` either (grepped
  fleet-wide) — `order-management`'s `Client.Reserve` was the sole
  caller affected by PR #98's contract change.

## Alternatives considered

- **A random UUID minted fresh per `Reserve` call:** rejected outright —
  it satisfies "every call gets *a* key" but defeats the entire point of
  the header: a genuine retry of the same attempt would mint a
  DIFFERENT key each time, so inventory-storage would never recognize it
  as a replay and could create a duplicate reservation on a retried
  network failure — precisely the bug this header exists to prevent.
- **A new dedicated nonce/attempt counter on `Order` or `OrderLine`,
  separate from `Version()`:** rejected as unnecessary duplication.
  `Order.Version()` (ADR 0024) already has exactly the required
  stable-within-a-pass / strictly-increasing-across-passes property, is
  already persisted, already tested, and already flows through every
  code path that calls `allocateLines`. Adding a second counter with the
  identical shape would be a parallel mechanism solving an already-solved
  problem, the same anti-pattern ADR 0023 §6 warned against for the
  transaction-join mechanism.
- **Hashing the full request body (SKU+Quantity+DemandRef) into the key,
  mirroring `RequireIdempotencyKey`'s own body-hash check:**
  rejected — inventory-storage ALREADY computes and compares its own
  `request_hash` server-side (that is what makes a same-key/
  different-body retry a `422`, not a silent overwrite); duplicating
  that hash into the key itself would be redundant and would also
  silently change the key whenever quantity legitimately differs between
  two genuinely different attempts for the same line, which is not a
  case this design needs to distinguish (the `LineNo`/`Attempt` pair
  already disambiguates attempts correctly).
