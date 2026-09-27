---
id: 0024-optimistic-concurrency-version-column
slug: /adr/0024-optimistic-concurrency-version-column
title: 24. Optimistic concurrency (version column) for the Order aggregate
sidebar_label: 24. Optimistic concurrency
description: "ADR 0024 — order-management adds a version column to the orders table and version-guards OrderRepo.Save, closing a real lost-update race between concurrent requests that read-mutate-save the SAME existing order (RetryAllocation, ReleaseHeldOrder, CancelOrder, the RepromiseOrder Kafka consumer). A version mismatch surfaces as ports.ErrConcurrentModification / HTTP 409. Complementary to, not overlapping with, PR #105's idempotency-key middleware."
---

# 24. Optimistic concurrency (version column) for the Order aggregate

## Status

Accepted — implemented in the same change that introduced this record.

## Context

`OrderRepo.Save` (`internal/adapters/outbound/postgres/order_repo.go`) did a
blind upsert on the `orders` row:

```sql
INSERT INTO orders (...)
VALUES (...)
ON CONFLICT (id) DO UPDATE SET
    promise_date = EXCLUDED.promise_date,
    promise_cpt_id = EXCLUDED.promise_cpt_id,
    promise_basis = EXCLUDED.promise_basis
```

and a similarly blind per-line upsert on `order_lines`. Neither carries
any notion of "the version of the row I read before I computed this
write" — the last `Save` to reach Postgres always wins, silently
overwriting whatever an earlier, slower writer had already computed.

This is a real risk on this specific aggregate because FOUR different
code paths each perform a full read-mutate-save cycle against an
**existing** order, and none of them coordinate with each other:

- `RetryAllocation` — an operator/recovery HTTP call
- `ReleaseHeldOrder` — an HTTP call, racing the above on a held order
- `CancelOrder` — an HTTP call
- `RepromiseOrder` — driven by the `warehouse.fulfillment.events` Kafka
  consumer (`RepromiseConsumer`), entirely independent of the HTTP
  request path

Two concrete, realistic interleavings:

1. `POST /orders/{id}/retry-allocation` and `POST /orders/{id}/release`
   arrive for the same order within the same few milliseconds (an
   operator retries allocation while a released-inventory automation
   independently calls release). Both load the order, both mutate
   different (or even the same) in-memory state, both `Save`. Whichever
   `Save` commits second silently discards the first's write.
2. A `TaskCPTMissed` message drives `RepromiseOrder` for an order at the
   exact moment an operator calls `RetryAllocation` for the same order.
   Same shape, same silent loss.

**This is a different problem from PR #105's idempotency-key
middleware.** That middleware protects `POST /orders` (order
*creation*) from a **retried** request double-creating the same order —
it keys on a client-supplied `Idempotency-Key` header and a
request-body hash, and only wraps the create path. It says nothing
about two **different** requests (retry-allocation vs release vs a
Kafka-driven repromise) racing to mutate the **same already-existing**
order — that is exactly the lost-update problem this ADR closes. The
two mechanisms are complementary and do not overlap: idempotency-key
prevents duplicate *creates*; the version column prevents lost updates
on existing rows. A future PUT-style endpoint could use both together —
the idempotency middleware wrapping the whole request, and the version
guard still protecting the aggregate write inside it.

## Decision

### 1. `version` column on `orders` only

Migration `0009_orders_version`:

```sql
ALTER TABLE orders ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
```

`order_lines` (and `order_promise_groups`) are **not** independently
versioned. The Order aggregate — the whole order, all its lines, all
its promise groups — is the consistency boundary (standard DDD
aggregate-root rule): every `Save` call already writes all three tables
inside one transaction, and the version guard on the parent `orders`
row is sufficient to make the ENTIRE aggregate write atomic
with respect to a concurrent aggregate write. Versioning `order_lines`
rows independently would let two different line updates from two
different (correctly-serialized) transactions interleave at the line
level, which is not a risk this aggregate has — a `Save` call always
writes the complete, coherent line set computed from one read.

### 2. Domain: inert version field

`Order` gained an unexported `version int` field, a `Version() int`
accessor, and `SetVersion(int)` (used only by the repository — see
§3). The hydration constructor `RehydrateHeld` gained a trailing
`version int` parameter; `New` (fresh, never-persisted orders) always
starts at version 1. No business-rule method reads or branches on
`Version()` — it is inert infrastructure metadata riding along on the
aggregate purely for the repository's use, exactly like the outbox's
event append does not become a domain concern.

### 3. `OrderRepo.Save`: explicit version-guarded UPDATE, THEN fallback INSERT

The upsert became an explicit two-statement pair rather than a single
`INSERT ... ON CONFLICT DO UPDATE ... WHERE version = $x`:

```sql
UPDATE orders SET
    promise_date = $3, promise_cpt_id = $4, promise_basis = $5,
    release_on_allocation = $6, required_ship_by = $7,
    version = version + 1
WHERE id = $1 AND version = $2
```

- **1 row affected** → the common case (every `Save` after the first
  for a given id): version matched, bumped by exactly one, done.
- **0 rows affected** → ambiguous by `RowsAffected()` alone: either
  no row exists yet (first `Save` ever for this id) or a row exists at
  some *other* version (a lost race). Disambiguated with a direct
  `SELECT EXISTS(...)`:
  - row does not exist → fall back to a plain `INSERT` storing
    `o.Version()` (1, from `order.New`) as the initial stored value.
  - row exists → return `ports.ErrConcurrentModification` **before
    touching `order_lines` or `order_promise_groups` at all** (see §4).

An `ON CONFLICT ... DO UPDATE ... WHERE <version predicate>` single
statement was considered and rejected for this change: pgx's
`CommandTag.RowsAffected()` for that shape conflates "no id at all" and
"id exists but version didn't match" into the same `0`, and confirming
which of those two cases actually happened still requires the same
existence check this decision already needs — so the explicit
UPDATE/INSERT pair buys three individually-testable, individually-named
outcomes (fresh insert / version match / version conflict) for no extra
round-trips in the common case, at the cost of one extra round-trip
only in the already-rare "conflict or first-ever-save" branch.

### 4. Version check runs BEFORE any line/group write

`Save`'s version-guarded `orders` UPDATE is the FIRST statement in the
transaction, before the `order_lines` upsert loop and before the
`order_promise_groups` delete-and-reinsert. A stale-version `Save`
returns `ports.ErrConcurrentModification` and rolls back the whole
transaction without ever issuing a single `order_lines` or
`order_promise_groups` statement — confirmed by
`TestOrderRepo_Save_VersionGuard_StaleVersionFailsAndLeavesLinesUntouched`,
which asserts the winner's `reservation_id` on the shared line survives
completely untouched after the loser's failed `Save`.

### 5. Sentinel error and HTTP mapping

`ports.ErrConcurrentModification` is a new sentinel alongside this
package's other `Err*` values in `internal/application/ports/ports.go`
(the existing home for cross-cutting outbound-port errors like
`ErrDownstreamNotConfigured`/`ErrInsufficientStock`). It is mapped in
`internal/adapters/inbound/http/errors.go`'s `statusFor`/`problemFor`
switches to `409 Conflict`, in the same case-group as the other
"business fact, not ambiguous failure" 409s (`ErrOrderAlreadyReleased`,
`ErrOrderNotHeld`, etc.), with its own RFC 7807 problem type
`concurrent-modification`.

### 6. RepromiseOrder / the Kafka consumer

`RepromiseOrder.Execute` returns the version conflict unchanged (it is
already an ordinary `error` return, and `allocateAndRelease`'s
`atomically()` wrapper propagates it as-is). `RepromiseConsumer.
handleMessage` gives it a dedicated branch, distinct from every other
error: the message is logged but **NOT committed**, so it is safely
redelivered (this consumer is already an at-least-once, real
Kafka-offset-committing consumer with its own `RepromiseOrder`-internal
idempotency gate keyed on `event_id` — reprocessing a redelivered
message is a normal, already-supported code path, not a new failure
mode). Every OTHER error from `handleFulfillmentEvent` is still logged
and committed (skip-and-move-on), matching this consumer's existing
convention for malformed/unhandleable messages — a version conflict is
the one case that is neither "malformed" nor "permanent," so it alone
gets the "leave it for redelivery" treatment. The consume loop itself
does not abort on a version conflict: one order's transient conflict
must not stop repromising every other order in the same partition.

## Consequences

- Two concurrent requests racing to mutate the SAME existing order now
  produce exactly one success and one `409 Conflict` — verified by a
  real two-goroutine integration test
  (`TestOrderRepo_Save_ConcurrentGoroutines_RaceOnSameOrder`) racing two
  calls to the real `ReleaseHeldOrder` use case against a
  testcontainers Postgres, with a `sync.WaitGroup` rendezvous barrier
  forcing both goroutines' reads to complete before either's write can
  start (removing timing luck from whether the race actually
  interleaves in a fast local test run).
- A caller that receives `409`/`ErrConcurrentModification` is expected
  to reload the order and retry its intent from the fresh state — this
  is the same contract every other 409 in this service already implies
  (`ErrOrderAlreadyReleased`, `ErrShipCompleteBlocked`, etc.).
- `order_lines`/`order_promise_groups` are unaffected structurally:
  still written every `Save`, still inside the same transaction as the
  now-version-guarded `orders` write — just now provably never written
  at all when the version check fails.
- This composes with, and does not replace, PR #105's idempotency-key
  middleware: a future PUT-style mutating endpoint could wrap the
  idempotency middleware AROUND a use case whose `Save` is still
  version-guarded underneath, giving both "a retried identical request
  is a no-op" and "two genuinely different concurrent requests don't
  lose an update" at once.
- `internal/pgtx`/`UnitOfWork.Execute` (from PR #103's outbox rollout)
  are unaffected: `Save` still joins an outer transaction via
  `beginOrJoin` exactly as before — the version guard is just more SQL
  inside the same transaction boundary, not a new transactional
  mechanism.
