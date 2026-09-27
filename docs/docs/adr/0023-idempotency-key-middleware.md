---
id: 0023-idempotency-key-middleware
slug: /adr/0023-idempotency-key-middleware
title: 23. Transactional Idempotency-Key middleware for POST /orders
sidebar_label: 23. Idempotency-Key middleware
description: "ADR 0023 — POST /orders requires a caller-supplied Idempotency-Key header. A route-scoped middleware begins the outer Postgres transaction, joins it with ReceiveOrder's own UnitOfWork via the existing tx-in-context mechanism (internal/pgtx), and lets the database's own unique-index lock do request de-duplication with no polling, no timeout, and no in-progress state."
---

# 23. Transactional Idempotency-Key middleware for POST /orders

## Status

Accepted — implemented in the same change that introduced this record.
Intended as the reference pattern the rest of the warehouse-systems
fleet copies for their own resource-creation endpoints.

## Context

`POST /orders` (`ReceiveOrder`) is this service's one mutating route
that creates a genuinely NEW resource with a server-generated id. A
client that never receives the response to a successful call — a
dropped connection, a load-balancer timeout, a client-side retry policy
— has no safe way to tell "my request never arrived" apart from "my
request arrived and succeeded but I never saw the response," and a
naive retry duplicates the order.

The other mutating routes (`POST /orders/{id}/retry-allocation`,
`POST /orders/{id}/release`) act on a caller-supplied `{id}`, which
already makes a byte-identical retry far safer (at worst a redundant
no-op against an already-allocated/released order — a separate,
narrower lost-update race, explicitly out of scope for this change).
`DELETE /orders/{id}` is idempotent by ordinary HTTP semantics already.
This ADR therefore scopes the new requirement to `POST /orders` only,
applied route-scoped (chi's `r.With(...)`) rather than to the whole
router.

ADR 0022 (transactional outbox) left this service with exactly the
transactional infrastructure this problem needs already in place:
`ports.UnitOfWork` + `postgres.UnitOfWork.Execute`, and the
context-based transaction-join mechanism (`querierFrom`/`beginOrJoin`,
now factored into `internal/pgtx`) that lets a repo's own SQL detect and
join an ALREADY-OPEN outer transaction rather than starting a second,
invisible one. This ADR's core design choice is to make idempotency
bookkeeping join that SAME transaction, rather than build a second,
parallel transactional mechanism — so the entire HTTP-request-to-response
cycle (idempotency bookkeeping, the `Order` aggregate write, the outbox
insert) commits or rolls back as one atomic unit.

## Decision

### 1. `Idempotency-Key` header, required on `POST /orders`

A request to `POST /orders` without an `Idempotency-Key` header gets
`400 application/problem+json` (`idempotency-key-required`). This is a
deliberate v1 choice: require the header on true resource-creation
endpoints rather than making it optional-but-recommended — an optional
header is trivial for a client to forget to set on exactly the retry
path where it matters most, defeating the point.

### 2. `idempotency_keys` table (migration `0008_idempotency_keys`)

```sql
CREATE TABLE idempotency_keys (
    key              TEXT PRIMARY KEY,
    method           TEXT NOT NULL,
    path             TEXT NOT NULL,
    request_hash     TEXT NOT NULL,
    status_code      INTEGER,
    response_body    BYTEA,
    response_headers JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);
CREATE INDEX idx_idempotency_keys_created_at ON idempotency_keys (created_at);
```

`request_hash` is `hex(sha256(request body))`. `status_code`,
`response_body`, and `response_headers` start `NULL` and are populated
by exactly one `UPDATE`, in the same transaction that inserted the row,
immediately before that transaction commits (§4).

### 3. `RequireIdempotencyKey` middleware (`internal/adapters/inbound/http/idempotency.go`)

Route-scoped only: `r.With(RequireIdempotencyKey(pool)).Post("/orders", ...)`
in `server.go`, never `r.Use(...)` on the whole router.

The request body is read fully into memory once (needed both for
hashing and for replaying to the real handler), hashed, and then
restored via `io.NopCloser(bytes.NewReader(...))` so downstream JSON
decoding in `ReceiveOrder`'s handler is completely unaffected.

The middleware then begins a `pgxpool` transaction directly (it does
not need to import `internal/adapters/outbound/postgres` to do this)
and binds it into the request's `context.Context` via the SAME
mechanism `postgres.UnitOfWork` already uses — see §5 for why this had
to be factored out rather than reused as-is.

`INSERT INTO idempotency_keys (key, method, path, request_hash) VALUES
($1,$2,$3,$4) ON CONFLICT (key) DO NOTHING` runs inside that
transaction:

- **1 row inserted** (genuinely new key): call the real handler with
  the tx-carrying context, using an `httptest.ResponseRecorder` to
  CAPTURE its status/headers/body rather than writing to the real
  `http.ResponseWriter` yet (§4).
- **0 rows** (a row for this key already exists): roll back this new,
  empty transaction (it never wrote anything) and fall through to a
  plain, non-transactional `SELECT` (§6 explains why this is safe
  without any additional locking or waiting):
  - `request_hash` mismatch → `422` (`idempotency-key-reused`):
    "Idempotency-Key was already used with a different request."
  - `request_hash` match → this is a genuine retry: write
    `response_headers`, then `status_code`, then `response_body`
    VERBATIM to the real `http.ResponseWriter`, and return WITHOUT
    calling the real handler at all.

### 4. The no-null-status-code-ever-committed invariant

On the fresh-key path, after the wrapped handler runs against the
recorder:

- **Normal completion (no panic):** within the SAME transaction that
  inserted the bare row, `UPDATE idempotency_keys SET status_code=$1,
  response_body=$2, response_headers=$3, completed_at=now() WHERE
  key=$4`, then `COMMIT`. Only AFTER a successful commit does the
  middleware copy the recorder's headers/status/body onto the real
  `http.ResponseWriter`.
- **Panic:** the middleware recovers it, `ROLLBACK`s the transaction (so
  neither the idempotency row nor any domain write the handler made
  ever becomes visible), and re-panics so the outer chi `Recoverer`
  still produces the service's normal `500`. A panic's outcome is
  never cached — a retry after a panic must re-attempt the real work.

The invariant this ordering buys: **a transaction that reads a
COMMITTED `idempotency_keys` row can never observe a `NULL`
`status_code`.** A row is either (a) never committed at all — the
inserting transaction rolled back, so no other transaction can ever see
it under Postgres's normal visibility rules — or (b) committed, in
which case `status_code`/`response_body`/`response_headers` were
already populated by the `UPDATE` that ran, in the same transaction,
strictly before the `COMMIT` that made the row visible at all. There is
no third state: no "in-progress" marker, no client-facing
retry-after/409, no polling loop or timeout anywhere in this design —
unlike naive two-phase idempotency-key implementations that need an
explicit in-progress status precisely because they do not enforce this
invariant.

### 5. Concurrency: Postgres' own unique-index lock does the serialization, not application logic

Two concurrent requests carrying the SAME key race on the `INSERT ...
ON CONFLICT (key) DO NOTHING` above. Postgres serializes them at the
primary-key unique index: the SECOND (and every later) inserter's
statement BLOCKS until the FIRST inserter's transaction resolves —
commits or rolls back. So by the time ANY transaction observes
`rowsAffected() == 0` on this insert, the ORIGINAL inserting
transaction has unconditionally finished. Combined with §4's invariant:
whenever the middleware takes the "0 rows" branch, the pre-existing row
— if that original transaction committed — already has its outcome
fully populated; if it rolled back, the row does not exist at all, and
THIS caller's own (previously blocked) insert instead succeeds with
`rowsAffected() == 1`, taking the fresh-key branch. Either way, no
polling loop, no lock-retry budget, and no timeout are needed anywhere
in this code — the database's own MVCC/locking semantics ARE the
synchronization primitive. This claim is proven with a real test (not
asserted from theory): `TestIdempotency_Concurrent_SameKeySameBody_ExactlyOneOrderCreated`
fires five real goroutines at the real router with the same key and
body and asserts, via a direct DB count, that exactly one `orders` row
exists afterward.

### 6. Reusing, not duplicating, the transaction-join mechanism (`internal/pgtx`)

The brief for this change required the middleware to put its
transaction into `ctx` "using the exact same context key/mechanism"
`postgres.UnitOfWork.Execute` already checks for, so that
`ReceiveOrder`'s own `uc.UnitOfWork.Execute(ctx, fn)` call detects the
outer transaction and joins it (runs `fn` directly) instead of opening
a nested one.

That mechanism pre-existed as unexported symbols inside
`internal/adapters/outbound/postgres/unit_of_work.go`
(`withTx`/`txFrom`, an unexported `txKey{}` context-key type). The
middleware, however, lives in `internal/adapters/inbound/http` — and
this repo's architecture fitness tests
(`internal/architecture/fitness_test.go`) forbid the inbound HTTP
adapter from importing the outbound Postgres adapter (and vice versa).
Reusing the mechanism unchanged was therefore impossible without
either violating that boundary or inventing a second, parallel
mechanism (explicitly disallowed by the brief).

The fix, at the lowest correct layer: extract the bare
key-type-plus-`WithTx`/`TxFrom` pair into a new, tiny, dependency-free
package, `internal/pgtx`, that both adapter packages import.
`postgres.withTx`/`postgres.txFrom` (and therefore `querierFrom`,
`beginOrJoin`, and `UnitOfWork.Execute`) now delegate to
`pgtx.WithTx`/`pgtx.TxFrom` — an internal refactor with no change in
observable behaviour for any existing caller. The idempotency
middleware calls the exact same `pgtx.WithTx`/`pgtx.TxFrom` functions.
This means `ReceiveOrder`'s own use case and its `atomically()`/
`UnitOfWork.Execute` call needed **zero changes** to pick up the
middleware's transaction — `Execute`'s existing "already in a
transaction? just run `fn(ctx)`" branch (`if _, ok := txFrom(ctx); ok {
return fn(ctx) }`) already does exactly the right thing once the
context it receives carries a `pgtx`-bound transaction from any
source, not just its own. This join behaviour is asserted with a real
test, not assumed: every integration test scenario in
`idempotency_integration_test.go` runs the full `POST /orders` request
through the real chi router and a real Postgres, and
`TestIdempotency_FreshKey_CreatesOrderAndRecordsOutcome` /
`TestIdempotency_Replay_...` directly assert both the `idempotency_keys`
row AND the `orders` row exist/don't-exist exactly as the atomic-commit
argument predicts.

### 7. Response caching scope: every normal response, including business errors

The middleware caches every NORMAL (non-panic) response the wrapped
handler produces — including a `4xx` business-logic error (e.g. a
line's non-positive quantity, rejected by the domain and surfaced as a
`422`). This is a deliberate v1 simplification: a client retrying the
exact same key + body deterministically gets the exact same answer,
including a validation error, rather than re-running (and potentially
re-deciding) the same validation. A client wanting a genuinely
different outcome must use a new `Idempotency-Key`. Proven by
`TestIdempotency_BusinessErrorResponse_IsCachedAndReplayed`: a request
with an invalid line quantity gets `422` and has that `422` — not a
`201` — stored in `idempotency_keys`, and a retry with the same key +
body replays the identical `422` body rather than re-validating.

## Consequences

- `POST /orders` now requires an `Idempotency-Key` header; every
  existing client/BDD/contract-test caller of that route needed (and
  received, in this same change) a header on every call.
- The whole request cycle — idempotency bookkeeping, `Order` aggregate
  write, outbox insert — is one Postgres transaction; a failure
  anywhere in that cycle after the idempotency row's `INSERT` rolls
  back the ENTIRE cycle, including the idempotency row itself. A
  retried request after such a failure re-attempts the real work from
  scratch (there is no stale, half-written idempotency row left
  behind to confuse it — see §4).
- `internal/pgtx` is a new, tiny shared package; every future
  cross-cutting-transaction feature in this service (or the 6+ other
  repos adopting this pattern) should extend it rather than re-invent
  a parallel tx-in-context mechanism, or reach for it directly instead
  of importing the postgres adapter package from inbound code.
- **Known follow-up, explicitly deferred:** no TTL/cleanup job exists
  yet for old `idempotency_keys` rows. The table grows unboundedly
  today; `idx_idempotency_keys_created_at` exists specifically so a
  future scheduled job (e.g. `DELETE ... WHERE created_at < now() -
  interval '30 days'`) can find old rows without a full table scan.
  Building that job is out of scope for this change.
- The two `{id}`-scoped mutating routes
  (`retry-allocation`, `release`) remain unprotected by this
  middleware; they are naturally safer (caller-supplied id) but still
  have a separate lost-update race under concurrent retries, which
  this ADR explicitly does not address.

## Alternatives considered

- **Application-level in-memory de-duplication (e.g. a local cache of
  recently-seen keys):** rejected — does not survive a pod restart or
  work across replicas, both of which matter for a service meant to
  run more than one instance.
- **A three-state design (`pending`/`completed`/`failed`) with a
  timeout and a `409`-retry-later response for requests that arrive
  while another is still `pending`:** rejected in favor of the
  transactional design in §4/§5. Postgres' own lock on the unique index
  already serializes concurrent identical-key requests without any
  extra state, and the no-null-status-code invariant means there is
  never an observable "pending" row to design a timeout for in the
  first place — a real correctness argument backed by a real
  concurrent-goroutine test (§5), not a simplification that trades away
  correctness for less code.
