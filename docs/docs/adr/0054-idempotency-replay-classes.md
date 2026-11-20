---
id: 0054-idempotency-replay-classes
slug: /adr/0054-idempotency-replay-classes
title: "0054. Every problem type has an idempotency replay class; a retryable refusal is not stored"
sidebar_label: "54. Idempotency replay classes"
sidebar_position: 54
description: "ADR 0054 — fleet rule (owner, 2026-11-18). A retry under the same Idempotency-Key is answered from the store only when the first answer is final for the request. Every problem type declares FINAL, OUTCOME or RETRYABLE on its catalogue entry; the middleware reads the class and holds no list of slugs. A RETRYABLE refusal (concurrent-modification, a fact not delivered yet, a state not reached yet) is rolled back with its key row exactly like a 5xx. Narrows ADR 0023 §7."
---

# 0054. Every problem type has an idempotency replay class; a retryable refusal is not stored

## Status

Accepted (2026-11-19). **Narrows [ADR 0023](./0023-idempotency-key-middleware.md) §7**
("every normal response is cached, 4xx included") and the replay sentence of
[ADR 0048](./0048-adopt-fleet-rest-api-guideline.md); their Decision text is not rewritten,
both carry a dated pointer here. Implements the fleet rule
`.claude/rules/fleet/idempotency-and-outbox.md` (owner decision 2026-11-18; standard and
adoption steps in warehouse-harness-template `docs/idempotency-replay-classes.md`).

## Context

The middleware (`internal/adapters/inbound/http/idempotency.go`) inserts the key row in the
SAME Postgres transaction as the business unit of work (`pool.Begin` :147, `INSERT ... ON
CONFLICT (key) DO NOTHING` :154-156), binds the transaction into the context
(`internal/pgtx`) so the use cases' `UnitOfWork.Execute` joins it, and runs the handler behind
`SAVEPOINT handler_run` (:285). Before this ADR it stored every answer below 500 and rolled the
transaction back (key row included) only for a 5xx or a panic. So a `409 concurrent-modification`
or a `400 unknown-process-path` (the process-path catalogue had not received the path's event
yet) was replayed for 24 hours to a client that did the right thing and retried.

What a 4xx did to the handler's writes: nothing by itself. A joined unit of work runs in its own
savepoint (`postgres/unit_of_work.go` `executeInSavepoint` :132): rolled back when the use case
errors; a scope that committed stays and commits WITH the key row (`RELEASE SAVEPOINT` :309). The
three keyed routes are `POST /orders`, `POST /orders/{id}/releases` and
`POST /orders/{id}/allocation-retries` (`server.go` router wiring); `DELETE /orders/{id}` is not
keyed. The key lives 24 hours (`IDEMPOTENCY_KEY_TTL`, hourly sweeper, ADR 0032).

## Decision

### 1. One class per problem type, declared on the catalogue entry

| Class | Meaning | Stored and replayed |
|---|---|---|
| `FINAL` | the answer is a function of the request alone, or a stored answer can never become false | yes |
| `OUTCOME` | the command COMMITTED state, then answered an error that must be remembered | yes, its writes commit with it |
| `RETRYABLE` | nothing committed and the answer depends on state that can change (version conflict, a referenced fact not known yet, an aggregate state not reached yet, a dependency not ready) | no: the key is released |

A 2xx is stored; a 5xx and a panic are never stored; a replay carries `Idempotency-Replayed:
true`. The class is a field of `problemInfo` (`errors.go`): `problemInfo{slug, title, class}` is
the catalogue entry of a typed domain error (`problemCatalog`) and of every other problem type
(declared once with `register(...)` in `problems.go`). Reshape: `problemInfo` gained `class`,
the status switch `statusFor` is untouched, the ad hoc literals at the call sites became
`register` variables (slug, title and status on the wire unchanged). There is no default class.

### 2. Mechanism

`writeProblem` (so `writeError`) marks the middleware's capture (`replay_class.go`: `capture`,
`markReplayClass`) with the class of the problem it answers. `runFreshRequest` asks
`capture.storable()`: a 2xx yes, a 5xx no, any other answer only when the class is FINAL or
OUTCOME. Everything else, a RETRYABLE answer **and a refusal that declares no class** (a handler
that answers a 4xx without the problem writer; the safe fallback), takes the branch of the 5xx:
`tx.Rollback`, key row included, then the answer is written. The middleware holds no slug and no
status range other than 2xx and 5xx. 2xx, 5xx and FINAL answers are byte-identical to before.

**Releasing a key means a different body under the same key is a NEW request**, not
`409 idempotency-key-reused`: nothing is left to compare against. The same body runs again.

**What a concurrent duplicate sees when the first ends RETRYABLE:** it is blocked by Postgres on
the unique index at the key INSERT until the first transaction resolves; the first rolled back,
so the duplicate's own INSERT succeeds (the "fresh key" branch), it executes the command itself
and answers its own result, possibly a success. It never replays a released refusal and never
runs alongside the first. When the first ends FINAL, OUTCOME or 2xx the duplicate finds the row
and replays it.

### 3. No OUTCOME entry (re-checked per keyed route)

This store keeps a 4xx's committed scopes (`RELEASE SAVEPOINT`), so a refusal returned after a
committed scope WOULD commit with its key row. Read for each keyed use case; none commits and then
refuses:

- `POST /orders` (`receive_order.go`): validation and path resolution (:173-:181) run before any
  write; the order Save and `OrderReceived` are one `atomically` scope (:198); an allocation
  failure after that is swallowed and the stored order answered 201 (:220 `orderAfterAllocationFailure`).
- `POST /orders/{id}/releases` (`release_held_order.go`): refusals at :60 and :81 are before any
  write; already-released is a 200 (:89).
- `POST /orders/{id}/allocation-retries` (`retry_allocation.go`): refusals at :53 and :58 are
  before any write. The one commit-then-error path is `allocateAndRelease`'s salvage
  (`allocation_release.go:22`, `allocation.go:297-:306`): a partial allocation is saved and the
  hard failure returned. That failure is a `ports.ErrDownstreamUnavailable` (503) or a context
  error (500), a 5xx: the whole transaction, salvage included, is rolled back (nothing is stored
  and the retry re-executes; the reservations already made upstream replay by their key
  `res-<order>-line-<n>-att-<version>`, ADR 0028, the version being unchanged). A domain error
  after progress (`allocation.go` `Allocate`/`RetryAllocate`) cannot occur: the lines passed are
  selected by status on the loaded aggregate, which only this request touches.
- `ErrShipCompleteBlocked` is not a problem type (a hold answers 2xx).

So the middleware needs no new "keep writes" mechanism, and the one rule it needs is the
lost-writes case: an OUTCOME answer whose savepoint cannot be released is a 500 and the key
retries, like a 2xx behind a failed release (`idempotency.go:320`). The class stays in the
vocabulary so a future refusal that commits is declared OUTCOME, not forced into FINAL.

### 4. The class table

32 problem types: 14 FINAL, 0 OUTCOME, 18 RETRYABLE. This table is held equal to the catalogue
(slug, class) by `TestTheADRClassTableMatchesTheCatalogue`. Every non-FINAL row says why, with
lines. "Not keyed" means no keyed route can answer it today; its class is still declared.

| Problem type | Status | Class | Why |
|---|---|---|---|
| `concurrent-modification` | 409 | RETRYABLE | `OrderRepo.Save` version-guarded UPDATE found zero rows and the row exists (`postgres/order_repo.go:185`): depends on another writer's commit; the use case's savepoint rolled its writes back, and the retry reloads the new version. Proven by `TestReplayClass_ConcurrentModificationLoserIsReleasedAndItsRetryExecutes` (a staged genuine race). |
| `downstream-not-configured` | 503 | RETRYABLE | A 5xx is never stored (permissive mode of the inventory client). |
| `downstream-unavailable` | 503 | RETRYABLE | A 5xx is never stored (transport failure, open breaker, a callee 409 `idempotency-key-reused` mapped here). |
| `empty-order-id` | 400 | FINAL | `shared.NewOrderId("")` on the path id: a function of the request. |
| `empty-path-id` | 400 | FINAL | `shared.NewPathId("")`: a function of the value; `pathId` is never caller-supplied, so no keyed route answers it today. |
| `empty-sku` | 400 | FINAL | `shared.NewSKU("")` on a request line. |
| `held-order-must-be-ship-complete` | 422 | FINAL | `order.ValidateIntakeIntent` (`domain/order/intake_intent.go:24`): a pure function of two request flags. |
| `idempotency-key-invalid` | 400 | FINAL | The key header of this request; answered before the store. |
| `idempotency-key-required` | 400 | FINAL | The key header of this request; answered before the store. |
| `idempotency-key-reused` | 409 | FINAL | Answered from the stored row of the first request; the fingerprint is the request. |
| `insufficient-stock` | 503 | RETRYABLE | A 5xx is never stored. The use case turns inventory-storage's refusal into a Backordered line, so it does not reach the wire today. |
| `internal-error` | 500 | RETRYABLE | A 5xx is never stored. |
| `invalid-fulfillment-site-id` | 400 | FINAL | `shared.NewFulfillmentSiteID` checks the shape of the request field. |
| `invalid-query-parameter` | 400 | FINAL | `GET /planned-capacity` query; not keyed. |
| `invalid-report-query` | 400 | FINAL | `GET /reports/funnel` query; not keyed. |
| `line-ineligible-for-resolved-path` | 422 | RETRYABLE | `PathPolicy.Select` returns no path (`receive_order.go:259`) from the local copy of product-master events (`productAttributes`, ADR 0036) and the Kafka-fed path catalogue (ADR 0021): a later event can admit the line. The slug also covers a quantity above every path's limit (request only): mixed slug, so RETRYABLE (judgement call 3). |
| `malformed-request-body` | 400 | FINAL | `decodeJSON` and the unreadable body: a function of the bytes of the request. |
| `no-allocated-lines` | 409 | RETRYABLE | Mapped but returned by no use case today (`usecases/errors.go:14`); if returned, the allocated lines of an order change with allocation. Unsure: RETRYABLE. |
| `no-backordered-lines` | 409 | RETRYABLE | `retry_allocation.go:58`: the order's lines when loaded; a retry races a concurrent allocation. Nothing committed (before any write). |
| `non-positive-quantity` | 422 | FINAL | `order.NewOrderLine` (`domain/order/order_line.go:27`): the request's own quantity. |
| `order-already-released` | 409 | RETRYABLE | `Order.EnsureCancellable` (`domain/order/order.go:434`), `DELETE /orders/{id}` only: not keyed. A released line never reverts (ADR 0004), so a stored answer could not become false; RETRYABLE by the tie-break and the fleet precedent for terminal-state refusals (judgement call 2). |
| `order-line-already-allocated` | 409 | RETRYABLE | `Order.Allocate`/`MarkBackordered` (`order.go:315`, `:389`): a line moves on (Released, Shipped), so "already allocated" can become false. Unreachable from the routes (lines are chosen by status). |
| `order-line-not-allocated` | 409 | RETRYABLE | `ReconfirmReservation`/`LoseReservation`/`Release` (`order.go:352`, `:369`, `:403`): aggregate state; unreachable from the routes. |
| `order-line-not-backordered` | 409 | RETRYABLE | `RetryAllocate` (`order.go:333`): aggregate state; unreachable from the routes. |
| `order-line-not-found` | 400 | RETRYABLE | `Order.line` (`order.go:486`): no route takes a line number; an aggregate invariant, not a request property. Unsure: RETRYABLE. |
| `order-line-not-pending` | 409 | RETRYABLE | `Allocate`/`MarkBackordered` (`order.go:322`, `:391`): aggregate state; unreachable from the routes. |
| `order-not-found` | 404 | FINAL | 404 on the command's own target: ids are `ord-<uuid>` minted here (`postgres/order_repo.go:374`), `INSERT INTO orders` happens only in `Save`, no order is ever deleted, no event or other request supplies one (judgement call 1). |
| `order-not-held` | 409 | RETRYABLE | `release_held_order.go:81` reads the stored order's `heldAtIntake`: depends on stored state, nothing committed (before any write). Set once at intake (`receive_order.go:230`, never cleared, ADR 0020), but unsure: RETRYABLE (judgement call 2). |
| `order-without-lines` | 400 | FINAL | The request has no lines (`server.go` `handleReceiveOrder`). |
| `promise-date-not-set` | 409 | RETRYABLE | Mapped but returned by no use case today (`usecases/errors.go:31`); the promise date is set by allocation, state that changes. |
| `report-store-error` | 500 | RETRYABLE | A 5xx is never stored; `GET /reports/funnel`, not keyed. |
| `unknown-process-path` | 400 | RETRYABLE | `uc.Catalogue.IsActive(path)` false (`receive_order.go:269`): the catalogue is a local copy of process-path-management events (ADR 0013); the path can be announced after the first attempt. Proven by `TestReplayClass_UnknownProcessPathIsReleasedSoTheRetryFindsItOnceKnown`. |

## Consequences

- A client retrying through a `concurrent-modification`, an unknown process path or a not-ready
  state now gets its retry executed under the same key, and only the final answer is stored. The
  cost is one more execution of a refusal that has no side effect (nothing committed).
- A stored refusal is now only one a retry could not change: the request's own validation and an
  id that does not exist. `Idempotency-Replayed: true` therefore still means "the same answer".
- **Callers.** order-management's own outbound call, `POST /reservations` on inventory-storage,
  sends `res-<demandRef>-line-<lineNo>-att-<attempt>` (ADR 0028, `attempt` = the aggregate
  version): a refusal that persists a Backordered line bumps the version, so the next pass has a
  fresh key and was never stuck; a pass that failed before its Save (a lost race, a 5xx) now
  re-asks the same key and inventory-storage (its ADR 0053: `insufficient-usable` and the other
  state refusals RETRYABLE) executes it again instead of replaying the stale refusal. The client
  mapping is unchanged and still fits: a 409 other than `idempotency-key-reused` is "no usable
  stock" (Backordered), `idempotency-key-reused` (FINAL there) and every other non-2xx is
  `downstream-unavailable`. Nothing changed in the client.
- MCP and A2A have no write tool and no store of their own: nothing to classify
  (the MCP tools are L1 read-only, the A2A skills `order_status` and `promise_health` are reads).
- The spec descriptions of `Idempotency-Key` say what is replayed and what releases the key.

### Judgement calls (for the owner)

1. **`order-not-found` is FINAL**, although it is a 404 and the brief listed it as RETRYABLE
   ("order created by another request"). The id is `ord-<uuid>` minted by this service after the
   request, no order is deleted and no event creates one, so a client cannot hold an id that
   will exist later. Same criterion as inventory-storage's `reservation-not-found`. Flip to
   RETRYABLE costs one word and one extra read.
2. **State-derived refusals that cannot become false stay RETRYABLE** (`order-not-held`:
   `heldAtIntake` is immutable by convention; `order-already-released`: a released line never
   reverts but is on the unkeyed DELETE): unsure between FINAL and RETRYABLE, so RETRYABLE, as in
   inventory-storage's `held-unit-closed`.
3. **`line-ineligible-for-resolved-path` mixes causes** (state: classification and catalogue;
   request: a quantity above every path's limit): RETRYABLE; splitting it into its own problem
   types is a recorded follow-up, not part of this change. Same for `unknown-process-path` only
   if a future request-only cause is added.
4. **FINAL validation after a state-dependent check.** `buildDomainLines` validates a request's
   lines in order and resolves each path; a `422 non-positive-quantity` on line 2 is stored even
   though line 1's path resolution passed only because of the catalogue at that moment. The
   request is invalid whatever the catalogue says, so a retry is refused either way; only the
   slug it names could differ. Accepted.
5. `no-allocated-lines`, `promise-date-not-set`, `order-line-*` and `insufficient-stock` are
   mapped but not reachable from a keyed route today; they are classified so the unit test is
   total and a future route cannot answer an unclassified one.

### Follow-ups, not in this change

- Split `line-ineligible-for-resolved-path` into a request-only and a state-dependent type.
- A replay does not check the age of the row (the 24-hour window is enforced by the hourly purge
  alone, ADR 0032).
- Retire the unreachable mappings above if no route is expected to return them.

## Tests

`replay_class_test.go` (Docker-free): every problem type has a class (red before the field
existed: `undefined: allProblemTypes`; red with one entry blanked), no `problemInfo` literal
outside the catalogue and `register`, this table equals the catalogue, `statusFor` equals the
accessor, `writeProblem`/`writeError` mark the capture, the `storable` truth table.
`idempotency_replay_classes_template_integration_test.go`: the fleet template, every entry
through the real middleware and Postgres. `idempotency_retryable_integration_test.go`: a genuine
lost race and a refusal that depends on a later fact, through the real router and Postgres.
