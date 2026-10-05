---
id: 0022-transactional-outbox
slug: /adr/0022-transactional-outbox
title: 22. Transactional outbox for order-management's dual-topic event publishing
sidebar_label: 22. Transactional outbox
description: "ADR 0022 — order-management stops publishing to Kafka from inside its use cases and now commits every domain event to an outbox table, once per (event x topic), in the same transaction as the Order aggregate write, with an in-process relay draining rows to both the integration and analytics topics."
---

# 22. Transactional outbox for order-management's dual-topic event publishing

## Status

Accepted — implemented in the same change that introduced this record.

## Context

Every publishing use case in this service (`ReceiveOrder`, `allocateAndRelease`
— shared by `ReleaseHeldOrder`/`RetryAllocation`, `CancelOrder`, `RepromiseOrder`)
follows the same two-independent-writes shape:

```go
if err := uc.Orders.Save(ctx, o); err != nil { return err }
if err := uc.Events.Publish(ctx, evt); err != nil { return err }
```

A crash, a broker timeout, or a pod eviction between the two leaves an
`Order` row that **no downstream consumer will ever learn about**:
`wes-work-planning` builds its release/allocation picture exclusively
from `warehouse.order-management.events`, and the analytics data product
(ADR-0006) builds its whole report from `warehouse.order-management.analytics`.
Unlike `process-path-management` (ADR 0003, the sibling service this
rollout mirrors), this divergence risk is doubled here: `FanOutPublisher`
already fans every event out to **two** independent Kafka writes in
sequence (`kafka.Publisher` then `kafka.AnalyticsPublisher`), so a crash
between them can leave the integration topic and the analytics topic
disagreeing with each other, on top of either disagreeing with the store.

This repo also already has a **dead-end** first attempt at durability:
`postgres.EventPublisher` appends every event to an `events` table, but
its own doc comment says the plan for a relay was "deferred in v1" — no
code anywhere drains that table. It is strictly worse than the direct
Kafka publisher it sits behind: it makes state changes durable, but
never actually delivers the event it was supposed to guarantee, and an
operator reading `events` would reasonably but wrongly assume it was
being consumed. This ADR retires it.

`process-path-management` ADR 0003 already solved the identical
single-transaction problem for a **single**-topic publisher; this ADR is
that same pattern, generalized to `order-management`'s two-topic
fan-out and its **singular** `ports.EventPublisher.Publish(ctx, event)`
signature (this service's port is intentionally NOT widened to a
variadic/batch shape — see Decision §2).

## Decision

Adopt the transactional outbox pattern, structured so exactly this
service's existing dual-topic fan-out survives unchanged in observable
behaviour, just deferred past the transaction boundary.

### 1. `outbox_events` table (migration `0007_outbox`)

Retires `events` (migration `0001_init`) outright — it held no
undelivered history worth preserving, since nothing ever drained it.

Each row is one already-encoded, single-topic Kafka message:

| column         | purpose                                                    |
|----------------|-------------------------------------------------------------|
| `topic`        | which topic this row belongs on — the relay's `Sink` has no fixed topic of its own, it routes purely off this column |
| `event_type`   | for observability/debugging queries                        |
| `key`          | the Kafka partition key (nullable — the integration publisher does not key)|
| `value`        | the JSON envelope, byte-for-byte what the topic will carry |
| `headers`      | JSONB array of the W3C trace-context headers captured at encode time |
| `published_at` | NULL until the relay confirms the send                     |
| `attempts`, `last_error` | relay bookkeeping                                 |

One event enqueues **one row per encoder that has a wire form for it** —
today that means up to two rows (`kafka.Publisher`'s integration encoding
and `kafka.AnalyticsPublisher`'s analytics encoding), since
`OrderReceived`/`OrderCancelled`/etc. are analytics-only while
`OrderAllocated`/`OrderPartiallyAllocated`/`OrderRepromised` reach both.
A partial index over `published_at IS NULL` keeps the relay's scan tiny.

### 2. `ports.UnitOfWork` — new driven port, `ports.EventPublisher` unchanged

```go
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}
```

`ports.EventPublisher.Publish(ctx, event)` (singular) is **not** widened
to a variadic/batch shape. `process-path-management`'s own outbox rollout
generalized its encoder to accept a variadic event list; this service
deliberately keeps `Publish` exactly as every existing use case already
calls it, one event at a time, and instead pushes the "one event, many
topics" fan-out down into `postgres.OutboxPublisher`, which loops over
its configured encoders on a single `Publish` call. This keeps the port
contract stable for every existing caller and test double.

The port is optional (`nil` = "run `fn` directly"), which is exactly the
in-memory / log-publisher dev configuration — `atomically()` in the
`usecases` package is the one place every publishing use case goes
through, so a nil `UnitOfWork` is handled once, not five times.

### 3. `postgres.UnitOfWork` and the querier/context threading

Opens a `pgx.Tx`, binds it to the context, commits or rolls back around
`fn`. `OrderRepo.Save` — which historically opened its **own** internal
transaction for its three-table write (`orders`, `order_lines`,
`order_promise_groups`) — now calls `beginOrJoin`, which joins the
already-bound transaction when one exists on the context instead of
opening a second, invisible-to-each-other transaction. `FindByID`,
`RepromiseProcessedEventsRepo.MarkProcessed`, and `OutboxPublisher.Publish`
all resolve their querier via `querierFrom(ctx, pool)`: the pool when
standalone, the bound transaction when inside a `UnitOfWork` scope.
Nested `Execute` calls join the outer transaction rather than opening a
second one (needed because `ReceiveOrder` calls `allocateAndRelease`,
which itself wraps its own work in `atomically`).

### 4. `kafka` package: split Encode from Send

Both `Publisher` and `AnalyticsPublisher` gain an `Encode(ctx, event) (Encoded,
bool, error)` method — the same envelope-building logic `Publish` already
had, minus the broker write. `Encoded{Topic, EventType, Key, Value, Headers}`
is the unit `postgres.OutboxPublisher` stores and `postgres.OutboxRelay`
later hands to a `Sink`, so the direct-publish and outbox paths can never
disagree about wire format. `Publish` itself becomes `Encode` + the
existing span/write logic, unchanged in observable behaviour — it is
still what a Kafka-direct, no-Postgres dev run uses.

### 5. `postgres.OutboxPublisher` implements `ports.EventPublisher`

`Publish(ctx, event)` calls `Encode` on every configured encoder in
order (mirroring `FanOutPublisher`'s existing publisher order:
integration first, then analytics) and `INSERT`s one row per encoder
that returned `ok=true`. It never touches the broker.

### 6. `postgres.OutboxRelay` and `kafka.RelaySink`

Runs as a goroutine inside the `order` process, next to the HTTP server
and the `RepromiseOrder` Kafka consumer. Each pass claims up to 100
pending rows with `SELECT … FOR UPDATE SKIP LOCKED ORDER BY id`, sends
each to a `Sink` (production: `kafka.RelaySink`, a `*kafka-go.Writer`
with no fixed topic, routing per message off `Encoded.Topic`), and marks
`published_at`. On a send failure it stops the pass at that row
(preserving per-topic ordering — a later event on the same topic must
never overtake a failed earlier one), records the error, commits what
was already sent, and retries next tick. Sleep between empty passes is
`OUTBOX_RELAY_INTERVAL` (default `1s`).

### 7. Composition root (`cmd/order/main.go`)

| `DATABASE_URL` | `EVENT_PUBLISHER` | Publisher wired          | Relay |
|----------------|--------------------|--------------------------|-------|
| unset          | `log` (default)    | log                      | none  |
| unset          | `kafka`             | direct Kafka fan-out (no outbox) | none |
| set            | `log`               | log                      | none  |
| set            | `kafka`             | **outbox (both topics)** | **yes** |

The cluster runs the last row. Graceful shutdown (ADR-0025 §8) flips
`/readyz` to not-ready, waits `SHUTDOWN_DRAIN_DELAY`, stops the HTTP
server and the `RepromiseOrder` consumer — the only two outbox writers —
and only then cancels the relay and waits for its final in-flight pass,
so an event committed by a request or consumer message that completed a
moment before SIGTERM is not stranded until the next pod boots.

### Delivery semantics (what consumers may now rely on)

- **Atomicity**: an `Order` change and every event it raises — across
  BOTH topics — commit together or not at all. Verified by an
  integration test that forces the outbox insert to fail (a broken
  `Encoder`) and asserts the `orders` row is absent.
- **At-least-once**: a crash between a successful `Send` and the row's
  `UPDATE` republishes that row on the next pass. `wes-work-planning`
  and the analytics consumer already tolerate this (idempotent
  overwrite / their own dedupe).
- **Per-topic ordering**: preserved — rows are drained in insertion
  order within one relay pass, and a failed row blocks everything
  behind it on the SAME topic rather than being skipped. Two different
  topics for the SAME event can still be sent slightly out of step with
  each other if one topic's Sink fails and the other's does not; this
  is unchanged from the pre-outbox `FanOutPublisher`'s own fail-fast
  ordering guarantee, which only ever promised in-process publisher
  order, not cross-topic atomicity of delivery (only of the DB commit).
- **Latency**: events reach both topics within one relay interval (≤1s)
  of the HTTP response, versus "before the response" under the old
  direct publish.

## Alternatives considered

- **Widen `ports.EventPublisher.Publish` to variadic/batch, mirroring
  `process-path-management`'s later fan-out ADR.** Rejected per the
  task's explicit constraint and Decision §2's reasoning: every existing
  caller and test fake in this repo already depends on the singular
  shape, and the two-topic fan-out is fully expressible as "two encoders
  on one `OutboxPublisher`" without touching the port.
- **One `outbox_events` row per event (not per event×topic), with the
  relay fanning out to both topics at send time.** Rejected: it would
  require `RelaySink` (or the relay itself) to carry topic-routing logic
  that belongs to the Kafka adapters (`Encode`), and would make a
  partial send (integration succeeds, analytics fails) leave a single
  row in an ambiguous "half published" state with no natural column to
  express it. One row per topic keeps `published_at` a clean boolean
  per destination.
- **Keep the old `postgres.EventPublisher`/`events` table and bolt a
  relay onto it later.** Rejected per the task brief: it is a dead end
  with no relay today, and its schema (one row per event, no topic
  column) cannot express "this event needs to reach two topics
  independently" without the same redesign this ADR already does.

## Consequences

**Positive**
- The two-writes-two-systems race this service always had — and its
  doubled version across two Kafka topics — cannot happen any more:
  there is no code path that persists an `Order` without also
  persisting every event it raised, for every topic that event belongs
  on.
- No broker dependency on the request path: `POST /orders` (and every
  other write endpoint) succeeds when Kafka is down; events publish once
  the relay catches up.
- The dead-end `postgres.EventPublisher`/`events` table — durable but
  never delivered — is gone; there is exactly one Postgres-backed event
  path now, and it actually reaches Kafka.
- `ports.EventPublisher` stays exactly as every existing caller,
  fake, and test in this repo already uses it.

**Negative / accepted**
- One more table, one more goroutine, one more failure mode (the relay)
  to observe. `outbox_events.attempts`/`last_error` are queryable for
  operators; the relay logs every failed pass at ERROR with the row id,
  topic, and broker error.
- Events are no longer synchronous with the HTTP response — documented
  above, acceptable for this domain (same trade-off ADR 0003 already
  accepted for the sibling service).
- `allocateAndRelease`'s outbound `Inventory.Reserve` HTTP call now runs
  INSIDE the same Postgres transaction as the eventual `Save`/`Publish`
  for a successful allocation pass, holding a DB connection for the
  duration of that call. Accepted trade-off in exchange for the
  stronger atomicity guarantee — this service's inventory reservation
  calls are already synchronous and bounded by client-side timeouts
  the way `process-path-management`'s outbox rollout never had to
  consider (that service has no outbound synchronous call inside its
  transactional use cases).
