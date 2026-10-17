---
id: 0043-release-confirmation-from-workunitcreated
slug: /adr/0043-release-confirmation-from-workunitcreated
title: "0043. Confirm an order-line release from WorkUnitCreated, and flag OrderReleaseUnconfirmed"
sidebar_label: "43. Release confirmation loop"
sidebar_position: 43
description: "ADR 0043 — closes gap OM-0005 (ADR 0005's missing release-confirmation loop). A new consumer of wes-work-planning's WorkUnitCreated (now carrying reference and line_no) confirms each released order line in a release_confirmations process-manager table; a timeout sweep inside the ADR 0032 housekeeping sweeper flags lines that never get confirmed with ONE analytics-only fact, OrderReleaseUnconfirmed. The outcome is a fact, never an action: nothing is cancelled, re-released or retried."
---

# 0043. Confirm an order-line release from WorkUnitCreated, and flag OrderReleaseUnconfirmed

## Status

**Accepted** (2026-10-17). Gap OM-0005. Amends the "no confirmation loop"
decision of [ADR 0005](./0005-choreographed-release-via-kafka.md) (v1
deliberately ships fire-and-forget) and builds on
[ADR 0034](./0034-raise-order-line-released-and-order-released.md)
(`OrderLineReleased` and `OrderReleased` as analytics-only facts) and on
[ADR 0032](./0032-housekeeping-sweeper-idempotency-keys-and-outbox.md) (the
housekeeping sweeper that now also runs the timeout). Companion to
wes-work-planning ADR 0041, which adds the two optional fields this record
consumes to `WorkUnitCreated`.

## Context

This service releases order lines (`allocateAndRelease`, and
`ReleaseHeldOrder` for held orders) by publishing `OrderAllocated` /
`OrderPartiallyAllocated`, whose `lines[]` carry `line_no`, `sku` and
`path_id`. It also raises the analytics-only facts `OrderLineReleased` and
`OrderReleased` (ADR 0034) with the frozen work-unit id
`usecases.WorkUnitID` = `{orderID}-line-{lineNo}`. It never learns whether
wes-work-planning admitted the work. ADR 0005 recorded that as a deliberate v1
choice: "v1 deliberately ships fire-and-forget, with NO release-confirmation
reply event from wes-work-planning ... a confirmation-loop pattern is a real,
documented v1 gap."

The operational cost is a silent failure. If the `OrderAllocated` message is
dead-lettered in wes-work-planning, or its consumer is down for hours, the
order is `Released` here, the funnel counts it released, and nothing ever
reaches the floor. Nothing in this service can tell.

wes-work-planning already holds the answer. Its consumer of `OrderAllocated`
enqueues one work unit per line (`WorkUnitId = {order_id}-line-{line_no}`,
`Reference = order_id`, `LineNo`) and raises `WorkUnitCreated` on
`warehouse.work-planning.events` (CloudEvents type
`com.warehouse.wes.work-planning.workunit.WorkUnitCreated`). That event is the
enqueue confirmation, but its `data` carried only `path_id` and
`work_unit_id`, so it could not be tied back to an order line without parsing
the work-unit id. Transfer, RELOCATE and legacy work share the same event and
the same topic.

## Decision

### 1. Wire shape (consumed)

wes-work-planning's `WorkUnitCreated.data` gains two OPTIONAL fields on the
integration stream only (its analytics stream is unchanged). The CloudEvents
type, the `subject` (the work unit id) and the `dataschema` v1 are unchanged.

| `data` field | Type | Meaning |
|---|---|---|
| `path_id` | string | existing, not read here |
| `work_unit_id` | string | existing, not read here |
| `reference` | string, optional | the work unit's originating reference; for order work, the order id |
| `line_no` | integer > 0, optional | the order line number; omitted when unknown |

This service decodes exactly those two fields. A `WorkUnitCreated` without
`reference` or without a positive `line_no`, or whose `(reference, line_no)` is
not a line this service opened a confirmation for (a transfer, RELOCATE or
legacy work unit, or an order released before this ADR), is **ignored and
counted**, never dead-lettered. A `data` that cannot be decoded at all (a
`line_no` that is a string, say) is malformed and goes to the DLQ.

### 2. The consumer

A new Kafka consumer of `warehouse.work-planning.events`
(`internal/adapters/inbound/kafka/release_confirmation_consumer.go`), copied
from the existing consumers' loop, not a new loop:

- It acts on the FULL type
  `com.warehouse.wes.work-planning.workunit.WorkUnitCreated` only. Every other
  type on the topic is committed past.
- Mode `RELEASE_CONFIRMATION_MODE=off|kafka`, default **`off`**: the feature
  ships dark. In `off` nothing changes: no rows are opened, no consumer runs,
  no sweep runs.
- Its own STABLE consumer group, `RELEASE_CONFIRMATION_CONSUMER_GROUP`, with no
  default in the binary (a locally run process must not join a live group). The
  chart renders `order-management-release-confirmation` when the mode is
  `kafka`.
- The CloudEvents `id` is the idempotency key, claimed in
  `release_confirmation_processed_events`. The claim and the state change run in
  ONE `UnitOfWork`, so a rollback un-claims the id and the redelivery is
  processed (the fleet's atomicity rule for consumers).
- Poison goes to `warehouse.work-planning.events.dlq`, named like every other
  consumer's DLQ (`<topic>.dlq`). `RequiredAcks` is `All` on every writer, the
  DLQ writer included. `Topic` is set on exactly one of writer and message (the
  DLQ writer pins `<topic>.dlq`, the dead-lettered message sets none; kafka-go
  refuses both). Shutdown is not poison: `ctx` is checked before dead-lettering
  and `Run` returns `nil` when cancelled.
- Boot validation (`cmd/order`): `kafka` mode needs `DATABASE_URL`,
  `KAFKA_BROKERS`, `EVENT_PUBLISHER=kafka` (without it nothing is published to
  wes-work-planning, so no confirmation could ever arrive) and a consumer group.
  A missing one fails boot with the variable named.

### 3. State: a process manager, not the Order aggregate

A new table `release_confirmations` (additive migration `0012`, down migration
included), one row per released line:

| Column | Meaning |
|---|---|
| `order_id`, `line_no` | primary key |
| `work_unit_id` | `usecases.WorkUnitID(order_id, line_no)` |
| `path_id` | the path the line was released onto |
| `released_at` | when this service released the line |
| `state` | `PENDING` \| `CONFIRMED` \| `UNCONFIRMED` |
| `confirmed_at`, `unconfirmed_at` | set on the matching transition |

The Order aggregate is untouched: the confirmation is knowledge about another
context's behaviour, not an invariant of the order, and a late or missing
confirmation must never change an order's status or block a transition. The
state machine is the domain type `releaseconfirmation.Confirmation`:

- A `PENDING` row is inserted in the SAME unit of work as the release
  transition, at the one place lines move `Allocated` -> `Released` (the spot
  `publishReleaseFacts` uses), only for the lines released in that pass. Orders
  released before the migration, or while the mode was `off`, get no row (no
  backfill; their `WorkUnitCreated` is ignored).
- `WorkUnitCreated` for a `(reference, line_no)` with a `PENDING` or
  `UNCONFIRMED` row sets `CONFIRMED` (`confirmed_at`). It is idempotent: a
  second one is a no-op. A late confirmation after `UNCONFIRMED` is allowed and
  clears the problem.
- A `PENDING` row becomes `UNCONFIRMED` exactly once (section 4). Only
  `PENDING` can become `UNCONFIRMED`.

### 4. Timeout sweep

`PENDING` rows with `released_at` older than `RELEASE_CONFIRMATION_TIMEOUT`
become `UNCONFIRMED`, inside the ADR 0032 housekeeping sweeper (`postgres.Sweeper`
runs a hook each pass; there is no second scheduler). Each batch claims its rows
with `FOR UPDATE SKIP LOCKED`, flips them and writes the outbox rows in one
transaction, so several replicas flag a line exactly once. A confirmation racing
the sweep serialises on the row lock: whichever commits first wins, and a
confirmation that loses becomes a late confirmation.

| Env var | Default | Meaning |
|---|---|---|
| `RELEASE_CONFIRMATION_TIMEOUT` | `15m` | `PENDING` older than this becomes `UNCONFIRMED`. `0` disables the sweep. |
| `RELEASE_CONFIRMATION_RETENTION` | `168h` (7d) | `CONFIRMED` rows older than this (by `confirmed_at`) are deleted. `0` keeps them. |

Chart values `releaseConfirmation.timeout` and `releaseConfirmation.retention`
reach the process through the chart ConfigMap that the api Deployment mounts
with `envFrom` (fleet rule R10).

**Why 15 minutes, and why it is an operational parameter.** A healthy loop
confirms within seconds. This service's outbox relay (1s interval), and in
wes-work-planning its consumer's in-process retry (3 attempts, backoff capped at
2s), its DLQ publish (at most 40 waits of 250ms for an auto-created topic) and
its own outbox relay (1s interval) are each seconds-scale. What the timeout
must outlast is not the happy path but the ordinary degraded one: a
wes-work-planning pod restart or rolling deploy, a consumer-group rebalance, a
broker blip and the consumer's capped recovery backoff (up to 30s per try).
Those last seconds to a few minutes. 15 minutes clears them by a wide margin,
so a deploy does not flag hundreds of lines, yet it still raises the problem
inside a shift, while a
person can act. It is not a business rule: no promise, status or SLA depends on
it, nothing is derived from it, and changing it needs no domain change. `0`
turns the sweep off while keeping confirmations.

The sweep rides the ADR 0032 interval, so a line is flagged on the first pass
after its timeout: with the 1h `HOUSEKEEPING_INTERVAL` default that is up to
1h15m. Deployments that enable the loop should set `HOUSEKEEPING_INTERVAL` to
about `5m`; the other sweeps are cheap batched deletes.

`CONFIRMED` rows are pruned after seven days, the same window as published
outbox rows (`OUTBOX_RETENTION`, ADR 0032). `PENDING` and `UNCONFIRMED` rows are
never pruned: they are open problems, and an `UNCONFIRMED` row is still
cleared by a late confirmation.

### 5. The outcome of a timeout is a fact, never an action

Flagging a line raises ONE new analytics-only fact, `OrderReleaseUnconfirmed`,
in the same transaction as the state change (the transactional outbox, ADR
0022). Same precedent as ADR 0034: structured CloudEvents 1.0 on
`warehouse.order-management.analytics`, built by
`internal/adapters/kafka/cloudevents` from the event-name constant:

- `type`: `com.warehouse.wes.order-management.order.OrderReleaseUnconfirmed`
  (same segments as `OrderLineReleased`), `subject` and Kafka key = the order
  id, `dataschema`
  `urn:warehouse:order-management:analytics:OrderReleaseUnconfirmed:v1`,
  `time` = when the line was flagged.
- `data`: `order_id`, `line_no`, `path_id`, `work_unit_id`, `released_at`.

It also logs at ERROR (`release unconfirmed`, with the order, line, work unit
and `released_at`) and increments the counter
`order.release_confirmation.unconfirmed`. Every handled `WorkUnitCreated`
increments `order.release_confirmation.events` with an `outcome` attribute
(`confirmed`, `late_confirmed`, `duplicate`, `ignored_no_reference`,
`ignored_unknown_line`).

It does NOT cancel, re-release or retry anything. That is the conservative and
reversible choice: automatic compensation (revoking reservations, re-publishing
`OrderAllocated`, cancelling the order) would act on a signal that has a benign
explanation (a slow consumer, a lost event that is about to be replayed) and
would contradict ADR 0004's cancellation boundary. It needs its own ADR. The
integration topic `warehouse.order-management.events` keeps its contract: the
new fact is analytics-only, and the in-repo projector ignores the unknown type.

## Consequences

**Easier**

- A released line that wes-work-planning never admitted is visible: an ERROR
  log, a counter, an analytics fact and a SQL query
  (`state = 'UNCONFIRMED'`), instead of silence.
- The loop is consumer-side and additive. wes-work-planning changes only by
  adding two optional fields; nothing else in either contract moves.
- It ships dark and is reversible: switch the mode to `off` and the service
  behaves as before. The table stays in place and inert.

**Harder**

- A new consumer, a new table, a new analytics type and two new config
  parameters to operate. The runbook gains the procedure "an order line is
  UNCONFIRMED".
- Coverage starts at enablement. Lines released earlier are not backfilled and
  their `WorkUnitCreated` is ignored. Enabling a new consumer group replays
  the whole `warehouse.work-planning.events` topic from its first offset; the
  replay is harmless (nothing matches) but takes minutes before the group is
  caught up.
- `UNCONFIRMED` is a signal, not proof. A line can be flagged and still be
  admitted a minute later; the late confirmation clears the row, but the
  analytics fact already went out and is not retracted.
- Until wes-work-planning ships ADR 0041, no `WorkUnitCreated` carries
  `reference`/`line_no`, every one is ignored, and every line is flagged after
  the timeout. Do not enable the mode before that change is deployed.

## Alternatives considered

- **Keep the state on the Order aggregate** (a per-line `confirmed` flag).
  Rejected: it would make a sibling context's behaviour part of the order's
  invariants and force an optimistic-concurrency write (ADR 0024) on the order
  for every confirmation, racing with cancellation and re-promise.
- **A dedicated reply event from wes-work-planning.** Rejected: `WorkUnitCreated`
  already is the confirmation; a new event is a second contract for the same
  fact.
- **Correlate on `work_unit_id` alone** (parse `{order}-line-{n}` with
  `ParseWorkUnitID`). Rejected: it works for the frozen order format, but it
  makes the consumer guess whether any work unit is order work and would treat a
  transfer id that happens to match as an order line. The explicit
  `reference` and `line_no` say what the unit is for.
- **Poll wes-work-planning over REST.** Rejected: sibling-context data arrives
  by Kafka, never by a new REST call (ADR 0005, the fleet's context-boundary
  rule).
- **Compensate automatically on timeout.** Rejected for now, see section 5.
- **A second scheduler for the timeout.** Rejected: ADR 0032 already runs a
  loop, with graceful shutdown and replica-safe batches; the timeout is one more
  hook on it.

## Verification

- Domain (`internal/domain/releaseconfirmation/confirmation_test.go`): the
  state machine, `PENDING` -> `CONFIRMED`, `PENDING` -> `UNCONFIRMED` once,
  late confirmation, duplicate confirmation, overdue boundary.
- Use cases (`internal/application/usecases/release_confirmation_test.go`): the
  `PENDING` row is written in the same unit of work as the release (a failure
  after it rolls it back), only for lines released in that pass; confirmation of
  an unknown line and of an event missing `reference`/`line_no` is ignored and
  counted without opening a transaction; the sweep flags overdue rows exactly
  once and `0` disables it.
- Decoder (`internal/adapters/inbound/kafka/release_confirmation_consumer_test.go`):
  fixture JSON with and without the new fields, malformed data to the DLQ,
  unknown types committed past, shutdown never dead-letters.
- Adapters (Testcontainers Postgres): the repository and the sweeper hook,
  including two concurrent sweepers flagging a row once.
- Kafka (Testcontainers):
  `internal/adapters/inbound/kafka/release_confirmation_consumer_integration_test.go`
  produces a `WorkUnitCreated` CloudEvent and asserts `CONFIRMED`, produces a
  malformed one and asserts the DLQ, and cancels `Run` mid-handle and asserts it
  returns `nil` with the DLQ empty.
- Contract and chart: `apis/asyncapi.yaml` declares the consumed
  `WorkUnitCreated` and the analytics `OrderReleaseUnconfirmed`; the Helm render
  test asserts the new values reach the ConfigMap.
