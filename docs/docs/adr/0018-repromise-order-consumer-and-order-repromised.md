---
id: 0018-repromise-order-consumer-and-order-repromised
slug: /adr/0018-repromise-order-consumer-and-order-repromised
title: 18. RepromiseOrder consumer and OrderRepromised — closing ADR 0014's feedback loop
sidebar_label: 18. RepromiseOrder consumer and OrderRepromised
description: "ADR 0018 — order-management's half of ADR 0014 §5's feedback loop: a new inbound Kafka consumer on warehouse.fulfillment.events reacting to fulfillment-execution's real shipped TaskCPTMissed/PackageManifested events, parsing the WorkUnitId-shaped order_ref back into (OrderId, LineNo) via this repo's own frozen WorkUnitID formula, recomputing the affected PromiseGroup via the existing PromisePolicy.PromiseGroups, and publishing OrderRepromised when the promise moves. Closes ADR 0014's entire rollout (Phases 0-5)."
---

# 18. RepromiseOrder consumer and OrderRepromised — closing ADR 0014's feedback loop

## Status

> **Superseded by ADR-0030** (the flat inbound envelope shown here; the consumer now dispatches on the full CloudEvents types and dedupes on the CloudEvents `id`). Every Kafka message is now a
> CloudEvents 1.0 event — see
> [ADR 0030](./0030-cloudevents-mandatory-event-envelope.md). The rest of
> this record stands.

Accepted — implemented in the same change that introduces this record.
This is the final piece of [ADR 0014](/docs/adr/0014-promise-derived-from-fulfillment-capability)'s
entire rollout: Phase 0 (the ADR pair with process-path-management's ADR
0010) through Phase 5 (this record) are now all shipped. Companion to
fulfillment-execution's own ADR 0025 (`TaskCPTMissed` sweep and
`PackageManifested` on the outbox), which ADR 0014 §5 explicitly deferred
to "fulfillment-execution's own ADR" and which has since been accepted,
implemented, and merged on that repo's `develop`. That ADR's own text
says as much: "order-management's `RepromiseOrder` consumer is out of
scope here entirely — a separate, later task." This is that task.

## Context

ADR 0014 §5 named the shape of the feedback loop but explicitly deferred
building it until fulfillment-execution's companion events existed on
the wire. They now do — real, shipped, verified via `git show
origin/develop` on that repo:

Envelope for `TaskCPTMissed` (a task still open past its CPT):

```json
{"event_id": "...", "event_type": "TaskCPTMissed", "occurred_at": "...", "source": "fulfillment-execution",
 "data": {"task_id": "...", "order_ref": "...", "task_type": "PICK|PACK|SLAM|REBIN", "cpt": "2026-.. RFC3339"}}
```

Envelope for `PackageManifested` (a SLAM pass):

```json
{"event_id": "...", "event_type": "PackageManifested", "occurred_at": "...", "source": "fulfillment-execution",
 "data": {"package_id": "...", "order_ref": "..."}}
```

Both are published on `warehouse.fulfillment.events` — the SAME
shared/fan-out topic labor-performance already consumes for
`TaskCompleted`, per this fleet's established one-topic-many-consumers
convention on that topic.

**The non-obvious finding this ADR is built around**: `order_ref` on
both events is fulfillment-execution's `Task.OrderRef()` /
`Package.OrderRef()`, populated at task-creation time from
`WorkReleasedData.WorkUnitId` (see that repo's
`internal/adapters/inbound/kafka/consumer.go`:
`orderRef := shared.OrderRef(env.Data.WorkUnitId)`). `WorkUnitId` is
wes-work-planning's per-LINE identifier, itself derived from **this
repo's own frozen formula**, `usecases.WorkUnitID`
(`internal/application/usecases/allocation.go`):

```go
func WorkUnitID(orderID shared.OrderId, lineNo int) string {
	return fmt.Sprintf("%s-line-%d", orderID.String(), lineNo)
}
```

So `order_ref` on the wire looks like
`"ord-7c9e6679-...-line-2"` — NOT a bare `OrderId`. A naive consumer
treating `order_ref` as an `OrderId` would silently corrupt every lookup
(`FindByID("ord-...-line-2")` never matches a real order). The consumer
must parse it back apart to recover both the `OrderId` and the specific
`lineNo` the missed task/manifested package belongs to.

Both facts are already scoped to a single LINE. Per ADR 0017 (already
shipped), a line belongs to exactly one `PromiseGroup` — the shipment
group it was promised as part of. ADR 0014 §5's "recompute the promise
for the affected shipment group" is therefore precisely: find the
`PromiseGroup` containing the affected `lineNo`, and recompute it.

## Decision

### 1. `usecases.ParseWorkUnitID` — the reverse of `WorkUnitID`, never a panic

```go
func ParseWorkUnitID(workUnitID string) (orderID shared.OrderId, lineNo int, ok bool)
```

It splits on the **last** occurrence of the `"-line-"` marker, not the
first. This repo's real `OrderId` values (`"ord-" + uuid.NewString()`,
see `OrderRepo.NextID`) are hex digits and hyphens only and can never
contain the literal substring `"line"` — so first-vs-last split is not
observable against this repo's real data today. Splitting on the last
occurrence is still the deliberately safer choice: it degrades correctly
even against a hypothetical future `OrderId` shape that legitimately
contains `"-line-"` as a substring, where a first-occurrence split would
silently truncate the order id and misattribute the line number. `ok` is
`false` — never a panic — for anything that isn't a positive integer
trailing an order id via that exact marker (no marker, empty order-id
portion, non-numeric or non-positive line number, trailing garbage).
Round-trip and malformed-input tests pin this exactly (see
`allocation_test.go`).

### 2. `RepromiseOrder` — reuse `PromisePolicy.PromiseGroups`, don't reinvent promise math

```go
type RepromiseOrder struct {
	Orders    ports.OrderRepo
	Promise   order.PromisePolicy
	Events    ports.EventPublisher
	Clock     ports.Clock
	Processed ports.RepromiseProcessedEvents
	Logger    *slog.Logger
}
```

`Execute` does exactly what ADR 0014 §5 describes, using ADR 0017's
already-shipped recompute unchanged:

1. Idempotency check via `Processed.MarkProcessed` — no-op if already
   evaluated.
2. Load the `Order`.
3. Find the line's **current** `PromiseGroup` (from `o.PromiseGroups()`).
4. Call `PromisePolicy.PromiseGroups(now, o)` — the SAME domain service
   ADR 0017 introduced for per-group promising, called again fresh, from
   whatever capability/capacity inputs are current now.
5. Find the line's **fresh** `PromiseGroup` in that new result.
6. Compare the two `Promise` values (`CptId`/`Basis`/`CutoffAt` — any one
   differing counts as "moved", not just `CptId` alone, so a
   same-CPT-identity cutoff shift or a basis flip is never missed). If
   moved: `o.SetPromiseGroups(freshGroups)`, `Orders.Save`,
   `Events.Publish(NewOrderRepromised(...))`.
7. Mark processed regardless of whether the promise moved — idempotency
   covers "already evaluated this event", not "already moved".

No new promise-computation logic was written. This is deliberate,
per this repo's own established pattern of extending rather than
replacing: `PromisePolicy.PromiseGroups` already IS "recompute the
promise for the affected shipment group from the current
capability/capacity inputs" — RepromiseOrder's only real job is finding
which group to compare before and after.

**Fail-soft, not fail-hard, for every "this signal doesn't map to a
live, promotable order line" condition**: order not found, `lineNo` not
present in any current group (a stale/already-cancelled line, or an
order never allocated), or the fresh recompute has nothing to promise at
all (`PromisePolicy.PromiseGroups`' own `ok=false`). Each of these is
logged and treated as a no-op — never an error returned to the Kafka
consumer — mirroring this fleet's convention for a soft reconciliation
input (the same permissive spirit as a SKU-not-found lookup elsewhere in
the fleet, not a hard business-rule violation). Only a genuine
infrastructure failure (`Processed`/`Orders`/`Events` erroring) is
returned.

`Orders.Save` then `Events.Publish` run sequentially, matching this
repo's OWN existing convention (see `allocation.go`'s
`allocateAndRelease`) — no transactional wrapper was invented.
**This repo has no `UnitOfWork`/transactional-outbox port at all**
(unlike fulfillment-execution/labor-performance), and introducing one
here would be over-engineering relative to the fleet's stated intent for
this repo.

### 3. Idempotency key: `event_id` alone, not the full `(orderId, sourceEventId)` composite

ADR 0014 §5's own words: `RepromiseOrder` is idempotent on
`(orderId, sourceEventId)`. This ADR implements it as **`event_id`
alone**. The simplification is deliberate and documented, not a silent
substitution: `event_id` is already globally unique per message — every
publisher in this fleet mints it with `uuid.NewString()` — so scoping
the check by `orderId` too is redundant. Two different orders can never
coincidentally share the same `event_id`, and the SAME `event_id` always
refers to the SAME order. The composite key buys nothing a UUID
collision wouldn't already rule out, at the cost of a wider index. Either
choice is defensible; this is the simpler one, chosen and stated
honestly (see `ports.RepromiseProcessedEvents`' doc comment).

A NEW, OLTP-side port was added for this: `ports.RepromiseProcessedEvents`.
This repo's only existing `ProcessedEvents`-shaped interface
(`internal/adapters/inbound/kafka/analytics_consumer.go`) is declared
**local to that file** specifically so the analytics side owns its own
port and the OLTP application layer stays untouched — its own doc
comment says so. `RepromiseOrder` is OLTP-side (it mutates the real
`Order` aggregate and publishes a real integration event), so it needed
its own port rather than reusing that one.

Postgres-backed implementation: new migration `0004_repromise_processed_events`,
a `(event_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ)` table —
mirroring labor-performance's own `processed_events` migration shape
exactly, the proven precedent for this exact table shape in this fleet
— named `repromise_processed_events`, not `processed_events`, so a
future OLTP-side consumer added to this service never collides with
this one's table by accident.

**Why idempotency matters here specifically**: fulfillment-execution's
`SweepCPTMisses` (its ADR 0025 §4) re-fires `TaskCPTMissed` on **every
sweep pass** for as long as a task stays overdue — a deliberate design
choice on that side, explicitly justified by citing this ADR's own
idempotency requirement. Without a real idempotency gate here, every
sweep tick would re-evaluate (and, worse, the fail-soft "already moved"
outcome notwithstanding, could theoretically re-publish) the same fact.
An integration test in this repo
(`TestHandleFulfillmentEvent_MimicsFulfillmentExecutionSweepReemission`)
pins exactly this: two DIFFERENT `event_id`s for the same overdue task
are both evaluated (never deduped away, since the ids differ), but only
the first actually moves the promise — the second sees no further
movement and publishes nothing.

### 4. `OrderRepromised` — the fleet's "your delivery is delayed" trigger

```go
type OrderRepromised struct {
	base
	OrderID  OrderId
	CptIdOld string // may be empty (LeadTime-basis previous promise had no CPT identity)
	CptIdNew string // may be empty (LeadTime-basis new promise)
	Reason   string // "TaskCPTMissed" | "PackageManifested" — verbatim fulfillment-execution event_type
}
```

`Reason` carries the triggering fulfillment-execution `event_type`
verbatim — `"TaskCPTMissed"` or `"PackageManifested"` — rather than a
paraphrased sentence, so a downstream reader can distinguish "still
open past CPT" from "a SLAM pass revealed a new capacity/eligibility
picture" without a second lookup. `CptIdOld`/`CptIdNew` may independently
be empty: a `LeadTime`-basis promise (either side) has no CPT departure
identity at all, only a computed cutoff instant — this mirrors
`order.Promise`'s own existing `CptId` semantics exactly (see
`promise_basis.go`'s doc comment) rather than inventing a new
empty-vs-nil convention.

Published on `warehouse.order-management.events` — the SAME existing
integration topic `OrderAllocated`/`OrderPartiallyAllocated` already
use, additively (a new `case` in `internal/adapters/outbound/kafka/publisher.go`'s
type switch, `cpt_id_old`/`cpt_id_new` both `omitempty`). Nothing about
the existing wire shape changed; a consumer reading only
`OrderAllocated` sees zero behavior change.

### 5. The inbound consumer — stable shared group, NOT per-process-unique

`internal/adapters/inbound/kafka/repromise_consumer.go` decodes the
fleet envelope, filters strictly for `TaskCPTMissed`/`PackageManifested`
(every other event type on this shared topic — including anything
fulfillment-execution someday adds — is silently skipped, mirroring
labor-performance's own consumer's convention on this exact topic),
parses `order_ref` via `ParseWorkUnitID`, and drives `RepromiseOrder.Execute`.

**Consumer group**: `order-management-repromise`, a fixed, shared
constant — deliberately NOT a per-process-unique group. This distinction
matters and is easy to get backwards: this repo's OTHER three Kafka
consumers (`kafkacatalog`, `kafkacptschedule`, `kafkapathcapacity`) build
a full in-memory read-model cache by replaying their topic from
`FirstOffset` on every start, and per-process-unique groups are a
correctness REQUIREMENT for that pattern (a shared group there lets a
new process resume from a prior process's committed offset and report
itself ready with an empty cache, having never actually replayed
anything — a real, previously-hit bug class in this fleet). This
consumer is a DIFFERENT shape entirely: a normal at-least-once "process
each new message once, commit as you go" consumer, structurally
identical to labor-performance's own `TaskCompleted` consumer on this
same topic. Using a per-process-unique group here would be the wrong
fix applied to the wrong pattern: every restart would replay the ENTIRE
topic history from the beginning, re-driving `RepromiseOrder` for years
of already-handled messages. `RepromiseOrder`'s own event_id idempotency
gate makes that merely wasteful rather than corrupting, but it is still
the wrong behavior — a shared, stable group is correct here specifically
because ordinary Kafka consumer-group semantics (resume from the
group's last committed offset) are exactly what this pattern wants.

Malformed/unparseable messages (bad JSON, an `order_ref` that doesn't
parse as a `WorkUnitId`) are logged and committed — never redelivered
forever, never a crash — mirroring every other Kafka consumer in this
fleet's malformed-message handling. Only a genuine infrastructure
failure from `RepromiseOrder.Execute` itself aborts the consume loop,
consistent with `RepromiseOrder`'s own fail-soft/fail-hard split.

Wired in `cmd/order/main.go` gated on `KAFKA_BROKERS` alone (independent
of `PATH_CATALOGUE_SOURCE`/`EVENT_PUBLISHER`), the same
`KAFKA_BROKERS`-gated-conditional-construction-plus-goroutine-`Run`-loop
convention this repo already uses for its other Kafka consumers.

## Consequences

**Easier**

- ADR 0014's entire rollout — Phases 0 through 5 — is now complete. A
  missed CPT or a SLAM pass genuinely reaches the order that is waiting
  on it, and the promise it carries is recomputed from the SAME real
  capability/capacity inputs `PromisePolicy` already uses everywhere
  else, not a bespoke recompute.
- No new promise-computation logic exists anywhere in this codebase.
  `PromisePolicy.PromiseGroups` is the ONE place that logic lives,
  reused unchanged by both the intake path (`allocateAndRelease`) and
  this feedback-loop path.
- The idempotency simplification (event_id-only) is honestly documented
  rather than silently chosen, and is provably safe given every
  publisher's UUID-based `event_id` minting.

**Harder**

- `RepromiseOrder`'s fail-soft posture means a genuinely broken
  `order_ref`/parsing bug on fulfillment-execution's side would be
  silently absorbed here (logged, never surfaced as an error) rather
  than loudly failing. This is the deliberate, stated trade for a soft
  reconciliation input — the alternative (treating every unmatched line
  as an error) would make this consumer fragile against completely
  routine conditions (a cancelled line, an order that finished
  shipping before its sweep-reported task caught up).
- `OrderRepromised` is a trigger, not a notification — nothing in this
  fleet contacts a customer. A future notification context would
  subscribe to this event, not be built here.
- This closes the LAST open step in ADR 0014's rollout. Phase 6
  (KPIs/MCP tools/e2e-tests scenarios for the promise feedback loop) is
  explicitly deferred to a separate, later task — not started here.

## Alternatives considered

- **Composite `(orderId, sourceEventId)` idempotency key**, matching ADR
  0014 §5's literal words. Rejected in favor of `event_id`-alone: see
  §3 above — the composite key is provably redundant given globally
  unique `event_id`s, and the simpler key is easier to reason about and
  index.
- **A per-process-unique consumer group**, mirroring this repo's other
  three Kafka consumers. Rejected: those three build a full-replay
  local cache, a genuinely different correctness shape from this
  normal at-least-once "process and commit" consumer. Applying that
  pattern here would replay years of history on every restart — see §5.
- **New promise-recompute logic scoped specifically to a
  repromise event**, rather than reusing `PromisePolicy.PromiseGroups`.
  Rejected: `PromiseGroups` already computes exactly what ADR 0014 §5
  asks for ("recompute the promise for the affected shipment group from
  the current capability/capacity inputs"); a second implementation
  would only risk drifting from the intake path's behavior.
- **Reusing the analytics consumer's local `ProcessedEvents` interface**
  instead of a new OLTP-side port. Rejected: that interface is
  deliberately scoped to the analytics side by its own doc comment —
  reusing it would blur a boundary this repo already drew on purpose.

## Verification performed

All run locally in the worktree before pushing; every command below
produced the quoted result.

- `gofmt -l .`: clean (no output).
- `go vet ./...`, `go vet -tags=integration ./...`: clean.
- `make check` (fmt-check, vet, build, lint, test -race): all green,
  `golangci-lint run ./...` → `0 issues.`
- `make coverage`: **93.6%** on
  `./internal/domain/...,./internal/application/...` (gate: 90%).
- `make arch-test`: all hexagonal/analytics-isolation/ports/MCP-boundary/
  auth-revert/Kafka-consumer-group fitness tests pass, including
  `TestKafkaConsumerGroupNeverHardcodedInline` (the new
  `RepromiseConsumerGroup` constant is a named symbol, never an inline
  literal).
- `make bdd`: unaffected, all pre-existing scenarios still pass.
- `make mutation-fast`: **Test efficacy: 91.30%, Mutator coverage:
  87.34%** (gate: 89%/83%) — Killed 63, Lived 6, Not covered 10, all
  pre-existing survivors on `promise.go`/`promise_policy.go` this ADR
  did not touch; no new survivors introduced.
- `make integration`: all green, including the new
  `internal/adapters/inbound/kafka` testcontainers suite
  (`TestRepromiseConsumer_RealTaskCPTMissedMessage_DrivesRepromiseOrder`,
  28.163s) — a real `TaskCPTMissed` message on a real Kafka broker,
  fulfillment-execution's exact shipped envelope/payload shape,
  correctly drives `RepromiseOrder.Execute` end to end: the order's
  persisted `PromiseGroups` breakdown changes and a real
  `OrderRepromised` is published.
- Every existing test still passes unmodified.
