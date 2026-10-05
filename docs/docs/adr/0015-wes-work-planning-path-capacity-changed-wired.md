---
id: 0015-wes-work-planning-path-capacity-changed-wired
slug: /adr/0015-wes-work-planning-path-capacity-changed-wired
title: 15. wes-work-planning's PathCapacityChanged is wired as the real PathCapacity adapter
sidebar_label: 15. PathCapacityChanged wired as the real PathCapacity adapter
description: "ADR 0015 — replace UnknownPathCapacity as the production PathCapacity default with a Kafka-fed adapter consuming wes-work-planning's PathCapacityChanged event, widening ports.PathCapacity.Remaining to accept cutoffAt so correlation is exact rather than invented."
---

# 15. wes-work-planning's PathCapacityChanged is wired as the real PathCapacity adapter

## Status

> **Partially superseded (envelope only) by ADR-0030** (the flat-envelope `event_type == "PathCapacityChanged"` dispatch shown here; the consumer now dispatches on the full CloudEvents type `com.warehouse.wes.work-planning.workpool.PathCapacityChanged`). Every Kafka message is now a
> CloudEvents 1.0 event — see
> [ADR 0030](./0030-cloudevents-mandatory-event-envelope.md). The rest of
> this record stands.

Accepted. Closes the gap ADR 0014 explicitly deferred (its rollout step
3): "This service, step A" shipped `ports.PathCapacity` with exactly one
implementation, `UnknownPathCapacity`, documented as "filled in a later
phase". wes-work-planning has now shipped and merged its own ADR-0018,
publishing `PathCapacityChanged` on `warehouse.work-planning.events`.
This ADR is that later phase.

## Context

`order.PromisePolicy.linesFitWindow` (ADR 0014) evaluates three
conditions per line against a candidate CPT window: path eligibility,
cycle time, and remaining capacity. The third condition,
`ports.PathCapacity.Remaining`, has never had a real data source — until
now every promise was computed as if capacity were unknown, which
`PromisePolicy` correctly treats as "not a constraint" per the ADR's
explicit condition (c). That is honest but incomplete: a genuinely
saturated path has been promised at full speed the entire time ADR 0014
has been live.

wes-work-planning's `PathCapacityChanged` (its own ADR-0018, PR #69) is
the missing input. Its wire shape, on `warehouse.work-planning.events`
(a topic this service has never consumed before):

```json
// event_type == "PathCapacityChanged"
{
  "path_id": "pick",
  "cutoff_at": "2026-09-13T18:00:00Z",
  "remaining_units": 42,
  "known": false
}
```

`known` is false when the path's WorkPool is FlowFed (no hard admission
ceiling) or its WIP limit is unset/zero — a legitimate, permanent
"not applicable" answer for some paths, not a transient gap.
`remaining_units` is only meaningful when `known` is true.

The one real design question this ADR has to answer is correlation.
`ports.PathCapacity.Remaining` today takes `(pathId, cptId string)` —
process-path-management's stable cutoff identity (e.g. `"sp1-1800"`).
wes-work-planning's event carries no `cptId` at all; it carries its own
native `CutoffAt` (`time.Time`), because wes-work-planning has no
knowledge of process-path-management's schedule identity — it only
knows the wall-clock instant a WorkPool's admission window closes. A
Kafka-fed cache keyed on `cptId` therefore cannot be built directly from
this event; something has to bridge `cptId <-> cutoffAt`.

## Decision

### 1. Widen `ports.PathCapacity.Remaining` to accept `cutoffAt`

```go
type PathCapacity interface {
	Remaining(pathId shared.PathId, cptId string, cutoffAt time.Time) (units int, known bool)
}
```

The one real caller, `PromisePolicy.linesFitWindow`, already has both
`cptId` and `cutoffAt` in scope — it is iterating `CPTWindow{CptId,
CutoffAt, EligiblePathIds}` values it obtained from
`ScheduleSource.NextCutoffs` moments earlier in the same call. Passing
`cutoffAt` alongside `cptId` costs the caller nothing: no new lookup, no
new dependency, one more field on an existing call. This is a
non-breaking-in-spirit but source-breaking widening: `order.PromisePolicy.
CapacitySource` (the domain's own mirror of this port) and its one call
site are widened identically, and `UnknownPathCapacity` gains the
parameter as a no-op. Every existing test's assertions are preserved,
only their fake's method signature grows a parameter.

`cptId` is kept in the widened signature — an adapter may still find it
useful for logging/correlation/metrics — but it plays no role in this
adapter's actual lookup; see below.

### 2. The Kafka-fed cache is keyed on `(PathId, CutoffAt)`, matched exactly

The alternative — keep the port's `cptId`-only signature and have the
new adapter independently resolve `cptId -> cutoffAt` itself, either by
also consuming `kafkacptschedule`'s schedule cache or by having the
composition root inject it — was rejected. It buys nothing: the
information already exists at the only call site that matters, one call
frame up, for free. Manufacturing a second path to the same fact inside
the adapter (parsing schedules, or coupling two Kafka-fed adapters
together via a new internal dependency) is strictly more code, more
coupling, and a second place the schedule/capacity mapping can disagree
with itself. Widening the port is the "make the honest thing cheap"
choice ADR 0014 itself repeatedly favors.

Given `cutoffAt` is available, the cache is keyed by
`(PathId, CutoffAt.UTC().UnixNano())` — an **exact match** on the cutoff
instant, not nearest-window-within-tolerance. Exact match is chosen
because:

- wes-work-planning's `CutoffAt` and process-path-management's
  `CPTScheduleChanged`-derived `CutoffAt` (via `kafkacptschedule.
  computeNextCutoffs`) both derive from the *same* recurring schedule
  rule (a WorkPool's admission window closes exactly when a site's CPT
  cuts off) — they are expected to agree to the instant, not merely be
  close. A tolerance window would silently paper over a real schedule
  drift between the two services instead of surfacing it as
  known=false.
- A tolerance introduces a magic constant (how close is "close enough"?)
  with no principled default and every wrong value either false-matches
  an unrelated cutoff or false-misses a real one. Exact match has no
  such tuning surface.
- If exact match ever proves too brittle in practice (clock skew,
  independent schedule authoring drifting apart), that is itself useful
  signal — visible as an unexpected rise in known=false — worth
  investigating rather than silently absorbing with a fudge factor.

### 3. A third, independent Kafka consumer, on a third topic

`internal/adapters/outbound/kafkapathcapacity.Consumer` copies
`kafkacatalog`'s and `kafkacptschedule`'s proven design byte-for-byte:
per-process-unique consumer group (`uniqueConsumerGroup()`, never a
fixed shared string — a real, previously-hit bug class in this fleet),
`FirstOffset` replay, a target-offset readiness gate captured *before*
consuming starts (the fix for "readiness never fires on an ordinary
restart" when a consumer group is already fully caught up), and a
`NewConsumerForTopic` variant so integration tests point the identical
replay/readiness logic at a throwaway topic. What is new here is the
topic: `warehouse.work-planning.events` is wes-work-planning's own
integration topic, never consumed by this service before — a genuinely
new upstream dependency, not another consumer on
`warehouse.process-path-management.events`. The consumer filters
strictly for `event_type == "PathCapacityChanged"`, ignoring every other
event type wes-work-planning publishes there (`ShiftPlanCommitted`,
`WorkUnitCreated`, etc.) — the same fan-out-topic convention
`kafkacatalog`/`kafkacptschedule` already established.

### 4. Composition root: same switch, no new operator-facing knob

`cmd/order/main.go`'s existing `PATH_CATALOGUE_SOURCE=none|kafka` switch
is extended a third time. When `kafka`, the path capacity consumer
starts alongside `kafkacatalog`/`kafkacptschedule` on the same
`KAFKA_BROKERS`, with its own `WaitReady` call before the HTTP server
starts serving traffic (mirroring the existing two `WaitReady` calls
sequentially — no goroutine fan-in, matching the existing pattern
exactly). `ports.PathCapacity` is wired to the new consumer in place of
`UnknownPathCapacity`. When `PATH_CATALOGUE_SOURCE=none` (or unset),
`UnknownPathCapacity` remains wired exactly as before — no behavior
change for that mode, and it stays a legitimate dev-mode/fallback
option, not a deprecated one.

**Nothing new to configure.** An operator who already set
`PATH_CATALOGUE_SOURCE=kafka` and `KAFKA_BROKERS` for ADR-0013/0014 gets
real path capacity automatically; there is no new environment variable.

## Consequences

**Easier**

- A saturated path with a real, observed `PathCapacityChanged` for the
  exact `(path, cutoff)` a promise is being evaluated against now
  genuinely constrains that promise — `PromisePolicy`'s condition (c) is
  no longer permanently vacuous.
- The widened `ports.PathCapacity`/`order.CapacitySource` signature
  carries exactly the information the one real caller already has;
  nothing is invented, guessed, or resolved through an extra hop.
- The third-consumer-on-a-new-topic pattern is now proven three times in
  this codebase (`kafkacatalog`, `kafkacptschedule`,
  `kafkapathcapacity`) — a fourth would be pure copy-and-adapt, no new
  design risk.

**Harder**

- `ports.PathCapacity.Remaining` and `order.CapacitySource.Remaining`
  are both source-breaking signature changes. Every implementation and
  every test double had to be touched in this one PR (`UnknownPathCapacity`,
  `fakeCapacity` in `promise_policy_test.go`, the new
  `kafkapathcapacity.Consumer`). Any future third-party fork of this
  port before this ADR would need the same mechanical update.
- **What this does NOT solve, and must not be mistaken for a bug**:
  capacity is known only for a `(path, cutoffAt)` pair wes-work-planning
  has actually published a `PathCapacityChanged` for. A path
  wes-work-planning has never sampled at that exact cutoff — because no
  work has moved through it yet, because the WorkPool is FlowFed, or
  because the WIP limit is unset — reports `known=false` forever, which
  `PromisePolicy` correctly treats as "not a constraint". This is the
  same honest-v1 shape ADR 0014 itself describes for the pre-capacity
  world: **"no `PathCapacityChanged` observed yet" is not "infinite
  capacity"; it is "no capacity opinion yet", and the promise proceeds on
  path/schedule feasibility alone.** Operators and future readers of a
  promise's `basis`/capacity behavior should not read a stretch of
  `known=false` capacity answers as evidence the fix isn't wired — it is
  the correct behavior for a path/cutoff wes-work-planning has not yet
  reported on.
- Exact-match correlation (§2) means a genuine schedule-authoring drift
  between process-path-management's `CPTScheduleChanged` and
  wes-work-planning's `PathCapacityChanged` `CutoffAt` values would
  silently degrade to `known=false` rather than a near-miss warning.
  This is treated as the correct fail-safe (never mis-attribute
  capacity to the wrong cutoff) but is a real operational surface to
  monitor if the two services' schedule configuration is ever edited
  independently.
- A third long-outage-blocks-startup trade-off, identical to the one
  ADR 0014 already accepted for the schedule cache: a long Kafka outage
  on `warehouse.work-planning.events` at startup blocks this service
  from serving traffic (bounded by `kafkapathcapacity.WaitReadyTimeout`,
  60s, matching the other two consumers). The alternative — serving
  promises from a possibly-stale or empty capacity cache — was rejected
  for the same reason ADR 0014 rejected it there.

## Alternatives considered

- **Keep the port's `cptId`-only signature; resolve `cptId -> cutoffAt`
  inside the new adapter.** Rejected (§2): more code, a new internal
  dependency between two Kafka-fed adapters, and no benefit over reading
  `cutoffAt` off the value the caller already has.
- **Nearest-window-within-tolerance matching instead of exact match.**
  Rejected (§2): no principled tolerance value exists, and a tolerance
  would hide real schedule drift between the two services rather than
  surface it.
- **Do nothing until wes-work-planning's event data is "proven stable"
  in production.** Rejected: the event is already shipped and merged
  (ADR-0018, PR #69); deferring further only prolongs
  `PromisePolicy`'s vacuous capacity condition with no new information
  gained by waiting.
