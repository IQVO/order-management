---
id: 0013-process-path-selection-as-a-domain-policy
slug: /adr/0013-process-path-selection-as-a-domain-policy
title: 13. Process-path selection as a real domain policy, validated against a live catalogue
sidebar_label: 13. Process-path selection as a domain policy
description: "ADR 0013 — replace the hardcoded PathID default with a real domain policy (order.PathSelectionPolicy), and validate the resolved path against process-path-management's live catalogue before an order is ever persisted, instead of only downstream in wes-work-planning."
---

# 13. Process-path selection as a real domain policy, validated against a live catalogue

## Status

Accepted — the single-rule scope and catalogue deferral recorded here
were extended by [ADR 0021](./0021-multi-path-attribute-driven-routing.md)
(attribute-driven multi-path routing closes the original deferral) and
[ADR 0016](./0016-eligibility-driven-process-path-selection.md)
(eligibility filtering).

## Context

Since [ADR 0005](0005-choreographed-release-via-kafka.md) made `pathId`
internal-only (removing the caller's ability to set it on public
intake), every `OrderLine` was resolved via
`shared.NewPathIdOrDefault("")` — called directly in the HTTP adapter,
before the request ever reached the domain or application layer. That
call always resolves to the same hardcoded constant,
`shared.DefaultPathId = "pick"`. There was no code path, anywhere in
this service, capable of producing any other value. `order.LeadTimePolicy`'s
`PerPath` override table — real, tested code — was consequently dead:
nothing ever routed a line to any path but `"pick"`, so a per-path lead
time could never fire.

At the same time, this service had zero awareness of the fleet's real
process-path catalogue. `process-path-management` (the bounded context
that owns the fleet's declared set of process paths, publishing
`ProcessPathCreated`/`Updated`/`Deactivated` on
`warehouse.process-path-management.events`) was already the catalogue's
source of truth for three other consumers — `wes-work-planning`,
`fulfillment-execution`, and `workforce-management` — each maintaining a
local, Kafka-fed read cache and rejecting an unrecognized path id before
doing anything with it. `order-management` had no equivalent. The only
place a `pathId` this service emitted was ever checked against that real
catalogue was one hop downstream, inside `wes-work-planning`'s
`handleOrderManagementEvent` Kafka consumer
(`c.catalogue.Lookup(pathId.String())`) — by which point `OrderReceived`
had already fired and this service's own choreographed allocate-then-
release flow had already run. A caller placing an order already saw a
`201 Created` and a persisted, allocated (possibly released) order
before any rejection could occur. A stale or malformed path id — which,
with a single hardcoded constant, could never actually happen in
practice, but which would become a live risk the moment path selection
became a real decision — would silently dead-end in `wes-work-planning`
with no feedback to whoever placed the order.

This also fell short of the products this service's own design
discussions have used as the target bar. Sourcing/routing decisions,
split-shipment policy, and promise-date calculation are treated as
first-class domain logic in Manhattan Active OM, Salesforce OMS, and
a major e-commerce retailer's own order platform — not hardcoded constants resolved at the
adapter boundary.

## Decision

We introduce `order.PathSelectionPolicy` (`internal/domain/order/path_selection.go`)
as a pure domain policy: a testable decision point over an `OrderLine`'s
own attributes (`SKU`, `Quantity`, `GiftWrap`), owned by the domain layer
rather than pre-baked in the HTTP adapter. `ReceiveOrder` now resolves an
empty `PathID` via this policy instead of the adapter calling
`NewPathIdOrDefault("")` before the use case ever sees the line.

We deliberately ship v1 of this policy with exactly **one** rule: every
line still resolves to `shared.DefaultPathId`. This is not a stopgap
disguised as a policy — it is the honest v1 model, in the same spirit as
`order.LeadTimePolicy`'s own doc comment ("real code with real
behaviour, not a hardcoded field pretending to be a calculation"). The
point of this phase is making the decision a real, unit-tested, domain-
owned policy object with a real extension point, not building
attribute-driven routing (hazmat-capable paths, oversize handling, a
gift-wrap-capable path) yet — `process-path-management`'s
`RequiredCapabilities` field has no real fleet data to route against
today, and building a capability-matching rule now would mean validating
lines against capabilities that don't exist anywhere. That is
deliberately deferred to a later phase, once real capability data
exists.

Separately — and this is the change that actually closes the gap — we
add `ports.ProcessPathCatalogue` (`IsActive(pathId) bool`), a read-only
outbound port, and validate every line's resolved path against it inside
`ReceiveOrder`, **before** `Orders.Save`/`OrderReceived` fire. An
unrecognized or inactive path returns the new domain error
`shared.ErrUnknownProcessPath` (HTTP 400, RFC 7807), and nothing is
persisted. This moves the rejection from "one saga step downstream, one
event type, and a Kafka consumer error log away" to "synchronous, at the
moment the caller's own request is evaluated" — the same guarantee
`wes-work-planning`, `fulfillment-execution`, and `workforce-management`
already give their own callers/queues.

The real implementation, `internal/adapters/outbound/kafkacatalog`, is a
Kafka-fed local cache — copied byte-for-byte in its concurrency and
readiness design from `wes-work-planning`'s and `fulfillment-execution`'s
own proven `kafkacatalog` packages, which consume the exact same
`warehouse.process-path-management.events` topic. This buys, for free,
two real bugs already found and fixed in those two prior
implementations rather than a third rediscovery:

1. A readiness gate that only re-evaluates "am I caught up" on a *new*
   message deadlocks forever on an ordinary restart where nothing new
   has been published since the last run. Fixed by capturing each
   partition's current last offset before consuming and checking
   already-committed offsets against that target immediately.
2. A fixed, shared consumer group name lets a new process resume from a
   *prior* process's committed offset, reporting itself ready with an
   empty local cache having never actually replayed anything. Fixed by
   making every `NewConsumer` call use a unique, per-process group id.

One deliberate difference from those two prior implementations: this
service's copy of the domain-layer catalogue type
(`internal/domain/processpath`) is smaller than their `pathcatalog`
package. `order-management` only ever needs to answer "is this path
active" — never `RequiredCapabilities` or `Direct` — so those fields are
omitted entirely rather than carried unused. Widening this shape is
explicitly left for the attribute-driven-routing follow-up phase, not
built speculatively now.

The catalogue source is selectable via `PATH_CATALOGUE_SOURCE=none|kafka`
on `cmd/order`, defaulting to `none` (validation skipped — a nil
`ports.ProcessPathCatalogue` is this fleet's established "not yet wired"
convention). Unlike the three services above, `order-management` never
had a static, file-based catalogue to preserve backward compatibility
with — there is no `file` mode here, only "skip" and "validate for
real."

## Consequences

**Easier:**

- A caller placing an order that resolves to an unknown/retired path now
  gets an honest, synchronous `400` instead of a silent `201` that dead-
  ends downstream with no trace back to the original request.
- `order.PathSelectionPolicy` gives this service a real extension point
  for attribute-driven routing later, rather than requiring a redesign
  of the intake path when that phase eventually lands.
- The Kafka consumer's readiness/consumer-group design inherits two
  already-hardened bugs' fixes for free, rather than risking a third
  independent (and likely slower) discovery of the same failure modes.

**Harder:**

- This service now has a live runtime dependency on Kafka reachability
  at startup when `PATH_CATALOGUE_SOURCE=kafka`: a Kafka outage lasting
  longer than `kafkacatalog.WaitReadyTimeout` (60s) fails this service's
  startup outright, the same trade-off the three prior consumers already
  accepted for the same reason (never serve traffic against a
  known-incomplete catalogue).
- `order.PathSelectionPolicy`'s single rule is, today, functionally
  indistinguishable from the hardcoded constant it replaces — the value
  of this change is structural (a real, testable extension point; real
  synchronous validation) rather than an immediately visible behavior
  change for any order that would have resolved to `"pick"` anyway. A
  reader unfamiliar with the deferred Phase 2 could reasonably ask "why
  did this need a whole domain type." The answer is in this ADR: the
  validation half is the actual fix; the policy half is scaffolding for
  work explicitly not yet justified by real data.
- The full discriminating "accept, then reject" live-propagation probe
  could not be run against a genuinely different path: v1's policy
  cannot resolve anything but `"pick"`, and `process-path-management`
  makes path deactivation permanent (a deactivated id can never be
  redefined) — so exercising the reject path for real would mean
  disabling `PICK` itself, which `wes-work-planning`,
  `fulfillment-execution`, and `workforce-management` all depend on
  live. That full probe is deferred to the attribute-driven-routing
  phase, once a second, genuinely disposable path exists for this
  service to route to.
