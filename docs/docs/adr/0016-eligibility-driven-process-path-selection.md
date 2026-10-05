---
id: 0016-eligibility-driven-process-path-selection
slug: /adr/0016-eligibility-driven-process-path-selection
title: 16. Eligibility-driven process-path selection (ADR 0014 step B, routing only)
sidebar_label: 16. Eligibility-driven process-path selection (routing only)
description: "ADR 0016 — order.PathSelectionPolicy evaluates a line's product attributes (via a new ProductClassificationLookup, mirroring wes-work-planning's ADR-0009 pattern) against the resolved path's declared Eligibility, rejecting an ineligible line instead of silently routing it. Per-shipment-group promising, ADR 0014's other half of step B, is explicitly deferred to a separate ADR/PR."
---

# 16. Eligibility-driven process-path selection (ADR 0014 step B, routing only)

## Status

Accepted — extended by [ADR 0021](./0021-multi-path-attribute-driven-routing.md),
which made the eligibility rules this record sketched attribute-driven
and multi-path.

## Context

ADR 0013 shipped `order.PathSelectionPolicy` with exactly one rule ("every
line resolves to `shared.DefaultPathId`"), deliberately deferring
attribute-driven routing until real eligibility data existed anywhere in
the fleet. ADR 0014 step A closed that data gap from the other side:
`ports.ProcessPathCatalogue` was widened to expose a path's declared
`Eligibility` (`MaxUnitsPerLine`, `RequiredProductAttributes`,
`ExcludedProductAttributes`, `NonSortable`), specifically so that "step B
(eligibility-driven `PathSelectionPolicy`)... does not need another
catalogue widening" (`ports.go`'s own doc comment on `Eligibility`).
`PathSelectionPolicy.Select` has, until now, still ignored every input it
was ever handed — v1's single rule never looked at `Eligibility` at all,
because nothing called this policy with the attributes it would need to
evaluate it against: this repo has never looked up a SKU's product
classification anywhere (`grep -r "ProductClassification"` returns zero
hits in this repo's own code, prior to this change), unlike
`wes-work-planning`, `fulfillment-execution`, and `workforce-management`,
which all already have a `ports.ProductClassificationLookup` synchronous
HTTP client against `inventory-storage`'s `GET
/products/{sku}/classification`, gated by
`PRODUCT_CLASSIFICATION_MODE=http|permissive`.

ADR 0014's own §4 originally described "step B" as bundling two things
together: eligibility-driven routing AND per-shipment-group promising
(multiple `Promise` values per order when `AllowPartialShipment` is set).
**This ADR is routing only.** Per-group promising is domain-invasive in a
way routing is not: it changes `Order`'s public promise shape from
exactly one `Promise` per order to potentially several, and every
downstream consumer of that shape — the Postgres schema, the Kafka wire
events (`OrderAllocated`/`OrderPartiallyAllocated`), and
`wes-work-planning`'s own consumer of them — currently assumes exactly
one. Splitting that into its own phase, with its own ADR and its own PR,
keeps this change reviewable and reversible on its own; bundling both
would make a single PR touch the aggregate's public shape, the wire
contract, and the routing policy all at once, with no way to accept one
without the other. Per-group promising remains explicitly out of scope
here and is deferred to a future ADR/PR.

## Decision

### 1. A new outbound port, `ports.ProductClassificationLookup`

Modeled on `wes-work-planning`'s own port of the identical name (its
ADR-0009), declared in `internal/application/ports/ports.go` alongside
this repo's other outbound ports (this repo does not split ports into a
separate `integration.go` file the way `wes-work-planning` does — its
existing single-file convention is kept):

```go
type ProductClassification struct {
	SKU          string
	HandlingTags []string
	Known        bool
}

type ProductClassificationLookup interface {
	GetClassification(ctx context.Context, sku string) (ProductClassification, error)
}
```

This is deliberately narrower than `wes-work-planning`'s own view (which
also carries `TemperatureClass`): `order.PathSelectionPolicy`'s
eligibility evaluation only ever needs the raw `HandlingTags` to match
against `Eligibility.RequiredProductAttributes()` /
`ExcludedProductAttributes()`. Per ADR-0013's own stated philosophy
("narrow port... rather than a fuller shape carried unused"), fields this
service has no consumer for are omitted rather than spec­ulatively carried.

`order-management` never imports `wes-work-planning`'s (or
`inventory-storage`'s) Go packages — see
`.claude/rules/bounded-context-boundary.md`. This port, and the adapter
below, are independently written code that calls the same real
`inventory-storage` HTTP contract; nothing is copied across the module
boundary, only the pattern.

### 2. A new outbound adapter package, `internal/adapters/outbound/productclassification`

Two implementations, mirroring `wes-work-planning`'s exact shape:

- **`Client`** — a plain `net/http` call to `inventory-storage`'s real
  `GET /products/{sku}/classification` (`apis/openapi.yaml`'s
  `getProductClassification` operation there; confirmed against the real
  spec, not guessed: the response schema is `sku: string`,
  `handlingTags: string[]` from the closed enum `[Hazmat, Fragile,
  TemperatureSensitive, Oversized, HighValue]`, `temperatureClass`,
  `dotHazardClass` — this port only decodes `sku`/`handlingTags`).
  Reuses `INVENTORY_STORAGE_BASE_URL`, the SAME env var this repo's
  existing `InventoryReservationClient` already reads for the same
  downstream service — there is deliberately no second base-URL knob.
- **`PermissiveLookup`** — the default, fail-open, no-op implementation.

Selected via `PRODUCT_CLASSIFICATION_MODE=http|permissive` on
`cmd/order`, defaulting to `permissive`, matching
`wes-work-planning`'s exact env-var name and default.

**The fail-open direction is the opposite of this repo's own
`InventoryReservationClient`.** `PermissiveClient` (inventory
reservation) fails LOUD — every call returns
`ErrDownstreamNotConfigured` rather than fabricating a reservation,
because reserving real stock must never appear to succeed against a
no-op. `PermissiveLookup` here fails OPEN — every call returns
`Known=false` with a `nil` error, and `Client.GetClassification` itself
converts a 404, a transport error, or any other non-2xx status into the
SAME `Known=false, nil error` result, never propagating an error to its
caller. This is deliberate, not an oversight: a classification lookup is
a soft routing/enrichment input to a domain policy, not a mutation of
real state, so it follows this fleet's general "fail LOUD for anything
that mutates real state, fail QUIET/open for a soft enrichment input"
rule — the identical rule `wes-work-planning`'s ADR-0009 already applied
to the same real endpoint.

### 3. `order.PathSelectionPolicy.Select` widens its signature and finally evaluates `Eligibility`

```go
type EligibilitySource interface {
	Eligibility(pathID shared.PathId) (shared.Eligibility, bool)
}

func (PathSelectionPolicy) Select(
	sku shared.SKU, quantity int, giftWrap bool,
	productAttributes []string, catalogue EligibilitySource,
) (shared.PathId, bool)
```

`EligibilitySource` is a small, domain-owned interface — the same
pattern `PromisePolicy`'s `CapabilitySource`/`ScheduleSource`/
`CapacitySource` already established rather than depending on the full
`ports.ProcessPathCatalogue` type. `ports.ProcessPathCatalogue` already
satisfies it by Go's structural interface-to-interface assignability
(it has an `Eligibility(pathId) (shared.Eligibility, bool)` method with
that exact shape since ADR-0014 step A) — **no adapter code changes**,
only a new caller.

The rule: evaluate `shared.DefaultPathId`'s declared `Eligibility`
against the line's `quantity` and `productAttributes` (gift wrap is
folded into `productAttributes` as the string `"giftWrap"` — the same
free-form vocabulary as `"hazmat"`/`"fragile"`, and `shared.Eligibility`'s
own doc comment already names `giftWrap` as exactly this kind of
attribute). Quantity must not exceed `MaxUnitsPerLine` when bounded;
every `RequiredProductAttribute` must be present; no
`ExcludedProductAttribute` may be present. `NonSortable` is a property of
the path's freight handling as a whole, not a per-line signal this policy
has, so it is not evaluated here. A `nil` catalogue, or a catalogue that
does not (yet) know `DefaultPathId`'s eligibility, fails OPEN — `(shared.
DefaultPathId, true)` — exactly ADR-0013's original floor: missing
catalogue data is a "not yet wired" state, never a rejection trigger.

`Select` now returns `(shared.PathId, bool)` instead of a bare
`shared.PathId`. `ReceiveOrder.Execute` (application layer, which alone
performs the classification lookup — the domain policy itself remains
pure, no I/O) turns `ok=false` into a new caller-facing error,
`shared.ErrLineIneligibleForResolvedPath` (RFC 7807 422), rejected
BEFORE `Save`/`OrderReceived`, exactly the same "reject synchronously,
before anything persists" discipline ADR-0013 already established for
`ErrUnknownProcessPath`.

### 4. The routing rule's real, honest scope — this is a v1 limitation, not an oversight

`ports.ProcessPathCatalogue` has **no "list every known active path"
method today** — only `IsActive(pathId)`, `CycleTimeP95(pathId)`, and
`Eligibility(pathId)`, each keyed on a caller-supplied id. There is
therefore no way for this policy to discover a genuinely different,
eligible alternative path when `shared.DefaultPathId`'s own `Eligibility`
rejects a line — it cannot ask the catalogue "which paths, if any, would
accept a hazmat line of quantity 5?" This repo also declares no other
`PathId` constant anywhere (`shared/path_id.go` has only
`DefaultPathId`), so there is no second hardcoded candidate to fall
back to either.

Given that constraint, the honest v1 rule implemented here is
deliberately narrower than "choose among multiple real paths": evaluate
the ONE candidate this service can reach (`shared.DefaultPathId`) against
the line, and report whether it is eligible at all. When it is not, the
line is rejected rather than silently routed onto a path whose declared
`Eligibility` explicitly excludes it — silently routing would be worse
than rejecting, because it would place real work on a path
`process-path-management`'s own configuration says cannot carry it. This
mirrors ADR-0013's own precedent exactly: ship a real, testable, honestly
narrower-than-ideal v1 rather than fabricate a multi-path decision this
service cannot actually make yet.

**What a real multi-path routing decision would need**, for a future
phase: `ports.ProcessPathCatalogue` widened with a genuine "list active
paths" method (or an equivalent Kafka-fed local index keyed by
attribute), so `PathSelectionPolicy` could enumerate every currently
active path, evaluate each one's `Eligibility` against the line, and pick
the best match (e.g. the ADR-0014 §4 original text's "shortest
`cycleTimeP95` among eligible paths" rule) — that full rule is *not*
implemented here, because building it against a catalogue with no
listing capability would mean inventing a fake enumeration (e.g.
hardcoding path-id guesses) that does not reflect what this service can
actually observe. This ADR ships the honest, narrower alternative and
documents the gap explicitly rather than pretend otherwise.

### 5. What this ADR deliberately does NOT touch

- `Order`'s promise fields, `PromisePolicy`'s per-order (single-`Promise`)
  contract, `SetPromise`, or anything about split-shipment/multi-group
  promising — all unchanged. Per-shipment-group promising is ADR-0014
  §3's other half of "step B" and is deferred to its own future ADR/PR
  (see Context above).
- `OrderLine`'s persisted shape — no new field is added to `OrderLine` to
  store a line's classification. The classification lookup happens ONCE,
  at intake, purely to feed this routing decision; it is not stamped onto
  the aggregate or re-evaluated later (a genuine difference from
  `wes-work-planning`'s ADR-0009 "read-once-at-release, stamp onto the
  wire" pattern — here it is "read-once-at-intake, use immediately for
  routing, do not persist").
- The number of paths this service can resolve to. `shared.DefaultPathId`
  remains the only PathId this policy can produce; see §4.

## Alternatives considered

- **Bundle per-group promising into this same PR, as ADR-0014 §4
  originally described "step B".** Rejected — see Context: it changes
  `Order`'s public promise shape (Postgres schema, Kafka wire events,
  `wes-work-planning`'s consumer), which is a materially larger and
  separately reviewable change than routing alone.
- **Fabricate a "list of known paths" by hardcoding likely path ids
  (e.g. `"singles"`, `"hazmat"`) and iterating them.** Rejected: this
  service has no real evidence any such path currently exists in
  `process-path-management`'s catalogue; inventing ids would produce
  false richness that looks like real multi-path routing but is
  actually guessing, exactly what ADR-0013 already rejected once for the
  same reason.
- **Silently route an ineligible line onto `DefaultPathId` anyway
  (today's pre-this-ADR behavior).** Rejected: once eligibility data is
  available and evaluated, ignoring a real "this path cannot carry this
  line" result is strictly worse than the status quo — it would place
  real work on a path known to reject it, rather than surfacing an
  honest, synchronous 422 to the caller.
- **Have `Select` return an error type instead of `(PathId, bool)`.**
  Rejected for symmetry: `ports.ProcessPathCatalogue.Eligibility` and
  `CycleTimeP95` already use the `(value, bool)` convention throughout
  this codebase (see `PromisePolicy`'s `CapabilitySource`/
  `ScheduleSource`); a domain policy returning a `bool` "known/ok" flag
  rather than an `error` keeps this consistent, and `ReceiveOrder` is
  the layer that turns `ok=false` into a typed domain error.

## Consequences

**Easier**

- A line whose declared classification conflicts with the only path this
  service can route to is now rejected honestly and synchronously, at
  intake, instead of being silently placed on a path that
  `process-path-management`'s own configuration says cannot carry it.
- `ports.ProductClassificationLookup` gives this service the exact same
  proven sync-edge pattern three other fleet services already run in
  production (`PRODUCT_CLASSIFICATION_MODE=http|permissive`), rather
  than inventing a new integration style.
- `EligibilitySource` costs `ports.ProcessPathCatalogue` zero new
  methods — the ADR-0014-step-A widening already anticipated exactly
  this consumer.

**Harder**

- Order intake now has a new synchronous outbound HTTP call on its hot
  path (permissive by default, so no behavior change for any deployment
  that has not opted in via `PRODUCT_CLASSIFICATION_MODE=http`) — the
  same trade-off ADR-0014's own Consequences section already flagged as
  "a new runtime dependency on the order-intake hot path" before this
  phase existed to implement it.
- This phase's routing rule cannot choose among multiple real paths —
  see §4's honest limitation. A future phase needs a real "list active
  paths" catalogue capability before a genuine multi-path
  attribute-driven decision (the fuller rule ADR-0014 §4 originally
  described) can be built without fabricating data this service does not
  have.
- `shared.ErrLineIneligibleForResolvedPath` is a new caller-facing
  rejection an existing integrator of `POST /orders` did not previously
  see. Any client currently sending, e.g., a gift-wrap line against a
  catalogue where an operator has configured `DefaultPathId` to exclude
  `giftWrap` will now get a 422 instead of silent (and incorrect)
  acceptance — this is the intended, honest behavior change, but it is a
  real behavior change worth calling out to any real caller.
