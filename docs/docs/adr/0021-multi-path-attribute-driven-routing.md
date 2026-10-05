---
id: 0021-multi-path-attribute-driven-routing
slug: /adr/0021-multi-path-attribute-driven-routing
title: 21. Multi-path attribute-driven routing (closes ADR-0013's original deferral)
sidebar_label: 21. Multi-path attribute-driven routing
description: "ADR 0021 — ProcessPathCatalogue gains ListActive, and order.PathSelectionPolicy enumerates every currently active path, filters by declared Eligibility, and picks the shortest cycleTimeP95 among the eligible candidates, closing the multi-path gap ADR-0013 deferred and ADR-0016 §4 documented as a named limitation."
---

# 21. Multi-path attribute-driven routing (closes ADR-0013's original deferral)

## Status

Accepted.

## Context

ADR-0013 introduced `order.PathSelectionPolicy` with exactly one rule
("every line resolves to `shared.DefaultPathId`"), explicitly deferring
attribute-driven routing until real capability/eligibility data existed
anywhere in the fleet: `process-path-management`'s own
`RequiredCapabilities` field had "no real fleet data to route against" at
the time.

`process-path-management` ADR-0010 closed that data gap: `ProcessPath`
now publishes `cycleTimeP95` and a structured `eligibility` rule
(`maxUnitsPerLine`, `requiredProductAttributes`, `excludedProductAttributes`,
`nonSortable`) on the same `warehouse.process-path-management.events`
topic this service already replays. ADR-0016 (this repo, "ADR-0014 step
B, routing only") was the first consumer of that data: it made
`PathSelectionPolicy.Select` evaluate `Eligibility` for real — but only
against the ONE candidate this service could reach,
`shared.DefaultPathId`. ADR-0016 §4 named this gap explicitly and left it
open: `ports.ProcessPathCatalogue` had "no 'list every known active
path' method today", so the policy could not discover a genuinely
different, eligible alternative path when `DefaultPathId`'s own
`Eligibility` rejected a line — an ineligible line was rejected outright
(422) rather than routed anywhere else, even when a real alternative
existed in `process-path-management`'s live catalogue.

That data now exists on the wire and is already decoded: this service's
own `kafkacatalog.Consumer` holds every currently active path (id,
`MatchPrefix`, `CycleTimeP95`, `Eligibility`) in its in-memory `paths`
map — it simply never exposed a way to enumerate that map to a caller.
Closing ADR-0016 §4's gap does not need a new integration; it needs the
existing local cache to expose what it already has, and the domain
policy to use it.

## Decision

### 1. `ports.ProcessPathCatalogue` gains one new method, `ListActive`

```go
type ProcessPathCatalogue interface {
	IsActive(pathId shared.PathId) bool
	CycleTimeP95(pathId shared.PathId) (cycleTime time.Duration, known bool)
	Eligibility(pathId shared.PathId) (shared.Eligibility, bool)

	// ListActive returns every currently active path this catalogue
	// knows about, each paired with its declared CycleTimeP95 (and
	// whether that value is known) and Eligibility. Order is
	// unspecified -- callers that need a deterministic pick (e.g.
	// "shortest cycle time") must sort themselves. A source with an
	// empty or not-yet-ready cache returns an empty slice, never an
	// error -- exactly this port's existing "missing data fails open"
	// convention (see IsActive/CycleTimeP95/Eligibility's own doc
	// comments).
	ListActive() []ActivePath
}

// ActivePath is one entry from ListActive: a candidate PathSelectionPolicy
// can evaluate for routing.
type ActivePath struct {
	PathId         shared.PathId
	CycleTimeP95   time.Duration
	CycleTimeKnown bool
	Eligibility    shared.Eligibility
}
```

This is a NON-BREAKING additive widening, the same shape ADR-0014 step A
and ADR-0016 already used twice for the identical port. `kafkacatalog.Consumer`
implements it directly from its existing `paths` map (already holds
everything `ActivePath` needs — no new decoding, no new wire field).

### 2. `order.PathSelectionPolicy.Select` enumerates and picks, instead of checking one id

```go
type EligibilitySource interface {
	Eligibility(pathID shared.PathId) (shared.Eligibility, bool)
	ListActive() []shared.ActivePathCandidate
}

func (PathSelectionPolicy) Select(
	sku shared.SKU, quantity int, giftWrap bool,
	productAttributes []string, catalogue EligibilitySource,
) (shared.PathId, bool)
```

`shared.ActivePathCandidate` is a domain-owned mirror of the port's
catalogue entry (same reasoning as `shared.Eligibility` already mirroring
`process-path-management`'s own value object independently — the domain
layer must not import the application layer's port types). **Naming
note, fixed after review:** the port sketch above shows a ports-local
`ports.ActivePath`; what shipped is simpler — `ProcessPathCatalogue.
ListActive` returns `[]shared.ActivePathCandidate` directly (see
`ports.go`), so the domain interface and the catalogue adapter share one
type and no mirror conversion exists.

The rule, evaluated in this order:

1. **Nil catalogue, or `ListActive()` returns nothing** — fail OPEN to
   `shared.DefaultPathId`, exactly ADR-0013's original floor and
   ADR-0016's fallback. A not-yet-wired or not-yet-ready catalogue must
   never block intake.
2. **Filter** `ListActive()`'s candidates to those whose declared
   `Eligibility` admits this line (`lineEligible`, unchanged from
   ADR-0016: quantity within `MaxUnitsPerLine`, every
   `RequiredProductAttribute` present, no `ExcludedProductAttribute`
   present).
3. **Pick the eligible candidate with the shortest known `CycleTimeP95`**
   — the rule ADR-0014 §4 originally described ("shortest cycle time
   among eligible paths"). A candidate with `CycleTimeKnown=false` sorts
   last (an unmeasured path is never preferred over a measured one, but
   is still a legal pick if it is the ONLY eligible candidate — an
   operator who declared eligibility but not yet a cycle time should not
   have that path silently excluded from routing).
4. **No eligible candidate at all** — `ok=false`, exactly ADR-0016's
   existing rejection behaviour (`shared.ErrLineIneligibleForResolvedPath`,
   422, before `Save`/`OrderReceived`). This is now a genuine "no path in
   the whole active catalogue can carry this line" result, not "the one
   hardcoded candidate rejected it."

Tie-breaking two candidates with an identical known `CycleTimeP95`: the
lower `PathId` (lexicographic) wins, giving a fully deterministic pick —
required so the same line always routes the same way given an unchanged
catalogue, and so this rule is unit-testable without depending on map
iteration order.

### 3. `ReceiveOrder` is unchanged

`ReceiveOrder.Execute` already calls `uc.PathPolicy.Select(...)` and turns
`ok=false` into `shared.ErrLineIneligibleForResolvedPath`; it has no
knowledge of how many candidates `Select` considered internally. No
application-layer change is needed beyond the catalogue still being
`ports.ProcessPathCatalogue` (now with `ListActive`).

### 4. What this ADR deliberately does NOT change

- The wire contract. `process-path-management` already publishes
  everything this ADR consumes (ADR-0010); no new event, no new topic.
- `Order`/`OrderLine`'s persisted shape, or the promise/CPT machinery
  (ADR-0014/0017/0018/0019/0020) — this is purely a routing-time decision
  among already-known candidates, made once per line at intake.
- The `IsActive`/`CycleTimeP95`/`Eligibility` single-id methods on
  `ProcessPathCatalogue` — kept as-is; `PromisePolicy`
  (`CapabilitySource`) still uses `CycleTimeP95` for its own, unrelated
  CPT-fit question and does not need `ListActive`.

## Alternatives considered

- **Hardcode a short list of "likely" path ids and iterate them.**
  Rejected for the exact reason ADR-0013 and ADR-0016 already rejected
  it: this service has no evidence any specific id besides `PICK`
  currently exists in a live catalogue, and fabricating one would produce
  false richness that looks like real routing but is actually guessing.
- **Widen `ports.ProcessPathCatalogue` with a filtered query method
  (e.g. `EligibleFor(quantity, attributes) []PathId`) instead of a raw
  `ListActive`.** Rejected: it would move a domain decision (what
  "eligible" means, how ties break) into the outbound adapter/port
  layer, which is exactly the inversion ADR-0013's original design
  avoided by keeping `PathSelectionPolicy` a pure domain type. `ListActive`
  stays a plain read; all decision logic stays in `order.PathSelectionPolicy`.
- **Prefer eligible-but-cycle-time-unknown paths over none, but never
  over a known one; break ties randomly.** Rejected: non-determinism in
  a business decision that is retried/tested is worse than a slightly
  arbitrary but STABLE tie-break rule (lowest `PathId` wins).

## Consequences

**Easier**

- A line that would have been rejected under ADR-0016 (ineligible for
  `DefaultPathId`) because a genuinely better path exists in
  `process-path-management`'s catalogue now actually routes there
  instead of bouncing a caller with a 422 that a human operator would
  read as "the fleet has no path for this," when in fact one exists.
- Closes ADR-0013's original deferral and ADR-0016 §4's named limitation
  for good — this was the last named gap in the process-path-selection
  arc (ADR-0013 → 0014 → 0016 → 0021).
- `order.LeadTimePolicy`'s `PerPath` override table, and
  `PromisePolicy`'s per-path `CycleTimeP95` lookups, both become
  reachable for more than `PICK` for the first time since ADR-0013 first
  observed them as dead code.

**Harder**

- `PathSelectionPolicy.Select`'s worst-case cost is now O(active paths)
  instead of O(1) — irrelevant at this fleet's scale (a handful of
  declared paths) but a real, documented change from "one hardcoded
  lookup" to "scan and sort every call."
- A caller who previously got routed to `DefaultPathId` unconditionally
  (any operator catalogue with only `PICK` declared) sees NO behaviour
  change — this ADR only changes outcomes once a SECOND active path
  exists and is more eligible for a given line, which requires deliberate
  operator configuration in `process-path-management`. Verifying this
  ADR's real effect therefore needs at least two active paths seeded in
  a live/integration environment, not just a unit test against the
  in-memory fake.
- The deterministic lowest-`PathId` tie-break is an arbitrary but
  documented convention; an operator relying on it to prefer one path
  over another with an identical declared cycle time should instead
  declare a distinguishing cycle time rather than depend on tie-break
  order.
