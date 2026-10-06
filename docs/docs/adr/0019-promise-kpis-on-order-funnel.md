---
id: 0019-promise-kpis-on-order-funnel
slug: /adr/0019-promise-kpis-on-order-funnel
title: 19. Promise KPIs on the Order Funnel data product (ADR 0014 §6, order-management half)
sidebar_label: 19. Promise KPIs on the Order Funnel
description: "ADR 0019 — widen ADR 0006's Order Funnel & Allocation Health data product with promise basis distribution, re-promise rate, split-shipment rate, and promise-to-cutoff gap, plus a new read-only get_promise_health MCP tool, per ADR 0014 §6. The order-management half of that section; on-time-to-CPT is fulfillment-execution's own companion analytics work."
---

# 19. Promise KPIs on the Order Funnel data product (ADR 0014 §6, order-management half)

## Status

Accepted. Amended 2026-10 (ADR-conformance audit): the basis distribution
now has a third bucket, `promiseBasisNetwork` (ADR 0020 §3), end to end —
projector, `report.Row`, REST `GET /reports/funnel`, and the
`get_promise_health` MCP tool (migration `0003_promise_basis_network`).
Before this, the projector had no `"Network"` case and silently dropped
every network-originated allocation from the distribution, so
`ordersAllocatedTotal` under-counted. `Network` promises remain excluded
from the promise-to-cutoff gap (their cutoff was dictated, not chosen).

## Context

ADR 0014 (the promise-derived-from-fulfillment-capability initiative) is
now fully shipped end to end within this service: the promise itself is a
CPT window (ADR 0014 §1–2), routing is eligibility-driven (ADR 0016),
promising is per-shipment-group (ADR 0017), and the feedback loop closes
with `OrderRepromised` (ADR 0018). The one piece of ADR 0014 left
unshipped is its own §6, verbatim:

> ADR 0006's Order Funnel report gains: promise basis distribution,
> re-promise rate, split-shipment rate, and promise-to-cutoff gap
> (`cutoffAt - allocatedAt`). On-time-to-CPT itself is measured where the
> evidence is, in fulfillment-execution's analytics (`cpt` vs
> `manifested_at`), not here. Both are exposed as read-only MCP tools per
> ADR 0010 so `warehouse-ops-agent` can reason about promise health
> without a REST detour.

ADR 0014 §6 spans two services. **This ADR is the order-management half
only**: the four KPIs it names as belonging here (promise basis
distribution, re-promise rate, split-shipment rate, promise-to-cutoff gap)
and the `get_promise_health` MCP tool. On-time-to-CPT is explicitly
fulfillment-execution's own analytics — a companion, separately-shipped
PR in that repo, whose real shipped shape this ADR does not describe
because it is not this service's ground truth to assert.

Before this ADR, none of the promise facts ADR 0014 introduced —
`PromiseBasis`, `PromiseCutoffAt`, per-shipment-group split, or
`OrderRepromised` — were visible anywhere in the analytical read side.
They existed only as OLTP domain state and (for `OrderAllocated`/
`OrderPartiallyAllocated`) as additive fields on the wire to
wes-work-planning. `OrderRepromised` specifically was not even published
onto the analytics topic (`warehouse.order-management.analytics`) at all
— the analytics publisher's `marshalData` switch, unchanged since before
ADR 0018, simply had no case for it.

## Decision

**Widen ADR 0006's existing Order Funnel & Allocation Health data
product** — same three processes, same one writer, same
`internal/analytics/report` isolated read-model region — with four new
KPI-bearing fields, and add one new read-only MCP tool,
`get_promise_health`, following process-path-management's ADR-0010
precedent naming convention (`get_cpt_schedule`) for a curated,
intent-level analytics read tool.

### 1. `report.Row` gains five new fields

`PromiseBasisCapability`, `PromiseBasisLeadTime` (the basis distribution:
raw counts, a caller computes the split itself — the same "expose counts,
not precomputed rates" convention the existing funnel counters already
use), `OrdersRepromised`, `OrdersSplitShipment`, and
`PromiseToCutoffGapSeconds` (the mean gap, plus a
`PromiseToCutoffGapSamples` weight so a caller aggregating multiple rows
computes a correctly *weighted* mean rather than naively averaging
per-bucket means). Every field is documented field-by-field in
`funnel.go`, matching that file's existing documentation density.

### 2. `OrdersRepromised` is deliberately NOT path-dimensioned

Every other counter on `report.Row` is keyed by `(PathId, HourBucket)`.
`OrderRepromised` (ADR 0018) carries no process path in its own domain
event — only `OrderID`, `CptIdOld`, `CptIdNew`, `Reason`. Two ways to give
it a path were considered:

- **Resolve a path via the existing `OrderRepo` lookup**, the same
  enrichment pattern `AnalyticsPublisher.orderPath`/`linePath` already use
  for every other event. Rejected: it would add a repo round-trip to a
  purely operational feedback-loop event whose own domain reasoning (ADR
  0018) never needed one, for a KPI whose natural unit of analysis is
  "how often does the fleet re-promise", not "how often does path X
  re-promise" — no consumer of this KPI asked for the per-path cut, and
  guessing a schema shape nobody needs is worse than an honest gap.
- **Track it fleet-wide, no path dimension** — the choice made. It lands
  in its own Postgres table, `repromise_rollup`, keyed by `hour_bucket`
  only (migration `0002_promise_kpis.up.sql`), not shoehorned into
  `funnel_rollup`'s `(path_id, hour_bucket)` primary key with an
  empty-string sentinel row that would collide with real "path lookup
  missed" semantics elsewhere in this data product. The in-memory/MCP-facing
  `report.Row` abstraction still surfaces it as the row whose `PathId` is
  `""`, matching this codebase's existing "empty string means
  unknown/inapplicable" convention (`AnalyticsPublisher.orderPath`'s own
  best-effort-miss behaviour) — so a `ReportStore.Query` caller sees one
  consistent shape, while the underlying storage keeps the two concerns
  physically separate. `PostgresReport.Query`'s SQL does the fold (a `UNION
  ALL` merging `repromise_rollup` onto the `path_id=""` funnel row for the
  same hour, or synthesizing a repromise-only row when no such funnel row
  exists) so a caller never needs to know two tables are involved.

### 3. `ApplyOrderAllocated`/`ApplyOrderPartiallyAllocated` are widened
in place, not split into a second method

The task brief asked whether to widen the existing Apply methods in place
or add new ones. Every `Apply*` method here claims its `eventId` exactly
once via `analytics_processed_events` (`INSERT ... ON CONFLICT DO
NOTHING`) before applying its effect. A hypothetical separate
`ApplyPromise(eventId, ...)` call keyed by the SAME `eventId` as
`ApplyOrderAllocated` would find the event already claimed by the first
call and silently skip its own effect — the promise KPIs would never be
recorded. Since basis/cutoff/split-shipment are read off the exact same
`OrderAllocated`/`OrderPartiallyAllocated` event as the funnel counter,
they MUST be applied in the SAME claim+transaction. The two existing
methods therefore gained three new parameters
(`basis string, cutoffAt *time.Time, splitShipment bool`) rather than a
new method being added alongside them — every existing call site (the
Kafka consumer, both Postgres and in-memory stores, every test) was
updated for the new signature; no old-shape call site remains.

### 4. The analytics publisher's `marshalData` gains the missing
`OrderRepromised` case, and enriches `OrderAllocated`/
`OrderPartiallyAllocated`

`marshalData` had cases for eight event types but not `OrderRepromised`
— a real gap, not something this ADR invents; the task brief flagged it
explicitly and `git show origin/develop` confirmed it. The new case
publishes `order_id`, `cpt_id_old`, `cpt_id_new`, `reason` — the same
shape already documented on the *integration* topic's `OrderRepromised`
message, now also on the analytics topic. It deliberately carries no
`path_id` (see §2 above).

`OrderAllocated`/`OrderPartiallyAllocated` gain `promise_basis` (mirrored
straight off the event, when non-empty), `promise_cutoff_at` (present only
for a Capability-basis promise — a LeadTime-basis "cutoff" is just
now-plus-a-configured-duration, not a real departure, so mixing it into
the gap's mean would measure the lead-time config rather than fulfillment
capability), and `split_shipment` (`len(o.PromiseGroups()) > 1`, via the
SAME `OrderRepo` lookup this file's existing `orderPath`/`linePath`/
`firstReleasedLinePath` helpers already establish as the precedent for
"look the order up to get something the domain event itself doesn't
carry" — best-effort, defaulting to `false` on a repo miss, exactly like
this file's existing path-enrichment failure mode).

### 5. Postgres: additive migration, sum+count pattern for the mean

`migrations/analytics/0002_promise_kpis.up.sql` adds five nullable-free,
defaulted columns to `funnel_rollup` (same `(path_id, hour_bucket)` grain
as every existing column) plus the new `repromise_rollup` sibling table.
`promise_to_cutoff_gap_seconds_sum`/`promise_to_cutoff_gap_samples` follow
the sum-and-count-columns-divided-in-the-reader pattern this fleet already
uses in fulfillment-execution's `throughput_rollup` for
`AvgClaimToCompleteSeconds` — the nearest real analog for "track a mean
via sum+count, divide client-side" in this fleet. `PostgresProjection`'s
new `applyAllocationOutcome` helper does the funnel-counter UPSERT and the
promise-KPI UPSERT in one `INSERT ... ON CONFLICT DO UPDATE` statement
inside the SAME claimed transaction (see §3).

### 6. In-memory test double, Kafka consumer, REST DTO, and the new MCP
tool all widened to match

`analyticsstore.MemoryStore` gained the same fields/behaviour as the
Postgres implementation (used by unit tests). The inbound Kafka analytics
consumer (`internal/adapters/inbound/kafka/analytics_consumer.go`)
decodes the new `promise_basis`/`promise_cutoff_at`/`split_shipment`
fields and routes `OrderRepromised` to `ApplyOrderRepromised`; its
`isProjecting` allow-list gained `"OrderRepromised"`. `reports_handler.go`'s
`funnelRowDTO` gained five new `camelCase` JSON fields, additively — every
existing field name/type is untouched, so an existing REST consumer sees
no change.

The new MCP tool, `get_promise_health` (naming taken verbatim from ADR
0014 §6's own text, matching process-path-management's `get_cpt_schedule`
naming precedent for the same "curated intent-level analytics read tool"
posture ADR 0006/0010's governance charter establishes), takes the same
`from`/`to`/`pathId` shape `GET /reports/funnel` already accepts, queries
the SAME `report.ReportStore`, and returns one aggregated summary: basis
distribution, split-shipment rate, re-promise rate (fleet-wide, sample-
weighted for the gap mean across rows — see `PromiseToCutoffGapSamples`'s
doc comment), and the promise-to-cutoff gap. It is registered
`ReadOnlyHint: true`, alongside the existing `get_order` tool.

**A real design constraint surfaced while wiring this tool**:
`internal/architecture/fitness_test.go`'s `TestMCPAdapterDependencyRule`
(ADR-0008) restricts `internal/adapters/inbound/mcp` to depend only on
`internal/application` and `internal/domain` — never on
`internal/analytics/report` directly. Rather than weaken that fitness
rule, the MCP package declares its OWN small port,
`PromiseHealthStore`/`PromiseHealthRow` (primitive-typed, no
`report.Row`/`report.ReportQuery` in the signature), and `cmd/mcp` — the
composition root, exempt from both layering rules by design — adapts the
real `report.ReportStore` into it at the wiring boundary. This mirrors,
in spirit, how `Deps.GetOrder` already depends on an application-layer use
case rather than an outbound adapter directly; here the arch-test
boundary sits one layer further out (the analytics region is its own
isolated read-model, not application/domain), so the adaptation happens
in `cmd/mcp` instead of in a use case.

### Alternatives considered

- **A `usecases.GetPromiseHealth` use case**, the initially-planned shape.
  Rejected once `internal/application/usecases`' own arch-test constraint
  (application depends only on domain) was checked: routing through a use
  case would just move the same "must not import
  `internal/analytics/report`" problem into a different arch-test-guarded
  layer, not solve it. The existing REST reader
  (`ReportsHandlers.GetFunnel`) already queries `report.ReportStore`
  directly from its adapter, without a use case in between — this tool
  follows that SAME established convention rather than inventing a
  second one.
- **Fold `OrdersRepromised` into `funnel_rollup` with a `path_id=''`
  sentinel row, written directly by `ApplyOrderRepromised`.** Rejected as
  a schema-collision risk: `funnel_rollup`'s `PRIMARY KEY (path_id,
  hour_bucket)` already treats `path_id=''` as meaningful (a
  best-effort-enrichment miss on any other event type), and letting two
  semantically different sources UPSERT the same row invites exactly the
  kind of cross-contamination bug this fleet's Postgres projection
  pattern is designed to avoid via one-claim-per-eventId. The separate
  `repromise_rollup` table plus a read-time `UNION ALL` fold (§2) keeps
  the write paths cleanly separated while still presenting one shape to
  a `Query` caller.

## Consequences

**Easier**

- `warehouse-ops-agent` (or any MCP-calling agent) can now ask "how
  healthy is our promise-making" in one call — basis distribution,
  split-shipment rate, re-promise rate, promise-to-cutoff gap — without a
  REST detour or hand-rolled aggregation over the raw funnel export.
- ADR 0014's entire six-section scope is now shipped end to end within
  order-management. The only remaining ADR-0014-adjacent work is
  fulfillment-execution's own companion on-time-to-CPT analytics (a
  separate repo, separate PR, not this service's) and later e2e-tests
  scenarios exercising both together.
- Every existing funnel counter, REST field, and MCP tool is byte-for-byte
  unchanged — verified by the existing `TestReports_GetFunnel_OK`/
  `TestMemoryStore_FunnelCountersIdempotent`/
  `TestPostgresProjectionAndReport_RoundTrip` tests still passing
  unmodified in their pre-existing assertions.

**Harder**

- Two storage shapes now answer one logical "funnel report" query
  (`funnel_rollup` plus `repromise_rollup`), joined at read time — a real,
  documented complexity trade accepted in §2/the alternatives section
  rather than a schema simplification deferred to "later".
- `OrdersRepromised`'s lack of a path dimension is a genuine, honest
  information loss: this data product cannot currently answer "which
  process path re-promises the most", only "how often does the fleet
  re-promise". If that question becomes real, it requires either
  widening `OrderRepromised`'s own domain event with a path (ADR 0018's
  concern, not this one's) or accepting the `OrderRepo`-lookup cost this
  ADR explicitly declined.
- The MCP adapter's own arch-test isolation rule (ADR-0008) meant the
  promise-health query logic could not simply reuse
  `report.ReportStore`/`report.ReportQuery` directly inside the MCP
  package the way the REST handler does — it needed its own small port
  plus a translation adapter in `cmd/mcp`. This is one more indirection
  layer than the REST reader has for the equivalent read, a real
  (small) cost of keeping the MCP surface's dependency boundary honest.

## References

- [ADR-0006 — Per-service analytical data product](./0006-analytical-data-product.md)
- [ADR-0010 — MCP as an inbound adapter, not a new service](./0010-mcp-inbound-adapter.md)
- [ADR-0014 — The delivery promise is a CPT window derived from fulfillment capability](./0014-promise-derived-from-fulfillment-capability.md)
- [ADR-0017 — Per-shipment-group promising](./0017-per-shipment-group-promising.md)
- [ADR-0018 — RepromiseOrder consumer and OrderRepromised](./0018-repromise-order-consumer-and-order-repromised.md)

## Verification (real output, this repository)

```
$ gofmt -l .
(clean)

$ go vet ./...
(clean)

$ make check
gofmt: clean
go vet ./...
go build ./...
golangci-lint run ./...
0 issues.
go test ./... -race
ok  	github.com/claudioed/order-management	...
... (all packages ok)

$ make coverage
Coverage: 93.6% (gate: 90%)

$ make arch-test
--- PASS: TestHexagonalDependencyRule (7.70s)
--- PASS: TestAnalyticsIsolation (1.47s)     # internal/analytics/report still
                                              # depends on nothing internal
--- PASS: TestPortsAreCustomerOwned (1.50s)
--- PASS: TestMCPAdapterDependencyRule (1.50s)  # mcp adapter still depends
                                                 # only on application+domain
--- PASS: TestNoAuthMiddlewareReintroduced
--- PASS: TestKafkaConsumerGroupNeverHardcodedInline
--- PASS: TestKafkaIntegrationTestsUseTestcontainers
ok  	github.com/claudioed/order-management/internal/architecture	12.394s

$ make mutation-fast
Killed: 63, Lived: 6, Not covered: 10
Test efficacy: 91.30%   (gate: > 89%)
Mutator coverage: 87.34%  (gate: > 83%)

$ make bdd
10 scenarios (10 passed)
75 steps (75 passed)
--- PASS: TestFeatures (0.01s)

$ make integration    # real Kafka via testcontainers, all 5 packages
ok  	.../internal/adapters/outbound/kafka	          24.397s
ok  	.../internal/adapters/outbound/kafkacatalog	 105.259s
ok  	.../internal/adapters/outbound/kafkacptschedule  71.437s
ok  	.../internal/adapters/outbound/kafkapathcapacity  99.873s
ok  	.../internal/adapters/inbound/kafka	           29.040s

# Postgres analytics round-trip, against a real local Postgres
# (docker-compose.yml's postgres service, database order_analytics),
# not testcontainers (this store's existing integration test pattern,
# unchanged by this ADR):
$ ANALYTICS_DATABASE_URL=postgres://order:order@localhost:5434/order_analytics?sslmode=disable \
  go test -tags=integration ./internal/adapters/outbound/analyticsstore/... -v
--- PASS: TestPostgresProjectionAndReport_RoundTrip   (pre-existing, unmodified assertions)
--- PASS: TestPostgresProjectionAndReport_PromiseKPIs (new: basis distribution,
                                                         split-shipment, gap mean,
                                                         path-free repromise counter,
                                                         all idempotent on re-apply)
--- PASS: TestReadOnlyPool_RejectsWrites
--- PASS: TestFreshnessLag_EmptyStore
```

Unverified in this session: `make vuln` (govulncheck) and the docs site
build (`cd docs && npm ci && npm run build`) were not both re-run to
completion inside this same session's shell state before writing this
ADR; `apis/asyncapi.yaml`'s `OrderRepromised` analytics-topic message
documentation and `docs/docs/adr/about.md`/`docs/sidebars.ts`/
`.claude/rules/adrs.md` registration are updated in this same PR — see
the PR description for the exact build/link-check output run against
this branch before it was opened.
