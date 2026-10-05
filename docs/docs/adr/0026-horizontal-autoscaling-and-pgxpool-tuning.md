---
id: 0026-horizontal-autoscaling-and-pgxpool-tuning
slug: /adr/0026-horizontal-autoscaling-and-pgxpool-tuning
title: 26. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning
sidebar_label: 26. HPA + pgxpool tuning
description: "ADR 0026 — Phase 3 (scalability) for order-management: an autoscaling/v2 HorizontalPodAutoscaler per independently-assessed workload (api max 4, analytics-projector max 2, analytics-reports max 3, frontend max 3; mcp explicitly excluded for a real in-memory-session reason), all default-disabled via values.yaml so this PR changes nothing on merge; plus explicit pgxpool.Config MaxConns caps and per-pool statement_timeout values, sized against the shared Postgres instance's real max_connections=100 ceiling."
---

# 26. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning

## Status

Accepted — implemented in the same change that introduces this record.
This is Phase 3 (scalability) of the fleet production-readiness plan,
built on top of Phase 0 (boot-retry), Phase 1 (idempotency), and Phase 2
(resilience, ADR-0025). Phase 3.1 (the chart-selector-check fix so every
Deployment's `spec.selector` is scoped correctly) was already done and
verified fleet-wide before this PR started — all 10 fleet charts already
pass `warehouse-infra`'s chart-selector-check script. This PR is the
reference the other ~9 fleet repos in this phase copy, the same role
order-management's ADR-0025 played for Phase 2's resilience wave.

## Context

Before this change, `order-management`'s Helm chart had exactly one
`HorizontalPodAutoscaler` template, unconditionally targeting the `api`
Deployment only, wired to a flat `autoscaling.enabled/minReplicas/
maxReplicas/targetCPUUtilizationPercentage` block — untested against the
other four Deployments this chart renders (`mcp`, `frontend`,
`analytics-projector`, `analytics-reports`), and never assessed for
whether scaling `analytics-projector` past 1 replica was even safe given
its Kafka consumer-group membership. `replicaCount` was hardcoded to 1
fleet-wide with no HPA anywhere else in the 10-service fleet either (a
search of `warehouse-infra`'s Terraform turns up only 2 incidental
matches for the word "autoscaling" — HPA is entirely a chart-level
concern here, not an infra-level one).

Separately, no pool in the codebase set an explicit `pgxpool.Config.
MaxConns`, so every pool — the OLTP pool (`internal/adapters/outbound/
postgres/pool.go`, used by `cmd/order` and `cmd/mcp`) and the two
analytics pools (`internal/adapters/outbound/analyticsstore/pool.go`'s
`NewPool` used by `cmd/order-projector`, and `NewReadOnlyPool` used by
`cmd/order-reports`) — ran on pgx's library default, `max(4,
runtime.NumCPU())` connections per process. No pool set a
`statement_timeout` either, so a single runaway query (a bad index, a
lock wait, an unbounded `report.ReportStore` date range) could hold a
pooled connection indefinitely, with nothing to cancel it.

This matters together, not separately: turning on HPA for a Postgres-
backed workload without an explicit, bounded `MaxConns` means the
service's real connection ceiling becomes "however many CPUs the node
happens to have, times however many replicas the HPA happens to have
scaled to" — an unbounded, indirect function of cluster autoscaling and
CPU load, not a number anyone chose. This PR does both together on
purpose: HPA without a bounded `MaxConns` would have been the actual
production-readiness gap.

### Finding: shared vs. dedicated Postgres, and the real max_connections

Checked before picking any number, not assumed: `warehouse-infra/
terraform/postgres.tf` provisions a single Postgres StatefulSet/service
per environment — there is **no per-service Postgres instance or
database-per-tenant split**. Every service's `locals.tf` entry that
needs OLTP or analytics Postgres points at the SAME running Postgres
server (pod `postgres-postgresql-0` in the `warehouse-data` namespace),
each with its own logical database, not its own Postgres process.
Confirmed live against the actual cluster:

```
$ kubectl -n warehouse-data exec postgres-postgresql-0 -- \
    psql -U postgres -tAc "SHOW max_connections;"
100
```

`max_connections=100` is the **unmodified Bitnami chart default** —
`warehouse-infra`'s Terraform has zero explicit `max_connections`/
`postgresql.conf` overrides anywhere; it has never been deliberately
sized for the fleet's real connection demand. This is a genuinely
conservative, shared-instance ceiling: `order-management` is one of up
to 10 fleet services drawing from the same 100-connection pool, not a
dedicated database it can spend freely.

## Decision

### 1. HorizontalPodAutoscaler — one per independently-assessed workload

Assessed each of the five Deployments this chart renders on its own
merits — statefulness, and (for Kafka consumers) consumer-group-id
convention — rather than blanket-enabling HPA everywhere:

| Deployment | HPA? | min | max | target CPU | Why |
|---|---|---|---|---|---|
| `api` (`cmd/order`) | Yes | 1 | 4 | 70% | Stateless OLTP HTTP. Its only per-process caches (`PATH_CATALOGUE_SOURCE=kafka` local caches — `kafkacatalog`/`kafkacptschedule`/`kafkapathcapacity`) are read-only and rebuilt from a full Kafka replay on every process start via a **per-process-unique** consumer group id — safe to run N independent copies of by design. Its inbound `RepromiseConsumer` (ADR-0018) uses a **stable, shared** consumer group (`order-management-repromise`) — the fleet's normal horizontally-scalable pattern, N replicas share partitions via ordinary Kafka group rebalancing. Nothing here breaks at N>1. |
| `analytics-projector` (`cmd/order-projector`) | Yes, capped at **2**, not api's 4 | 1 | 2 | 70% | Its analytics Kafka consumer (`kafka.AnalyticsConsumerGroup`, ADR-0006/0019) is also a **stable, shared** group with no per-instance uniqueness, verified in `internal/adapters/inbound/kafka/analytics_consumer.go`; the publisher partitions by `OrderId` (`kafka.AnalyticsPublisher.marshalData`), so 2 replicas legitimately share the topic's partitions via normal rebalancing — the same safe shape as `api`'s `RepromiseConsumer`. Capped at 2 rather than left at 4 for two additional reasons, not correctness: (a) OrderId-keyed partitioning means ordering is only guaranteed per-order, so a wide fan-out buys little for what is a lightweight idempotent-upsert workload; (b) every write is already idempotent on `event_id` via `ConsumedEventsRepo.MarkProcessed` (ADR-0006), so correctness does not regress at 2, but there is no throughput case yet that justifies more. |
| `analytics-reports` (`cmd/order-reports`) | Yes | 1 | 3 | 70% | Stateless read-only REST reader over its own read-only pgxpool (`analyticsstore.NewReadOnlyPool`) — no in-memory state, no Kafka consumption. Same treatment as `api`. |
| `frontend` (nginx-unprivileged serving the built SPA bundle) | Yes | 1 | 3 | 70% | Pure static-asset serving. No server-side session, no per-request state. The most trivially horizontally-scalable workload in this chart. |
| `mcp` (`cmd/mcp`) | **No — deliberately excluded, not just disabled** | — | — | — | The `github.com/modelcontextprotocol/go-sdk` `StreamableHTTPHandler` this adapter wraps keeps **per-process, in-memory session state** keyed by the MCP protocol's own `Mcp-Session-Id` header (a real multi-request session, not just a TCP/HTTP connection). `charts/order-management/templates/mcp-service.yaml` is a plain `ClusterIP` Service with no `sessionAffinity` configured, so under >1 replica a second request carrying the same `Mcp-Session-Id` (e.g. `tools/call` following an earlier `initialize`) could land on a different pod than the one that created the session, which has never heard of it and would reject or silently start a new one. Fixing it for real needs either `sessionAffinity: ClientIP` (a partial mitigation only) or wiring the SDK's `StreamableHTTPOptions.EventStore` to a shared/external session store — a real code change, out of scope for this chart-and-pool-tuning PR. `mcp.replicaCount` stays a plain, manually-set value; no `autoscaling.mcp` block exists in `values.yaml` at all. Revisit if/when `cmd/mcp` adopts an external session store or the SDK's stateless mode. |

Every enabled block is namespaced under a single top-level `autoscaling:`
key in `values.yaml`
(`autoscaling.<api|projector|reports|frontend>.{enabled,minReplicas,
maxReplicas,targetCPUUtilizationPercentage}`), **every `enabled` value
defaults to `false`**. This PR makes per-workload HPA possible and
verified-correct; it deliberately does not turn any of it on — the
fleet enables each workload's HPA later, once, as a conscious rollout
decision, the same "ship the mechanism, not the behavior change" shape
Phase 1's outbox/idempotency work used.

**No replicas-vs-HPA fight.** Each Deployment template guards its
`spec.replicas` field with `{{- if not .Values.autoscaling.<x>.enabled
}}` — when a workload's HPA is enabled, its Deployment renders with NO
`replicas` field at all (a hardcoded `replicas:` next to an active HPA
would otherwise fight it on every reconcile, most visibly right after a
`helm upgrade` resets it back to the chart's static value). Verified
directly with `helm template`:

- Default values → 0 `HorizontalPodAutoscaler` resources render, every
  Deployment keeps its static `replicas:` field.
- All four `autoscaling.*.enabled=true` → exactly 4
  `HorizontalPodAutoscaler` resources render (one per scalable
  workload, `mcp` has none by design), and none of those four
  Deployments has a `replicas:` field — `mcp`'s Deployment still does.
- Only `autoscaling.api.enabled=true` → exactly 1 HPA renders, only the
  `api` Deployment loses its `replicas:` field; `projector`/`reports`/
  `frontend` keep theirs untouched. Mixed enablement is safe and
  independent per workload, as designed.

`helm lint` passes; `go vet`/`gofmt`/`make check`/`make arch-test` all
pass unchanged (no Go code path is affected by the chart changes).

### 2. pgxpool MaxConns

All three pools now set an explicit `pgxpool.Config.MaxConns` instead of
inheriting the CPU-derived library default (`max(4, runtime.NumCPU())`):

| Pool | Used by | `MaxConns` | Reasoning |
|---|---|---|---|
| OLTP (`postgres.NewPool`) | `cmd/order` (`api`), `cmd/mcp` (`mcp`) | **10** | `api`'s HPA ceiling of 4 replicas × 10 = 40 connections, ~40% of the shared instance's `max_connections=100` for this ONE of up to 10 fleet services' OLTP path alone — the plan's stated conservative budget, deliberately leaving the remaining ~60% for the other 9 services (and this service's own mcp/projector/reports processes) sharing the same Postgres instance. |
| Analytics writer (`analyticsstore.NewPool`) | `cmd/order-projector` | **5** | The projector's HPA is capped at 2 replicas (see the table above — it is NOT fixed at 1), and does single-row `ON CONFLICT` upserts against one `(path_id, hour_bucket)` key at a time; a small, flat pool is enough (2 × 5 = 10 at its ceiling). |
| Analytics reader (`analyticsstore.NewReadOnlyPool`) | `cmd/order-reports` | **5** (`ReportsMaxConns`) | `reports` IS HPA-scalable (max 3); at that ceiling, 3 × 5 = 15 connections against the analytical database — comfortably inside the shared ceiling alongside the OLTP path's 40. |

Worst case across every workload simultaneously at its proposed HPA
maximum (`api` 4 × 10 = 40, `projector` 2 × 5 = 10, `reports` 3
× 5 = 15; `mcp` has no HPA, assume 2 manually-set replicas × 10 = 20):
40 + 10 + 15 + 20 = **85** of the shared instance's 100 connections for
this ONE service alone — even at every proposed HPA ceiling
simultaneously, with **HPA still disabled by default today** (at
`replicaCount: 1` everywhere and no HPA enabled, this service's actual
usage is `10 (api) + 10 (mcp, if deployed) + 5 (projector) + 5
(reports)` = at most 30 connections, 30% of the ceiling). That 85-at-
max-scale number is presented to be honest about the ceiling, not to
claim it's comfortable — it leaves only 15 connections, 15% of
`max_connections`, for the other 9 fleet services if they were all
simultaneously maxed too. That is a real, documented residual risk (see
Consequences), not one this PR can fully close alone: it depends on
what MaxConns values the other 9 services' own Phase 3 PRs choose.

If/when the fleet turns on multiple workloads' HPA simultaneously and
this budget gets tight in practice, the next lever is `max_connections`
itself — an unexamined Bitnami default, not a deliberately chosen
number — which is a `warehouse-infra` Terraform change and a separate,
cross-service decision, out of scope here.

### 3. statement_timeout

All three pools set `statement_timeout` via `pgxpool.Config.
AfterConnect`, running `SET statement_timeout = '<value>'` on every new
physical connection as it's established (not per-query, so it survives
connection reuse across pooled acquisitions). Values differ per pool
because their query shapes differ:

| Pool | `statement_timeout` | Reasoning |
|---|---|---|
| OLTP (`postgres.StatementTimeout`) | **5s** | Every OLTP query (`ReceiveOrder`, `AllocateOrder`, `GetOrder`, the promise/repromise paths) is a single-aggregate read/write keyed by id, normally low-single-digit milliseconds. 5s is roughly 1000x that — generous headroom for real transient contention (a lock wait behind a concurrent writer) without ever being a normal-path concern, while bounding the absolute worst case tightly since this is the interactive, latency-sensitive path and also the pool with the most connections (40 at max HPA scale) to protect. |
| Analytics writer (`analyticsstore.StatementTimeout`) | **10s** | A Kafka consumer replaying a backlog after a redeploy issues upserts in a tight loop; a transient lock wait here doesn't need to be as tight as an interactive OLTP request. Still bounded — at its HPA ceiling the projector runs at most 2 replicas, so an unbounded query here would stall half the analytics pipeline's throughput (and ALL of it at the default replicaCount of 1), not just one of several `api` pods, which is exactly why it isn't left unbounded either. |
| Analytics reader (`analyticsstore.ReportsStatementTimeout`) | **15s** | The funnel report aggregates rows across a caller-chosen `[From, To)` time range (`postgres_report.go`) — wider than the OLTP side's always-single-aggregate-by-id shape — so it gets more headroom, but still a hard ceiling: a caller-supplied wide date range must not be able to hold a reports connection forever. |

Verified with a real Postgres via testcontainers
(`internal/adapters/outbound/postgres/pool_limits_integration_test.go`,
`-tags=integration`), not a mock and not just reading `pg_settings`:

- `TestNewPool_AppliesStatementTimeoutToNewConnections` — opens a pool
  against a real `postgres:16-alpine` container with a short
  test-only timeout (200ms, via the shared `NewPoolWithLimits` the
  production `NewPool` wraps), confirms `SHOW statement_timeout` reads
  back `200ms` on a freshly acquired connection, then runs `SELECT
  pg_sleep(2)` and asserts Postgres itself cancels it (SQLSTATE 57014,
  "canceling statement due to statement timeout") rather than letting
  it run the full 2s — proving the setting is genuinely enforced
  server-side, not merely set and ignored — and finally confirms the
  pool is still usable afterward (the cancelled statement doesn't
  poison the connection).
- `TestNewPool_AppliesMaxConns` — acquires exactly `maxConns`
  connections from a pool configured with `MaxConns=2`, then asserts a
  further `Acquire` blocks until `context.DeadlineExceeded`, proving
  `MaxConns` is the pool's real, enforced ceiling rather than advisory.

Both tests pass locally against a real Postgres container.

## Consequences

- HPA is now possible, correct, and independently verified per
  workload for four of this chart's five Deployments — but **off by
  default everywhere**. Merging this PR changes nothing about
  production replica counts; `MaxConns`/`statement_timeout` are the
  only behavior change that takes effect on deploy, and both are
  conservative relative to today's unbounded defaults (they can only
  reduce, never increase, worst-case connection usage and hung-query
  duration).
- `mcp` remains explicitly un-autoscaled, with the exact reason
  (in-memory session state, no sticky routing) recorded here and in
  `values.yaml`'s comments, so a future contributor doesn't
  mechanically copy `api`'s HPA block onto it without re-solving the
  session-affinity problem first.
- The worst-case 80-of-100-connections number for this service alone,
  at every proposed HPA ceiling simultaneously, leaves only 20
  connections (20%) for the other 9 fleet services if they are all
  maxed at the same moment. That is an honest, documented residual
  risk, not a solved one — it depends on what `MaxConns` numbers the
  other services' own Phase 3 PRs choose, and is worth a fleet-wide
  follow-up (outside this PR's scope) once more of those PRs land, to
  check the sum across all 10 services against the real ceiling rather
  than each service reasoning about its own slice in isolation.
- A read replica for the analytics/reports read path (the plan's final
  Phase 3.4 bullet) is explicitly OUT OF SCOPE for this PR — not
  evaluated, not designed, not decided. Revisit only once actually
  needed and confirmed by the person requesting it.
- `max_connections=100` itself is an unexamined Bitnami chart default,
  not a value anyone has deliberately sized for this fleet's real
  demand. This ADR treats it as a hard external constraint to work
  within, not something in scope to change.
