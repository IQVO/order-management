---
id: 0006-analytical-data-product
slug: /adr/0006-analytical-data-product
title: 0006. Per-service analytical data product (report) via a separate analytics topic
sidebar_label: 0006. Analytical data product
description: ADR 0006 — an analytical read model (the "Order Funnel & Allocation Health" report) built from this service's own domain events on a dedicated warehouse.order-management.analytics topic, projected into a separate analytical database and served by a read-only reports binary over REST — a lightweight data mesh with no central data platform.
---

# 0006. Per-service analytical data product (the "report")

## Status

> **Superseded by ADR-0030** (the analytics "Envelope v1" with `schema_version` described here). Every Kafka message is now a
> CloudEvents 1.0 event — see
> [ADR 0030](./0030-cloudevents-mandatory-event-envelope.md). The rest of
> this record stands.

Accepted.

## Context

The warehouse-systems estate needs a per-service **report** that supports
analytics while each service stays the **OLTP** system of record for its own
bounded context. The requirement, stated deliberately simply: *follow data-mesh
principles, but without standing up a whole data platform.* No central
warehouse, no lake, no shared ETL team.

Order Management already has everything the analytical side needs as a
substrate:

- Past-tense **domain events** (`OrderReceived`, `OrderLineAllocated`,
  `OrderLineBackordered`, `OrderAllocated`, `OrderPartiallyAllocated`,
  `OrderAllocationPartiallyFailed`, `OrderLineReleased`, `OrderReleased`,
  `OrderCancelled`) raised by the Order aggregate.
- A Kafka **integration** path (`warehouse.order-management.events`) already
  carrying `OrderAllocated` / `OrderPartiallyAllocated` to wes-work-planning,
  with the CloudEvents-like envelope and OTel trace propagation established in
  [ADR-0005](./0005-choreographed-release-via-kafka.md).
- The single-event `EventPublisher` port already used by the log, Postgres and
  Kafka publishers.

So the event backbone exists; what is missing is the **analytical read side**.
The forces shaping the decision:

- **The integration contract must not become coupled to reporting.** The report
  needs many more event types than the integration topic exposes, and they
  change on a different cadence. Widening `warehouse.order-management.events`
  with analytics-only event types would risk surprising wes-work-planning's
  existing consumer and entangle two contracts that should evolve separately.
- **Analytics must never contend with OLTP.** A report query load, a long
  aggregation, or a projection rebuild must not touch the transactional
  database that serves order intake and allocation.
- **The service still owns its data as a product.** Data-mesh domain ownership
  means the read side lives in this repo, owned by the same team, with a
  contract, an owner, and a freshness SLA — not shipped off to a central team.
- **No new central platform.** Reuse what the estate already runs: Kafka,
  Postgres, chi, the Helm chart.

## Decision

**Order Management owns an analytical data product built solely from its own
domain events, delivered on a dedicated analytics topic, projected into a
separate analytical database, and served read-only over REST. Three processes;
one writer.**

### 1. Separate analytics topic

A new outbound adapter publishes the report-input event set to
**`warehouse.order-management.analytics`** (note the hyphen in the context
segment — it matches the integration topic exactly), using the shared
**Envelope v1** wrapper (`event_id`, `event_type`, `occurred_at`, `source`,
`schema_version`, `data`) with a per-`event_type` snake_case `data` payload
keyed by `order_id`. The existing integration publisher and
`warehouse.order-management.events` are **left untouched**, so no existing
consumer is affected. Analytics consumers switch on `event_type`, dedupe on
`event_id`, and ignore unknown types.

Because the report is keyed by **PathId** but the order-level events do not
carry a path, the analytics publisher **enriches** each event with its process
path via an `OrderRepo` lookup: an order's path is its first line's path (a v1
simplification that is exact because intake places every line of an order on
the same default path), and line-level events use their own line's path
(`OrderLineReleased` carries it directly).

### 2. Separate analytical database

Projections land in a **separate analytical database** with its own credentials
(`ANALYTICS_DATABASE_URL`), its own golang-migrate migration set
(`migrations/analytics/`), and a **read-only role** for the reader. Baseline is
a dedicated `*_analytics` database in the existing Postgres release; the
`ANALYTICS_DATABASE_URL` seam allows promotion to a physically separate instance
later without code changes. The OLTP `DATABASE_URL` database is never opened by
the analytical side.

### 3. Three processes, one writer

- **`cmd/order`** — the OLTP binary. Unchanged, except its composition root
  additionally publishes domain events to the analytics topic when
  `EVENT_PUBLISHER=kafka` (a fan-out over the untouched integration publisher
  and the new analytics publisher).
- **`cmd/order-projector`** — the analytics **writer**. Consumes
  `warehouse.order-management.analytics` (consumer group
  `order-management-analytics`, reading from the earliest offset), applies
  idempotent projections, and is the **only** writer of the analytical
  database. Runs the analytical migrations on start.
- **`cmd/order-reports`** — the **read-only reader**. Opens the analytical
  database with the read-only role and serves `GET /reports/funnel` and
  `GET /reports/funnel/freshness`. Never writes, never migrates.

### 4. Served over REST

The reports binary serves the REST report resource. There is **no MCP adapter**
in this service (none exists in v1 — see CLAUDE.md's deferred scope), so the
curated MCP report tool the estate's pilot added is deliberately **out of scope
here**; when an MCP inbound adapter is introduced, a read-only
`get_order_management_funnel_report` tool calling the reports REST is the
intended follow-up.

### 5. The report

An **Order Funnel & Allocation Health** read model, keyed per **PathId × hour
bucket**: the order funnel `received → allocated → released`, with
cancellations and hard allocation failures as leakage, alongside line-level
allocation, backorder and release counts. It is a **projection** from events,
eventually consistent to a freshness SLA (p95 event-to-report lag < 30s), not
real-time.

The analytical read model lives in a new `internal/analytics/report` region
that depends on nothing; the consumer and store adapters depend on it. The OLTP
**domain and application layers are not modified**, and the analytics consumer
owns its own `ProcessedEvents` idempotency port rather than reaching into the
OLTP application ports.

## Consequences

### Easier

- **The integration contract is untouched**, so widening what analytics
  consumes never risks an integration consumer.
- **Analytics cannot contend with OLTP** — separate database, separate
  connection, read-only reader pool (`default_transaction_read_only=on`) on top
  of the read-only DB role.
- **The report is rebuilt purely from events** — no dual-write from OLTP, so the
  transactional write path gains no new failure mode. The read model can be
  rebuilt from scratch by replaying the topic from the earliest offset.
- **No central platform.** Everything reuses the estate's Kafka, Postgres and
  chi.
- **Least privilege by construction.** The read-only reader makes "a report can
  never corrupt the analytical store" a hard guarantee, not a convention.

### Harder

- **One more topic, two more binaries, and a second database** to operate.
- **Eventual consistency.** The report lags the OLTP truth by the freshness SLA;
  it is not a real-time view.
- **The analytics publisher is a second producer path** for the same domain
  events, and it must enrich each event with its path via a repo lookup; the
  event set it publishes must be kept in step with the report's inputs.
- **First deploy has an empty report** until events flow; historical backfill
  requires replaying `warehouse.order-management.analytics` from earliest into a
  fresh projector, so Kafka retention must cover the desired backfill window.

## References

- Report contract: [Order Funnel & Allocation Health report](../analytics/order-funnel-report.md)
- [ADR-0005 — Choreographed release via Kafka](./0005-choreographed-release-via-kafka.md)
