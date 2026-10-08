---
slug: /adr
title: Architecture Decision Records
sidebar_label: About ADRs
description: Why this service keeps ADRs, the template it uses, and how to propose a new one.
---

# Architecture Decision Records

## What these are

An **Architecture Decision Record (ADR)** captures a single
architecturally significant decision, its context, and its consequences —
at the moment it was made, in the words that were true then.

The value is not the decision; it is the **context**. Six months later,
the code shows *what* was chosen and can never show *why*, or what the
alternatives were, or what was known at the time. A reader who does not
know the why has two bad options: assume the decision was arbitrary and
change it, or assume it was sacred and work around it. An ADR gives them a
third.

## The format

These records use **Michael Nygard's template** — the de facto standard,
the same one every other `warehouse-systems` service uses:

```markdown
# NNNN. Title (a short noun phrase)

## Status
Accepted | Proposed | Deprecated | Superseded by ADR-XXXX

## Context
The forces at play: technical, business, operational. What is true that makes
this a decision rather than an obvious default? Written in full sentences and
in the present tense, describing the situation as it is.

## Decision
The response to those forces, stated actively: "We will…"

## Consequences
What becomes easier and what becomes harder — both. A record with only
positive consequences has not been thought about.
```

One decision per file, numbered `0001-`, `0002-`, … Numbers are never
reused and files are never deleted.

## Immutability

**An accepted ADR is never edited to change its decision.** If a decision
is reversed, write a new ADR that supersedes it, and update the old one's
*Status* line to point at the successor. The historical record of what was
believed at the time is the entire asset.

Typos, broken links and formatting are of course fair game.

## The records

| # | Title | Status |
| --- | --- | --- |
| [0001](./0001-hexagonal-ports-and-adapters.md) | Hexagonal (ports & adapters) architecture | Accepted |
| [0002](./0002-http-consumer-of-inventory-and-wes-not-shared-code.md) | HTTP consumer of inventory-storage and wes-work-planning, not shared code | Accepted; release portion partially superseded by ADR 0005 |
| [0003](./0003-ship-complete-default-and-fail-closed-allocation.md) | Ship-complete by default and fail-closed allocation | Accepted |
| [0004](./0004-cancellation-boundary-at-release.md) | The cancellation boundary is release | Accepted, with a documented known gap |
| [0005](./0005-choreographed-release-via-kafka.md) | Choreographed release via Kafka, folded allocate-then-release, and pathId goes internal-only | Accepted; envelope superseded by ADR 0030 |
| [0006](./0006-analytical-data-product.md) | Per-service analytical data product (report) via a separate analytics topic | Accepted; Envelope v1 superseded by ADR 0030 |
| [0007](./0007-adopt-fleet-micro-frontend-console.md) | Adopt the fleet's micro-frontend console architecture (`order-mgmt-mfe`) | Accepted |
| [0008](./0008-fulfillment-class-demand-shape-classifier.md) | FulfillmentClass — a demand-shape classifier, not a process-path name | Accepted |
| [0009](./0009-standard-metrics-convention.md) | Standard metrics convention across the fleet (telemetry.Setup, otelchi, Tier-2 business-metric naming) | Accepted |
| [0010](./0010-mcp-inbound-adapter.md) | Model Context Protocol as an inbound adapter, not a new service | Accepted; auth section superseded by ADR 0012 |
| [0011](./0011-adopt-fleet-rest-identity.md) | Adopt the fleet REST identity (static bearer keys, read/read-write scopes) | Superseded by ADR 0012 |
| [0012](./0012-remove-rest-mcp-bearer-auth.md) | Remove the REST/MCP bearer auth layer | Accepted |
| [0013](./0013-process-path-selection-as-a-domain-policy.md) | Process-path selection as a real domain policy, validated against a live catalogue | Accepted; extended by ADR 0021 |
| [0014](./0014-promise-derived-from-fulfillment-capability.md) | The delivery promise is a CPT window derived from fulfillment capability, not a configured lead time | Accepted |
| [0015](./0015-wes-work-planning-path-capacity-changed-wired.md) | wes-work-planning's PathCapacityChanged is wired as the real PathCapacity adapter | Accepted; flat-envelope dispatch superseded by ADR 0030 |
| [0016](./0016-eligibility-driven-process-path-selection.md) | Eligibility-driven process-path selection (ADR 0014 step B, routing only) | Accepted; extended by ADR 0021; live classification lookup superseded by ADR 0036 |
| [0017](./0017-per-shipment-group-promising.md) | Per-shipment-group promising (ADR 0014 step B, the second half) | Accepted |
| [0018](./0018-repromise-order-consumer-and-order-repromised.md) | RepromiseOrder consumer and OrderRepromised — closing ADR 0014's feedback loop | Accepted; flat inbound envelope superseded by ADR 0030 |
| [0019](./0019-promise-kpis-on-order-funnel.md) | Promise KPIs on the Order Funnel data product (ADR 0014 §6, order-management half) | Accepted; amended 2026-10 (Network basis bucket) |
| [0020](./0020-network-originated-demand-hold-and-deadline-feasibility.md) | Network-originated demand — release-on-allocation, deadline feasibility, and the Network promise basis | Accepted |
| [0021](./0021-multi-path-attribute-driven-routing.md) | Multi-path attribute-driven routing — closes ADR-0013's original deferral | Accepted |
| [0022](./0022-transactional-outbox.md) | Transactional outbox for order-management's dual-topic event publishing | Accepted |
| [0023](./0023-idempotency-key-middleware.md) | Transactional Idempotency-Key middleware for POST /orders | Accepted |
| [0024](./0024-optimistic-concurrency-version-column.md) | Optimistic concurrency (version column) for the Order aggregate | Accepted |
| [0025](./0025-resilience-circuit-breakers-retry-dlq-shutdown.md) | Resilience — circuit breakers, retry, DLQ, graceful shutdown | Accepted; product-classification breaker and retry superseded by ADR 0036 |
| [0026](./0026-horizontal-autoscaling-and-pgxpool-tuning.md) | Horizontal autoscaling and pgxpool tuning | Accepted |
| [0027](./0027-kafka-integration-publisher-partition-key.md) | Partition key (OrderId) on the integration publisher's Kafka messages | Accepted |
| [0028](./0028-inventory-storage-reservations-idempotency-key.md) | Idempotency-Key on inventory-storage reservation calls | Accepted |
| [0029](./0029-migrations-direct-postgres-connection.md) | Migrations over a direct (non-PgBouncer) Postgres connection | Accepted |
| [0030](./0030-cloudevents-mandatory-event-envelope.md) | CloudEvents 1.0 as the mandatory event envelope | Accepted |
| [0031](./0031-consume-warehouse-planning-capacity-plans.md) | Consume warehouse-planning's capacity plans into a local planned-capacity read model | Accepted |
| [0032](./0032-housekeeping-sweeper-idempotency-keys-and-outbox.md) | Housekeeping sweeper for idempotency keys and published outbox rows | Accepted |
| [0033](./0033-bootretry-and-kafka-writer-tuning.md) | Boot-time first-dial retry, synchronous-writer BatchTimeout/acks, and DLQ topic-create retry | Accepted |
| [0034](./0034-raise-order-line-released-and-order-released.md) | Raise OrderLineReleased and OrderReleased at the release transition | Accepted |
| [0035](./0035-site-sku-demand-projection-event.md) | SiteSkuDemandChanged — an additive, PII-free site/SKU demand projection event | Accepted |
| [0036](./0036-product-classification-local-copy.md) | Product classification from a local copy of product-master events | Accepted |

Each of these reconstructs a decision that is actually visible in this
repository's `CLAUDE.md` and code — none is a generic placeholder.

## Proposing a new one

1. Copy the template above into `docs/docs/adr/NNNN-short-kebab-title.md`,
   taking the next free number.
2. Open it with **Status: Proposed**.
3. Write the *Context* before the *Decision*. If the context does not make
   the decision feel inevitable, the context is incomplete — or the
   decision is wrong.
4. Fill in *Consequences* honestly, including the ones you dislike.
5. Add it to the table above and to `sidebars.ts`.
6. Raise it as a pull request; the discussion belongs on the PR, not
   inside the record.
7. On merge, flip the status to **Accepted**.

## What deserves an ADR

Something is architecturally significant if reversing it later would be
expensive — it constrains the structure of the code, pins a contract other
teams depend on, or is costly to undo.

**Yes:** the layering rule; how allocation and release are orchestrated
across two Suppliers; the fail-closed distinction between a business fact
and an infrastructure failure; the cancellation boundary at release.

**No:** which linter rules are enabled; a library upgrade; a refactor with
no external consequence. Padding the log with trivia makes the
significant records harder to find.
