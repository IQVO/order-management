---
id: index
title: ADR index
sidebar_label: All ADRs
sidebar_position: 1
slug: /adr/all
description: Every numbered architecture decision record of order-management with its title and status.
---

# ADR index

Every numbered record in this directory, one row each: the title is the record's
own heading and the status is the word on the line after its `## Status`
heading. The longer status notes (what amends or supersedes what) are in
[About ADRs](./about.md), which also explains the format and how to propose a
new record.

| # | Title | Status |
| --- | --- | --- |
| [0001](./0001-hexagonal-ports-and-adapters.md) | Hexagonal (ports & adapters) architecture | Accepted |
| [0002](./0002-http-consumer-of-inventory-and-wes-not-shared-code.md) | HTTP consumer of inventory-storage and wes-work-planning, not shared code | Accepted |
| [0003](./0003-ship-complete-default-and-fail-closed-allocation.md) | Ship-complete by default and fail-closed allocation | Accepted |
| [0004](./0004-cancellation-boundary-at-release.md) | The cancellation boundary is release | Accepted |
| [0005](./0005-choreographed-release-via-kafka.md) | Choreographed release via Kafka, folded allocate-then-release, and pathId goes internal-only | Accepted |
| [0006](./0006-analytical-data-product.md) | Per-service analytical data product (the "report") | Accepted |
| [0007](./0007-adopt-fleet-micro-frontend-console.md) | Adopt the fleet's micro-frontend console architecture (`order-mgmt-mfe`) | Accepted |
| [0008](./0008-fulfillment-class-demand-shape-classifier.md) | FulfillmentClass — a demand-shape classifier, not a process-path name | Accepted |
| [0009](./0009-standard-metrics-convention.md) | Standard metrics convention across the fleet | Accepted |
| [0010](./0010-mcp-inbound-adapter.md) | Model Context Protocol as an inbound adapter, not a new service | Accepted |
| [0011](./0011-adopt-fleet-rest-identity.md) | Adopt the fleet REST identity — static bearer keys with read/read-write scopes | Superseded |
| [0012](./0012-remove-rest-mcp-bearer-auth.md) | Remove the REST/MCP bearer auth layer | Accepted |
| [0013](./0013-process-path-selection-as-a-domain-policy.md) | Process-path selection as a real domain policy, validated against a live catalogue | Accepted |
| [0014](./0014-promise-derived-from-fulfillment-capability.md) | The delivery promise is a CPT window derived from fulfillment capability, not a configured lead time | Accepted |
| [0015](./0015-wes-work-planning-path-capacity-changed-wired.md) | wes-work-planning's PathCapacityChanged is wired as the real PathCapacity adapter | Accepted |
| [0016](./0016-eligibility-driven-process-path-selection.md) | Eligibility-driven process-path selection (ADR 0014 step B, routing only) | Accepted |
| [0017](./0017-per-shipment-group-promising.md) | Per-shipment-group promising (ADR 0014 step B, the second half) | Accepted |
| [0018](./0018-repromise-order-consumer-and-order-repromised.md) | RepromiseOrder consumer and OrderRepromised — closing ADR 0014's feedback loop | Accepted |
| [0019](./0019-promise-kpis-on-order-funnel.md) | Promise KPIs on the Order Funnel data product (ADR 0014 §6, order-management half) | Accepted |
| [0020](./0020-network-originated-demand-hold-and-deadline-feasibility.md) | Network-originated demand — release-on-allocation as an intake choice, deadline feasibility, and the Network promise basis | Accepted |
| [0021](./0021-multi-path-attribute-driven-routing.md) | Multi-path attribute-driven routing (closes ADR-0013's original deferral) | Accepted |
| [0022](./0022-transactional-outbox.md) | Transactional outbox for order-management's dual-topic event publishing | Accepted |
| [0023](./0023-idempotency-key-middleware.md) | Transactional Idempotency-Key middleware for POST /orders | Accepted |
| [0024](./0024-optimistic-concurrency-version-column.md) | Optimistic concurrency (version column) for the Order aggregate | Accepted |
| [0025](./0025-resilience-circuit-breakers-retry-dlq-shutdown.md) | Per-dependency circuit breakers, read-only retry, Kafka DLQ, and graceful shutdown hardening | Accepted |
| [0026](./0026-horizontal-autoscaling-and-pgxpool-tuning.md) | Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning | Accepted |
| [0027](./0027-kafka-integration-publisher-partition-key.md) | Partition key (OrderId) on the integration publisher's Kafka messages | Accepted |
| [0028](./0028-inventory-storage-reservations-idempotency-key.md) | Send `Idempotency-Key` on `POST /reservations`, restoring the inventory-storage contract PR #98 changed | Accepted |
| [0029](./0029-migrations-direct-postgres-connection.md) | Run golang-migrate against a direct Postgres connection, not PgBouncer | Accepted |
| [0030](./0030-cloudevents-mandatory-event-envelope.md) | CloudEvents 1.0 as the mandatory event envelope | Accepted |
| [0031](./0031-consume-warehouse-planning-capacity-plans.md) | Consume warehouse-planning's capacity plans into a local planned-capacity read model | Accepted |
| [0032](./0032-housekeeping-sweeper-idempotency-keys-and-outbox.md) | Housekeeping sweeper for idempotency keys and published outbox rows | Accepted |
| [0033](./0033-bootretry-and-kafka-writer-tuning.md) | Boot-time first-dial retry, synchronous-writer BatchTimeout/acks, and DLQ topic-create retry | Accepted |
| [0034](./0034-raise-order-line-released-and-order-released.md) | Raise OrderLineReleased and OrderReleased at the release transition | Accepted |
| [0035](./0035-site-sku-demand-projection-event.md) | SiteSkuDemandChanged — an additive, PII-free site/SKU demand projection event | Accepted |
| [0036](./0036-product-classification-local-copy.md) | Product classification from a local copy of product-master events | Accepted |
| [0037](./0037-send-line-no-on-reservations.md) | Send the order line number (lineNo) on POST /reservations | Accepted |
| [0038](./0038-mfe-sends-idempotency-key-per-submission.md) | The operator MFE sends an Idempotency-Key per order submission | Accepted |
| [0039](./0039-reports-api-openapi-contract.md) | The reports binary's routes are a contract: `apis/openapi-reports.yaml` | Accepted |
| [0040](./0040-outbox-relay-attempt-cap.md) | The outbox relay stops claiming a row after `OUTBOX_MAX_ATTEMPTS` failures | Accepted |
| [0041](./0041-trace-context-crosses-the-outbox.md) | The W3C trace context crosses the outbox in `outbox_events.headers` (D27 reference) | Accepted |
| [0042](./0042-path-selection-honours-process-path-intake.md) | Path selection considers only paths whose intake admits customer orders | Accepted |
| [0043](./0043-release-confirmation-from-workunitcreated.md) | Confirm an order-line release from WorkUnitCreated, and flag OrderReleaseUnconfirmed | Accepted |
| [0044](./0044-caller-assigned-fulfillment-site-on-the-order.md) | An order may carry an optional, caller-assigned fulfillment site | Accepted |
| [0045](./0045-a-dead-consumer-takes-the-process-down.md) | A Kafka consumer that stops on its own takes the process down | Accepted |
| [0046](./0046-a2a-inbound-adapter.md) | A2A inbound adapter (third driving adapter, read-only) | Accepted |
| [0047](./0047-promise-at-the-orders-own-fulfillment-site.md) | An order is promised against its own fulfillment site | Accepted |
| [0048](./0048-adopt-fleet-rest-api-guideline.md) | Adopt the fleet REST API guideline | Accepted |
| [0049](./0049-agent-authority-levels-and-complete-mcp-surface.md) | Agent authority levels and complete MCP surface | Accepted |
| [0050](./0050-allocation-events-carry-the-orders-fulfillment-site.md) | OrderAllocated/OrderPartiallyAllocated carry the order's fulfillment site | Accepted |
| [0051](./0051-consume-shipment-confirmed-and-package-missedload.md) | Consume ShipmentConfirmed/PackageMissedLoad: the Order ships | Accepted |
