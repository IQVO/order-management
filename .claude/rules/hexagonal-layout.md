---
paths:
  - "internal/**"
  - "cmd/**"
  - "migrations/**"
  - "docker-compose*.yml"
---
# Hexagonal layout, binaries and local run

Hexagonal / Ports & Adapters, enforced by an `arch-go` fitness test
(`internal/architecture/architecture_test.go`, CI job `arch-test`). Strict
dependency rule: **domain depends on nothing; application depends on
domain; adapters depend on application/domain.** No framework, HTTP, or SQL
type ever appears in the domain layer.

- **Module:** `github.com/claudioed/order-management`, Go 1.26.
- **Four binaries, one module:** `cmd/order` (HTTP + Kafka-choreography
  OLTP service), `cmd/mcp` (MCP read server, ADR-0010), `cmd/order-projector`
  (analytics writer, ADR-0006, FirstOffset consumer), `cmd/order-reports`
  (analytics read-only REST API).
- **Suppliers:** `inventory-storage` (synchronous HTTP, `POST /reservations`,
  `DELETE /reservations/{id}`) and `wes-work-planning` (release via
  Kafka choreography — no HTTP call). Consumes Kafka facts from
  `process-path-management`, `wes-work-planning`, `fulfillment-execution`,
  `warehouse-planning` (ADR-0013..0018, 0031) and `product-master`
  (`ProductClassified` into a local copy, ADR-0036). `network-fulfillment`
  is an inbound HTTP caller (ADR-0020). See `bounded-context-boundary.md`.

## Non-obvious layout facts

- `internal/domain/order/`: Order aggregate, OrderLine, Status, invariants,
  `PromisePolicy` (+ `LeadTimePolicy` fallback), `PathSelectionPolicy`.
  `internal/domain/shared/`: OrderId, SKU, PathId, domain events, errors.
- `internal/application/ports/`: OrderRepo, EventPublisher, Clock,
  OrderMetrics, InventoryReservationClient, ProcessPathCatalogue,
  CPTScheduleCache, ProductClassificationLookup, PathCapacity,
  RepromiseProcessedEvents, PlannedCapacityRepo,
  PlannedCapacityProcessedEvents (ADR-0031), ProductClassificationCopy,
  ProductClassificationProcessedEvents (ADR-0036). **No `WorkReleaseClient` —
  deleted, ADR-0005.**
- `internal/application/usecases/`: ReceiveOrder, RetryAllocation,
  ReleaseHeldOrder, CancelOrder, GetOrder, RepromiseOrder (Kafka-driven),
  ApplyPlannedCapacity (Kafka-driven), GetPlannedCapacity,
  OrderCapacityConstraints (ADR-0031), ApplyProductClassification
  (Kafka-driven, ADR-0036).
  AllocateOrder/ReleaseOrder were deleted as public types (ADR-0005); the
  shared allocate-then-release logic lives in `allocation.go`.
- Inbound adapters: `inbound/http/` (chi handlers, DTOs, RFC 7807 error
  mapping, CORS, `Idempotency-Key` middleware, `/readyz`), `inbound/kafka/`
  (RepromiseConsumer on `warehouse.fulfillment.events`, ADR-0018;
  PlannedCapacityConsumer on `warehouse.warehouse-planning.events`,
  ADR-0031; ProductClassificationConsumer on
  `warehouse.product-master.events`, ADR-0036; AnalyticsConsumer for
  `cmd/order-projector`, ADR-0006),
  `inbound/mcp/` (read-only tools `get_order`, `get_promise_health`).
- Outbound adapters: `inventorystorage/` (HTTP `POST /reservations`,
  `DELETE /reservations/{id}`), `productclassificationcopy/` (local copy
  of product-master classifications + permissive lookup, ADR-0036),
  `kafkacatalog/`+`kafkacptschedule/`+`kafkapathcapacity/` (capability caches,
  `PATH_CATALOGUE_SOURCE=kafka`), `pathcapacity/` ("unknown capacity" default),
  `kafka/` (integration + analytics publishers, `EVENT_PUBLISHER=kafka`),
  `events/` (log publisher, default `EVENT_PUBLISHER=log`), `postgres/` (pgxpool
  repo + golang-migrate runner), `memory/` (in-memory repo for tests),
  `analyticsstore/` (ADR-0006), `telemetry/` (OTel, otelchi RED, ADR-0009).
- `internal/adapters/kafka/cloudevents/` is the ONLY CloudEvents 1.0
  New/Decode helper (ADR-0030) — see `cloudevents-events.md`.
- `internal/analytics/report/`: analytical read model — depends on nothing
  internal (enforced by arch-go).
- Migrations: `migrations/` (OLTP) and `migrations/analytics/` (analytics
  store), both golang-migrate SQL.
- Contracts: `apis/openapi.yaml` (REST: 5 order endpoints +
  `/planned-capacity` + `/healthz` + `/readyz`),
  `apis/asyncapi.yaml` (Kafka, CloudEvents 1.0, ADR-0030).

## Run locally

```bash
go run ./cmd/order                    # in-memory adapters if DATABASE_URL unset
docker compose up -d postgres         # Postgres 16 on :5434
docker compose -f docker-compose.kafka.yml up -d   # local Kafka (KRaft, :9092)
```
