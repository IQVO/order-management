---
title: Architecture
sidebar_label: Architecture
description: Hexagonal / ports-and-adapters layering, the strict dependency rule, and how it maps to Order Management's actual code.
---

# Architecture

The service is **hexagonal (ports & adapters)** with one non-negotiable rule:

> **domain depends on nothing; application depends on domain; adapters
> depend on application/domain.**

No framework type, no `chi` router, no `pgx` connection, and no SQL string
ever appears inside `internal/domain/`.

## Layout

```text
cmd/order/                    main.go — composition root of the OLTP service
cmd/mcp/                      read-only MCP server (Streamable HTTP, ADR 0010)
cmd/order-projector/          analytics projector — Kafka -> analytics DB (ADR 0006)
cmd/order-reports/            analytics read API (order funnel report)
internal/
  domain/
    order/                    Order aggregate, OrderLine, Status, invariants,
                               PromisePolicy (+ LeadTimePolicy fallback),
                               PathSelectionPolicy
    processpath/              process-path definitions
    shared/                   value objects: OrderId, SKU, PathId, events, errors
  application/
    ports/                    OUT: OrderRepo, EventPublisher, Clock, OrderMetrics,
                               InventoryReservationClient, ProcessPathCatalogue,
                               CPTScheduleCache, ProductClassificationLookup,
                               PathCapacity, RepromiseProcessedEvents
    usecases/                 ReceiveOrder, RetryAllocation, CancelOrder,
                               GetOrder, ReleaseHeldOrder, RepromiseOrder
                               (allocation + release folded into allocateAndRelease)
  adapters/
    inbound/http/             chi handlers, DTOs, RFC 7807 error mapping
    inbound/kafka/            RepromiseConsumer (warehouse.fulfillment.events)
    inbound/mcp/              MCP tools
    outbound/inventorystorage/  HTTP client: POST /reservations,
                                 DELETE /reservations/{id}
    outbound/productclassification/  HTTP client: GET /products/{sku}/classification
    outbound/kafka/           integration + analytics publishers
    outbound/kafkacatalog/    process-path catalogue cache (Kafka)
    outbound/kafkacptschedule/  CPT schedule cache (Kafka)
    outbound/kafkapathcapacity/ path capacity cache (Kafka)
    outbound/pathcapacity/    "unknown capacity" default
    outbound/postgres/        pgxpool repo, transactional event publisher
    outbound/memory/          in-memory repos + clocks
    outbound/events/          log publisher (default EVENT_PUBLISHER=log)
    outbound/analyticsstore/  analytics projection + report queries
    outbound/telemetry/       OpenTelemetry metrics
migrations/                   golang-migrate SQL files (+ migrations/analytics)
apis/openapi.yaml, apis/asyncapi.yaml
docs/docs/adr/                Architecture Decision Records
```

## The dependency flow

```mermaid
flowchart TB
  HTTP["inbound/http<br/>chi handlers, DTOs, RFC 7807"]
  KIN["inbound/kafka<br/>RepromiseConsumer"]
  UC["application/usecases<br/>ReceiveOrder, RetryAllocation, CancelOrder,<br/>GetOrder, ReleaseHeldOrder, RepromiseOrder"]
  P["application/ports<br/>OrderRepo · EventPublisher · Clock<br/>InventoryReservationClient · ProcessPathCatalogue<br/>CPTScheduleCache · PathCapacity · ..."]
  D["domain<br/>order · processpath · shared"]
  PG["outbound/postgres · memory"]
  EV["outbound/events · kafka"]
  INVC["outbound/inventorystorage<br/>productclassification"]
  CACHE["outbound/kafkacatalog<br/>kafkacptschedule · kafkapathcapacity"]

  HTTP --> UC
  KIN --> UC
  UC --> P
  UC --> D
  P --> D
  PG -.implements.-> P
  EV -.implements.-> P
  INVC -.implements.-> P
  CACHE -.implements.-> P

  classDef dom fill:#1d4ed8,stroke:#1e3a8a,color:#fff;
  classDef app fill:#2563eb,stroke:#1e3a8a,color:#fff;
  classDef adp fill:#64748b,stroke:#334155,color:#fff;
  class D dom;
  class UC,P app;
  class HTTP,KIN,PG,EV,INVC,CACHE adp;
```

Solid arrows are compile-time imports; dashed arrows are interface
satisfaction. No arrow points from the application layer into an adapter —
the application only ever names an interface in `application/ports`, and
`cmd/order/main.go` is the only place that decides which implementation
gets plugged in. See [ADR 0001](/docs/adr/0001-hexagonal-ports-and-adapters).
The rule is enforced by `internal/architecture` (the `arch-test` CI job).

## Ports

| Port | Responsibility | Implementations |
| --- | --- | --- |
| `OrderRepo` | Persist/retrieve `Order`; mint IDs | `postgres`, `memory` |
| `EventPublisher` | Publish a `shared.DomainEvent` | `events` (log), `postgres` (transactional outbox, ADR 0022), `kafka` (integration + analytics fan-out, direct or via the outbox relay) |
| `UnitOfWork` | Bracket a Save + Publish(es) atomically (ADR 0022) | `postgres` (real Postgres transaction), nil (pass-through) |
| `Clock` | `Now()` — makes promise computation deterministic in tests | `memory.SystemClock`, fixed clocks in tests |
| `OrderMetrics` | Order accepted/rejected counter | `telemetry` (OpenTelemetry) |
| `InventoryReservationClient` | `Reserve`/`Revoke` against inventory-storage | `outbound/inventorystorage` (http), permissive no-op |
| `ProductClassificationLookup` | Product attributes for path eligibility (ADR 0016) | `outbound/productclassification` (http), permissive |
| `ProcessPathCatalogue` | Active paths + capability (ADR 0013/0014) | `outbound/kafkacatalog` |
| `CPTScheduleCache` | Site CPT schedule (ADR 0014) | `outbound/kafkacptschedule` |
| `PathCapacity` | Remaining path capacity per cutoff (ADR 0015) | `outbound/kafkapathcapacity`, `pathcapacity.Unknown` |
| `RepromiseProcessedEvents` | Idempotency gate for the re-promise consumer (ADR 0018) | `postgres`, `memory` |

Both inventory-storage ports are env-selected `MODE=http|permissive`
(defaulting to `permissive`, so unit tests never hit the network). The
classification lookup fails open like the fleet's other soft lookups; the
reservation client does not — allocation against a permissive client
returns a clear `ErrDownstreamNotConfigured` rather than a fabricated
success. Only `http` mode is suitable for a real integration test or
deployment.

## Composition root

`cmd/order/main.go` is the only file that reads environment variables and
the only file that knows both a port and its implementation:

| Env var | Default | Effect |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Listen address |
| `LOG_LEVEL` | `info` | slog level |
| `DATABASE_URL` | *(unset)* | If unset, the in-memory adapters are used and no database is required. |
| `MIGRATIONS_PATH` | `migrations` | Where golang-migrate looks for SQL files |
| `INVENTORY_STORAGE_MODE` | `permissive` | `http` or `permissive` |
| `INVENTORY_STORAGE_BASE_URL` | *(unset)* | Required when mode is `http`; also used by the classification lookup |
| `PRODUCT_CLASSIFICATION_MODE` | `permissive` | `http` or `permissive` (ADR 0016) |
| `EVENT_PUBLISHER` | `log` | `kafka` adds the integration + analytics topics |
| `KAFKA_BROKERS` | `localhost:9092` when publishing; unset disables the re-promise consumer | Comma-separated brokers |
| `PATH_CATALOGUE_SOURCE` | `none` | `kafka` enables the catalogue, CPT-schedule and path-capacity caches (capability promise); `none` = lead-time promise only |
| `PROMISE_DEFAULT_LEAD_TIME` | `48h` | Lead-time fallback for any unlisted path |
| `PROMISE_PATH_LEAD_TIMES` | *(unset)* | Per-path overrides, e.g. `pick=24h,singles=6h` |
| `CORS_ALLOWED_ORIGINS` | *(unset)* | Console / MFE origins (ADR 0007) |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | *(unset)* | OpenTelemetry export |

The in-memory fallback is deliberate: `go run ./cmd/order` with no
environment at all starts a fully functional service, which is what makes
the `httptest` suite cheap to run.

## Quality gates

`.github/workflows/ci.yml` runs on every push and pull request:

| Job | What it enforces |
| --- | --- |
| `lint` | `golangci-lint` against the committed `.golangci.yml` |
| `test` | Unit tests with `-race`; coverage gate on `internal/domain/...,internal/application/...` |
| `bdd` | godog/Gherkin acceptance tests (`TestFeatures`) |
| `integration` | Kafka adapters against Testcontainers Kafka |
| `mutation-fast` | gremlins on the domain layer, thresholds in `.gremlins.yaml` |
| `vuln` | `govulncheck` |
| `api-lint` | Spectral on `apis/openapi.yaml` / `apis/asyncapi.yaml` |
| `arch-test` | The hexagonal dependency rule (`internal/architecture`) |
| `docs-api-drift` | Regenerates the REST reference and diffs it |
| `helm-lint` | `charts/order-management` |
| `web` | The MFE: lint, typecheck, test, build |
| `trivy-scan`, `docker-publish`, `release` | Image scan, publish, GitFlow release |

`mutation` (full) and `drift` run only on schedule / manual dispatch. This
documentation site is built and deployed by a separate workflow,
`.github/workflows/docs.yml`.
