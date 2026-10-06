# Order Management

> **⚠️ Study project.** This repository is an educational exercise in
> Domain-Driven Design applied to warehouse management/execution systems. It
> follows real industry-standard patterns and terminology (WMS/WES/WCS,
> chaotic storage, CloudEvents, RFC 7807, hexagonal architecture) but is
> **not a production system** and is **not affiliated with, endorsed by, or
> representative of any real-world company**.

Order intake, allocation, promise-date calculation, and release — the
missing upstream Open Host Service for the `warehouse-systems` fleet
(`inventory-storage`, `wes-work-planning`, `fulfillment-execution`,
`process-path-management`, `network-fulfillment`, and the rest of the fleet).

This context owns **Order** and **OrderLine** as first-class, validated
aggregates — closing a gap where "an order" was previously just an
unowned, unvalidated string (`OrderRef` / `DemandRef` / `Reference`)
threaded independently through three other services.

## Bounded-context boundary (read this first)

This service is a **pure HTTP consumer** of `inventory-storage`'s
already-published REST API (reservations, product classification) and
integrates with the rest of the fleet only through published Kafka events:
it announces release to `wes-work-planning`
([ADR 0005](docs/docs/adr/0005-choreographed-release-via-kafka.md)) and
consumes capability, CPT-schedule, capacity and fulfillment facts from
`process-path-management`, `wes-work-planning` and `fulfillment-execution`
(ADR 0013–0018), plus — opt-in — `warehouse-planning`'s capacity plans
([ADR 0031](docs/docs/adr/0031-consume-warehouse-planning-capacity-plans.md)).
`network-fulfillment` calls it over HTTP to raise held,
deadline-constrained orders
([ADR 0020](docs/docs/adr/0020-network-originated-demand-hold-and-deadline-feasibility.md)).
It imports no Go code from any sibling repository and has no write access
to their internal aggregates (`Reservation`, `WorkPool`, etc.) — only to
their published, versioned contracts.

This build is **100% additive** from those services' point of view: neither
repository is modified at all. See
[ADR 0002](docs/docs/adr/0002-http-consumer-of-inventory-and-wes-not-shared-code.md).

## Architecture

Hexagonal (ports & adapters), with a strict inward-only dependency rule —
**domain depends on nothing; application depends on domain; adapters depend
on application/domain** — identical in shape to the other Go services in
the fleet. See
[ADR 0001](docs/docs/adr/0001-hexagonal-ports-and-adapters.md).

```
cmd/order/                        OLTP service composition root
cmd/mcp/                          read-only MCP server (ADR 0010)
cmd/order-projector/              analytics writer (ADR 0006)
cmd/order-reports/                analytics read API (ADR 0006)
internal/
  domain/
    order/                        Order aggregate, OrderLine, statuses,
                                  invariants, PromisePolicy (+ LeadTimePolicy
                                  fallback), PathSelectionPolicy
    processpath/                  process-path definitions
    shared/                       OrderId, SKU, PathId, domain events, errors
  application/
    ports/                        OUT: OrderRepo, EventPublisher, UnitOfWork,
                                  Clock, OrderMetrics,
                                  InventoryReservationClient,
                                  ProcessPathCatalogue, CPTScheduleCache,
                                  PathCapacity, ProductClassificationLookup,
                                  RepromiseProcessedEvents,
                                  PlannedCapacityRepo,
                                  PlannedCapacityProcessedEvents
    usecases/                     ReceiveOrder (allocates+releases
                                  implicitly), RetryAllocation (retries+
                                  releases), ReleaseHeldOrder, CancelOrder,
                                  GetOrder, RepromiseOrder (Kafka-driven),
                                  ApplyPlannedCapacity (Kafka-driven),
                                  GetPlannedCapacity, OrderCapacityConstraints
  adapters/
    inbound/http/                 chi handlers, DTOs, RFC 7807 error mapping,
                                  Idempotency-Key middleware, /readyz gate
    inbound/kafka/                re-promise consumer (warehouse.fulfillment.events),
                                  planned-capacity consumer
                                  (warehouse.warehouse-planning.events),
                                  analytics consumer (cmd/order-projector)
    inbound/mcp/                  MCP tools
    kafka/cloudevents/            the only CloudEvents 1.0 New/Decode helper
    outbound/inventorystorage/    POST /reservations, DELETE /reservations/{id}
    outbound/productclassification/ GET /products/{sku}/classification
    outbound/kafkacatalog/, kafkacptschedule/, kafkapathcapacity/
                                  capability caches (PATH_CATALOGUE_SOURCE=kafka)
    outbound/pathcapacity/        "unknown capacity" default
    outbound/postgres/            pgxpool repo, transactional outbox + relay,
                                  housekeeping sweeper, golang-migrate runner
    outbound/memory/              in-memory repo + clocks for tests
    outbound/events/              log publisher (default, EVENT_PUBLISHER=log)
    outbound/kafka/               integration + analytics publishers
                                  (EVENT_PUBLISHER=kafka)
    outbound/analyticsstore/      analytics projection + report queries
    outbound/telemetry/           OpenTelemetry (OTLP) metrics and traces
  analytics/report/               analytical read model (depends on nothing internal)
migrations/                       golang-migrate SQL files (+ analytics/)
apis/openapi.yaml, asyncapi.yaml  REST and Kafka contracts
docker-compose.kafka.yml          Local Kafka broker (KRaft, single node)
docs/docs/adr/                    Architecture Decision Records
```

The domain layer is pure Go: no `chi`, no `pgx`, no `net/http`. The
order-level `Status` is **derived from the line statuses on every read** and
is never stored, so it cannot drift out of sync with the lines it
summarises.

## Business rules worth knowing before you read the code

- **BR2 — fail closed on ambiguity.** A `409` from inventory-storage's
  `POST /reservations` is the business fact "no usable stock" and backorders
  that one line. A transport failure, a 5xx, or any other non-2xx is *not* a
  business fact: the allocation pass fails and nothing is silently
  marked backordered.
- **BR3 — ship-complete by default.** `allowPartialShipment` defaults to
  `false`: any backordered line holds the WHOLE order back from release until
  `RetryAllocation` clears it. `RetryAllocation` is the only route from
  `Backordered` back to `Allocated`.
- **BR6 — the cancellation boundary is release.** Cancelling is legal only
  while no line is `Released`; the boundary is checked *before* any
  reservation is revoked. See
  [ADR 0004](docs/docs/adr/0004-cancellation-boundary-at-release.md), including
  its documented known gap.

BR2/BR3 are written up in
[ADR 0003](docs/docs/adr/0003-ship-complete-default-and-fail-closed-allocation.md).

## Running locally

### 1. Without a database (fastest)

With no `DATABASE_URL`, the service starts on the in-memory adapters and is
fully functional:

```bash
go run ./cmd/order
# {"level":"INFO","msg":"database url not configured; using in-memory adapters"}
# {"level":"INFO","msg":"http server listening","addr":":8080"}
```

### 2. With Postgres

```bash
docker compose up -d postgres          # Postgres 16 on localhost:5434

export DATABASE_URL='postgres://order:order@localhost:5434/order?sslmode=disable'
go run ./cmd/order                     # migrations run automatically at startup
```

Migrations live in `migrations/` and are applied by `golang-migrate` on boot;
there is no separate migrate step to remember.

### 3. Wired to the real Supplier

The outbound inventory-storage clients default to **permissive (no-op)
mode**, so tests and CI never reach the network. For reservations,
permissive does *not* mean fail-open: allocating real stock must never
appear to succeed against a no-op, so a permissive client refuses the
operation with a clear `downstream-not-configured` problem. Only `http` mode is suitable
for a real integration test or deployment:

```bash
export INVENTORY_STORAGE_MODE=http
export INVENTORY_STORAGE_BASE_URL=http://localhost:8080
export HTTP_ADDR=:8082
go run ./cmd/order
```

Release no longer calls any Supplier synchronously — see
[Kafka integration](#kafka-integration) below.

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Listen address. |
| `DATABASE_URL` | *(unset)* | Postgres DSN. Unset ⇒ in-memory adapters (and no `Idempotency-Key` middleware, no outbox, no sweeper). |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | Direct (non-PgBouncer) DSN used only by the migration step (ADR 0029). |
| `MIGRATIONS_PATH` | `migrations` | golang-migrate source directory. |
| `DEFAULT_SITE_ID` | `site-1` | Site whose CPT schedule the promise uses (ADR 0014). |
| `INVENTORY_STORAGE_MODE` | `permissive` | `http` or `permissive`. |
| `INVENTORY_STORAGE_BASE_URL` | *(unset)* | Required when mode is `http`; also the product-classification base URL. |
| `PRODUCT_CLASSIFICATION_MODE` | `permissive` | `http` or `permissive` — eligibility lookup for path selection (ADR 0016); fails open. |
| `PATH_CATALOGUE_SOURCE` | `none` | `kafka` enables the process-path catalogue, CPT-schedule and path-capacity caches that drive the capability-derived promise (ADR 0013–0015); `none` ⇒ lead-time promise only. |
| `EVENT_PUBLISHER` | `log` | `log` (default, structured-log publisher) or `kafka` — forwards `OrderAllocated`/`OrderPartiallyAllocated`/`OrderRepromised` to the integration topic **and** fans every domain event out to the analytics topic (ADR 0006). With `DATABASE_URL` set, `kafka` goes through the transactional outbox (`outbox_events` + in-process relay, ADR 0022); without it, events are written to Kafka directly. |
| `KAFKA_BROKERS` | `localhost:9092` for the publishers; unset disables the consumers | Comma-separated broker addresses. When set, `cmd/order` also runs the re-promise consumer (ADR 0018) and, if enabled, the planned-capacity consumer (ADR 0031). |
| `OUTBOX_RELAY_INTERVAL` | `1s` | Outbox relay poll interval (ADR 0022). |
| `HOUSEKEEPING_INTERVAL` | `1h` | Sweeper period; `0` disables it (ADR 0032). |
| `IDEMPOTENCY_KEY_TTL` | `24h` | Age after which `idempotency_keys` rows are swept (ADR 0032). |
| `OUTBOX_RETENTION` | `168h` | Age after which PUBLISHED `outbox_events` rows are swept (ADR 0032). |
| `PLANNED_CAPACITY_CONSUMER_GROUP` | *(unset)* | Stable consumer group for warehouse-planning's capacity plans; unset ⇒ the feature is off and `GET /planned-capacity` is not registered (ADR 0031). |
| `PLANNED_CAPACITY_SITE_ID` | `DEFAULT_SITE_ID` | Site whose planned-capacity windows annotate orders (ADR 0031). |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5181` | Console / MFE origins (ADR 0007). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTLP/gRPC endpoint for metrics and traces (ADR 0009); there is no `/metrics` scrape route. |
| `OTEL_SERVICE_NAME` | `order-management` | Service name on telemetry (per binary: `order-management-mcp`, `order-projector`, `order-reports`). |
| `SERVICE_VERSION`, `ENVIRONMENT` | `dev`, `local` | OpenTelemetry resource attributes. |
| `ANALYTICS_DATABASE_URL` | *(unset)* | Analytical Postgres DSN. **Required** by `cmd/order-projector` (writer) and `cmd/order-reports` (read-only reader); optional for `cmd/mcp` (`get_promise_health` reports empty KPIs without it); never read by the OLTP binary. |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | Analytical golang-migrate source directory (writer only). |
| `ADMIN_ADDR` | `:8091` | `cmd/order-projector` admin/health listen address. |
| `MCP_ADDR` | `:8090` | `cmd/mcp` listen address. |
| `PROMISE_DEFAULT_LEAD_TIME` | `48h` | Lead-time fallback promise for any unlisted path. |
| `PROMISE_PATH_LEAD_TIMES` | *(unset)* | Per-path overrides, e.g. `pick=24h,singles=6h`. |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error`. |
| `SHUTDOWN_DRAIN_DELAY` | `5s` | Graceful shutdown: wait this long after `/readyz` flips to not-ready, before closing the listener (ADR 0025 §8). `0` disables. |

## Kafka integration

Per [ADR 0005](docs/docs/adr/0005-choreographed-release-via-kafka.md),
release is no longer a synchronous HTTP call to `wes-work-planning`. It is
announced as a Kafka integration event, which that service (or any other
subscriber) consumes independently:

- **Topic:** `warehouse.order-management.events`
- **Forwarded events:** `OrderAllocated`, `OrderPartiallyAllocated` and
  (ADR 0018) `OrderRepromised` — every other domain event
  (`OrderReceived`, `OrderLineAllocated`, `OrderLineBackordered`,
  `OrderLineReleased`, `OrderReleased`, `OrderCancelled`,
  `OrderAllocationPartiallyFailed`) stays off the integration topic,
  mirroring `inventory-storage`'s own precedent of forwarding only a subset.
- **Payload (`data` field; the four line fields below are frozen, newer
  fields such as `fulfillment_class` and `promise_cpt_id`/`promise_basis`/
  `promise_cutoff_at` are additive — see `apis/asyncapi.yaml`):**

  ```json
  {
    "order_id": "ord-a1b2c3d4-0000-0000-0000-000000000001",
    "promise_date": "2026-08-27T09:00:00Z",
    "lines": [
      {"line_no": 1, "sku": "SKU-1", "path_id": "pick", "gift_wrap": false}
    ]
  }
  ```

- **Opt-in, off by default.** Set `EVENT_PUBLISHER=kafka` (and, if not
  `localhost:9092`, `KAFKA_BROKERS`) to publish for real; the default
  `log` publisher requires no broker at all, so `go test ./...` and local
  runs are unaffected either way.
- **Local broker:** `docker compose -f docker-compose.kafka.yml up -d`
  starts a single-node KRaft Kafka on `localhost:9092`, the same
  `apache/kafka:3.8.0` image/config shape the fleet's shared
  `docker-compose.kafka.yml` uses (this repo has its own copy rather than
  a cross-repo reference).
- **Fire-and-forget, deliberately.** There is no release-confirmation
  reply event from `wes-work-planning` in v1 — see "Deferred" below.
- **Consumed topics:** `warehouse.process-path-management.events`
  (`ProcessPath*`, `CPTScheduleChanged`) and `warehouse.work-planning.events`
  (`PathCapacityChanged`) feed local caches when
  `PATH_CATALOGUE_SOURCE=kafka`; `warehouse.fulfillment.events`
  (`TaskCPTMissed`, `PackageManifested`) drives the re-promise consumer
  (group `order-management-repromise`) whenever `KAFKA_BROKERS` is set;
  `warehouse.warehouse-planning.events` (`CapacityPlanCreated`,
  `CapacityPlanPublished`, `CapacityShortageDetected`) feeds the
  planned-capacity read model when `PLANNED_CAPACITY_CONSUMER_GROUP` is set
  (ADR 0031). Poison messages on the two stable-group consumers go to
  `<topic>.dlq` (ADR 0025).
- **Transactional outbox.** With `DATABASE_URL` set, every Kafka-bound
  event is written to `outbox_events` in the same transaction as the
  `Order` save and drained by an in-process relay
  ([ADR 0022](docs/docs/adr/0022-transactional-outbox.md)); messages are
  keyed by `OrderId` with a hash balancer
  ([ADR 0027](docs/docs/adr/0027-kafka-integration-publisher-partition-key.md)).

## Analytics data product

Per [ADR 0006](docs/docs/adr/0006-analytical-data-product.md), this service
additionally owns an **analytical read model** — the *Order Funnel & Allocation
Health* report — built from its own domain events. It is a lightweight data mesh
with **no central data platform**: a separate analytics topic, a separate
analytical database, and two extra binaries, all owned by this repo. The report
contract is documented at
[`docs/docs/analytics/order-funnel-report.md`](docs/docs/analytics/order-funnel-report.md).

- **Separate topic:** `warehouse.order-management.analytics` — distinct from the
  integration topic, so widening the report's inputs never risks an integration
  consumer. A **new** analytics publisher emits the full report-input event set
  as CloudEvents 1.0 events (keyed by `order_id`, `dataschema`
  `urn:warehouse:order-management:analytics:<Event>:v1` — see ADR 0030); the
  integration publisher is untouched. When `EVENT_PUBLISHER=kafka`, the OLTP binary fans out to both.
- **Separate analytical database:** its own `ANALYTICS_DATABASE_URL`, its own
  migrations in `migrations/analytics/`, and a **read-only role** for the reader.
- **Three processes, one writer:**
  - `cmd/order` — the OLTP binary (unchanged; fans out to the analytics topic
    when `EVENT_PUBLISHER=kafka`).
  - `cmd/order-projector` — the **only** writer. Consumes the analytics topic
    from the earliest offset, applies idempotent projections, runs the
    analytical migrations on start. Admin health on `:8091`.
  - `cmd/order-reports` — the **read-only** reader. Serves the report over REST
    on `:8092`; never writes, never migrates.

Run the writer and reader locally against the local Kafka and an analytical
database:

```bash
# Start the local broker (same as the Kafka integration section).
docker compose -f docker-compose.kafka.yml up -d

export ANALYTICS_DATABASE_URL='postgres://order:***@localhost:5434/order_analytics?sslmode=disable'
export KAFKA_BROKERS=localhost:9092

# 1. The writer — consumes the analytics topic, projects into the analytical DB,
#    and runs its migrations on start.
go run ./cmd/order-projector

# 2. The reader — serves the report read-only from the analytical DB.
go run ./cmd/order-reports
# then:
curl -s 'localhost:8092/reports/funnel?from=2026-06-01T00:00:00Z&to=2026-06-02T00:00:00Z'
curl -s localhost:8092/reports/funnel/freshness

# 3. Drive an order lifecycle through the OLTP binary with kafka fan-out on,
#    and watch the report reflect it within the freshness SLA.
EVENT_PUBLISHER=kafka go run ./cmd/order
```

The report is **eventually consistent** by design — a projection of the event
stream to a freshness SLA (p95 event-to-report lag < 30s), not a real-time view.
Its promise KPIs (ADR 0019) are also exposed through the read-only MCP tool
`get_promise_health`.

## MCP server

`cmd/mcp` ([ADR 0010](docs/docs/adr/0010-mcp-inbound-adapter.md)) serves
two read-only tools over Streamable HTTP on `MCP_ADDR` (default `:8090`):
`get_order` (the `GetOrder` use case) and `get_promise_health` (the
analytics report store). There is no write tool and, since
[ADR 0012](docs/docs/adr/0012-remove-rest-mcp-bearer-auth.md), no
authentication.

## API

Five order endpoints, an opt-in planned-capacity read, and liveness/readiness
probes. The full contract, including the RFC 7807 error schema, is in
[`apis/openapi.yaml`](apis/openapi.yaml).

| Method | Path | Use case |
| --- | --- | --- |
| `POST` | `/orders` | ReceiveOrder — allocates and releases automatically. With `DATABASE_URL` set it requires an `Idempotency-Key` request header (ADR 0023); a request without one gets `400`. |
| `GET` | `/orders/{id}` | GetOrder |
| `POST` | `/orders/{id}/retry-allocation` | RetryAllocation — retries and releases automatically |
| `POST` | `/orders/{id}/release` | ReleaseHeldOrder — releases an order received with `releaseOnAllocation: false` (ADR 0020) |
| `DELETE` | `/orders/{id}` | CancelOrder |
| `GET` | `/planned-capacity?site=` | GetPlannedCapacity — registered only when `PLANNED_CAPACITY_CONSUMER_GROUP` is set (ADR 0031) |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe — flips to `503` first on shutdown (ADR 0025); not declared in `apis/openapi.yaml` |

`POST /orders/{id}/allocate` and the old general-purpose
`POST /orders/{id}/release` were removed by
[ADR 0005](docs/docs/adr/0005-choreographed-release-via-kafka.md): a caller
expresses ONE intent, placing an order, and this service internally
attempts allocation-then-release automatically, right after intake and
again on retry. The current `/release` route is ADR 0020's opt-in hold:
`POST /orders` with `releaseOnAllocation: false` allocates and stops, and
optional `requiredShipBy` constrains the promise to a window that meets
that deadline (no `promiseDate` in the response means it cannot).

Every error response is `application/problem+json` (RFC 7807), the same shape
the other services emit.

### Curl walkthrough

**Health:**

```bash
curl -s localhost:8080/healthz
# {"status":"ok"}
```

**ReceiveOrder** — allocates and releases automatically, in the same call.
`pathId` is never part of the request (each line's path is resolved by
`PathSelectionPolicy` — the shortest-cycle-time eligible active path, or the
default `pick` when no catalogue is configured, ADR 0021);
`allowPartialShipment` defaults to `false` (ship-complete).
This is the one route that creates a NEW resource with a
server-generated id, so (with Postgres) it requires a caller-supplied `Idempotency-Key`
header (ADR 0023): a retried request with the SAME key and the SAME body
replays the original response instead of creating a second order; the
same key with a DIFFERENT body is rejected with `422`; a missing key is
rejected with `400`.

```bash
curl -s -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 5f8a1e2e-....' \
  -d '{
        "allowPartialShipment": false,
        "lines": [
          {"sku": "SKU-1", "quantity": 2},
          {"sku": "SKU-2", "quantity": 1, "giftWrap": true}
        ]
      }'
# 201 Created, Location: /orders/ord-...
# Both lines allocate and release in this SAME call when inventory-storage
# has stock, so a ship-complete order typically comes back already
# Released:
# {"id":"ord-...","status":"Released","allowPartialShipment":false,
#  "promiseDate":"2026-08-27T09:00:00Z",
#  "lines":[{"lineNo":1,"sku":"SKU-1","quantity":2,"pathId":"pick",
#            "giftWrap":false,"status":"Released","reservationId":"res-..."}, ...]}
```

A `409` from inventory-storage backorders that one line instead — a
business fact, not a call failure — and, per BR3, a ship-complete order
with any backordered line stays `Backordered` and releases nothing until
`retry-allocation` clears it:

```bash
# {"id":"ord-...","status":"Backordered","allowPartialShipment":false,
#  "lines":[..., {"lineNo":2,"sku":"SKU-2","quantity":1,"pathId":"pick",
#                 "giftWrap":true,"status":"Backordered"}]}
```

**GetOrder:**

```bash
ORDER_ID=ord-...   # from the response above
curl -s localhost:8080/orders/$ORDER_ID
```

**RetryAllocation** — re-attempts the backordered lines and only those,
then releases whatever it newly clears:

```bash
curl -s -X POST localhost:8080/orders/$ORDER_ID/retry-allocation
# 200 OK — status becomes Released (or PartiallyReleased) once every
# backorder clears and BR3 permits release
```

**CancelOrder** — revokes every allocated line's reservation, then cancels:

```bash
curl -s -i -X DELETE localhost:8080/orders/$ORDER_ID
# 204 No Content
```

Rejected by BR6 once any line has been released — which, since a
ship-complete order that allocates cleanly is released automatically
inside `POST /orders`, can happen right after intake:

```bash
curl -s -X DELETE localhost:8080/orders/$ORDER_ID
# 409 Conflict, application/problem+json
# {"type":"https://errors.order-management.warehouse-systems.dev/order-already-released",
#  "title":"Order already has released lines and can no longer be cancelled",
#  "status":409,...}
```

## Quality gate

Every `make` target mirrors a step in `.github/workflows/ci.yml`, so the same
feedback CI gives you post-push is available locally, pre-commit:

```bash
make check       # fmt-check + vet + build + lint + test -race
make check-all   # check + coverage (gate: 90% on domain + application)
```

| Target | What it runs |
| --- | --- |
| `build` | `go build ./...` |
| `vet` | `go vet ./...` |
| `fmt` / `fmt-check` | `gofmt -w .` / fail if `gofmt -l .` is non-empty |
| `lint` | `golangci-lint run ./...` (CI pins `v2.13.1`) |
| `test` | `go test ./... -race` |
| `coverage` | coverage profile + the 90% gate |

Git hooks are wired through [lefthook](https://github.com/evilmartians/lefthook)
— `pre-commit` runs fmt-check/vet/lint, `pre-push` runs `make check`. Hooks are
not tracked by git, so activate them once per clone:

```bash
brew install lefthook   # or: go install github.com/evilmartians/lefthook@latest
lefthook install
```

CI mirrors the local gate and the fleet's sensor set (same shape as
wes-work-planning's CI): **`lint`**, **`guide-lint`** (agent-guide and
harness sensors under `scripts/harness/`), **`complexity`** (gocyclo/
cyclop/gocognit/nestif/funlen only), **`test`** (+ coverage gate), **`bdd`**
(godog/Gherkin acceptance suite under `features/`), **`contract`**
(Schemathesis over `apis/openapi.yaml`, `scripts/contract-test.sh`),
**`evals-tests`** (MCP evals E1–E3), **`integration`**
(`-tags=integration` Kafka and Postgres adapter suites; every test boots its
own Testcontainers broker/database, so the job needs only Docker — no
`services:` container, no `DATABASE_URL`), **`mutation-fast`** (gremlins on
`./internal/domain/order`, thresholds pinned in `.gremlins.yaml`) with the
exhaustive weekly **`mutation`** run on schedule, **`vuln`**
(govulncheck), **`api-lint`** (Spectral on `apis/openapi.yaml` and
`apis/asyncapi.yaml`), **`arch-test`** (arch-go fitness tests in
`internal/architecture/`), and **`docs-api-drift`** (regenerates the
Docusaurus API reference from the spec; its diff step currently checks the
wrong path and cannot fail — see `.claude/rules/ci-quality-gates.md`),
**`web`** (MFE lint/typecheck/test/build) — plus the packaging/security/
publish jobs (`helm-lint` and `trivy-scan` on PRs into `main`;
`docker-publish` and `release` on pushes to `main`) and the advisory
scheduled **`drift`** job.

## Operator micro-frontend (`web/`)

`web/` is `order_mgmt_mfe`, this context's Module Federation remote. It talks
only to this service's own REST API and is never part of `make check`.

**Standalone development** is unchanged:

```bash
cd web && npm install && npm run dev     # http://localhost:5181
```

**Deployed to the kind cluster**, it is built into a static bundle and served
by its own `nginx-unprivileged` pod:

```bash
cd web
docker build --build-context uikit=../../warehouse-ui-kit \
  -t warehouse/order-management-frontend:local .
```

The cluster's localhost topology separates the two kinds of traffic onto two
independent entrypoints, and neither proxies to the other:

| URL | Served by | Carries |
|---|---|---|
| `http://localhost/mfes/order-management/` | Nginx web gateway → this remote's nginx pod | HTML, JS, CSS, fonts, `remoteEntry.js` |
| `http://localhost:8000/api/order-management/` | Kong | this service's REST API |

Kong never serves frontend assets, and the Nginx gateway never proxies an API.
Enable the workload with `frontend.enabled=true` in the Helm chart; the Service
is deliberately `ClusterIP` with no Ingress/HTTPRoute, because frontend path
routing belongs to the Nginx web gateway in `warehouse-infra`.

Because one image must work in more than one environment, the remote reads its
API origin at runtime from `window.__WAREHOUSE_CONFIG__.apiOrigin` (published
by the console shell from `/config.json`) rather than baking a hostname in at
build time. A production build with no runtime config **fails loudly** instead
of silently falling back to a developer port; standalone `npm run dev` still
uses `http://localhost:8086`. See `web/src/config.ts`.

Chart invariants are asserted by:

```bash
python3 charts/order-management/tests/test_service_selectors.py
```

which proves every Service selects exactly one Deployment — the OLTP Service
must never select the frontend, analytics or MCP pods.

## Deferred

The following are **deliberately out of scope for this first pass**. They are
listed so an absence is never mistaken for an oversight — each is a decision,
not a gap someone forgot about. (Originally the "Deferred (v1)" list; items
since shipped — Helm chart, MCP adapter, Docker publishing/releases,
Kafka integration, analytics data product, and the full CI sensor set
above — have been removed from it.)

- **Kafka release-confirmation reply events from wes-work-planning.**
  This service publishes `OrderAllocated`/`OrderPartiallyAllocated` to
  Kafka fire-and-forget — it never learns whether wes-work-planning's
  consumer actually processed the event or successfully enqueued its own
  work. This is a deliberate choice (mirroring inventory-storage's own
  ADR 0004 stance on transactional guarantees), not an oversight: a
  confirmation-loop pattern (e.g. this service subscribing to a
  `WorkEnqueued` reply event) is real, scoped-down future work. See
  [ADR 0005](docs/docs/adr/0005-choreographed-release-via-kafka.md).
- **Real carrier-rate / transit-time promise.** The promise is a CPT window
  derived from fulfillment capability (ADR 0014), with `LeadTimePolicy` as the
  tagged fallback — it ends at the building's door. There is no live carrier
  integration, and no such service exists in this fleet to call.
- **Sweeping an orphaned hold.** Nothing expires an order held with
  `releaseOnAllocation: false` that its caller never releases or cancels
  (ADR 0020); it keeps its inventory reservations until someone does.
- **Clawing back released work on cancellation.** Documented in detail in
  [ADR 0004](docs/docs/adr/0004-cancellation-boundary-at-release.md).
- **Killing the surviving promise boundary mutants.** The mutation gate
  is pinned just under the measured baseline (efficacy 89 / mutant-coverage
  83 — see `.gremlins.yaml`); ratcheting those thresholds toward the fleet's
  99/99 is tracked follow-up work.

### Docusaurus site

The documentation site mirrors the other `warehouse-systems` repositories'
structure (Docusaurus 3.10.2, `docusaurus-plugin-openapi-docs`/
`docusaurus-theme-openapi-docs`). It lives under `docs/`:

```bash
cd docs
npm ci
npm run clean-api-docs order && npm run gen-api-docs order   # regenerate docs/docs/api-reference/rest/
npm run typecheck
npm run build          # onBrokenLinks is 'throw'
```

All ADRs under `docs/docs/adr/` are wired into the sidebar's
"Architecture Decision Records" category alongside the `adr/about.md`
index page. `.github/workflows/docs.yml` builds and deploys the site to
GitHub Pages on every push to `main` that touches `docs/**`, publishing to
**https://iqvo.github.io/order-management/**.

## Architecture Decision Records

1. [0001 — Hexagonal (ports & adapters) architecture](docs/docs/adr/0001-hexagonal-ports-and-adapters.md)
2. [0002 — HTTP consumer of inventory-storage and wes-work-planning, not shared code](docs/docs/adr/0002-http-consumer-of-inventory-and-wes-not-shared-code.md) (partially superseded by 0005 for release)
3. [0003 — Ship-complete by default and fail-closed allocation](docs/docs/adr/0003-ship-complete-default-and-fail-closed-allocation.md)
4. [0004 — The cancellation boundary is release](docs/docs/adr/0004-cancellation-boundary-at-release.md)
5. [0005 — Choreographed release via Kafka, folded allocate-then-release, and pathId goes internal-only](docs/docs/adr/0005-choreographed-release-via-kafka.md)
6. [0006 — Analytical data product](docs/docs/adr/0006-analytical-data-product.md)
7. [0007 — Adopt the fleet micro-frontend console](docs/docs/adr/0007-adopt-fleet-micro-frontend-console.md)
8. [0008 — Fulfillment-class demand-shape classifier](docs/docs/adr/0008-fulfillment-class-demand-shape-classifier.md)
9. [0009 — Standard metrics convention](docs/docs/adr/0009-standard-metrics-convention.md)
10. [0010 — MCP inbound adapter](docs/docs/adr/0010-mcp-inbound-adapter.md)
11. [0011 — Adopt the fleet REST identity (static bearer keys, read/read-write scopes)](docs/docs/adr/0011-adopt-fleet-rest-identity.md) (superseded by 0012)
12. [0012 — Remove the REST/MCP bearer auth layer](docs/docs/adr/0012-remove-rest-mcp-bearer-auth.md)
13. [0013 — Process-path selection as a real domain policy, validated against a live catalogue](docs/docs/adr/0013-process-path-selection-as-a-domain-policy.md)
14. [0014 — The delivery promise is a CPT window derived from fulfillment capability](docs/docs/adr/0014-promise-derived-from-fulfillment-capability.md)
15. [0015 — wes-work-planning's PathCapacityChanged wired as the real PathCapacity adapter](docs/docs/adr/0015-wes-work-planning-path-capacity-changed-wired.md)
16. [0016 — Eligibility-driven process-path selection](docs/docs/adr/0016-eligibility-driven-process-path-selection.md)
17. [0017 — Per-shipment-group promising](docs/docs/adr/0017-per-shipment-group-promising.md)
18. [0018 — RepromiseOrder consumer and OrderRepromised](docs/docs/adr/0018-repromise-order-consumer-and-order-repromised.md)
19. [0019 — Promise KPIs on the Order Funnel data product](docs/docs/adr/0019-promise-kpis-on-order-funnel.md)
20. [0020 — Network-originated demand: release-on-allocation, deadline feasibility, Network promise basis](docs/docs/adr/0020-network-originated-demand-hold-and-deadline-feasibility.md)
21. [0021 — Multi-path attribute-driven routing](docs/docs/adr/0021-multi-path-attribute-driven-routing.md)
22. [0022 — Transactional outbox for dual-topic event publishing](docs/docs/adr/0022-transactional-outbox.md)
23. [0023 — Transactional Idempotency-Key middleware for POST /orders](docs/docs/adr/0023-idempotency-key-middleware.md)
24. [0024 — Optimistic concurrency (version column) for the Order aggregate](docs/docs/adr/0024-optimistic-concurrency-version-column.md)
25. [0025 — Per-dependency circuit breakers, read-only retry, Kafka DLQ, graceful shutdown](docs/docs/adr/0025-resilience-circuit-breakers-retry-dlq-shutdown.md)
26. [0026 — Per-workload HorizontalPodAutoscaler and pgxpool tuning](docs/docs/adr/0026-horizontal-autoscaling-and-pgxpool-tuning.md)
27. [0027 — Partition key (OrderId) on the integration publisher's Kafka messages](docs/docs/adr/0027-kafka-integration-publisher-partition-key.md)
28. [0028 — Send Idempotency-Key on POST /reservations](docs/docs/adr/0028-inventory-storage-reservations-idempotency-key.md)
29. [0029 — Run golang-migrate against a direct Postgres connection, not PgBouncer](docs/docs/adr/0029-migrations-direct-postgres-connection.md)
30. [0030 — CloudEvents 1.0 as the mandatory event envelope](docs/docs/adr/0030-cloudevents-mandatory-event-envelope.md)
31. [0031 — Consume warehouse-planning's capacity plans into a local planned-capacity read model](docs/docs/adr/0031-consume-warehouse-planning-capacity-plans.md)
32. [0032 — Housekeeping sweeper for idempotency keys and published outbox rows](docs/docs/adr/0032-housekeeping-sweeper-idempotency-keys-and-outbox.md)
33. [0033 — Boot-time first-dial retry, synchronous-writer BatchTimeout/acks, and DLQ topic-create retry](docs/docs/adr/0033-bootretry-and-kafka-writer-tuning.md)

## License

MIT (or match the other repos' licensing — TBD).
