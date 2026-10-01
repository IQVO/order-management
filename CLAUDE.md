# Order Management

Generic/Supporting bounded context: order intake, per-line stock allocation,
a capability-derived delivery promise, and choreographed release — the missing upstream
Open Host Service for the `warehouse-systems` fleet. Owns **Order** and
**OrderLine** as first-class aggregates. Before this context existed, "an
order" was just an unowned string (`OrderRef`/`DemandRef`/`Reference`)
independently reinvented by three other services.

Design source of truth: `/Users/claudioed/warehouse-systems/.hermes/plans/2026-08-25_023800-order-management.md`
and the fleet's shared DDD reference docs at `/Users/claudioed/docs/amazon-fulfillment-ddd.md`
and `/Users/claudioed/warehouse-systems-ddd.md`. Honor their ubiquitous
language and strategic classifications.

Study project — not a production system, not affiliated with any real-world
company (see README.md banner).

## Project Overview

- **Module:** `github.com/claudioed/order-management`, Go 1.26.
- **Four binaries, one module:** `cmd/order` (HTTP + Kafka-choreography
  OLTP service), `cmd/mcp` (MCP read server, ADR-0010), `cmd/order-projector`
  (analytics writer, ADR-0006), `cmd/order-reports` (analytics read-only API).
- **Public intent, one command.** Since ADR-0005, a caller expresses "place
  an order" via `POST /orders` alone — allocation and release happen inside
  that same call as a folded, best-effort saga. There is no public
  `/allocate` verb; `POST /orders/{id}/release` exists only for orders
  received with `releaseOnAllocation=false` (ADR-0020 hold).
- **Suppliers:** `inventory-storage` (synchronous HTTP, `POST /reservations`,
  `GET /products/{sku}/classification`) and `wes-work-planning` (release via
  Kafka choreography — no HTTP call). Consumes Kafka facts from
  `process-path-management`, `wes-work-planning` and `fulfillment-execution`
  (ADR-0013..0018). `network-fulfillment` is an inbound HTTP caller
  (ADR-0020). See `.claude/rules/bounded-context-boundary.md`.

## Architecture (NON-NEGOTIABLE — identical shape to the other fleet services)

Hexagonal / Ports & Adapters, enforced by an `arch-go` fitness test
(`internal/architecture/architecture_test.go`, CI job `arch-test`). Strict
dependency rule: **domain depends on nothing; application depends on
domain; adapters depend on application/domain.** No framework, HTTP, or SQL
type ever appears in the domain layer.

```
cmd/
  order/                       main.go — HTTP + Kafka-choreography OLTP service
  mcp/                         MCP server composition root (ADR-0010)
  order-projector/             analytics writer (ADR-0006), FirstOffset consumer
  order-reports/                analytics read-only REST API
internal/
  domain/
    order/                     Order aggregate, OrderLine, Status, invariants,
                                PromisePolicy (+ LeadTimePolicy fallback), PathSelectionPolicy
    processpath/               process-path definitions
    shared/                    OrderId, SKU, PathId, domain events, errors
  application/
    ports/                     OrderRepo, EventPublisher, Clock, OrderMetrics,
                                InventoryReservationClient, ProcessPathCatalogue,
                                CPTScheduleCache, ProductClassificationLookup,
                                PathCapacity, RepromiseProcessedEvents
                                (NO WorkReleaseClient — deleted, ADR-0005)
    usecases/                  ReceiveOrder, RetryAllocation, ReleaseHeldOrder,
                                CancelOrder, GetOrder, RepromiseOrder (Kafka-driven)
                                (AllocateOrder/ReleaseOrder deleted as public types, ADR-0005;
                                shared allocate-then-release logic lives in allocation.go)
  adapters/
    inbound/http/               chi handlers, DTOs, RFC 7807 error mapping, CORS
    inbound/kafka/              RepromiseConsumer (warehouse.fulfillment.events), ADR-0018
    inbound/mcp/                MCP read-only tools (get_order, get_promise_health)
    outbound/inventorystorage/  HTTP client: POST /reservations, DELETE /reservations/{id}
    outbound/productclassification/  HTTP client: GET /products/{sku}/classification
    outbound/kafkacatalog/, kafkacptschedule/, kafkapathcapacity/
                                capability caches (PATH_CATALOGUE_SOURCE=kafka)
    outbound/pathcapacity/      "unknown capacity" default adapter
    kafka/cloudevents/          the ONLY CloudEvents 1.0 New/Decode helpers (ADR-0030)
    outbound/kafka/             integration + analytics publishers (EVENT_PUBLISHER=kafka)
    outbound/events/            log publisher (default, EVENT_PUBLISHER=log)
    outbound/postgres/          pgxpool repo + golang-migrate runner
    outbound/memory/            in-memory repo for tests
    outbound/analyticsstore/    analytics read/write store (ADR-0006)
    outbound/telemetry/         OTel MeterProvider/TracerProvider, otelchi RED (ADR-0009)
  analytics/report/             analytical read model — depends on nothing internal (enforced)
migrations/                    golang-migrate SQL (OLTP)
migrations/analytics/          golang-migrate SQL (analytics store)
apis/openapi.yaml              REST contract (5 order endpoints + /healthz)
apis/asyncapi.yaml             Kafka contract — CloudEvents 1.0 (5 channels, 20 messages, ADR-0030)
web/                           order-mgmt-mfe — Vite/React MFE remote (ADR-0007), NOT part of the Go module
docs/docs/adr/                 20 ADRs — see .claude/rules/adrs.md
```

**Note on `web/`:** owns its own `package.json`/build/dev-server (`:5181`),
talks only to this service's own REST API, and never participates in
`make check`/`check-all`. See `.claude/rules/frontend-mfe.md`.

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message this service produces or consumes (integration
`warehouse.<ctx>.events` AND analytics `warehouse.<ctx>.analytics`) is a
CloudEvents 1.0 event in structured content mode. This is a hard fleet rule,
not a preference:

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  via `internal/adapters/kafka/cloudevents/`; transport stays kafka-go.
- Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.
- Required attributes: `specversion=1.0`, `id` (UUID, stable across outbox
  redelivery), `source=/warehouse/order-management`, `type`, `subject` (aggregate id), `time`
  (occurred-at, UTC), `datacontenttype=application/json`,
  `dataschema=urn:warehouse:order-management:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
  for this service: `com.warehouse.wes.order-management.<entity>.<EventName>`. Breaking payload
  change => new `.v2` type + new dataschema version, never mutate.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
  `id`, and DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.

Full standard and the fleet's cross-service type catalogue: ADR-0030
(`docs/docs/adr/`).

For this service: every Order* event uses entity `order`
(`com.warehouse.wes.order-management.order.<EventName>`); `subject` and the
Kafka key are the order id. Consumed types (exact):
`com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed`,
`com.warehouse.wes.fulfillment-execution.package.PackageManifested`,
`com.warehouse.wes.process-path-management.processpath.ProcessPath{Created,Updated,Deactivated}`,
`com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged`,
`com.warehouse.wes.work-planning.workpool.PathCapacityChanged`, plus this
service's own analytics types (projector).

## Key Commands

```bash
# Local quality gate (mirrors .github/workflows/ci.yml)
make check          # fmt-check + vet + build + lint + test — run after every change
make check-all       # check + coverage (90% gate) + arch-test + bdd
make integration     # Kafka adapters against a Testcontainers broker
make coverage         # go test -race -coverprofile + the 90% gate
make bdd              # godog/Gherkin acceptance suite (features/*.feature)
make arch-test        # arch-go hexagonal/analytics-isolation/ports fitness tests
make mutation-fast    # gremlins subset (internal/domain/order), thresholds in .gremlins.yaml
make vuln             # govulncheck ./...

# Run locally
go run ./cmd/order                    # in-memory adapters if DATABASE_URL unset
docker compose up -d postgres         # Postgres 16 on :5434
docker compose -f docker-compose.kafka.yml up -d   # local Kafka (KRaft, :9092)

# Docs site (Docusaurus — regenerate after ANY apis/openapi.yaml change)
cd docs && npm ci
npm run clean-api-docs order && npm run gen-api-docs order   # -> docs/docs/api-reference/rest/
npm run typecheck && npm run build   # onBrokenLinks / onBrokenAnchors are both 'throw'

# golangci-lint (CI-pinned version)
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1
```

Full CI job list, coverage/mutation gates, and the docs-drift check live in
`.claude/rules/ci-quality-gates.md` — read it before touching `.github/workflows/`
or `apis/*.yaml`.

## Code Standards / Testing

- gofmt/go vet clean; every package has a doc comment.
- `.golangci.yml` copied verbatim from `inventory-storage`'s own config.
- Table-driven tests: domain + application layers use the in-memory adapter
  and fake HTTP clients — **never hit a real network in a unit test.**
  Kafka-touching `-tags=integration` tests use **testcontainers**, never a
  skip-gated `KAFKA_BROKERS` env check (CI's `integration` job provisions no
  external broker; testcontainers is the only variant that actually runs).
- One httptest per REST endpoint: at least one success and one error path.
- Every named invariant in `.claude/rules/domain-model.md` needs a dedicated
  failing-path test.
- Coverage gate: **90%** on `./internal/domain/...,./internal/application/...`.
- Mutation gate (gremlins, `mutation-fast` on every push/PR, full `mutation`
  weekly): thresholds efficacy 89 / mutant-coverage 83 on
  `internal/domain/order` (measured 89.29% / 83.58%) — see `.gremlins.yaml`.
  Do not let a change silently regress below the pinned threshold.
- godog/BDD acceptance suite: `features/*.feature`, run via
  `go test ./... -run TestFeatures` (CI job `bdd`).

## Where the rest of the detail lives

This file is the index. Full DDD/tactical detail, API contracts, ADR
summaries, and CI mechanics are split into `.claude/rules/` so this file
stays short and current:

- **`.claude/rules/bounded-context-boundary.md`** — the Customer/Supplier
  rule, what this repo MUST NOT do (import Go packages, touch another
  service's DB).
- **`.claude/rules/domain-model.md`** — ubiquitous language, aggregate
  invariants (BR2/BR3/BR6), the 10 domain events, the promise and hold
  concepts, and the folded allocate-then-release use-case flow (ADR-0005).
- **`.claude/rules/api-contracts.md`** — the REST endpoints, the exact
  outbound HTTP contracts to inventory-storage, the Kafka integration/
  analytics/inbound channels (asyncapi.yaml) and local-cache consumers,
  and the MCP tools.
- **`.claude/rules/adrs.md`** — one-line summary + link for all 20 ADRs,
  including which ones supersede an earlier one.
- **`.claude/rules/ci-quality-gates.md`** — every CI job (`lint`, `test`,
  `bdd`, `integration`, `mutation-fast`, `vuln`, `api-lint`, `arch-test`,
  `docs-api-drift`, `helm-lint`, `web`, `trivy-scan`, `docker-publish`,
  `release`, scheduled `mutation`/`drift`, plus `codeql`, `scorecard`,
  `docs`), branch triggers, and the OpenAPI/AsyncAPI drift-regeneration
  procedure.
- **`.claude/rules/deferred-and-known-gaps.md`** — what is deliberately NOT
  built yet, and why each gap is a decision, not an oversight.
- **`.claude/rules/frontend-mfe.md`** — the `web/` micro-frontend remote,
  its isolation from the Go module, CORS config.

## Git workflow

GitFlow: `feature/*` branches off `develop`, PR into `develop`
(`gh pr create --base develop`); `develop` promotes to `main` for release.
Do not merge your own PR — leave it open for independent review. Do not
force-push over history once pushed.
