---
id: configuration
title: Configuration
sidebar_label: Configuration
description: Every environment variable each order-management binary reads, with its default, whether it is required, what it does and the file that reads it.
---

# Configuration

Every setting is an environment variable. There is no config file and no
flag parsing. Each binary in `cmd/` is its own composition root and reads
its own variables, so the same name can have a different default per
binary (`HTTP_ADDR` is `:8080` for `cmd/order` and `:8092` for
`cmd/order-reports`).

Conventions that apply to every table below:

- **Unset and empty are the same.** Every reader uses `os.Getenv` (or the
  `getenv(key, fallback)` helper that treats `""` as unset).
- **Durations** are Go durations (`500ms`, `90s`, `48h`).
- **Invalid values never crash boot**, except where the table says
  *boot error*. An unparsable duration logs
  `ignoring invalid duration env var` at WARN and uses the default.
- `ENVIRONMENT` is read inside the shared telemetry adapter
  (`internal/adapters/outbound/telemetry/telemetry.go`), so every binary
  reads it, even though no `main.go` names it.
- Test-only variables (`*_TEST_*`, `TESTCONTAINERS_*`) are not listed.

## `cmd/order`: the OLTP REST service

Sources live in `cmd/order/` unless stated otherwise.

### Process and HTTP

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST API. | `main.go`, `http.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn` or `error` (case-insensitive). Anything else means `info`. Logs are JSON on stdout. | `main.go`, `config.go` (`newLogger`) |
| `SHUTDOWN_DRAIN_DELAY` | `5s` | no | How long to wait after `/readyz` turns `503` before the listener closes (ADR 0025). `0` disables the wait. A negative or invalid value logs a warning and uses `5s`. | `shutdown.go` |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5181` | no | Comma-separated browser origins allowed to call the API: the console shell's and this repo's MFE dev ports (ADR 0007). Allowed headers are `Content-Type`, `Authorization` and `Idempotency-Key` (the MFE sends the last one on `POST /orders`, ADR 0038). | `internal/adapters/inbound/http/server.go` |

### Persistence

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `DATABASE_URL` | unset | no | Postgres DSN for the request pool (`pgxpool`, `MaxConns` 10, `statement_timeout` 5s). **Unset means the in-memory adapters are used.** That also turns off the `Idempotency-Key` middleware, the transactional outbox, the housekeeping sweeper and the `order.outbox.lag_seconds` gauge. | `config.go`, `adapters.go` |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct, non-PgBouncer DSN used **only** by the golang-migrate step at boot (ADR 0029). The request pool always uses `DATABASE_URL`. | `config.go` |
| `MIGRATIONS_PATH` | `migrations` | no | Directory of the `*.up.sql` files applied at boot. Relative to the working directory (`/app` in the image). | `config.go` |

### Events and Kafka

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `EVENT_PUBLISHER` | `log` | no | `log` writes each domain event as an INFO line (`domain event published`). `kafka` publishes to `warehouse.order-management.events` (integration) and `warehouse.order-management.analytics`. With `DATABASE_URL` set, `kafka` goes through the `outbox_events` table and the in-process relay (ADR 0022). Without it, events go straight to the broker. | `main.go`, `adapters.go` |
| `KAFKA_BROKERS` | `localhost:9092` for the publishers; unset for everything else | depends | Comma-separated broker list. The publishers fall back to `localhost:9092`. Each consumer has its own rule: the re-promise consumer starts only when this is set; the planned-capacity consumer needs it too; `PATH_CATALOGUE_SOURCE=kafka` and `PRODUCT_CLASSIFICATION_MODE=kafka` are **boot errors** without it. | `kafka.go`, `wiring.go`, `planned_capacity.go`, `product_classification.go` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | How long the outbox relay sleeps after a pass that found less than a full batch (100 rows). `0` or invalid means `1s`. | `kafka.go` |
| `HOUSEKEEPING_INTERVAL` | `1h` | no | Period of the housekeeping sweeper (ADR 0032). `0` disables the sweeper. Only used when `DATABASE_URL` is set. | `housekeeping.go` |
| `IDEMPOTENCY_KEY_TTL` | `24h` | no | Age after which `idempotency_keys` rows are deleted. `0` keeps them forever. | `housekeeping.go` |
| `OUTBOX_RETENTION` | `168h` | no | Age after which **published** `outbox_events` rows are deleted. Unpublished rows are never swept. `0` keeps them forever. | `housekeeping.go` |
| `DEMAND_PROJECTION_SITE_ID` | unset | no | Turns on the `SiteSkuDemandChanged` projection (ADR 0035): every allocated, re-allocated, re-promised or cancelled line emits one event for this static site, with `assignment_version` `static-site-v1`. Unset means the event is never emitted. | `config.go` |

### Downstream dependencies and promise policy

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `INVENTORY_STORAGE_MODE` | `permissive` | no | `http` calls inventory-storage `POST /reservations` and `DELETE /reservations/{id}` behind a circuit breaker. Any other value means `permissive`: allocation refuses to run (`downstream-not-configured`) instead of faking a reservation. | `outbound.go` |
| `INVENTORY_STORAGE_BASE_URL` | unset | yes, when mode is `http` | Base URL of inventory-storage, for example `http://inventory-storage.warehouse-systems.svc.cluster.local:80`. Not validated at boot: an empty value with `http` mode fails each call instead. | `outbound.go` |
| `PRODUCT_CLASSIFICATION_MODE` | `permissive` | no | `kafka` keeps a local copy of product-master's `ProductClassified` events (ADR 0036) for path eligibility. `permissive` treats every SKU as unclassified. `http` was removed and is a **boot error**, and so is any other unknown value. | `product_classification.go` |
| `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` | unset, no default on purpose | yes, when mode is `kafka` (**boot error** otherwise) | Stable consumer group of the classification consumer on `warehouse.product-master.events`. The fleet deployment uses `order-management-product-classification`. | `product_classification.go` |
| `PATH_CATALOGUE_SOURCE` | `none` | no | `kafka` starts three replay caches (process-path catalogue, CPT schedule, path capacity) and blocks boot until each has replayed, up to 60 s each. Any other value skips path validation and uses only the lead-time promise. | `outbound.go`, `wiring.go` |
| `DEFAULT_SITE_ID` | `site-1` | no | Site whose CPT schedule the promise reads. Also the fallback of `PLANNED_CAPACITY_SITE_ID`. | `main.go`, `outbound.go` |
| `PROMISE_DEFAULT_LEAD_TIME` | `48h` | no | Lead-time fallback promise for any path without an override. `0` or invalid means `48h`. | `outbound.go` |
| `PROMISE_PATH_LEAD_TIMES` | unset | no | Per-path overrides, `pathId=duration` pairs separated by commas, for example `pick=24h,singles=6h`. A malformed pair is skipped with a WARN. | `outbound.go`, `config.go` (`perPathLeadTimes`) |
| `PLANNED_CAPACITY_CONSUMER_GROUP` | unset, no default on purpose | no | The on/off switch of the warehouse-planning integration (ADR 0031). Set: consume `warehouse.warehouse-planning.events` into the planned-capacity read model, register `GET /planned-capacity` and annotate order responses with `capacityConstraint`. Unset: none of that exists. The fleet uses `order-management-planned-capacity`. | `planned_capacity.go` |
| `PLANNED_CAPACITY_SITE_ID` | value of `DEFAULT_SITE_ID` | no | The warehouse-planning `location` an order's promise is matched against (the fleet sets `SIM1`). | `planned_capacity.go` |

### Telemetry

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC endpoint for traces and metrics. A bare `host:port` is turned into `http://host:port`. Export never blocks boot. | `main.go`, `internal/adapters/outbound/telemetry/telemetry.go` |
| `OTEL_SERVICE_NAME` | `order-management` | no | `service.name` resource attribute, also the otelchi server name. | `main.go` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute. The chart sets it to the image tag. | `main.go` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go` |

## `cmd/mcp`: the MCP server

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address of the Streamable HTTP endpoint (mounted on `/*`). | `cmd/mcp/main.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `warn` or `error`, **case-sensitive** here (unlike `cmd/order`). Anything else means `info`. | `cmd/mcp/main.go` (`logLevel`) |
| `DATABASE_URL` | unset | no | OLTP Postgres DSN for `get_order`. Unset means an empty in-memory repository, so every lookup is `order not found`. When set, this binary also runs the OLTP migrations at boot. | `cmd/mcp/main.go` |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct DSN for the migration step only (ADR 0029). | `cmd/mcp/main.go` |
| `MIGRATIONS_PATH` | `migrations` | no | OLTP migrations directory. | `cmd/mcp/main.go` |
| `ANALYTICS_DATABASE_URL` | unset | no | Analytical database for `get_promise_health`, opened read-only (no ping at boot). Unset, or a pool that cannot be created, means an empty in-memory store, so every KPI is `0`. | `cmd/mcp/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `cmd/order`. | `cmd/mcp/main.go` |
| `OTEL_SERVICE_NAME` | `order-management-mcp` | no | `service.name` resource attribute. | `cmd/mcp/main.go` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute. | `cmd/mcp/main.go` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go` |

## `cmd/order-projector`: the analytics writer

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | unset | **yes** (boot error `ANALYTICS_DATABASE_URL is required`) | Read-write DSN of the analytical database (`MaxConns` 5, `statement_timeout` 10s). The projector owns this schema. | `cmd/order-projector/main.go` |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | no | Directory of the analytical migrations, applied at boot. | `cmd/order-projector/main.go` |
| `KAFKA_BROKERS` | `localhost:9092` | no | Brokers of the `warehouse.order-management.analytics` topic, consumed with group `order-management-analytics`. | `cmd/order-projector/main.go` |
| `ADMIN_ADDR` | `:8091` | no | Listen address of the admin server, which serves only `GET /healthz`. | `cmd/order-projector/main.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn`/`warning` or `error` (case-insensitive). | `cmd/order-projector/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `cmd/order`. | `cmd/order-projector/main.go` |
| `OTEL_SERVICE_NAME` | `order-projector` | no | `service.name` resource attribute. | `cmd/order-projector/main.go` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute. | `cmd/order-projector/main.go` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go` |

## `cmd/order-reports`: the analytics reader

| Variable | Default | Required? | Meaning | Source |
| --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | unset | **yes** (boot error `ANALYTICS_DATABASE_URL is required`) | DSN of the analytical database, opened with `default_transaction_read_only=on` (`MaxConns` 5, `statement_timeout` 15s). Use a read-only role. The chart feeds it from the secret key `ANALYTICS_READER_DATABASE_URL`. | `cmd/order-reports/main.go` |
| `HTTP_ADDR` | `:8092` | no | Listen address of the reports API (`/reports/funnel`, `/reports/funnel/freshness`, `/healthz`). | `cmd/order-reports/main.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn`/`warning` or `error` (case-insensitive). | `cmd/order-reports/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `cmd/order`. | `cmd/order-reports/main.go` |
| `OTEL_SERVICE_NAME` | `order-reports` | no | `service.name` resource attribute. | `cmd/order-reports/main.go` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute. | `cmd/order-reports/main.go` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/adapters/outbound/telemetry/telemetry.go` |

## How the Helm chart sets these

`charts/order-management` renders a subset of the variables from dedicated
values. Everything else (`PATH_CATALOGUE_SOURCE`, `DEFAULT_SITE_ID`,
`PROMISE_*`, `OUTBOX_RELAY_INTERVAL`, `CORS_ALLOWED_ORIGINS`, `LOG_LEVEL`)
goes through `extraEnv` (or `mcp.extraEnv`).

| Chart value | Variable (container) | Rendered when |
| --- | --- | --- |
| `config.httpAddr` | `HTTP_ADDR` (order) | always |
| `config.eventPublisher` | `EVENT_PUBLISHER` (order) | always |
| `config.shutdownDrainDelay` | `SHUTDOWN_DRAIN_DELAY` (order) | always |
| `config.housekeepingInterval`, `config.idempotencyKeyTtl`, `config.outboxRetention` | `HOUSEKEEPING_INTERVAL`, `IDEMPOTENCY_KEY_TTL`, `OUTBOX_RETENTION` (order) | a database is configured |
| `kafka.brokers` | `KAFKA_BROKERS` (order) | `config.eventPublisher=kafka`, or `plannedCapacity.consumerGroup` or `productClassification.consumerGroup` is set |
| `productClassification.mode`, `.consumerGroup` | `PRODUCT_CLASSIFICATION_MODE`, `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` (order) | the value is non-empty |
| `plannedCapacity.consumerGroup`, `.siteId` | `PLANNED_CAPACITY_CONSUMER_GROUP`, `PLANNED_CAPACITY_SITE_ID` (order) | the group is non-empty |
| `demandProjection.siteId` | `DEMAND_PROJECTION_SITE_ID` (order) | non-empty |
| `inventoryStorage.mode`, `.baseUrl` | `INVENTORY_STORAGE_MODE`, `INVENTORY_STORAGE_BASE_URL` (order) | mode always, URL when non-empty |
| `database.existingSecret` / `database.url` | `DATABASE_URL`, `MIGRATIONS_DATABASE_URL` (order, mcp) | a database is configured; the migrations key is `optional: true` |
| `otel.endpoint`, `otel.serviceName` | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` (order) | `otel.enabled` |
| `image.tag`, `environment` | `SERVICE_VERSION`, `ENVIRONMENT` (order, projector, reports) | always |
| `mcp.httpAddr` | `MCP_ADDR` (mcp) | `mcp.enabled` |
| `analytics.projector.adminAddr`, `analytics.migrationsPath` | `ADMIN_ADDR`, `ANALYTICS_MIGRATIONS_PATH` (projector) | `analytics.enabled` |
| `analytics.reports.httpAddr` | `HTTP_ADDR` (reports) | `analytics.enabled` |
| `analytics.database.*` | `ANALYTICS_DATABASE_URL` (projector from key `ANALYTICS_DATABASE_URL`, reports from key `ANALYTICS_READER_DATABASE_URL`) | `analytics.enabled` |

:::note[Gaps worth knowing]
- The chart never renders `ANALYTICS_DATABASE_URL` for the MCP container.
  Unless it is added through `mcp.extraEnv`, `get_promise_health` in a chart
  deployment always reports zeros.
- `KAFKA_BROKERS` is not rendered when only `extraEnv` sets
  `PATH_CATALOGUE_SOURCE=kafka` with `config.eventPublisher=log`. That
  combination fails boot with `PATH_CATALOGUE_SOURCE=kafka requires
  KAFKA_BROKERS to be set`.
:::

See also: [Runbook](./runbook.md) for what each setting changes at run
time, and [Troubleshooting](./troubleshooting.md) for the boot errors
named above.
