---
title: Entity-Relationship Diagram
sidebar_label: ER Diagram
sidebar_position: 8
description: "Final Postgres schema of order-management (OLTP and analytics databases) after every golang-migrate migration, with real foreign keys only and a table-to-aggregate mapping."
---

# Entity-Relationship Diagram

Two databases, two migration sets, both applied by golang-migrate on boot
(`postgres.RunMigrations`):

- **OLTP** (`DATABASE_URL`, migrations `migrations/0001`–`0010`), migrated
  by `cmd/order` and by `cmd/mcp` (through `MIGRATIONS_DATABASE_URL`,
  ADR 0029).
- **Analytics** (`ANALYTICS_DATABASE_URL`, migrations
  `migrations/analytics/0001`–`0003`), migrated only by
  `cmd/order-projector`; `cmd/order-reports` and `cmd/mcp` only read it.

The diagrams show the **final** schema after applying every `*.up.sql` in
order. `0001_init` created an `events` table that `0007_outbox` dropped, so
it is not shown.

## OLTP database

```mermaid
erDiagram
  orders {
    TEXT id PK "ord-uuid"
    BOOLEAN allow_partial_shipment
    TIMESTAMPTZ promise_date "nullable"
    TEXT promise_cpt_id "nullable, 0002"
    TEXT promise_basis "nullable, 0002"
    BOOLEAN release_on_allocation "default true, 0005"
    TIMESTAMPTZ required_ship_by "nullable, 0006"
    INTEGER version "default 1, 0009"
  }
  order_lines {
    TEXT order_id PK,FK
    INTEGER line_no PK "check gt 0"
    TEXT sku
    INTEGER quantity "check gt 0"
    TEXT path_id
    BOOLEAN gift_wrap
    TEXT line_status
    TEXT reservation_id "nullable"
  }
  order_promise_groups {
    TEXT order_id PK,FK
    INT group_no PK
    INT_ARRAY line_nos "INT array"
    TEXT cpt_id "nullable"
    TIMESTAMPTZ cutoff_at
    TEXT basis
  }
  repromise_processed_events {
    TEXT event_id PK "CloudEvents id"
    TIMESTAMPTZ processed_at
  }
  planned_capacity_windows {
    TEXT plan_id PK
    TEXT warehouse_id
    TEXT location
    TEXT path_id
    TIMESTAMPTZ window_start
    TIMESTAMPTZ window_end "check gt window_start"
    DOUBLE_PRECISION assigned_demand
    DOUBLE_PRECISION capacity_over_window
    DOUBLE_PRECISION shortage "check gte 0"
    TEXT bottleneck_step
    TEXT status "DRAFT or PUBLISHED"
    TIMESTAMPTZ event_time
    TIMESTAMPTZ updated_at
  }
  planned_capacity_processed_events {
    TEXT event_id PK "CloudEvents id"
    TIMESTAMPTZ processed_at
  }
  product_classification_copy {
    TEXT sku PK
    TEXT_ARRAY handling_tags
    TEXT temperature_class "nullable"
    SMALLINT dot_hazard_class "nullable, 1 to 9"
    BIGINT version "check gte 1"
    TIMESTAMPTZ updated_at
  }
  product_classification_processed_events {
    TEXT event_id PK "CloudEvents id"
    TIMESTAMPTZ processed_at
  }
  outbox_events {
    BIGSERIAL id PK
    TEXT topic
    TEXT event_type "full CloudEvents type"
    BYTEA key "OrderId"
    BYTEA value "CloudEvents JSON"
    JSONB headers
    TIMESTAMPTZ created_at
    TIMESTAMPTZ published_at "nullable"
    INTEGER attempts
    TEXT last_error "nullable"
  }
  idempotency_keys {
    TEXT key PK
    TEXT method
    TEXT path
    TEXT request_hash "sha256 of body"
    INTEGER status_code "nullable"
    BYTEA response_body "nullable"
    JSONB response_headers "nullable"
    TIMESTAMPTZ created_at
    TIMESTAMPTZ completed_at "nullable"
  }
  schema_migrations {
    BIGINT version PK
    BOOLEAN dirty
  }

  orders ||--|{ order_lines : "has (ON DELETE CASCADE)"
  orders ||--o{ order_promise_groups : "promised as"
```

Source: `migrations/0001_init.up.sql` … `migrations/0011_product_classification_copy.up.sql`;
`schema_migrations` is golang-migrate's own bookkeeping table
(`internal/adapters/outbound/postgres/migrate.go`). Omits: secondary
indexes (`idx_order_lines_sku`, `idx_order_lines_status`,
`idx_outbox_events_unpublished` partial on `published_at IS NULL`,
`idx_idempotency_keys_created_at`, `planned_capacity_windows_location_end`).
`INT_ARRAY` stands for Postgres `INT[]`, `TEXT_ARRAY` for `TEXT[]`, `DOUBLE_PRECISION` for
`DOUBLE PRECISION`.

Only two real `FOREIGN KEY`s exist: `order_lines.order_id` and
`order_promise_groups.order_id`, both to `orders.id`. Everything else is
deliberately unlinked:

- `order_lines.reservation_id` points into **inventory-storage's** database
  (a `Reservation` id) — another bounded context, so no FK is possible or
  wanted (ADR 0002).
- `order_lines.path_id` and `planned_capacity_windows.path_id` are
  process-path ids owned by process-path-management / warehouse-planning.
- `order_promise_groups.line_nos` refers to `order_lines.line_no` of the
  same order, but as an array, so it is enforced in the aggregate
  (`Order.SetPromiseGroups`), not by the database.
- `outbox_events.key` holds the `OrderId` as the Kafka key — infrastructure,
  not a relation.
- `repromise_processed_events.event_id`,
  `planned_capacity_processed_events.event_id` and
  `product_classification_processed_events.event_id` hold **inbound**
  CloudEvents ids from other contexts.
- `product_classification_copy.sku` is a product-master SKU (ADR 0036): a
  local copy, never joined to `order_lines.sku` by a constraint.

## Analytics database

```mermaid
erDiagram
  funnel_rollup {
    TEXT path_id PK
    TIMESTAMPTZ hour_bucket PK
    BIGINT orders_received
    BIGINT orders_allocated
    BIGINT orders_partially_allocated
    BIGINT orders_allocation_failed
    BIGINT orders_released
    BIGINT orders_cancelled
    BIGINT lines_allocated
    BIGINT lines_backordered
    BIGINT lines_released
    BIGINT promise_basis_capability "0002"
    BIGINT promise_basis_lead_time "0002"
    BIGINT promise_basis_network "0003"
    BIGINT orders_split_shipment "0002"
    DOUBLE_PRECISION promise_to_cutoff_gap_seconds_sum "0002"
    BIGINT promise_to_cutoff_gap_samples "0002"
  }
  repromise_rollup {
    TIMESTAMPTZ hour_bucket PK
    BIGINT orders_repromised
  }
  analytics_processed_events {
    TEXT event_id PK "CloudEvents id"
    TIMESTAMPTZ occurred_at
    TIMESTAMPTZ applied_at
  }
  analytics_consumed_events {
    TEXT event_id PK "CloudEvents id"
    TIMESTAMPTZ processed_at
  }
  schema_migrations {
    BIGINT version PK
    BOOLEAN dirty
  }
```

Source: `migrations/analytics/0001_report.up.sql`,
`0002_promise_kpis.up.sql`, `0003_promise_basis_network.up.sql`. Omits:
indexes `idx_funnel_rollup_hour_bucket`,
`idx_analytics_processed_events_occurred_at`,
`idx_repromise_rollup_hour_bucket`. There are **no** foreign keys in this
database: every table is a projection or an idempotency ledger.
`analytics_processed_events` is claimed inside the projection transaction
(`analyticsstore.PostgresProjection`) and also drives the freshness lag;
`analytics_consumed_events` backs the consumer's own
`ConsumedEventsRepo`.

## Table ≠ aggregate

| Table | What it is | Aggregate / owner |
| --- | --- | --- |
| `orders` | Aggregate root row | **Order** (`order.Order`) |
| `order_lines` | Entity rows inside the aggregate | **Order** — `order.OrderLine` |
| `order_promise_groups` | Value objects of the aggregate (delete-and-reinsert on every save) | **Order** — `order.PromiseGroup` |
| `planned_capacity_windows` | Read model of another context's data | none — `order.PlannedCapacityWindow` (ADR 0031) |
| `repromise_processed_events` | Infrastructure: inbound idempotency ledger | `ports.RepromiseProcessedEvents` |
| `planned_capacity_processed_events` | Infrastructure: inbound idempotency ledger | `ports.PlannedCapacityProcessedEvents` |
| `product_classification_copy` | Local copy of another context's data (product-master, ADR 0036) | none — `ports.ProductClassificationCopy` / `ProductClassificationLookup` |
| `product_classification_processed_events` | Infrastructure: inbound idempotency ledger | `ports.ProductClassificationProcessedEvents` |
| `outbox_events` | Infrastructure: transactional outbox (ADR 0022) | `postgres.OutboxPublisher` / `OutboxRelay` |
| `idempotency_keys` | Infrastructure: HTTP request de-duplication (ADR 0023) | `inbound/http.RequireIdempotencyKey` |
| `schema_migrations` | Infrastructure: golang-migrate | — |
| `funnel_rollup`, `repromise_rollup` | Analytics projection (ADR 0006/0019) | `report.Row` via `cmd/order-projector` |
| `analytics_processed_events`, `analytics_consumed_events` | Infrastructure: projector idempotency | `analyticsstore` |

One `Order` is persisted as one `orders` row, `N` `order_lines` rows and up
to `N` `order_promise_groups` rows, written in one transaction by
`postgres.OrderRepo.Save` with the `version` guard (ADR 0024).
