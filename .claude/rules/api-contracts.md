# API contracts

## REST API (6 routes — `apis/openapi.yaml`; ADR-0005's `/allocate` and old `/release` verbs are gone)

- `POST   /orders`                       → `receiveOrder` — intake; folds
  allocation-then-release into the same call (ADR-0005). 201 always, even
  if the implicit allocation pass hits a hard failure. Optional
  `releaseOnAllocation` (default `true`; `false` = hold after allocation,
  ADR-0020 — combining it with `allowPartialShipment: true` is rejected
  422, `order.ErrHeldOrderMustBeShipComplete`; NOTE `problemFor` has no
  case for that error yet, so the body's `type` is currently
  `internal-error` — a known code gap, not the intended contract) and optional `requiredShipBy`
  (RFC 3339 deadline; the promise is constrained to the latest CPT window
  at or before it via `PromisePolicy.FeasibleBy`, and an infeasible
  deadline comes back with NO `promiseDate` rather than an error).
- `GET    /orders/{id}`                  → `getOrder`
- `POST   /orders/{id}/release`          → `releaseHeldOrder` (ADR-0020) —
  releases a HELD order's allocated lines through the same release leg.
  Idempotent for a held order (200, no event, if already released); 409
  `order-not-held` for an order that was never held.
- `POST   /orders/{id}/retry-allocation` → `retryAllocation` — 503 on a
  genuine hard failure (this IS an explicit ask for allocation now).
- `DELETE /orders/{id}`                  → `cancelOrder` — 409 if any line
  already `Released` (BR6); 204 on success.
- `GET    /healthz`                      → `getHealthz` — liveness only,
  does not check Postgres/inventory-storage.

JSON DTOs live in the http adapter; never leak domain structs. Every error
response is RFC 7807 `application/problem+json` (identical shape to the
other fleet services).

**No auth on any route** (ADR-0012 — the fleet-wide static-bearer rollout
from ADR-0011 was rolled back; `apis/openapi.yaml` carries no
`securitySchemes`/`security:` keys at all as of that ADR). Re-adopting auth
later means adopting whatever supersedes both ADR-0011 and ADR-0012, not
resurrecting the deleted `internal/adapters/inbound/auth` package verbatim.

CORS middleware (`go-chi/cors`) is enabled on every route, allowing
`CORS_ALLOWED_ORIGINS` (env, default
`http://localhost:5173,http://localhost:5181` — the `warehouse-console`
shell and this repo's own `order-mgmt-mfe` remote), for the fleet's browser
console (ADR-0007).

## Analytics REST API (`cmd/order-reports`, default `:8092`)

- `GET /reports/funnel` — Order Funnel & Allocation Health report, keyed
  per `pathId × hour`. Query params `from`/`to` (required, RFC3339),
  `pathId` (optional filter), `granularity` (only `hour`).
- `GET /reports/funnel/freshness` — projection lag (`lagSeconds`).
- `GET /healthz`

## Outbound HTTP contract to inventory-storage (EXACT — verified against live openapi.yaml)

Base URL via env `INVENTORY_STORAGE_BASE_URL`, mode via
`INVENTORY_STORAGE_MODE` (default `permissive`):

- `POST /reservations` — request
  `{"sku":"...","quantity":N,"demandRef":"..."}` (this Order's `OrderId` as
  `demandRef`) — 201 response:
  `{"id":"...","sku":"...","quantity":N,"demandRef":"...","status":"...","allocations":[{"stockUnitId":"...","quantity":N}],"expiresAt":"..."}`.
  A 409 (RFC 7807) means insufficient usable stock → `Backordered` for that
  line (BR2). Any other non-2xx or transport error propagates as a hard
  failure — never silently backordered.
- `DELETE /reservations/{id}` — 204 on success. Used by `CancelOrder`.
- `GET /products/{sku}/classification` — product-classification lookup
  for eligibility-driven path selection (ADR-0016), same
  `INVENTORY_STORAGE_BASE_URL`, mode via `PRODUCT_CLASSIFICATION_MODE`
  (default `permissive`). Unlike reservations this one fails OPEN: a 404,
  transport error or non-2xx yields "unknown classification", never an
  intake rejection.

**Permissive mode is NOT soft here.** Unlike other fleet adapters that
fail-open on optional/soft lookups, allocating real stock is never allowed
to silently "succeed" against a no-op client. In permissive mode,
allocation returns a clear `ErrDownstreamNotConfigured`. Only `http` mode
is suitable for a real integration test or deployment.

## Kafka contract (`apis/asyncapi.yaml`, AsyncAPI 2.6.0 — Spectral-gated, `api-lint` CI job)

**CloudEvents 1.0, mandatory, on every topic produced or consumed** (ADR-0030).
Structured content mode; Kafka header
`content-type: application/cloudevents+json; charset=UTF-8`. No flat
envelope, no dual-read, no `schema_version`, no envelope toggle.

- Required attributes: `specversion=1.0`, `id` (UUID minted once, persisted
  in the outbox row), `source=/warehouse/order-management`,
  `type=com.warehouse.wes.order-management.order.<EventName>`,
  `subject=<order id>`, `time` (occurred-at, UTC),
  `datacontenttype=application/json`,
  `dataschema=urn:warehouse:order-management:<events|analytics>:<EventName>:v1`.
- Both topics keyed by `OrderId` (Hash balancer), at-least-once.
- Build/parse ONLY via `internal/adapters/kafka/cloudevents` (sdk-go
  `event` package). Consumers dispatch on the FULL `type`, ignore unknown
  types, dedupe on `id`, and DLQ/skip anything failing `cloudevents.Decode`.

### Channel: `warehouse.order-management.events` (integration)

- `subscribe` operationId `consumeOrderManagementEvents`.
- `OrderAllocated` / `OrderPartiallyAllocated` messages (frozen —
  `data.lines[]` entry shape is shared verbatim with
  `wes-work-planning`'s consumer and MUST NOT change without
  coordinating both sides; `fulfillment_class` is additive, ADR-0008)
  plus — since ADR-0018 — `OrderRepromised`, the fleet's "your delivery
  is delayed" trigger, raised by the new `RepromiseOrder` use case.
  Consumer today: `wes-work-planning` (`OrderAllocated`/
  `OrderPartiallyAllocated` only).

### Channel: `warehouse.fulfillment.events` (inbound, ADR-0018)

- `subscribe` operationId `consumeFulfillmentEvents`. fulfillment-
  execution's shared/fan-out topic (the SAME one `labor-performance`
  already consumes for `TaskCompleted`); this context reacts ONLY to
  `com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed` and
  `com.warehouse.wes.fulfillment-execution.package.PackageManifested`
  (full CloudEvents `type`), decoding its own independent copy of that
  service's payload — never a Go import. Non-CloudEvents messages go to
  `<topic>.dlq`.
  `data.order_ref` on both is a `WorkUnitId`-shaped reference
  (`{orderId}-line-{lineNo}`), NOT a bare `OrderId` — parsed back via
  `usecases.ParseWorkUnitID`, the reverse of `WorkUnitID` below.
  Stable shared consumer group `order-management-repromise`, gated on
  `KAFKA_BROKERS` alone.

### Local-cache consumers (declared in `apis/asyncapi.yaml` as subscribe channels)

Enabled only by `PATH_CATALOGUE_SOURCE=kafka` (default `none`); each uses
its own per-process-unique consumer group and a full-replay readiness gate
before `cmd/order` serves traffic:

- `kafkacatalog` — `warehouse.process-path-management.events`,
  `com.warehouse.wes.process-path-management.processpath.ProcessPathCreated`/
  `...ProcessPathUpdated`/`...ProcessPathDeactivated` (ADR-0013).
- `kafkacptschedule` — same topic,
  `com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged`
  (ADR-0014).
- `kafkapathcapacity` — `warehouse.work-planning.events`,
  `com.warehouse.wes.work-planning.workpool.PathCapacityChanged` (ADR-0015).
- Non-CloudEvents messages are logged at WARN and skipped.

### Channel: `warehouse.order-management.analytics`

- `publish` operationId `publishOrderAnalytics` (this service is the
  producer).
- `subscribe` operationId `projectOrderAnalytics` (`cmd/order-projector` is
  the sole consumer/writer — FirstOffset, idempotent on the CloudEvents `id`).
- Carries all 10 domain event types (see `domain-model.md`'s event
  table) as 10 distinct messages — `OrderRepromised` added by ADR-0019.

### Deterministic work-unit id (frozen, zero wire-level enforcement)

`WorkUnitID(orderID, lineNo) = "{orderID}-line-{lineNo}"`
(`internal/application/usecases/allocation.go`). `wes-work-planning`'s
consumer independently reconstructs the SAME formula from
`(order_id, lines[].line_no)` — never transmitted on the wire. Must match
byte-for-byte or idempotent redelivery breaks. Nothing catches a drift
except manual review and cross-repo test discipline.

## MCP server (`cmd/mcp`, ADR-0010) — read-only, two tools

- `internal/adapters/inbound/mcp/` — driving adapters over the
  **existing** `GetOrder` use case (`get_order`) and, since ADR-0019, the
  analytics `report.ReportStore` (`get_promise_health`) — never a
  parallel code path for `get_order`.
- **`get_order`**: one order's current state by id.
- **`get_promise_health`** (ADR-0019, closing ADR 0014 §6's
  order-management half): promise basis distribution, re-promise rate,
  split-shipment rate, and promise-to-cutoff gap for a `from`/`to`/
  optional `pathId` window — the same query shape `GET /reports/funnel`
  accepts. Declares its own `PromiseHealthStore` port (never imports
  `internal/analytics/report` directly, per the MCP adapter's own
  arch-test dependency rule); `cmd/mcp` adapts the real
  `report.ReportStore` into it.
- **No write tool** — every write use case here (`ReceiveOrder`,
  `CancelOrder`, `RetryAllocation`, `ReleaseHeldOrder`) enforces a real domain invariant an
  MCP-calling agent should not trigger directly.
- No auth (ADR-0012 rolled back the bearer-key layer this adapter
  originally had per ADR-0011).
