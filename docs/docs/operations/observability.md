---
id: observability
title: Observability
sidebar_label: Observability
description: Every OpenTelemetry metric instrument and span order-management emits, its log fields, how telemetry is exported, the Grafana dashboard that exists for it, and suggested alerts.
---

# Observability

All four binaries call the same `telemetry.Setup`
(`internal/adapters/outbound/telemetry/telemetry.go`): a TracerProvider
and a MeterProvider exporting over **OTLP/gRPC** to
`OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`), a W3C
`traceparent` + baggage propagator, and Go runtime metrics. Metrics are
pushed every 15 s.

- **There is no `/metrics` route.** Nothing is scraped from the pods. The
  OTel Collector receives OTLP and its Prometheus exporter re-publishes
  each instrument, turning dots into underscores and adding unit and
  `_total` suffixes (`order.orders.received` becomes
  `order_orders_received_total`).
- **Export never blocks.** The exporters connect lazily. With no collector
  reachable, telemetry is dropped and the failure is logged at DEBUG only
  (`opentelemetry error`).
- **Resource attributes:** `service.name` (`OTEL_SERVICE_NAME`; defaults
  `order-management`, `order-management-mcp`, `order-projector`,
  `order-reports`), `service.version` (`SERVICE_VERSION`),
  `deployment.environment.name` (`ENVIRONMENT`), plus whatever
  `OTEL_RESOURCE_ATTRIBUTES` adds (read by the SDK's default resource).

## Metrics

### Service instruments

| Instrument | Type | Unit | Attributes | Emitted by | Meaning |
| --- | --- | --- | --- | --- | --- |
| `order.orders.received` | Int64Counter | `{order}` | `outcome` = `accepted` or `rejected` | `cmd/order`, `ReceiveOrder` (`internal/adapters/outbound/telemetry/metrics.go`) | One increment per intake. `accepted` is counted once the order is persisted and `OrderReceived` raised, before the best-effort allocation. `rejected` is counted when the use case refuses the intake: a held order that allows partial shipment, a line with no eligible path, an invalid line (for example a non-positive quantity), a resolved path the catalogue does not know, or `order.New` refusing the lines. Requests the HTTP adapter refuses first (malformed JSON, empty `lines`, empty SKU) are **not** counted. |
| `circuit_breaker.state` | Int64Gauge | `{state}` | `dependency` = `inventory-storage` | `cmd/order` with `INVENTORY_STORAGE_MODE=http` (`telemetry/circuit_breaker_metrics.go`) | `0` closed, `1` half-open, `2` open. Recorded on each state **change** only, so the series does not exist until the breaker first changes state. Prometheus name `circuit_breaker_state`. |
| `order.outbox.lag_seconds` | Float64ObservableGauge | `s` | none | `cmd/order` with `DATABASE_URL` and `EVENT_PUBLISHER=kafka` (`internal/adapters/outbound/postgres/outbox_metrics.go`) | Age of the oldest `outbox_events` row with `published_at IS NULL`; `0` when the outbox is drained. Sampled by a query on every collection. |

The two service meters are scoped
`github.com/claudioed/order-management` (counter and gauge) and
`github.com/claudioed/order-management/internal/adapters/outbound/postgres`
(outbox lag).

### HTTP instruments (otelchi v0.12.3)

| Instrument | Type | Unit | Attributes | Binaries |
| --- | --- | --- | --- | --- |
| `http.server.request.duration` | Float64Histogram, buckets 5 ms to 10 s | `s` | `http.method`, `http.scheme`, `http.route` (the chi pattern, for example `/orders/{id}`) | `cmd/order`, `cmd/order-reports`, `cmd/mcp` |
| `http.server.active_requests` | Int64UpDownCounter | `{request}` | same | `cmd/order-reports`, `cmd/mcp` only (the OLTP router does not register it) |

otelchi records **no status-code attribute** on these metrics. Error
rates have to come from Kong (`kong_http_requests_total{code=~"5.."}`),
from the span status, or from the `status` field of the request log line.
`cmd/order-projector` serves only `/healthz` on a plain `net/http` mux and
emits no HTTP metrics.

### Go runtime (contrib `runtime` v0.71.0)

`go.memory.used`, `go.memory.limit`, `go.memory.allocated`,
`go.memory.allocations`, `go.memory.gc.goal`, `go.goroutine.count`,
`go.processor.limit`, `go.config.gogc`, from every binary.

## Traces

| Span name | Kind | Where | Notable attributes |
| --- | --- | --- | --- |
| `<route pattern>` such as `/orders/{id}` | server | otelchi on the `cmd/order`, `cmd/order-reports` and `cmd/mcp` routers | `http.route`, HTTP request semconv |
| `kafka.publish warehouse.order-management.events` | producer | `Publisher.Publish`, the **direct** path (`EVENT_PUBLISHER=kafka` without `DATABASE_URL`) | `messaging.system=kafka`, `messaging.destination.name`, `messaging.operation.name=publish`, `messaging.message.id`, `cloudevents.event_type` |
| `kafka.publish warehouse.order-management.analytics` | producer | `AnalyticsPublisher.Publish`, direct path | same |
| `kafka.consume warehouse.fulfillment.events` | consumer | re-promise consumer | parent extracted from the message headers; `messaging.message.id`, `cloudevents.event_type`, `cloudevents.event_source` |
| `kafka.consume warehouse.warehouse-planning.events` | consumer | planned-capacity consumer | same shape |
| `kafka.consume warehouse.product-master.events` | consumer | product-classification consumer | same shape |
| `kafka.consume warehouse.order-management.analytics` | consumer | analytics consumer in `cmd/order-projector` | parent extracted from the headers; `messaging.system`, `messaging.destination.name`, `messaging.operation.name=process` |
| `mcp.tool get_order`, `mcp.tool get_promise_health` | internal | `cmd/mcp`, `internal/adapters/inbound/mcp/tools.go` | `mcp.tool.name`; status `Error` with the message when the tool fails |

With the transactional outbox there is **no publish span**. The encoder
injects the trace context of the request that wrote the outbox row into
the stored headers, and the relay sends those headers unchanged. A
consumer's span is therefore parented on the original HTTP request, not
on the relay pass.

Postgres, the inventory-storage HTTP client and the Kafka replay caches
(`kafkacatalog`, `kafkacptschedule`, `kafkapathcapacity`) are not
instrumented.

## Logs

Every binary writes JSON lines to stdout through `log/slog`, at
`LOG_LEVEL`. Standard fields are `time`, `level`, `msg`. The log lines do
**not** carry `trace_id`/`span_id`: correlate by `request_id` or by time.

| Message | Fields | Source |
| --- | --- | --- |
| `http request` | `method`, `route` (chi pattern; the sanitised raw path for a 404), `status`, `bytes`, `duration_ms`, `request_id` (chi `RequestID`) | `internal/adapters/inbound/http/logging.go`, on the `cmd/order` and `cmd/order-reports` routers |
| `domain event published` | `event_name`, `payload` | log publisher (`EVENT_PUBLISHER=log`) |
| `outbox relay pass failed` | `error` | outbox relay, ERROR |
| `housekeeping sweep` / `housekeeping sweep failed` | `idempotency_keys_deleted`, `outbox_events_deleted`, `error` | sweeper |
| `repromise: kafka unavailable, retrying` | `stage`, `retry_in`, `error`, `partition`, `offset` | re-promise consumer |
| `repromise: non-CloudEvents message, sending to dead-letter topic` | `topic`, `partition`, `offset`, `dlq_topic`, `error` | re-promise consumer |
| `repromise: exhausted retries, sending to dead-letter topic` | `topic`, `dlq_topic`, `ce_id`, `ce_type`, `attempts`, `error` | re-promise consumer |
| `retrying` / `succeeded after retry` | `op`, `attempt`, `in`, `err` | `internal/bootretry`, at boot |
| `shutdown: readiness flipped to not-ready; waiting for traffic to drain` | `drain_delay` | `cmd/order` shutdown |
| `telemetry configured` | `service_name`, `service_version`, `environment`, `otlp_endpoint` | `cmd/order` boot |

The startup warnings that say which integrations are off are listed in
[Troubleshooting](./troubleshooting.md#startup-log-lines).

## Dashboard

warehouse-infra provisions the Grafana dashboard **"Warehouse — Order
Management"** (uid `warehouse-order-management`) from
`terraform/dashboards/contexts/order-management.json`. Its rows:

- **API Gateway (Kong):** request rate by status code, 5xx error ratio,
  p95 request vs upstream latency for this context's HTTPRoute.
- **Service HTTP RED:** `http_server_request_duration_seconds` rate by
  `service_name` and `http_route`, 5xx rate by process, p95 duration by
  process, filtered on `service_name=~"order-management.*"`.
- **Business metrics:** `sum by (outcome) (rate(order_orders_received_total[5m]))`.
- **Go runtime:** `go_goroutine_count` and `go_memory_used_bytes`.
- **Logs (Loki):** live stream, volume by level, and WARN/ERROR only, for
  `{app="order-management"}` without the Istio containers.

Two limits of that dashboard, both from the code above: the projector and
reports processes report `service.name` `order-projector` and
`order-reports`, which the `order-management.*` regex does not match; and
the per-process 5xx panel filters on `http_response_status_code`, which
otelchi does not emit. Use the Kong row for error rates. No panel shows
`order_outbox_lag_seconds` or `circuit_breaker_state` yet.

## Suggested alerts

These are suggestions built on the instruments above. None of them is
provisioned today.

| Alert | Expression (Prometheus names) | Why |
| --- | --- | --- |
| Outbox stuck | `max(order_outbox_lag_seconds) > 60` for 5 m | The relay retries forever on the same row; a growing lag means Kafka is unreachable or one row cannot be sent, and every later event waits behind it. |
| Inventory breaker open | `max(circuit_breaker_state{dependency="inventory-storage"}) == 2` for 2 m | Every allocation fails with `downstream-unavailable` (503) while open. |
| Intake rejections climbing | `sum(rate(order_orders_received_total{outcome="rejected"}[15m])) / sum(rate(order_orders_received_total[15m])) > 0.2` | A caller or upstream integration is sending orders the domain refuses. |
| API 5xx | `sum(rate(kong_http_requests_total{service=~"httproute\\.warehouse-systems\\.order-management\\..*", code=~"5.."}[5m])) > 0` for 10 m | 503 is how downstream outages and permissive mode surface. |
| Analytics stale | probe `GET /reports/funnel/freshness` and alert on `lagSeconds > 300` | The report's SLA is p95 event-to-report lag under 30 s. |
| DLQ traffic | broker-side end offset of `warehouse.fulfillment.events.dlq` or `warehouse.warehouse-planning.events.dlq` increasing | A dead-lettered message is a re-promise or a capacity window that was not applied. |
| Consumer lag | broker-side lag of groups `order-management-repromise`, `order-management-analytics` and the planned-capacity and classification groups | The service exports no consumer-lag metric of its own. |
