---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
description: Symptom, cause, check and fix for the failure modes visible in order-management's code — boot errors, readiness, problem types, idempotency keys, the circuit breaker, Kafka consumers, DLQs, the outbox and migrations.
---

# Troubleshooting

Each row is a failure mode that the code on `develop` can actually
produce. The checks use the log messages, problem types and metrics
listed in [Observability](./observability.md); the settings are in
[Configuration](./configuration.md).

## Startup log lines

`cmd/order` logs one line per optional integration at boot. On a pod that
does not behave as expected, read these first: they tell you which
integrations are switched off.

| Level | Message | Meaning |
| --- | --- | --- |
| INFO | `database url not configured; using in-memory adapters` | No `DATABASE_URL`: data is lost on restart, and there is no idempotency middleware, no outbox and no sweeper. |
| WARN | `inventory-storage client in permissive (no-op) mode; allocation will refuse to run` | `INVENTORY_STORAGE_MODE` is not `http`. |
| WARN | `product classification lookup in permissive (fail-open) mode; …` | `PRODUCT_CLASSIFICATION_MODE` is not `kafka`. |
| WARN | `process-path catalogue source not configured; ReceiveOrder will skip path validation, …` | `PATH_CATALOGUE_SOURCE` is not `kafka`; promises use the lead-time fallback. |
| INFO | `site/SKU demand projection disabled` | `DEMAND_PROJECTION_SITE_ID` unset. It is logged twice because two call sites build the policy. |
| INFO | `planned capacity integration disabled` | `PLANNED_CAPACITY_CONSUMER_GROUP` unset. |
| WARN | `KAFKA_BROKERS not configured; RepromiseOrder consumer will not run, OrderRepromised will never fire` | No broker list. |
| INFO | `event publisher configured (transactional outbox)` | Postgres plus `EVENT_PUBLISHER=kafka`: the relay will run. |
| INFO | `repromise consumer configured` | `topic`, `group_id` of the re-promise consumer. |
| INFO | `http server listening` | Boot finished; the probes can now pass. |

## Boot and readiness

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Pod exits at once with `invalid product classification configuration: PRODUCT_CLASSIFICATION_MODE=http was removed by ADR 0036 …` | A deployment still sets the removed `http` mode. | `kubectl logs` of the failed container. | Set `PRODUCT_CLASSIFICATION_MODE=kafka` with `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`, or `permissive`. |
| Exit with `… PRODUCT_CLASSIFICATION_MODE=kafka requires PRODUCT_CLASSIFICATION_CONSUMER_GROUP …` or `… requires KAFKA_BROKERS` | `kafka` mode without its group or brokers. | Container env. | Set both. The chart renders `KAFKA_BROKERS` when `productClassification.consumerGroup` is set. |
| Exit with `PATH_CATALOGUE_SOURCE=kafka requires KAFKA_BROKERS to be set` | Catalogue enabled through `extraEnv` while the chart did not render `KAFKA_BROKERS` (it only does for `eventPublisher=kafka` or a consumer group). | Rendered Deployment env. | Add `KAFKA_BROKERS` to `extraEnv` or enable one of the conditions. |
| Exit with `process-path catalogue did not become ready within 1m0s` (or the CPT-schedule or path-capacity variant) | The cache could not replay its topic in 60 s: broker unreachable, or a very large topic. | Broker health; the offsets of `warehouse.process-path-management.events` and `warehouse.work-planning.events`. | Fix broker reachability. Each boot replays the whole topic, so very long retention slows boot. |
| Exit with `run migrations (after 5 attempts): …` or `ping database (after 5 attempts): …` | Postgres unreachable or rejecting the credentials for longer than the boot retry budget (5 attempts, about 15 s of backoff). | The last error in the message (it is the real one, for example `password authentication failed`). | Fix the DSN or the database. One or two `retrying` lines in the first ~10 s are normal: the Istio sidecar resets the first dial. |
| Pods crash-loop with `pq: unnamed prepared statement does not exist` or `canceling statement due to statement timeout` during migrations | Migrations go through PgBouncer in transaction-pooling mode, where golang-migrate's advisory lock does not hold (ADR 0029). | Is `MIGRATIONS_DATABASE_URL` set, and is it a direct DSN? | Provision the direct DSN under the Secret key `MIGRATIONS_DATABASE_URL`. |
| Exit with `Dirty database version N. Fix and force version.` | A previous migration failed half-way; golang-migrate refuses to continue. | `SELECT * FROM schema_migrations;` | Repair the schema by hand, then `migrate force <N>` against the direct DSN. The binary does not force. |
| Pod never becomes Ready; startup probe kills it after 60 s | Boot work (retries plus cache replay) exceeds 60 s. The listener only opens after boot. | Last log line before the kill: `waiting for … to replay` or `retrying`. | Fix the dependency. Raise `startupProbe.failureThreshold` only if boot is legitimately slower. |
| Requests keep reaching a terminating pod | warehouse-infra overrides `readinessProbe` to `/healthz`, which never flips. | `kubectl get deploy order-management -o yaml` and look at `readinessProbe.httpGet.path`. | Use the chart's `/readyz` default. |
| `cmd/order-projector` or `cmd/order-reports` exits with `ANALYTICS_DATABASE_URL is required` | Analytics enabled without a DSN. | `analytics.database.*` values or the analytics Secret. | Provide the DSN, or keep `analytics.enabled=false`. |

## REST problem types

Every error is `application/problem+json` with
`type = https://errors.order-management.warehouse-systems.dev/<slug>`.
There is no `412`: the API has no `If-Match`/ETag precondition.
Optimistic concurrency surfaces as `409 concurrent-modification`.

| Status | Slug | Cause | Fix |
| --- | --- | --- | --- |
| 400 | `malformed-request-body` | Body is not valid JSON. | Fix the client. |
| 400 | `order-without-lines`, `empty-sku`, `empty-order-id`, `empty-path-id`, `order-line-not-found` | Validation in the adapter or domain. | Fix the request. |
| 400 | `unknown-process-path` | The path chosen for a line is not active in the process-path catalogue (`PATH_CATALOGUE_SOURCE=kafka`). | Check process-path-management's catalogue for a deactivated path. |
| 400 | `idempotency-key-required` | `POST /orders` without `Idempotency-Key` while `DATABASE_URL` is set. | Send a fresh key per new order. The operator MFE (`web/src/api.ts`) does not send one, and `CORS_ALLOWED_ORIGINS` does not allow the header, so order entry from the MFE fails against a Postgres-backed service. |
| 400 | `invalid-query-parameter` | `GET /planned-capacity` without `site`, or with a `from` that is not RFC 3339. | Fix the query. |
| 404 | `order-not-found` | Unknown id. In-memory mode loses every order on restart. | Check the id and `DATABASE_URL`. |
| 404 (no problem body) | n/a | `GET /planned-capacity` while `PLANNED_CAPACITY_CONSUMER_GROUP` is unset: the route is not registered. | Set the group. |
| 409 | `order-already-released` | Cancel after any line was released (BR6). | Expected; v1 has no claw-back (ADR 0004). |
| 409 | `no-backordered-lines`, `no-allocated-lines`, `promise-date-not-set`, `order-not-held`, `order-line-already-allocated`, `order-line-not-pending`, `order-line-not-backordered`, `order-line-not-allocated` | The order is not in the state the command needs: for example `retry-allocation` on an order with nothing backordered, or `release` on an order that was not held. | Read the order first (`GET /orders/{id}`). |
| 409 | `concurrent-modification` | Another writer (an HTTP call or the re-promise consumer) saved the order first; the `version` column check failed (ADR 0024). | Re-read and retry. |
| 422 | `non-positive-quantity`, `line-ineligible-for-resolved-path` | Line validation; no eligible path for the line's attributes. | Fix the line, or check the product classification copy. |
| 422 | `held-order-must-be-ship-complete` | `releaseOnAllocation: false` with `allowPartialShipment: true`. | Send one or the other (ADR 0020). |
| 422 | `idempotency-key-reused` | Same `Idempotency-Key` with a different body. | Use a new key for a different request. |
| 503 | `downstream-not-configured` | `INVENTORY_STORAGE_MODE` is permissive. `POST /orders` still answers `201` with the order `Received` and its lines `Pending`; commands that must call inventory-storage answer 503. | Set `INVENTORY_STORAGE_MODE=http` and `INVENTORY_STORAGE_BASE_URL`. |
| 503 | `downstream-unavailable` | inventory-storage timed out (5 s), answered 5xx or an unexpected status, or the circuit breaker is open. | See [circuit breaker](#circuit-breaker-and-allocation). |
| 503 | `insufficient-stock` | `ports.ErrInsufficientStock` reached the adapter as an error rather than becoming a backorder. | Retry later. |
| 500 | `internal-error` | Unmapped error (database, encoding). | Read the `detail` and the ERROR log for the same `request_id`. |

### Idempotency-Key behaviour

- The middleware wraps `POST /orders` only, and only when `DATABASE_URL`
  is set.
- The key and a SHA-256 of the body are stored in `idempotency_keys` in
  the **same transaction** as the order and its outbox rows.
- **Every** normal response is cached, errors included. A retry with the
  same key and body replays the stored status and body, even a `500`.
  To try again after a failure, use a new key.
- Keys expire after `IDEMPOTENCY_KEY_TTL` (24 h). After that the same key
  creates a new order.
- A panic inside the handler rolls back and caches nothing.

## Circuit breaker and allocation

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Every allocation fails with `503 downstream-unavailable` | Breaker open: 5 consecutive failures, or more than half of at least 10 requests failed within 30 s. It stays open 30 s, then lets one probe through. | `circuit_breaker_state{dependency="inventory-storage"}` = 2; inventory-storage health. | Fix inventory-storage. The breaker closes after one successful probe. Calls are never retried, because reservations are writes. |
| Orders stay `Received` with `Pending` lines | Permissive inventory client, or the implicit allocation at intake hit a hard failure. `POST /orders` answers `201` either way. | Startup WARN; the `status` of the lines. | Configure `http` mode first. No endpoint re-runs allocation for `Pending` lines: `retry-allocation` only takes `Backordered` lines and answers `409 no-backordered-lines` (observed on a local run). Cancel the order (`DELETE /orders/{id}` answers `204` while nothing is released) and submit it again with a new `Idempotency-Key`. |
| Lines `Backordered` | inventory-storage answered `409` (no usable stock). This is a business fact, not an error (BR2). | The order response. | `POST /orders/{id}/retry-allocation` once stock exists. |
| Promise always `LeadTime`, never a CPT window | Catalogue caches not enabled, or the site has no CPT schedule. | Startup WARN; `DEFAULT_SITE_ID`. | `PATH_CATALOGUE_SOURCE=kafka`; check process-path-management publishes `CPTScheduleChanged` for that site. |
| `capacityConstraint` never appears on orders | No published shortage window overlaps the promise at `PLANNED_CAPACITY_SITE_ID`, or the site code differs from warehouse-planning's `location`. | `GET /planned-capacity?site=<code>`. | Align `PLANNED_CAPACITY_SITE_ID` (the fleet uses `SIM1`). |

## Kafka, DLQ and outbox

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| `order_outbox_lag_seconds` keeps growing | The relay cannot send the oldest row. It stops the pass at the first failure, so every later row waits. | `SELECT id, topic, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 5;` and the `outbox relay pass failed` ERROR log. | Fix the cause in `last_error`. See the [runbook](./runbook.md#re-publish-events-stuck-in-the-outbox). |
| wes-work-planning never sees releases | `EVENT_PUBLISHER=log`: events are only logged. | Startup log; `domain event published` lines. | `config.eventPublisher=kafka`. |
| `OrderRepromised` never published | `KAFKA_BROKERS` unset (the consumer does not start), or no `TaskCPTMissed`/`PackageManifested` arrives. | Startup WARN; lag of group `order-management-repromise`. | Set `KAFKA_BROKERS`. |
| Consumer lag on `order-management-repromise` grows | Handling is slow or retrying: each failed message takes up to 3 attempts, and a broker outage makes the consumer back off without limit. | `repromise: kafka unavailable, retrying` logs; broker health. | Fix the dependency. Add replicas: the group is shared. |
| Messages in `warehouse.fulfillment.events.dlq` | Not a CloudEvent, or handling still failed after 3 attempts (including repeated `concurrent-modification`). | `x-dlq-error` header; `repromise: … sending to dead-letter topic` logs with `ce_id`. | Fix the cause and replay (see the [runbook](./runbook.md#replay-a-dead-lettered-message)). |
| Messages in `warehouse.warehouse-planning.events.dlq` | A capacity-plan event could not be decoded or applied. | `x-dlq-error` header. | Fix and replay. |
| A consumer stops consuming | The DLQ write or the offset commit failed. That is the one error that ends a consumer loop, so that nothing is skipped silently. For the re-promise consumer the error ends the whole `cmd/order` process (it exits 1 and Kubernetes restarts it); the planned-capacity and classification consumers log `planned capacity consumer stopped` / `product classification consumer stopped` and the process keeps serving HTTP without them. | Pod restarts; those ERROR lines. | Restore broker access. Restart the pod for the two consumers that do not restart on their own. |
| Product classification copy stays empty | Mode `permissive`, wrong group, or product-master publishes nothing. | Startup log `product classification consumer configured`; `SELECT count(*) FROM product_classification_copy;` | Fix the mode or group. Invalid messages are logged at WARN and skipped, not dead-lettered. |
| Report numbers lag | Projector down or slow. | `GET /reports/funnel/freshness`; lag of group `order-management-analytics`. | Restart the projector. Processing is idempotent on the event id. |
| `GET /reports/funnel` answers `500 report-store-error` | A query took longer than the 15 s `statement_timeout`, or the database is unreachable. | `detail` of the problem. | Narrow `from`/`to` or add `pathId`. |

## MCP

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| `get_promise_health` always returns zeros | `ANALYTICS_DATABASE_URL` is not set on the MCP container. The chart never renders it. | Startup log `analytics database url not configured; get_promise_health will report empty KPIs`. | Add it through `mcp.extraEnv`. |
| `get_order` answers `order not found` for an order that exists | The MCP pod has no `DATABASE_URL` and uses an empty in-memory repository. | Startup log `database url not configured; using in-memory adapters`. | Point it at the OLTP database. |
| Session errors after `initialize` | More than one MCP replica: sessions live in process memory and the Service has no affinity. | `kubectl get deploy order-management-mcp`. | Keep one replica. |
