---
id: runbook
title: Runbook
sidebar_label: Runbook
description: How order-management is deployed and operated — Deployments, ports and probes, migrations, Kafka topics and consumer groups, the outbox relay, DLQs, housekeeping, scaling and routine procedures, including what to do with a released order line that was never confirmed (OrderReleaseUnconfirmed).
---

# Runbook

This page is for whoever deploys or is on call for order-management. It
describes what runs, what each process waits for, which Kafka topics and
groups it owns, and the routine procedures that are safe to perform.
Every value below is read from `charts/order-management` and the `cmd/`
composition roots on `develop`. Settings are listed in
[Configuration](./configuration.md); symptoms and fixes are in
[Troubleshooting](./troubleshooting.md).

## What gets deployed

One image (`ghcr.io/iqvo/order-management`, built by the root
`Dockerfile`) contains all four binaries under `/app` plus the
`migrations/` directory. The Helm chart `order-management`
(`charts/order-management`, published on each release to
`oci://ghcr.io/iqvo/order-management`) starts each binary as its own
Deployment. `<fullname>` is the chart's full name; in the kind cluster the
release is `order-management`, so the names below drop the placeholder
there.

| Deployment | Binary | Enabled by | Container port | Service | Probes |
| --- | --- | --- | --- | --- | --- |
| `<fullname>` | `/app/order` (image entrypoint) | always | 8080 (`service.targetPort`) | `<fullname>`, ClusterIP, port 80 → 8080 | startup `GET /healthz` every 2 s × 30; liveness `GET /healthz` (5 s delay, every 10 s); readiness `GET /readyz` (3 s delay, every 5 s) |
| `<fullname>-mcp` | `/app/mcp` | `mcp.enabled` | 8090 | `<fullname>-mcp`, port 8090 | TCP socket on all three probes (`cmd/mcp` serves no `/healthz`) |
| `<fullname>-projector` | `/app/order-projector` | `analytics.enabled` | 8091 (admin) | none | startup, liveness and readiness on `GET /healthz` |
| `<fullname>-reports` | `/app/order-reports` | `analytics.enabled` | 8092 | `<fullname>-reports`, port 80 → 8092 | startup, liveness and readiness on `GET /healthz` |
| `<fullname>-frontend` | nginx serving `web/` (separate image `warehouse/order-management-frontend`) | `frontend.enabled` | 8080 | `<fullname>-frontend`, port 80 | HTTP probes defined in `frontend-deployment.yaml` |

In the kind cluster the REST API is reached through Kong at
`http://localhost:8000/api/order-management/` (Kong strips the prefix) and
the micro-frontend through the Nginx web gateway at
`http://localhost/mfes/order-management/`. REST and MCP are
unauthenticated.

`terminationGracePeriodSeconds` is 30 for the OLTP pod: it has to cover
`SHUTDOWN_DRAIN_DELAY` (5 s) plus the 10 s shutdown budget.

### Readiness: what the pod waits for

`cmd/order` opens its listener **only after** all boot work is done, so
the startup probe fails until then. In order:

1. Validate the `PRODUCT_CLASSIFICATION_*` settings (a bad value exits
   before anything else is built).
   `RELEASE_CONFIRMATION_*` is validated right after, the same way: with
   `RELEASE_CONFIRMATION_MODE=kafka` a missing consumer group,
   `KAFKA_BROKERS`, `DATABASE_URL` or `EVENT_PUBLISHER=kafka` is a boot error
   that names the variable.
2. With `DATABASE_URL`: run the OLTP migrations against
   `MIGRATIONS_DATABASE_URL`, open the pool and ping it. Both steps are
   retried by `internal/bootretry` (5 attempts, 1+2+4+8 s backoff, about
   15 s of waiting plus the attempts themselves; ADR 0033's "≈ 31s" counts a
   fifth sleep that never happens) because the cluster resets each pod's first outbound
   dial roughly 10 s after start (Istio native sidecars).
3. With `PATH_CATALOGUE_SOURCE=kafka`: start the process-path catalogue,
   CPT-schedule and path-capacity consumers (each constructor retried the
   same way), then wait until each has replayed its topic. Each wait has
   its own 60 s timeout (`WaitReadyTimeout`). A timeout is a boot error.
4. Start listening on `HTTP_ADDR`.

The startup probe allows 60 s (2 s × 30), which covers the retry budget.

`GET /readyz` answers `200 {"status":"ready"}` while the process runs and
`503 {"status":"not_ready"}` from the first step of shutdown. It does
**not** check Postgres or Kafka. `GET /healthz` always answers
`200 {"status":"ok"}`.

:::warning[Fleet override]
warehouse-infra's `helm-values/order-management.yaml` overrides
`readinessProbe` to `GET /healthz`. With that override the pod stays in
the Service endpoints during the shutdown drain. The `/readyz` flip
described below only takes effect where the chart default is kept.
:::

### Graceful shutdown (ADR 0025)

On SIGTERM `cmd/order`:

1. flips `/readyz` to `503`;
2. sleeps `SHUTDOWN_DRAIN_DELAY` (default 5 s) so the endpoint controller
   stops routing new requests;
3. drains in-flight HTTP requests;
4. cancels the re-promise consumer and waits for its in-flight message
   and offset commit;
5. lets the outbox relay finish its last pass;
6. stops the planned-capacity, product-classification and (when
   `RELEASE_CONFIRMATION_MODE=kafka`) release-confirmation consumers (up
   to 10 s each) and the catalogue caches, then the housekeeping sweeper,
   and finally closes the Postgres pool.

Steps 3 to 5 share one 10 s budget. The other three binaries stop their
consumer (projector) and shut the HTTP server down within 10 s.

## Migrations

| Database | Directory | Applied by | Connection |
| --- | --- | --- | --- |
| OLTP | `migrations/` (`0001_init` … `0012_release_confirmations`) | `cmd/order` **and** `cmd/mcp`, at every boot, through golang-migrate | `MIGRATIONS_DATABASE_URL`, falling back to `DATABASE_URL` |
| Analytical | `migrations/analytics/` (`0001_report` … `0003_promise_basis_network`) | `cmd/order-projector`, at every boot | `ANALYTICS_DATABASE_URL` |

- There is no separate migrate Job and no `migrate` make target. A
  release that adds a migration applies it when the first new pod boots.
- golang-migrate serialises concurrent runs with a session-level
  `pg_advisory_lock`. That lock does not hold through PgBouncer in
  transaction-pooling mode, which is why `MIGRATIONS_DATABASE_URL` must be
  a **direct** DSN wherever `DATABASE_URL` points at PgBouncer
  (ADR 0029). The chart reads it from the same Secret as `DATABASE_URL`,
  key `MIGRATIONS_DATABASE_URL`, marked `optional: true`.
- `cmd/order-reports` never migrates. It only reads.
- All migrations are additive except `0007_outbox`, which drops the old
  undrained `events` table.

OLTP tables: `orders`, `order_lines`, `order_promise_groups`,
`repromise_processed_events`, `outbox_events`, `idempotency_keys`,
`planned_capacity_windows`, `planned_capacity_processed_events`,
`product_classification_copy`, `product_classification_processed_events`,
`release_confirmations`, `release_confirmation_processed_events` (migration
`0012`, ADR 0043; both stay empty while `RELEASE_CONFIRMATION_MODE=off`).
Analytical tables: `analytics_processed_events`,
`analytics_consumed_events`, `funnel_rollup`, `repromise_rollup`.

## Kafka

There is one broker for the whole fleet (in-cluster Bitnami Kafka; from
the host, `localhost:9092` through the kind cluster's external access).
The chart default `kafka.brokers` is `kafka:9092`; the fleet sets
`kafka.warehouse-systems.svc.cluster.local:9092`. Every message is a
CloudEvents 1.0 event in **structured** mode (`content-type:
application/cloudevents+json; charset=UTF-8`, ADR 0030).

### Produced

| Topic | Producer | Event types | Key | When |
| --- | --- | --- | --- | --- |
| `warehouse.order-management.events` | `cmd/order` | `com.warehouse.wes.order-management.order.OrderAllocated`, `…order.OrderPartiallyAllocated`, `…order.OrderRepromised`, `com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged` | order id (`<order_id>/line/<line_no>` for `SiteSkuDemandChanged`), hash balancer (ADR 0027) | `EVENT_PUBLISHER=kafka`; `SiteSkuDemandChanged` also needs `DEMAND_PROJECTION_SITE_ID` |
| `warehouse.order-management.analytics` | `cmd/order` | the eleven `com.warehouse.wes.order-management.order.*` analytics events (`OrderReceived`, `OrderAllocated`, `OrderPartiallyAllocated`, `OrderAllocationPartiallyFailed`, `OrderReleased`, `OrderCancelled`, `OrderLineAllocated`, `OrderLineBackordered`, `OrderLineReleased`, `OrderRepromised`, and, only with `RELEASE_CONFIRMATION_MODE=kafka`, `OrderReleaseUnconfirmed`) | order id | `EVENT_PUBLISHER=kafka` |
| `<source topic>.dlq` | `cmd/order` | raw copy of a dead-lettered message | original key | see [DLQs](#dead-letter-topics) |

Writers set `AllowAutoTopicCreation`; the fleet broker creates topics on
first write with 8 partitions.

### Consumed

| Topic | Consumer (binary) | Consumer group | Event types | Enabled by |
| --- | --- | --- | --- | --- |
| `warehouse.fulfillment.events` | re-promise consumer (`cmd/order`) | `order-management-repromise` (fixed in code) | `com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed`, `com.warehouse.wes.fulfillment-execution.package.PackageManifested` | `KAFKA_BROKERS` set |
| `warehouse.warehouse-planning.events` | planned-capacity consumer (`cmd/order`) | `PLANNED_CAPACITY_CONSUMER_GROUP` (fleet: `order-management-planned-capacity`) | `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated`, `…CapacityPlanPublished`, `…CapacityShortageDetected`, `…BottleneckDetected` | group and `KAFKA_BROKERS` set |
| `warehouse.product-master.events` | product-classification consumer (`cmd/order`) | `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` (fleet: `order-management-product-classification`) | `com.warehouse.wms.product-master.product.ProductClassified` | `PRODUCT_CLASSIFICATION_MODE=kafka` |
| `warehouse.process-path-management.events` | catalogue cache and CPT-schedule cache (`cmd/order`) | per process: `order-management-process-path-catalogue-<host>-<pid>-<nanos>` and `order-management-cpt-schedule-<host>-<pid>-<nanos>` | `…process-path-management.processpath.ProcessPathCreated`, `…ProcessPathUpdated`, `…ProcessPathDeactivated`, `…process-path-management.cptschedule.CPTScheduleChanged` | `PATH_CATALOGUE_SOURCE=kafka` |
| `warehouse.work-planning.events` | path-capacity cache (`cmd/order`) | per process: `order-management-path-capacity-<host>-<pid>-<nanos>` | `com.warehouse.wes.work-planning.workpool.PathCapacityChanged` | `PATH_CATALOGUE_SOURCE=kafka` |
| `warehouse.work-planning.events` | release-confirmation consumer (`cmd/order`) | `RELEASE_CONFIRMATION_CONSUMER_GROUP` (stable, no default in the binary; the chart renders `order-management-release-confirmation` when `releaseConfirmation.mode=kafka`) | `com.warehouse.wes.work-planning.workunit.WorkUnitCreated` (reads only `reference` and `line_no`) | `RELEASE_CONFIRMATION_MODE=kafka` (**off by default**) |
| `warehouse.order-management.analytics` | analytics consumer (`cmd/order-projector`) | `order-management-analytics` (fixed in code), starts at the earliest offset | the ten analytics types above (it ignores `OrderReleaseUnconfirmed`) | always (projector) |

The three caches replay their topic from the start under a new group on
every boot. Their groups are disposable: expect one stale group per pod
restart on the broker. Every other group is stable and shared, so
replicas split partitions.

### Transactional outbox and relay

With `DATABASE_URL` and `EVENT_PUBLISHER=kafka`, a use case writes one
`outbox_events` row per (event, topic) in the same transaction as the
order. The relay runs inside `cmd/order`:

- each pass claims up to 100 unpublished rows in `id` order with
  `FOR UPDATE SKIP LOCKED` (two pods never claim the same row);
- a full batch is followed immediately by another pass; otherwise it
  sleeps `OUTBOX_RELAY_INTERVAL` (1 s);
- on the first send failure it records `attempts` and `last_error` on
  that row and **stops the pass**, so a later row never overtakes an
  earlier one. The same row is retried on the next pass;
- a row that has failed `OUTBOX_MAX_ATTEMPTS` times (default 20, `0` =
  unlimited, ADR 0040) is no longer claimed: the relay logs
  `outbox relay: row reached the attempt cap and is no longer claimed` at
  ERROR once, with `outbox_id`, `topic`, `event_type` and `key`, and later
  rows (including later rows with the same key) publish past it. The row
  stays in `outbox_events` for inspection and is never swept;
- delivery is at least once: a crash between send and the `UPDATE`
  re-sends the row. Consumers dedupe on the CloudEvents `id`, which is
  stored in the row and identical on redelivery.

`order.outbox.lag_seconds` reports the age of the oldest unpublished row
(see [Observability](./observability.md)).

### Dead-letter topics

| Consumer | DLQ | What goes there | Retry before DLQ |
| --- | --- | --- | --- |
| re-promise | `warehouse.fulfillment.events.dlq` | messages that are not valid CloudEvents (immediately); messages whose handling still fails after 3 attempts (100 ms to 2 s jittered backoff), including repeated `ErrConcurrentModification` | 3 attempts |
| planned-capacity | `warehouse.warehouse-planning.events.dlq` | not a CloudEvent, undecodable or invalid payload (immediately); transient failures after the bounded retry | bounded, in process |
| product-classification | none | invalid messages are logged at WARN and committed; the next `ProductClassified` for the SKU repairs the copy. Transient failures retry the same message until they succeed | unbounded |
| release-confirmation (`RELEASE_CONFIRMATION_MODE=kafka` only) | `warehouse.work-planning.events.dlq` | not a CloudEvent, or a `WorkUnitCreated` whose `data` cannot be decoded at all (for example a `line_no` that is a string); transient failures after the bounded retry. A `WorkUnitCreated` with no `reference`/`line_no`, or for a line this service never released, is **not** dead-lettered: it is committed and counted (`ignored_no_reference`, `ignored_unknown_line`) | 3 attempts (100 ms to 2 s jittered backoff) |
| analytics (projector) | none | invalid messages are logged and skipped; unknown types are acknowledged | n/a |

A DLQ message is the raw original value and key with three headers:
`x-dlq-source-topic`, `x-dlq-error`, `x-dlq-failed-at` (RFC 3339). The
offset of the original message is committed only after the DLQ write
succeeds; if the DLQ write fails, the consumer stops instead of skipping.
The release-confirmation consumer is the exception: it keeps retrying the
same message with jittered backoff (logging `release confirmation: kafka
unavailable, retrying`) until the DLQ write succeeds, and never skips it.

## Housekeeping

The sweeper (ADR 0032) runs in every Postgres-backed `cmd/order` pod,
every `HOUSEKEEPING_INTERVAL` (1 h), in batches of 1000 rows:

- `idempotency_keys` older than `IDEMPOTENCY_KEY_TTL` (24 h);
- **published** `outbox_events` older than `OUTBOX_RETENTION` (168 h);
- `CONFIRMED` `release_confirmations` rows older than
  `RELEASE_CONFIRMATION_RETENTION` (168 h, by `confirmed_at`; `0` keeps
  them). `PENDING` and `UNCONFIRMED` rows are open problems and are never
  swept.

Then, once per pass and only with `RELEASE_CONFIRMATION_MODE=kafka` and
`RELEASE_CONFIRMATION_TIMEOUT` above `0`, it runs the release-confirmation
timeout (ADR 0043): every `PENDING` row whose `released_at` is older than
`RELEASE_CONFIRMATION_TIMEOUT` (15 m) becomes `UNCONFIRMED`, in batches of
100 lines per transaction, and each raises one `OrderReleaseUnconfirmed`
event through the outbox. There is no second scheduler. A line is therefore
flagged on the first pass after its timeout, so with the default 1 h
`HOUSEKEEPING_INTERVAL` that is **up to 1 h 15 m after release**. When the
loop is on, set `HOUSEKEEPING_INTERVAL` to about `5m` (the other sweeps are
cheap batched deletes). `HOUSEKEEPING_INTERVAL=0` disables the whole
sweeper, so no line is ever flagged and `CONFIRMED` rows are not pruned.

It logs `housekeeping sweep` with `idempotency_keys_deleted`,
`outbox_events_deleted`, `release_confirmations_pruned` and
`releases_unconfirmed` when it did anything, and `housekeeping sweep
failed` on error. At boot, `housekeeping sweeper started` shows
`release_confirmation_retention` and `release_confirmation_sweep` (`true`
when the timeout is armed). Several replicas sweeping at once is safe: the
timeout claims its rows with `FOR UPDATE SKIP LOCKED`, so a line is flagged
exactly once.

Nothing sweeps `repromise_processed_events`,
`planned_capacity_processed_events`,
`product_classification_processed_events`,
`release_confirmation_processed_events`, `analytics_processed_events`
or `analytics_consumed_events`. They grow by one row per consumed event.

## Scaling

| Workload | HPA block (`autoscaling.*`, all `enabled: false` by default) | Notes |
| --- | --- | --- |
| `cmd/order` | `api`: 1 to 4 replicas, 70 % CPU | Stateless. The replay caches are per process and rebuilt on boot. The re-promise, planned-capacity, classification and (when on) release-confirmation groups are shared, so replicas split partitions. Each pod opens up to 10 Postgres connections (`postgres.MaxConns`, `statement_timeout` 5 s). |
| `cmd/order-projector` | `projector`: 1 to 2 | Stable group; writes are idempotent on `event_id`. Pool of 5 (`statement_timeout` 10 s). |
| `cmd/order-reports` | `reports`: 1 to 3 | Read-only pool of 5 (`statement_timeout` 15 s). |
| `cmd/mcp` | none on purpose | The MCP SDK keeps sessions in process memory (`Mcp-Session-Id`) and the Service has no session affinity, so a second replica breaks multi-call sessions. Keep `mcp.replicaCount: 1`. |
| frontend | `frontend`: 1 to 3 | Static assets only. |

When an HPA is enabled, the chart omits `replicas` from that Deployment.
Size PgBouncer and Postgres for `maxReplicas × MaxConns` (ADR 0026).

## Analytics reports API (`cmd/order-reports`)

These routes are not in `apis/openapi.yaml`. The full field list is on
[Order Funnel & Allocation Health](../analytics/order-funnel-report.md).

| Route | Query parameters | Response |
| --- | --- | --- |
| `GET /reports/funnel` | `from`, `to` (required, RFC 3339; window is `[from, to)`), `pathId` (optional, exact match), `granularity` (optional, only `hour`) | `200 {"rows":[{pathId, hourBucket, ordersReceived, …, promiseToCutoffGapSeconds}]}`; `400` problem `invalid-report-query`; `500` problem `report-store-error` |
| `GET /reports/funnel/freshness` | none | `200 {"lagSeconds": <float>}`, the age of the most recently applied event (0 when empty) |
| `GET /healthz` | none | `200 {"status":"ok"}` |

## Routine procedures

### Deploy a new version

1. Merge to `main` (GitFlow release). CI's `docker-publish` pushes
   `ghcr.io/iqvo/order-management:<sha>`, and `release` tags `vX.Y.Z` and
   pushes the chart.
2. Roll the Deployment. New pods run any new migration before they
   listen. Old and new pods can overlap safely: the relay uses
   `SKIP LOCKED` and the sweeper is idempotent.
3. Watch `kubectl rollout status deploy/order-management` and the startup
   logs listed in [Troubleshooting](./troubleshooting.md#startup-log-lines).

### Re-publish events stuck in the outbox

The relay retries an unpublished row until it has failed
`OUTBOX_MAX_ATTEMPTS` times (default 20, ADR 0040); after that the row is
parked and no longer claimed. To see what is stuck or parked:

```sql
SELECT id, topic, event_type, key, attempts, last_error, created_at
FROM outbox_events
WHERE published_at IS NULL
ORDER BY id
LIMIT 20;
```

Rows with `attempts` at or above `OUTBOX_MAX_ATTEMPTS` are parked (the
relay also logged `outbox relay: row reached the attempt cap and is no
longer claimed` for each). Fix the cause shown in `last_error` (broker
reachability, a message over the broker's size limit, a topic that cannot
be created), then re-arm the row so the next relay pass claims it again:

```sql
UPDATE outbox_events SET attempts = 0 WHERE id = 12345;
-- after a broker outage that parked many rows:
UPDATE outbox_events SET attempts = 0 WHERE published_at IS NULL AND attempts >= 20;
```

A re-armed row publishes AFTER the later rows that overtook it, so a
consumer sees that event out of order. Do not set `published_at` by hand
unless you have decided to drop that event: it is the only copy.

### Replay a dead-lettered message

DLQ messages keep their original value and key. After fixing the cause,
produce the value back onto the source topic with the same key (for
example with `kafka-console-producer` and `--property parse.key=true`).
The consumers' idempotency tables (`repromise_processed_events`,
`planned_capacity_processed_events`, and for the release-confirmation
consumer `release_confirmation_processed_events`) make a replay of an
already-applied event a no-op, because the CloudEvents `id` is unchanged. The consumers
decode only the message value (`cloudevents.Decode`), so a replay without
the `content-type` header is still accepted.

### An order line was released but never confirmed (OrderReleaseUnconfirmed)

This procedure exists only when `RELEASE_CONFIRMATION_MODE=kafka` (ADR 0043).
The mode is `off` by default; then no confirmation row is written, no
consumer runs, no timeout sweep runs, and none of the signals below exist.

**Symptom.** Any of these:

- an ERROR log line `release unconfirmed` with `order_id`, `line_no`,
  `path_id`, `work_unit_id`, `released_at` and `timeout`;
- the counter `order.release_confirmation.unconfirmed` increasing
  (Prometheus `order_release_confirmation_unconfirmed_total`);
- an `OrderReleaseUnconfirmed` CloudEvent on
  `warehouse.order-management.analytics` (`subject` and key = the order id).

It means this service released the line (`OrderAllocated` or
`OrderPartiallyAllocated` was published for it) and wes-work-planning's
`WorkUnitCreated` for `{order_id}-line-{line_no}` did not arrive within
`RELEASE_CONFIRMATION_TIMEOUT` (default 15 m) of `released_at`. It is a signal,
not proof that the work is missing: a slow consumer or a delayed event looks
the same. The flag is raised by the housekeeping sweep, so it appears up to
`RELEASE_CONFIRMATION_TIMEOUT` plus `HOUSEKEEPING_INTERVAL` after release (up to
1 h 15 m at the default 1 h interval; see [Housekeeping](#housekeeping)).

**1. List the open lines.** The state lives in `release_confirmations`, one
row per released line, never on the order:

```sql
SELECT order_id, line_no, work_unit_id, path_id, released_at, unconfirmed_at
FROM release_confirmations
WHERE state = 'UNCONFIRMED'
ORDER BY unconfirmed_at;

-- how much is in each state (PENDING rows are still inside the timeout)
SELECT state, count(*) FROM release_confirmations GROUP BY state;
```

Read the pattern before chasing one order. Many lines released over the same
few minutes point at wes-work-planning's consumer, or at its path to Kafka,
being down or stuck. A single line points at that one message. Every line
flagged since the loop was enabled points at the producer not stamping
`reference`/`line_no` on `WorkUnitCreated`: check
`order_release_confirmation_events_total{outcome="ignored_no_reference"}`,
which then climbs.

**2. Check this service's side.**

- Was the release actually published? The key of an outbox row is the order id
  (`bytea`):

  ```sql
  SELECT id, topic, event_type, attempts, last_error, created_at, published_at
  FROM outbox_events
  WHERE convert_from(key, 'UTF8') = 'ord-a1b2c3d4-0000-0000-0000-000000000001'
  ORDER BY id;
  ```

  A row on `warehouse.order-management.events` with `published_at IS NULL` never
  left this service: follow [Re-publish events stuck in the
  outbox](#re-publish-events-stuck-in-the-outbox). Once it is published,
  wes-work-planning enqueues the work and the late confirmation clears the row.
  Published rows older than `OUTBOX_RETENTION` (7 days) are gone.
- Is the consumer of `WorkUnitCreated` running? At boot it logs
  `release confirmation consumer configured` (`topic`, `group_id`, `timeout`)
  and the sweeper logs `housekeeping sweeper started` with
  `release_confirmation_sweep=true`. While the broker is unreachable it logs
  `release confirmation: kafka unavailable, retrying`. The service exports no
  consumer-lag metric: read the lag of `RELEASE_CONFIRMATION_CONSUMER_GROUP` on
  `warehouse.work-planning.events` from the broker. A lagging group only delays
  confirmations; they land late once it catches up.
- Was a `WorkUnitCreated` dead-lettered here? This service's DLQ for that
  consumer is `warehouse.work-planning.events.dlq` (`x-dlq-error` header, and
  a `release confirmation: … sending to dead-letter topic` WARN, with `ce_id`
  when the message was a CloudEvent). Only an event that cannot be decoded at
  all, or that still fails after 3 attempts, lands there; an event without
  `reference`/`line_no` or for an unknown line is counted as `ignored_*` and
  committed. After fixing the cause, [replay it](#replay-a-dead-lettered-message)
  onto `warehouse.work-planning.events`.

**3. Check wes-work-planning's side.** This service never calls it (ADR 0005),
so the rest is checked there, in its own runbook: whether its consumer of
`warehouse.order-management.events` is running and caught up, and whether it
dead-lettered the `OrderAllocated`/`OrderPartiallyAllocated` for the order. By
the fleet's `<source topic>.dlq` naming that dead-letter topic would be
`warehouse.order-management.events.dlq`; this repository cannot verify
wes-work-planning's configuration, so confirm the real name there. Also check
that its own outbox relay is publishing `WorkUnitCreated`, and that the event
carries `reference` and `line_no`.

**4. What happens next, and what does not.**

- **A late confirmation clears it.** A `WorkUnitCreated` that arrives after the
  flag sets the row `CONFIRMED` (`unconfirmed_at` is kept as the audit trail),
  logs the WARN `release confirmed late: the line flagged unconfirmed was
  admitted after all` and counts `outcome="late_confirmed"`. The
  `OrderReleaseUnconfirmed` event already published is not retracted. To list
  such lines: `SELECT * FROM release_confirmations WHERE state = 'CONFIRMED'
  AND unconfirmed_at IS NOT NULL;`.
- **Nothing is cancelled, re-released or retried automatically.** The order's
  status stays `Released`, the funnel still counts it released, and this
  service never re-publishes `OrderAllocated` on its own (a released line also
  cannot be cancelled here, `409 order-already-released`, BR6 / ADR 0004).
  Getting the work admitted means fixing the cause on the wes-work-planning side
  (restart or unstick its consumer, replay its dead-lettered message). The row
  clears when its `WorkUnitCreated` arrives; do not edit it by hand to silence
  the signal.
- `PENDING` and `UNCONFIRMED` rows are never pruned. `CONFIRMED` rows are
  deleted after `RELEASE_CONFIRMATION_RETENTION` (168 h).

### Turn the release-confirmation loop on or off

Off is the default and changes nothing. To switch it on:

1. Make sure wes-work-planning already stamps `reference` and `line_no` on
   `WorkUnitCreated` (its ADR 0041). Without it no event matches and every
   released line is flagged after the timeout.
2. Set `RELEASE_CONFIRMATION_MODE=kafka` and a stable
   `RELEASE_CONFIRMATION_CONSUMER_GROUP`. The pod also needs `DATABASE_URL`,
   `KAFKA_BROKERS` and `EVENT_PUBLISHER=kafka`; a missing one is a boot error
   that names it. The Helm chart has no dedicated values for these settings
   yet (no `releaseConfirmation.*`, nothing in its ConfigMap): set them through
   `extraEnv`.
3. Set `HOUSEKEEPING_INTERVAL` to about `5m`, so a line is flagged soon after
   its timeout instead of up to 1 h 15 m later.
4. Expect coverage to start at enablement: lines released before the switch
   (or while it was `off`) have no row and their `WorkUnitCreated` is ignored.
   A new consumer group also replays the whole retained
   `warehouse.work-planning.events` topic from the first offset; on first
   enablement no row exists yet, so the replay matches nothing and is
   harmless, but it takes minutes before the group is caught up.

To switch it off, set the mode back to `off` and restart. The tables stay in
place and inert; rows still `PENDING` are no longer swept or confirmed.

### Rebuild the analytics read model

The projector dedupes on the event id in `analytics_consumed_events` and
`analytics_processed_events`, so resetting the consumer group alone
re-reads the topic but applies nothing. To rebuild:

1. scale `<fullname>-projector` to 0;
2. truncate `funnel_rollup`, `repromise_rollup`,
   `analytics_processed_events` and `analytics_consumed_events` in the
   analytical database;
3. reset group `order-management-analytics` on
   `warehouse.order-management.analytics` to the earliest offset;
4. scale the projector back up.

The report only covers what the topic still retains.

### Rotate database credentials

`DATABASE_URL`, `MIGRATIONS_DATABASE_URL` and the analytics DSNs are read
once at boot. Update the Secret (`database.existingSecret`, or the
analytics Secret with keys `ANALYTICS_DATABASE_URL` and
`ANALYTICS_READER_DATABASE_URL`), then restart the Deployments that read
it: `order-management`, `order-management-mcp`, and with analytics
enabled `-projector` and `-reports`.

### Change a consumer group

`PLANNED_CAPACITY_CONSUMER_GROUP`,
`PRODUCT_CLASSIFICATION_CONSUMER_GROUP` and
`RELEASE_CONFIRMATION_CONSUMER_GROUP` must stay fixed. A new value has
no committed offset, and kafka-go then starts at the **earliest** offset
(its `StartOffset` default, which these readers do not override), so the
new group re-reads the whole retained topic. That is safe, because each
event id is claimed once in `planned_capacity_processed_events`,
`product_classification_processed_events` or
`release_confirmation_processed_events`, but it takes time and load.
Never share a group with another environment: the replicas would split
partitions with it.
