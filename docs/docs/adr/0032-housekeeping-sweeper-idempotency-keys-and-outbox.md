---
id: 0032-housekeeping-sweeper-idempotency-keys-and-outbox
slug: /adr/0032-housekeeping-sweeper-idempotency-keys-and-outbox
title: "0032. Housekeeping sweeper for idempotency keys and published outbox rows"
sidebar_label: "32. Housekeeping sweeper"
sidebar_position: 32
description: "ADR 0032 — a small background sweeper in cmd/order that deletes idempotency_keys older than a TTL (default 24h) and PUBLISHED outbox_events older than a retention (default 7d), plus an order.outbox.lag_seconds gauge, closing the unbounded-growth gap left open by ADR 0022 and ADR 0023."
---

# 0032. Housekeeping sweeper for idempotency keys and published outbox rows

## Status

Accepted. Closes the "grows unboundedly" follow-up recorded in
[ADR 0022](./0022-transactional-outbox.md) (`outbox_events`) and
[ADR 0023](./0023-idempotency-key-middleware.md) (`idempotency_keys`),
and the observability gap that a stuck outbox row was invisible to
metrics. Mirrors inventory-storage's housekeeping-sweeper ADR (its ADR
0026; same env knobs, same defaults) so operators learn one convention
fleet-wide.

## Context

Two tables only ever grow:

- `idempotency_keys` ([ADR 0023](./0023-idempotency-key-middleware.md))
  gets one row per `Idempotency-Key` seen on `POST /orders`, including
  the stored response body. Nothing deleted them; ADR 0023 explicitly
  deferred the cleanup job and created
  `idx_idempotency_keys_created_at` specifically for it.
- `outbox_events` ([ADR 0022](./0022-transactional-outbox.md)) keeps
  every row after the relay marks it `published_at`. Published rows are
  only useful for short-term forensics. ADR 0022's Consequences flagged
  the relay as a failure mode observable only through logs and the
  `attempts`/`last_error` columns — there was no metric for "the relay
  is behind."

Both were accepted as known gaps when introduced. At production traffic
they are an unbounded storage and vacuum cost.

## Decision

`internal/adapters/outbound/postgres.Sweeper` — **one** small type with
one loop (both jobs are "delete old rows in batches" and share an
interval) — started by the `cmd/order` composition root for every
Postgres-backed process (idempotency keys are written regardless of
`EVENT_PUBLISHER`):

| Env var | Default | Meaning |
|---|---|---|
| `HOUSEKEEPING_INTERVAL` | `1h` | Sweep period. `0` disables the sweeper. |
| `IDEMPOTENCY_KEY_TTL` | `24h` | Delete `idempotency_keys` rows with `created_at` older than this. `0` keeps them forever. |
| `OUTBOX_RETENTION` | `168h` (7d) | Delete **published** `outbox_events` rows with `published_at` older than this. `0` keeps them forever. |

(Chart values: `config.housekeepingInterval`,
`config.idempotencyKeyTtl`, `config.outboxRetention`, rendered only when
a database is configured.) An unparsable or negative value logs a
warning and falls back to the default.

Rules the implementation guarantees:

1. **Unpublished outbox rows are never deleted**, however old — an event
   still waiting for the relay (broker outage) is not garbage.
2. Deletes are **batched** (1000 rows per statement, repeated until a
   batch comes back short) so a large backlog is removed in short
   transactions, not one long lock. Each statement targets an explicit
   key/id set chosen by a subquery, so it is safe to run in several
   replicas at once (HPA).
3. It runs one pass **immediately on start** and then every interval,
   never returns an error (a failed pass is logged and retried), and is
   stopped **before** the pool closes during graceful shutdown (bounded
   by `sweeperShutdownBudget`).
4. `cmd/mcp`, `cmd/order-reports` and `cmd/order-projector` do not
   sweep: none of them writes `idempotency_keys` (no mutating POST is
   served there) and the projector/reports pair owns the analytical
   database, not the OLTP one. The relay stays in `cmd/order` with the
   write path it drains.

Additionally, `postgres.RegisterOutboxLagGauge` installs an asynchronous
`order.outbox.lag_seconds` gauge (age of the oldest unpublished
`outbox_events` row, 0 when drained), registered by `cmd/order` when —
and only when — `EVENT_PUBLISHER=kafka` (the outbox is then the publish
path), and unregistered before the pool closes. This is the fleet's
workforce-management convention, ported.

## Consequences

- The 24h TTL is now the **replay window** of the idempotency contract:
  a retry of a `POST /orders` whose key is older than the TTL is treated
  as a brand-new request, not replayed. Clients must retry within that
  window (the intended use is retrying a dropped response, seconds to
  minutes).
- Published outbox rows older than 7 days are gone; forensics beyond
  that must come from Kafka itself (topic retention) rather than the
  outbox table.
- The lag gauge makes a stuck relay visible on a dashboard before
  downstream consumers notice stale data: it rises with the age of the
  oldest unpublished row regardless of cause (relay down, broker down,
  poison row), which is exactly the alert "something is waiting."
- Proven against a real Postgres (testcontainers):
  `TestSweeper_DeletesOnlyExpiredIdempotencyKeysAndPublishedOutboxRows`,
  `TestSweeper_ZeroTTLAndRetentionKeepEverything`,
  `TestSweeper_RunSweepsOnStartAndStopsOnCancel`
  (`internal/adapters/outbound/postgres/sweeper_integration_test.go`),
  `TestRegisterOutboxLagGauge_ReportsAgeOfStuckEvent`
  (`outbox_lag_integration_test.go`), and the env wiring end to end by
  `TestBuildRepoAdapters_StartsHousekeepingSweeperFromEnv`
  (`cmd/order/housekeeping_integration_test.go`).

## Alternatives considered

- **A Kubernetes CronJob / pg_cron.** Rejected: another deployable (or
  extension) to operate, and the retention knobs would live outside the
  service's own config.
- **Partitioned tables with drop-partition.** Rejected as
  over-engineered for the current row volume.
- **Two independent jobs.** Rejected: one loop with two statements is
  simpler and shares the interval, shutdown and logging.
