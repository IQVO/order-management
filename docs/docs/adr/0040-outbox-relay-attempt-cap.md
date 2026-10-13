---
id: 0040-outbox-relay-attempt-cap
slug: /adr/0040-outbox-relay-attempt-cap
title: "0040. The outbox relay stops claiming a row after OUTBOX_MAX_ATTEMPTS failures"
sidebar_label: "40. Outbox relay attempt cap"
sidebar_position: 40
description: "ADR 0040 — postgres.OutboxRelay gets WithMaxAttempts: a row that failed OUTBOX_MAX_ATTEMPTS times (default 20, 0 = unlimited) is excluded from future claims, kept in outbox_events with attempts and last_error, and logged once at ERROR. Later rows with the same key overtake it. Fleet gap-fix decision D12."
---

# 0040. The outbox relay stops claiming a row after `OUTBOX_MAX_ATTEMPTS` failures

## Status

Accepted — implemented by the change that introduces this record (fleet
gap-fix wave 2026-10-12, decision D12).

Amends the relay behaviour of [ADR 0022](./0022-transactional-outbox.md)
(which retried a failing row without bound). Same semantics as
inventory-storage's `WithMaxAttempts`, the fleet convention.

## Context

`postgres.OutboxRelay` claims unpublished `outbox_events` rows oldest first
and stops a pass at the first send failure, so a later row never overtakes
an earlier failed one. A row the broker will never accept (a message over
the broker's size limit, a topic that cannot be created) therefore failed on
every pass, forever, and every event written after it waited behind it:
`order_outbox_lag_seconds` grew without bound and nothing published until an
operator deleted or fixed the row by hand.

## Decision

1. **Attempt cap.** `postgres.WithMaxAttempts(n)` makes the claim query skip
   a row whose `attempts` already reached `n`
   (`AND ($2 <= 0 OR attempts < $2)`). `n = 0` keeps the old unlimited
   behaviour.
2. **Configuration.** `cmd/order` reads `OUTBOX_MAX_ATTEMPTS` (default `20`,
   `0` = unlimited; a negative or non-numeric value logs a warning and uses
   `20`) and passes it to the relay. The chart exposes it as
   `config.outboxMaxAttempts`.
3. **Nothing is deleted.** A capped row stays in `outbox_events` with
   `published_at IS NULL`, its `attempts` and its `last_error`, for
   inspection. The housekeeping sweeper (ADR 0032) only deletes published
   rows, so it never removes it.
4. **One ERROR line.** The failure that brings a row to the cap is logged at
   ERROR, `outbox relay: row reached the attempt cap and is no longer
   claimed`, with `outbox_id`, `topic`, `event_type`, `key`, `attempts`,
   `max_attempts` and the send error.
5. **Re-arm.** An operator re-arms a row after fixing the cause with
   `UPDATE outbox_events SET attempts = 0 WHERE id = ...`; the next pass
   claims it again.
6. **Sensor.** `TestOutboxRelayCallsSetMaxAttempts`
   (`internal/architecture/outbox_relay_fitness_test.go`) fails when a
   non-test file under `cmd/` calls `NewOutboxRelay` without
   `WithMaxAttempts`.

## Consequences

- **Per-key ordering has an exception.** Once a row is capped, later rows
  with the same Kafka key (the same order) publish before it, and it may
  never publish at all. A consumer can therefore see, for example,
  `OrderReleased` without the `OrderAllocated` that preceded it. That is the
  price of the cap, and it is accepted: without it one poison row stops
  every order's events, not just one order's. The capped row is visible (the
  ERROR log, the row itself) and can be re-armed.
- A transient broker outage longer than 20 relay passes also caps rows. The
  relay sleeps `OUTBOX_RELAY_INTERVAL` (1 s) between failing passes, so 20
  attempts is about 20 s of continuous failure for the head row. Re-arm the
  rows after the outage (`UPDATE outbox_events SET attempts = 0 WHERE
  published_at IS NULL AND attempts >= 20`), or raise `OUTBOX_MAX_ATTEMPTS`
  where outages are expected to last longer.
- `order_outbox_lag_seconds` still measures the oldest unpublished row, so a
  capped row keeps the gauge growing. That is intended: the gauge is the
  alert that a row needs an operator.
- Tests: `TestOutboxRelay_MaxAttempts_PoisonRowIsExcludedAndTheNextRowPublishes`
  and `TestOutboxRelay_WithoutMaxAttempts_RetriesWithoutBound`
  (testcontainers Postgres, `outbox_integration_test.go`),
  `TestOutboxMaxAttemptsFromEnv` (`cmd/order`), the fitness test above and
  the chart render test (`charts/order-management/tests/render_test.sh`).

## Alternatives considered

- **Dead-letter the row to a Kafka DLQ topic.** Rejected: the row failed
  because Kafka would not take it, so a DLQ on the same broker usually fails
  too, and the fleet convention (inventory-storage) keeps the row in Postgres.
- **Skip the failed row within the same pass.** Rejected: it would break
  per-key ordering on every transient failure, not only for a poison row.
- **Keep unlimited retries and alert.** Rejected: one row still stops all
  publishing until a human acts.
