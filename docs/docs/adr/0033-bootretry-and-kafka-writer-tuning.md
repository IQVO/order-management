---
id: 0033-bootretry-and-kafka-writer-tuning
slug: /adr/0033-bootretry-and-kafka-writer-tuning
title: "0033. Boot-time first-dial retry, synchronous-writer BatchTimeout/acks, and DLQ topic-create retry"
sidebar_label: "33. Boot retry + Kafka writer tuning"
sidebar_position: 33
description: "ADR 0033 — records three already-shipped operational hardenings that had no record: internal/bootretry (every composition root retries its first Postgres/Kafka dial past the Istio native-sidecar first-dial reset), a 10ms BatchTimeout + RequireAll on every synchronous kafka-go writer (the 1s default capped each service at ~1 event/s and masked at-most-once writes), and a bounded retry on the DLQ publish while the auto-created dead-letter topic elects a leader."
---

# 0033. Boot-time first-dial retry, synchronous-writer BatchTimeout/acks, and DLQ topic-create retry

## Status

Accepted — this record documents behaviour that already shipped (it was
added in pieces alongside ADR-0025's resilience work, the ADR-0022 outbox
relay, and the ADR-0029 migration-path fix) but had no decision record of
its own. Nothing new is being decided here; the record exists so the
*why* is not lost.

## Context

Three operational defects, all observed live in this cluster:

1. **Boot crash-loops on the first dial.** Istio 1.30 native sidecars run
   `istio-proxy` as an init container with `restartPolicy=Always`, so
   `holdApplicationUntilProxyStarts` is a no-op for them: every injected
   pod's FIRST outbound TCP dial (Postgres, Kafka) is reset ~10s after
   the app starts. A binary that dials once and exits 1 on failure turns
   that known, transient condition into CrashLoopBackOff — confirmed on
   order-management (one pod at 129 restarts).

2. **kafka-go's synchronous writes are slow and were silently
   at-most-once.** `kafka-go.Writer`'s default `BatchTimeout` is 1s: a
   synchronous `WriteMessages` waits up to a full second for a batch to
   fill before flushing. The outbox relay sends one row per call, so the
   default capped the service at ~1 event/s — a day's ~2,600 fulfillment
   events sat in `outbox_events` for an hour in the warehouse-day
   simulation. Worse, the default `RequiredAcks` is `RequireNone`:
   `WriteMessages` returns nil without waiting for the broker, so the
   relay marked rows published the broker never stored — silently
   at-most-once, the opposite of the outbox's guarantee. The 1s batch
   wait masked it; with prompt flushing, probes against a fresh
   8-partition topic lost whole batches in 3 of 6 runs.

3. **The first poison message could halt a consumer forever.** The
   dead-letter topic (`<topic>.dlq`) does not exist on a fresh broker
   (the fleet auto-creates topics on first write). Without
   `AllowAutoTopicCreation` on the DLQ writer, the first DLQ publish
   fails with `[3] Unknown Topic Or Partition` — and even with it, the
   freshly created topic briefly has no leader, so the publish can fail
   while the topic is being created.

## Decision

1. **`internal/bootretry`** is the one shared boot-time retry helper,
   used by all four composition roots (`cmd/order`, `cmd/mcp`,
   `cmd/order-projector`, `cmd/order-reports`) for every boot-time dial:
   migrations, the post-pool ping, and the Kafka catalogue/consumer
   dials. Five attempts with exponential backoff (1+2+4+8+16 ≈ 31s —
   past the ~10s reset, inside the startupProbe's 60s), returning the
   LAST error so a permanent failure still reports its real cause. It
   does NOT weaken fail-closed: on exhaustion the caller still refuses
   to boot. It mirrors network-fulfillment PR #7's helper exactly.

2. **Every synchronous kafka-go writer in this service sets
   `BatchTimeout: 10ms` and `RequiredAcks: RequireAll`** (pinned by the
   source-level fitness test `TestEverySyncWriterSetsBatchTimeout` in
   `internal/adapters/outbound/kafka`): the integration publisher, the
   analytics publisher, the outbox relay sink, and the DLQ writer
   (`dlqBatchTimeout`).

3. **The DLQ writer sets `AllowAutoTopicCreation` and the DLQ publish
   retries, bounded, while the topic is being created**:
   `writeDLQ` retries only on the not-ready errors
   (`UnknownTopicOrPartition`, `LeaderNotAvailable`, including inside
   `kafkago.WriteErrors`) for up to 40 attempts × 250ms backoff
   (~10s), then returns the error — the offset is not committed, the
   consumer stops loudly rather than dropping the message.

## Consequences

- Pods survive the sidecar warm-up; a genuinely unreachable dependency
  still fails the pod within the probe budget.
- Outbox relay throughput is not batch-wait-bound, and every drained row
  is broker-acknowledged before it is marked published — the outbox's
  at-least-once guarantee is real, not nominal.
- A fresh broker's first poison message dead-letters successfully once
  the topic has a leader; if the broker is genuinely broken the consumer
  halts visibly instead of losing the message.
- All three behaviours are pinned by tests:
  `internal/bootretry/retry_test.go`,
  `TestBuildRepoAdapters_RetriesTheDatabaseNotJustOnce`
  (`cmd/order/wiring_test.go`),
  `TestEverySyncWriterSetsBatchTimeout`
  (`internal/adapters/outbound/kafka/writer_batchtimeout_test.go`),
  `TestNewDLQWriter_AutoCreatesTheDeadLetterTopic` and
  `TestWriteDLQ_RetriesOnlyWhileTheTopicIsBeingCreated`
  (`internal/adapters/inbound/kafka/dlq_writer_test.go`), plus the
  end-to-end `repromise_dlq_autocreate_integration_test.go`.

## Alternatives considered

- **Drop the retry and raise the startupProbe thresholds.** Rejected:
  it treats a known-transient condition as a pod failure and slows every
  real rollout (each restart re-pays the probe ladder).
- **Async (fire-and-forget) writers with background flush.** Rejected
  for the relay/DLQ paths: they exist precisely to make durability
  visible on the call; async would reintroduce the silent-loss window.
- **Pre-create the DLQ topic from the chart.** Rejected: the fleet
  convention is auto-creation on first write
  (warehouse-infra `kafka.tf`, 8 partitions); a chart-owned topic for
  every consumer's suffix would fork that convention.
