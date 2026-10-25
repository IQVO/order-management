---
id: 0045-a-dead-consumer-takes-the-process-down
slug: /adr/0045-a-dead-consumer-takes-the-process-down
title: "0045. A Kafka consumer that stops on its own takes the process down"
sidebar_label: "45. A dead consumer ends the process"
sidebar_position: 45
description: "ADR 0045 — every Kafka consumer goroutine of cmd/order (process-path catalogue, CPT schedule, path capacity, product classification, planned capacity, release confirmation, RepromiseOrder) and of cmd/order-projector reports an unexpected stop on one channel (internal/consumerdeath); run() returns the first death, the process exits non-zero and the kubelet restarts the pod. A stop caused by the service's own shutdown is clean. No in-process retry, no env var, no chart value, no wire change."
---

# 0045. A Kafka consumer that stops on its own takes the process down

## Status

Accepted — implemented by the change that introduces this record. Mirrors
`network-inventory-planning` ADR 0019 and the death-watcher of
`network-fulfillment`.

## Context

`cmd/order` started its Kafka consumers in goroutines and, for six of the seven,
only logged `<name> consumer stopped` when `Run` returned an error (the
RepromiseOrder consumer already put its error on `errCh`). `cmd/order-projector`
did the same for the analytics consumer. The HTTP server kept running, `/healthz`
and `/readyz` kept answering 200, and the read model behind the dead consumer froze
with nothing unhealthy to alert on. This was seen live in the fleet: under host load
the broker reset connections and answered `Group Load In Progress`, and a consumer
in `network-inventory-planning` stopped and never came back while its pod stayed
Ready for hours.

Which of our consumers can actually return an error:

- The process-path catalogue, CPT schedule and path capacity consumers
  (`kafkacatalog`, `kafkacptschedule`, `kafkapathcapacity`) and the analytics
  consumer of `order-projector` **return** the first `ReadMessage` error that is not
  a cancellation. These are the ones a broker reset kills. They feed the promise
  policy (path validation, CPT schedule, path capacity), so a dead one makes every
  promise quietly use stale or fallback data.
- The product classification, planned capacity, release confirmation and
  RepromiseOrder consumers retry fetch and handle failures in-process with a
  backoff and return only when their context ends. They cannot die on a broker
  error today; they are wired to the same channel anyway, so a future change that
  makes one return (or a `nil` return while the service runs) cannot go silent.

## Decision

A consumer that stops while the service is still meant to run ends the process.

1. `internal/consumerdeath` provides `Deaths`: one buffered channel and
   `Watch(ctx, name, logger, run)`, which runs a consumer to completion on its
   goroutine and classifies how it stopped. Every consumer goroutine of `cmd/order`
   (7) and `cmd/order-projector` (1) goes through `Watch`.
2. `serveAndShutdown` selects on `deaths.C()` beside the HTTP server's error
   channel and the signal context; `run()` returns the first death, so `main` exits
   with status 1 and the kubelet restarts the pod. The consumers rejoin their groups
   and resume from the committed offset (the three caches replay their topic, as
   they do on any boot). `order-projector` does the same in `waitForStop`.
3. "Unexpected" means: the consumer returned an error that is not
   `context.Canceled`, or returned nil, while **its own context** was still live.
   A stop after the context ended — the stop func of a consumer, the catalogue
   cleanup, the RepromiseOrder drain — is a requested stop, is clean and reports
   nothing, whatever raw network error `Run` returned on the way out.
4. A death that lands together with SIGINT/SIGTERM is part of the shutdown, not a
   crash: `serveAndShutdown` and `waitForStop` fall through to the graceful drain.
   This is the race `network-fulfillment` fixed in PR #119; the consumers here are
   not derived from the signal context, but the check keeps the rule explicit.
5. The channel holds one death. Further deaths are logged and dropped, so a second
   dying consumer never blocks on a send.

The graceful-shutdown sequence of ADR 0025 is unchanged for a requested stop: the
readiness flip, drain delay, HTTP drain, RepromiseOrder stop and relay stop run in
the same order as before.

In-process retry is deliberately not added: a restart rejoins every group with
fresh connections and is the repair the fleet already relies on, and an in-process
loop would need its own backoff, jitter and give-up rule.

## Consequences

- A transient broker problem now shows as a pod restart (visible in
  `kubectl get pods` and restart-count alerts) instead of a silent stale read model.
- If the broker is down for a long time the pod crash-loops with backoff, which is
  the accurate state of a service that cannot consume.
- INFRA VALUES REQUIRED: none. No env var, chart value, migration or wire change.
- Not changed: `order-projector`'s admin HTTP server failing is still only logged;
  the outbox relay returning `nil` while the service runs is not treated as a death.
