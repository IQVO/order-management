---
id: 0025-resilience-circuit-breakers-retry-dlq-shutdown
slug: /adr/0025-resilience-circuit-breakers-retry-dlq-shutdown
title: 25. Per-dependency circuit breakers, read-only retry, Kafka DLQ, and graceful shutdown hardening
sidebar_label: 25. Circuit breakers, retry, DLQ, shutdown
description: "ADR 0025 — Phase 2 resilience for order-management: sony/gobreaker/v2 circuit breakers per outbound dependency (never one global breaker) that reuse each client's EXISTING permissive/fail-loud or fail-open behaviour as the OPEN-state fallback rather than inventing a new one; cenkalti/backoff/v4 jittered retry on productclassification's read-only GET only, never on inventorystorage's mutating POST/DELETE; a dead-letter topic for RepromiseConsumer so one poison message cannot block its partition; and a readiness-flip-first graceful shutdown sequence."
---

# 25. Per-dependency circuit breakers, read-only retry, Kafka DLQ, and graceful shutdown hardening

## Status

Accepted — implemented in the same change that introduced this record.
This is Phase 2 (resilience) of the fleet production-readiness plan,
built on top of the already-merged Phase 0 (boot-retry) and Phase 1
(idempotency: outbox, inbox/dedup, idempotency-key middleware,
optimistic concurrency). Intended as the reference pattern the other
~5 fleet repos in this phase copy verbatim, the same way order-
management's ADR-0023 (idempotency-key middleware) and the outbox ADR
became the fleet's copy-verbatim Phase 1 references.

## Context

Before this change, order-management had two sync cross-context HTTP
clients — `inventorystorage.Client` (mutating: `POST /reservations`,
`DELETE /reservations/{id}`) and `productclassification.Client`
(read-only: `GET /products/{sku}/classification`) — each with a
`permissive`/`http` mode switch but **no circuit breaker, no bounded
retry, and no context-deadline propagation**: a slow or failing
downstream just hung every caller until its own (fresh, hardcoded)
per-call timeout, and a failing dependency was retried on every single
request forever, with no mechanism to stop hammering it. Its inbound
`RepromiseConsumer` (ADR-0018) had **no dead-letter handling at all** —
a single message whose handler always errors (a malformed payload from
a future producer, a permanently-broken downstream dependency) would
retry that same message on every poll indefinitely, blocking every
other message behind it on the same partition. Graceful shutdown
already existed (`signal.NotifyContext` + `httpServer.Shutdown`,
~line 190-202 pre-change) but had no readiness-flip step, no bounded
wait for the Kafka consumer's in-flight message to actually finish and
commit, and no `terminationGracePeriodSeconds` in the Helm chart to
give any of that room to run before Kubernetes SIGKILLs the pod.

## Decision

### 1. One circuit breaker PER downstream dependency, never one global breaker

`internal/resilience` (a new, tiny, dependency-free top-level package —
the SAME "outbound adapters may depend on other top-level `internal/*`
helper packages" pattern already established by `internal/pgtx` and
`internal/bootretry`) holds the tuning EVERY breaker in this service
shares:

```go
const (
    DefaultMaxRequests = 1                // 1 probe per half-open cycle
    DefaultInterval    = 30 * time.Second // closed-state rolling-Counts reset window
    DefaultTimeout     = 30 * time.Second // open-state cooldown before a half-open probe
)

func ReadyToTrip(counts gobreaker.Counts) bool {
    if counts.ConsecutiveFailures >= 5 {
        return true
    }
    if counts.Requests < 10 { // minimum sample size before the error-rate leg engages
        return false
    }
    return float64(counts.TotalFailures)/float64(counts.Requests) > 0.5
}
```

`inventorystorage.NewBreakerClient` and `productclassification.NewBreakerClient`
each construct their OWN `*gobreaker.CircuitBreaker[...]` instance using
this shared `ReadyToTrip`/tuning — a slow or failing
`inventory-storage` deployment tripping its breaker can never affect
`productclassification`'s breaker (or vice versa), and each keeps its
own independent `*http.Client`/connection pool (bulkhead, §2). The
minimum-sample-size guard on the error-rate leg exists so a single
early failure (1 request, 100% error rate) does not trip the breaker on
its own — that is exactly what the `ConsecutiveFailures>=5` leg already
covers sensibly; the error-rate leg only matters once there is enough
traffic for "over half failed" to be meaningful.

`inventorystorage.BreakerClient` wraps `Reserve` AND `RevokeReservation`
through the SAME breaker instance (one breaker per DEPENDENCY, not per
HTTP verb) — `ports.ErrInsufficientStock` (a real, correct 409 from a
healthy dependency) is explicitly excluded from counting as a breaker
failure via `gobreaker.Settings.IsSuccessful`, so a normal run of
backorders can never trip the breaker the way a real outage would.

### 2. The breaker's OPEN-state fallback REUSES each client's existing mode semantics — it does not invent a new one

This is the central design constraint from the plan, and it holds
exactly: **the breaker decides WHEN to fall back; the pre-existing
`permissive`/`http` mode switch decides WHAT the fallback behaviour
is.**

- `inventorystorage.BreakerClient`, while OPEN, calls
  `PermissiveClient.Reserve`/`RevokeReservation` — the SAME fail-LOUD
  behaviour (`ports.ErrDownstreamNotConfigured`) this client already
  had in `permissive` mode, because reserving real stock must never
  silently appear to succeed against a tripped breaker any more than
  against a no-op.
- `productclassification.BreakerClient`, while OPEN, calls
  `PermissiveLookup.GetClassification` — the SAME fail-OPEN behaviour
  (`Known=false, nil error`) this client already had for a transport
  error or a 500, because this is a soft routing/enrichment input, not
  a mutation (order-management's own fail-loud-for-mutations
  vs. fail-open-for-soft-inputs rule).

`isBreakerRejection(err)` (duplicated, unexported, in each package —
adapters never depend on each other per the hexagonal fitness test)
distinguishes gobreaker refusing to even ATTEMPT the call
(`gobreaker.ErrOpenState`/`ErrTooManyRequests`) from a real error a call
gobreaker DID let through; only the former routes to the fallback — a
real error from an attempted call propagates unchanged, exactly as
before this breaker existed.

### 3. Context deadline propagation: `resilience.CallTimeout`

```go
func CallTimeout(ctx context.Context, maxPerCall time.Duration) (context.Context, context.CancelFunc)
```

Every outbound call derives its timeout from the inbound request's OWN
remaining `ctx.Deadline()`, capped at `maxPerCall` (`DefaultTimeout`,
30s) when that remaining budget is larger or absent — never a fresh,
hardcoded timeout that could outlast the caller's own patience. Both
`BreakerClient.Reserve`/`RevokeReservation` and
`BreakerClient.GetClassification` call this before `breaker.Execute`.

### 4. Bulkhead: confirmed, not newly built

Both `inventorystorage.NewClient` and `productclassification.NewClient`
already each construct their own `*http.Client` (defaulting when a nil
`HTTPDoer` is passed) — there was never a shared client across the two
dependencies to begin with. This ADR only confirms and documents that
invariant; no code change was needed for it specifically.

### 5. Retry (`cenkalti/backoff/v4`, jittered, max 3 attempts) ONLY on `productclassification`'s read

`productclassification.BreakerClient.GetClassification` retries
`Client.fetch` (a newly-split-out raw call with real, un-swallowed
errors — see §5a) up to 3 total attempts
(`backoff.WithMaxRetries(policy, 2)`), jittered exponential backoff
(50ms–500ms), bounded by the SAME `callCtx` `CallTimeout` derived. A
404 (a legitimate `Known=false` answer, not a failure) returns on the
FIRST attempt, exactly like a 200 does — it never consumes retry
budget. The retry loop runs INSIDE one `breaker.Execute` call, so a
retry storm against an already-degraded dependency still only ever
counts as ONE success/failure toward the breaker's trip condition, not
three.

**`inventorystorage`'s `Reserve`/`RevokeReservation` deliberately do
NOT get blind retry**, per the plan's explicit instruction and this
service's own risk analysis:

- `POST /reservations` (create) is already covered by Phase 1's
  idempotency-key middleware — a caller-level retry of a lost response
  is already safe; adding a SECOND, lower-level blind retry inside this
  client would just duplicate that protection with none of its
  request-hash-matching safety.
- `DELETE /reservations/{id}` (revoke) is already idempotent at the
  HTTP-status level (`RevokeReservation` treats 404 as success — "the
  reservation is already gone"), but a genuinely BLIND retry inside
  this client of a delete whose response was merely LOST (not failed)
  is a distinct risk this ADR chooses not to take on now: inventory-
  storage's revoke contract does not (yet) guarantee exactly-once
  semantics for a retried delete the way the idempotency-key middleware
  guarantees it for creates. Default: do not retry. A future ADR can
  revisit this once inventory-storage's own delete contract is
  audited.

#### 5a. `productclassification.Client` split into `GetClassification` (fail-open, port-facing) and `fetch` (raw, retry-facing)

Before this change, `GetClassification` swallowed every error into
`Known=false, nil` at the SAME call site that made the HTTP request —
there was no way for a wrapping retrier to tell "this SKU has no
classification" (404, a real answer) apart from "the request itself
failed" (a transport error, an unexpected status — a candidate for
retry). `fetch` is the new, un-swallowed raw call
(`internal/adapters/outbound/productclassification/client.go`);
`GetClassification` is now a one-line fail-open wrapper around it,
preserving its exact pre-existing external behaviour and its full
existing test suite unchanged. `BreakerClient.retryingFetch` calls
`fetch` directly.

### 6. Dead-letter queue for `RepromiseConsumer`

`RepromiseConsumer.handleMessage` now retries
`handleFulfillmentEvent` in-process, with jittered backoff
(`cenkalti/backoff/v4`, 100ms–2s), up to `maxHandlerAttempts` (3) total
attempts, via `handleWithRetry`. `ports.ErrConcurrentModification` (the
optimistic-concurrency sentinel, Phase 1) is explicitly excluded from
this retry loop via `backoff.Permanent` — it already has its OWN,
pre-existing, separate handling: leave the message UNCOMMITTED for safe
redelivery on the next rebalance/restart, because a version conflict
means some OTHER writer already advanced the order and reprocessing
this exact message against the order's now-current state is exactly
the at-least-once semantics this consumer already relies on generally.

Every OTHER genuine infrastructure error, once all 3 attempts are
exhausted, is published — raw payload byte-for-byte, plus
`x-dlq-source-topic`/`x-dlq-error`/`x-dlq-failed-at` headers carrying
replay/debugging context — to `<source-topic>.dlq` via a
`*kafkago.Writer` the consumer now owns (`RepromiseConsumer.dlqWriter`,
closed alongside the reader in `Close`), and **the offset is committed
anyway**: one poison message must never permanently block every order
behind it on the same partition. This is logged at ERROR level
(`"repromise: exhausted retries, sending to dead-letter topic"`) with
enough context (topic, dlq_topic, event_id, event_type, attempts,
error) to be an alert-worthy signal and support a manual replay tool,
not a silent drop.

`NewRepromiseConsumerForTopic` derives the DLQ topic as
`<its own source topic>+".dlq"` (never a fixed constant) — an isolated
integration-test topic automatically gets its own isolated DLQ topic
for free, exactly mirroring how the existing constructor already lets
tests isolate the source topic/group without touching production
names.

Proven end to end with a real testcontainers Kafka
(`repromise_dlq_integration_test.go`,
`TestRepromiseConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition`):
a message whose handler is made to always fail (via a
`ports.RepromiseProcessedEvents` wrapper that unconditionally errors
for one specific `event_id` — the failure is injected at
`RepromiseOrder.Execute`'s very first call, so it never has a side
effect to undo across the 3 retry attempts) lands on the `.dlq` topic
after exactly 3 attempts, with the raw original JSON payload and the
error-context headers intact, and — published right after the poison
message on the SAME topic — a well-formed message for a real order is
processed without delay, proving the partition was never blocked.

### 7. Circuit breaker state as a Prometheus gauge

`telemetry.CircuitBreakerMetrics` (new,
`internal/adapters/outbound/telemetry/circuit_breaker_metrics.go`)
registers ONE OTel `Int64Gauge`, `circuit_breaker.state` — re-exported
by this fleet's OTel Collector prometheus exporter as
`circuit_breaker_state{dependency="inventory-storage"|"product-classification"}`
(0=closed, 1=half-open, 2=open, gobreaker's own numbering verbatim, no
translation table) — on the SAME global `otel.Meter`
`telemetry.Setup`/`NewOrderMetrics` already use, not a second,
parallel registry. `resilience.RecordStateChange(dependency, recorder)`
adapts a `resilience.StateRecorder` (the interface
`CircuitBreakerMetrics` implements) into `gobreaker.Settings.OnStateChange`'s
signature; a `nil` recorder is a documented no-op, mirroring this
repo's existing nil-Logger/nil-Metrics convention, so a test that does
not care about the metric never needs to construct one.

### 8. Graceful shutdown hardening

`cmd/order/main.go`'s pre-existing `signal.NotifyContext` +
`httpServer.Shutdown(shutdownCtx)` sequence is extended, not rewritten,
into this order:

1. **Flip readiness to not-ready FIRST**
   (`inboundhttp.Readiness.SetNotReady`, backing a new `GET /readyz`,
   distinct from the pre-existing `GET /healthz` which stays a pure
   liveness signal and is never flipped by shutdown) — before anything
   else stops, so a Kubernetes `readinessProbe` polling `/readyz` has a
   window to observe the flip and stop routing NEW traffic to this pod
   before step 2 ever closes the listener.
2. **Stop accepting new HTTP connections and drain in-flight requests**
   — `httpServer.Shutdown(shutdownCtx)`, unchanged from before.
3. **Stop the outbox relay and the `RepromiseConsumer` loop cleanly** —
   cancel each one's own context (no NEW work is picked up after this)
   and WAIT, bounded by the same `shutdownCtx`, for each goroutine to
   actually finish in-flight work — for the Kafka consumer, this means
   a message already being handled runs to completion INCLUDING its
   offset commit (`handleMessage`'s commit-before-return shape) before
   `Run` returns — rather than merely firing the cancel and moving on.
   This is the "final offset commit" guarantee: no message is left
   processed-but-uncommitted by an abrupt stop.
4. **Close the pgx pool LAST** — `closeAdapters()`/`closeCatalogue()`
   are `defer`red near the TOP of `run()`, so by `defer`'s LIFO order
   they run AFTER every consumer/relay goroutine (and
   `repromiseConsumer.Close()`, also deferred, closing both the Kafka
   reader and the new DLQ writer) has already stopped touching the
   pool, not before.

`Readiness`'s zero value (and a `nil *Readiness`) is always ready —
every existing test and any caller that predates this type behaves
exactly as before.

`charts/order-management/values.yaml`'s `readinessProbe` now points at
`/readyz` (was `/healthz`) — `startupProbe`/`livenessProbe` are
UNCHANGED, still `/healthz`, because liveness must never be flipped by
a graceful drain or Kubernetes would SIGKILL the pod mid-drain instead
of letting it finish. A new `terminationGracePeriodSeconds: 30` was
added (previously unset, a confirmed gap) — the HTTP `Shutdown` budget
(10s) plus the relay/consumer stop budget (the same 10s, reused) plus
margin for the `readinessProbe`'s `periodSeconds: 5` to have actually
observed the not-ready flip before traffic fully stops arriving.

## Consequences

- Every outbound call from `inventorystorage`/`productclassification`
  now derives its timeout from the caller's remaining budget rather
  than a fresh hardcoded one — a caller with a short deadline gets a
  short-lived outbound call, not one that outlives its own patience.
- A failing `inventory-storage` deployment now trips ONE breaker after
  5 consecutive failures (or a sustained >50% error rate with enough
  volume) and stops sending real traffic to it for `DefaultTimeout`
  (30s) before probing again — bounded load on a struggling dependency,
  instead of unbounded retry-forever pressure.
- `RepromiseConsumer` can no longer be permanently wedged by one poison
  message; every other message on the partition keeps flowing. The
  `.dlq` topic is a new operational surface: it needs monitoring/
  alerting (out of scope for this change — the ERROR-level log line is
  the interim signal) and a manual replay tool (also out of scope).
- `GET /readyz` is a new, distinct endpoint fleet operators/SRE tooling
  should point `readinessProbe`s at going forward, on any service
  adopting this pattern — `/healthz` alone is no longer sufficient for
  a pod that participates in a graceful drain.
- `productclassification.Client.GetClassification`'s OBSERVABLE
  behaviour (its port contract: always `Known=false, nil` on any
  problem) is unchanged; only its internal implementation split
  (`fetch` vs. the wrapper) changed, to make retry possible without
  breaking that contract.
- `sony/gobreaker/v2` and `cenkalti/backoff/v4` (promoted from indirect
  to direct; `v5` still present transitively but NOT adopted — v4 is
  what the plan specifies and what this service now uses directly) are
  new direct dependencies.

## Alternatives considered

- **One global circuit breaker for all outbound calls:** rejected per
  the plan's explicit instruction and sound isolation reasoning — a
  `product-classification` outage tripping the SAME breaker that guards
  `inventory-storage`'s reservation calls would incorrectly degrade
  order intake for a problem that is, at worst, a soft enrichment-input
  outage.
- **A brand-new fallback behaviour when a breaker opens (e.g. a cached
  last-known-good classification, or a synthetic "would probably
  succeed" reservation):** rejected — the plan is explicit that the
  breaker only decides WHEN to fall back, not WHAT the fallback is;
  inventing new fallback semantics here would diverge from the
  pre-existing, already-understood `permissive` mode contracts this
  service (and its operators) already reason about.
- **Retrying `inventorystorage`'s DELETE (revoke) blindly, since 404 is
  already treated as idempotent success:** considered and rejected for
  now — see §5's DELETE discussion. Revisit once inventory-storage's
  delete contract's retry-safety is independently confirmed.
- **Dropping a DLQ message instead of publishing it:** rejected — an
  alert-worthy signal with full replay context (raw payload + error)
  is strictly more operationally useful than a silent drop, at
  negligible extra cost (one more Kafka write on the already-rare
  poison-message path).
- **A three-state (`pending`/`retrying`/`dead`) status column for DLQ
  bookkeeping in Postgres, instead of a plain Kafka topic:** rejected
  as unnecessary complexity for v1 — the `.dlq` topic itself IS the
  durable, replayable record; a status table would duplicate
  information Kafka already retains without adding a capability this
  phase needs.

## References

- Plan: `~/.hermes/plans/2026-09-26_wms-production-readiness.md`,
  sections 2.2-2.5 (Phase 2: resilience).
- Phase 1 references this design builds on and does not re-derive:
  ADR-0022 (transactional outbox), ADR-0023 (idempotency-key
  middleware), ADR-0024 (optimistic concurrency / version column).
- ADR-0016 — the fail-loud-for-mutations vs. fail-open-for-soft-inputs
  rule this ADR's breaker-fallback design directly inherits.
- ADR-0018 — `RepromiseConsumer`'s original design (stable shared
  consumer group, at-least-once commit-as-you-go), extended here with
  the DLQ, not re-derived.
