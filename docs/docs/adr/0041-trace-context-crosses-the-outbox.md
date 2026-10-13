---
id: 0041-trace-context-crosses-the-outbox
slug: /adr/0041-trace-context-crosses-the-outbox
title: "0041. The W3C trace context crosses the outbox in outbox_events.headers (D27 reference)"
sidebar_label: "41. Trace context across the outbox"
sidebar_position: 41
description: "ADR 0041 — the fleet D27 reference: each outbox row stores the W3C traceparent/tracestate of the transaction that wrote it, the relay sends them as Kafka headers, and consumers that start a span extract them. In order-management the store is the existing outbox_events.headers column (migration 0007), not a new trace_headers column. Pinned by two testcontainers tests (Postgres + Kafka)."
---

# 0041. The W3C trace context crosses the outbox in `outbox_events.headers` (D27 reference)

## Status

Accepted — recorded by the change that introduces this record (fleet
gap-fix wave 2026-10-12, decision D27). This repo is the reference the
other contexts copy in wave 7.

Builds on [ADR 0022](./0022-transactional-outbox.md) (the outbox) and
[ADR 0009](./0009-standard-metrics-convention.md) (telemetry setup and the
W3C propagator).

## Context

Fleet decision D27 asks that a trace started by a request continue through
the transactional outbox: the outbox stores the W3C `traceparent` and
`tracestate` of the transaction that wrote the row (the addendum names an
additive nullable column `trace_headers jsonb`), the publisher fills it from
`ctx` through the global propagator, the relay sets those values as Kafka
message headers, rows without a value relay unchanged, and every consumer
that starts a span extracts the context from the headers.

Verifying that against this repo showed the mechanism already exists, under
a different column name:

- `migrations/0007_outbox.up.sql` created `outbox_events.headers JSONB NOT
  NULL DEFAULT '[]'`, documented as "the W3C trace-context headers captured
  at Encode time".
- Both encoders build the headers from `ctx`:
  `kafka.Publisher.Encode` (`internal/adapters/outbound/kafka/publisher.go`,
  `otel.GetTextMapPropagator().Inject` next to the CloudEvents
  `content-type` header) and `kafka.AnalyticsPublisher.Encode`
  (`analytics_publisher.go`).
- `postgres.OutboxPublisher.Publish` stores them in that column inside the
  use case's transaction (`outbox_publisher.go`, `marshalHeaders`), and
  `postgres.OutboxRelay` reads them back (`unmarshalHeaders`) and hands them
  to `kafka.RelaySink.Send`, which writes them as the Kafka message headers.
- Every inbound consumer that starts a span extracts first:
  re-promise, planned-capacity, product-classification and the analytics
  consumer (`otel.GetTextMapPropagator().Extract` in
  `internal/adapters/inbound/kafka`). The three replay caches start no span
  and need no extraction.

What was missing was proof: the only trace test covered the direct publish
path (`TestPublisher_InjectsTraceContextIntoHeaders`), not the outbox path
that every Postgres-backed deployment takes.

## Decision

1. **The trace store is `outbox_events.headers`.** No `trace_headers`
   column is added here. The column already holds exactly the headers the
   message will carry (`content-type`, `traceparent`, `tracestate` when
   present), and a second column would hold a second copy of the same
   `traceparent` with no rule for which one wins.
2. **Capture point.** The encoder injects from the `ctx` of the use case
   (the HTTP server span from otelchi, or the consumer span of the message
   that triggered the use case), inside the same transaction that writes the
   aggregate. The relay never injects: it has no span, and the stored
   context is the only correct parent.
3. **Rows without trace context relay unchanged.** A row written with no
   active span stores only `content-type`; a raw row with `[]` relays with no
   headers at all. Neither carries an all-zero `traceparent`.
4. **Consumers.** A consumer that starts a span calls
   `otel.GetTextMapPropagator().Extract` on the message headers first. A
   consumer that starts no span (the replay caches) is left alone.
5. **The reference tests** (testcontainers Postgres and Kafka, in
   `internal/adapters/outbound/postgres/outbox_trace_integration_test.go`):
   - `TestOutboxTrace_TraceparentCrossesTheOutboxToKafka`: a use case runs in
     a request span; the outbox row stores that span's `traceparent`; a relay
     pass on a context WITHOUT the span sends it to a real broker; extracting
     the consumed message's headers yields the request's trace id and span id
     as a remote parent.
   - `TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged`: a row with no
     stored headers reaches Kafka with the same key and value and no headers.

### How wave 7 copies this

For each repo with an outbox:

1. Check whether the outbox already stores the propagator output (a
   `headers` column filled by an encoder that calls `Inject` on the use
   case's `ctx`). If it does, add this ADR's two tests, adapted, and record
   the existing column the same way. If it does not, add the additive
   nullable `trace_headers jsonb` column the addendum names, fill it from
   `ctx` in the outbox publisher, merge it into the message headers in the
   relay, and leave rows with `NULL` unchanged.
2. Check each inbound consumer that starts a span for an `Extract` before
   `Start`.
3. Add the ADR (next free number), the index and sidebar entries, and the
   observability and testing page lines.

## Consequences

- A trace in Tempo/Jaeger runs from the HTTP request (or the consumed
  message) through the downstream consumer's span, even though the Kafka
  write happens later in the relay goroutine. There is still no publish
  span on the outbox path.
- The relay keeps a row's original parent even when it sends the row much
  later (after a broker outage or an `OUTBOX_MAX_ATTEMPTS` re-arm, ADR
  0040): the consumer span then starts long after its parent ended, which
  is expected for asynchronous messaging.
- No migration, no new configuration, no behaviour change in this repo: the
  change is the record and the two tests that keep the behaviour from
  regressing.

## Alternatives considered

- **Add `trace_headers jsonb` anyway, as the addendum words it.** Rejected
  for this repo: it would duplicate the `traceparent` already in `headers`
  and force a precedence rule. The addendum's goal (continuity, nullable
  safety, unchanged legacy rows) is met by the existing column, and the
  tests prove it.
- **Start a publish span in the relay and inject it.** Rejected: the relay
  span would become the consumer's parent and cut the link to the request
  that caused the event.
