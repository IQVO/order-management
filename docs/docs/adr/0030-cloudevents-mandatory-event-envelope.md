---
id: 0030-cloudevents-mandatory-event-envelope
slug: /adr/0030-cloudevents-mandatory-event-envelope
title: 30. CloudEvents 1.0 as the mandatory event envelope
sidebar_label: 30. CloudEvents 1.0 mandatory envelope
description: "ADR 0030 — every Kafka message order-management produces or consumes (integration AND analytics topics) is a CloudEvents 1.0 event in structured content mode, built and validated with the official sdk-go event package. The flat fleet envelope, the analytics Envelope v1 (schema_version), and the dual-read CloudEvents/flat discriminator code are removed. No coexistence, no toggle."
---

# 30. CloudEvents 1.0 as the mandatory event envelope

## Status

Accepted — fleet-wide standard, implemented in this service by the change
that introduces this record. Supersedes the envelope parts of
[ADR 0005](./0005-choreographed-release-via-kafka.md) (the flat
`event_id`/`event_type`/`occurred_at`/`source`/`data` integration envelope)
and [ADR 0006](./0006-analytical-data-product.md) (the analytics
"Envelope v1" with `schema_version`), and the CloudEvents/flat dual-read
code this service carried for fulfillment-execution's ADR-0027 and
wes-work-planning's ADR-0021 migrations.

## Context

The fleet had three envelope shapes in flight at once: a flat integration
envelope, an analytics variant of it with `schema_version`, and (in some
producers) CloudEvents 1.0 behind dual-write/dual-read toggles.
order-management decoded both flat and CloudEvents messages on its
fulfillment and path-capacity consumers via a `specversion` probe, still
dispatched on short event names (stripping the reverse-DNS `type` back to
`TaskCPTMissed`), and published only the flat shapes itself. Every consumer
had to carry two decoders and every producer had to decide which shape to
emit. The fleet decided to cut over in one coordinated release instead of
running a long coexistence period.

## Decision

1. **Scope.** EVERY message this service writes to or reads from Kafka —
   `warehouse.order-management.events`, `warehouse.order-management.analytics`,
   `warehouse.fulfillment.events`, `warehouse.process-path-management.events`,
   `warehouse.work-planning.events` — is a CloudEvents 1.0 event. No flat
   envelope, no dual-write, no dual-read, no envelope toggle.
2. **Encoding.** Kafka protocol binding, **structured content mode**: the
   message value is the JSON event format; every produced message carries
   the header `content-type: application/cloudevents+json; charset=UTF-8`.
   The Kafka key stays the aggregate id (`OrderId`) with the `kafkago.Hash`
   balancer ([ADR 0027](./0027-kafka-integration-publisher-partition-key.md)).
   W3C trace context stays in `traceparent`/`tracestate` headers.
3. **Library.** Events are built, validated and (un)marshalled with
   `github.com/cloudevents/sdk-go/v2/event` (v2.16.2) only — never a
   hand-rolled struct. Transport stays `segmentio/kafka-go`. The single home
   for the helpers is `internal/adapters/kafka/cloudevents`
   (`New`, `Decode`, `ContentTypeHeader`, `Type`, `DataSchema`).
4. **Attributes (all required).**

   | attribute | value in this service |
   |---|---|
   | `specversion` | `1.0` |
   | `id` | UUID v4 minted once at encode time; the outbox persists the encoded bytes, so a relay redelivery carries the same `id` |
   | `source` | `/warehouse/order-management` |
   | `type` | `com.warehouse.wes.order-management.order.<EventName>` |
   | `subject` | the order id (same as the Kafka key) |
   | `time` | the domain event's occurred-at, UTC |
   | `datacontenttype` | `application/json` |
   | `dataschema` | `urn:warehouse:order-management:<events\|analytics>:<EventName>:v1` |

   `data` is byte-for-byte the payload this service published before the
   migration. The analytics `schema_version` field is removed (replaced by
   `dataschema`). The same `type` names an occurrence on both topics;
   `dataschema` distinguishes the integration and analytics payloads.
5. **Versioning.** Additive payload changes keep `type` and `dataschema`. A
   breaking payload change requires a new `dataschema` version AND a new
   `.v2`-suffixed `type`, published as a new event.
6. **Consumers.** Decode with `cloudevents.Decode` (SDK unmarshal +
   `specversion == 1.0` + `Validate()`), dispatch on the FULL `type`
   string, ignore unknown types, read `time`/`subject` from attributes and
   the payload via `DataAs`, dedupe on `id`. A message that is not a valid
   CloudEvent (bad JSON, the retired flat envelope) is a deterministic
   poison message: the repromise consumer sends it unmodified to its
   existing `<topic>.dlq` and commits; the local-cache consumers and the
   analytics projector log it at WARN with topic/partition/offset and move
   on. Never crash, never block a partition, never parse a legacy shape.

### Types this service publishes

    com.warehouse.wes.order-management.order.OrderAllocated            (events + analytics)
    com.warehouse.wes.order-management.order.OrderPartiallyAllocated   (events + analytics)
    com.warehouse.wes.order-management.order.OrderRepromised           (events + analytics)
    com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged (events only, ADR 0035)
    com.warehouse.wes.order-management.order.OrderReceived             (analytics)
    com.warehouse.wes.order-management.order.OrderAllocationPartiallyFailed (analytics)
    com.warehouse.wes.order-management.order.OrderReleased             (analytics)
    com.warehouse.wes.order-management.order.OrderCancelled            (analytics)
    com.warehouse.wes.order-management.order.OrderLineAllocated        (analytics)
    com.warehouse.wes.order-management.order.OrderLineBackordered      (analytics)
    com.warehouse.wes.order-management.order.OrderLineReleased         (analytics)

`OrderAllocated` and `OrderPartiallyAllocated` are consumed by
wes-work-planning with these exact strings.
`SiteSkuDemandChanged` (ADR 0035) is the one type raised outside the
`order` entity: its `subject`/Kafka key is the line-scoped
`<order_id>/line/<line_no>`, not the bare order id, and it is emitted
only while `DEMAND_PROJECTION_SITE_ID` is configured.

### Types this service consumes (exact, fleet cross-service table)

    com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed                 (repromise consumer)
    com.warehouse.wes.fulfillment-execution.package.PackageManifested          (repromise consumer)
    com.warehouse.wes.process-path-management.processpath.ProcessPathCreated   (kafkacatalog)
    com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated   (kafkacatalog)
    com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated (kafkacatalog)
    com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged   (kafkacptschedule)
    com.warehouse.wes.work-planning.workpool.PathCapacityChanged               (kafkapathcapacity)
    com.warehouse.wms.product-master.product.ProductClassified                 (product-classification copy, ADR 0036)
    com.warehouse.wes.order-management.order.*  (the ten above, analytics projector)

## Consequences

- One decoder and one encoder per service; the `specversion` probe,
  `bareEventType` stripping, flat envelope structs and `AnalyticsEnvelope`
  are deleted.
- The `outbox_events.event_type` column now stores the full CloudEvents
  `type` (it is only used for inspection/logging). `OrderRepromised`'s
  `reason` payload field keeps its existing short values
  (`TaskCPTMissed`/`PackageManifested`) — `data` is unchanged.
- **Breaking wire change; no coexistence.** This service must deploy
  together with the rest of the fleet cutover. Before deploying, drain the
  outbox (rows were pre-encoded in the flat shape), delete and recreate the
  `warehouse.*.events`/`warehouse.*.analytics` topics and re-seed the
  process-path catalogue so no flat message remains for a FirstOffset
  replay — see warehouse-infra `docs/cloudevents-cutover.md`. A flat
  message that slips through is skipped/dead-lettered, never applied.
- Tests: a golden exact-JSON test per published type (all attributes, the
  type string and the content-type header), a legacy-flat-rejected test
  per consumer, and the testcontainers Kafka integration tests run on the
  CloudEvents wire format.

## Alternatives considered

- **Keep dual-read for a bake period.** Rejected fleet-wide: it doubles
  every consumer's decode surface and the cutover is coordinated anyway.
- **Binary content mode (attributes as `ce_*` headers).** Rejected:
  structured mode keeps the whole event in the value, which the outbox
  already persists verbatim, and keeps trace headers the only headers.
- **Hand-rolled CloudEvent structs.** Rejected: the SDK's `Validate()` is
  the conformance check every consumer relies on.
