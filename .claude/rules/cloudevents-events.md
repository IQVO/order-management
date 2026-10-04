---
paths:
  - "internal/adapters/outbound/kafka/**"
  - "internal/adapters/outbound/kafkacatalog/**"
  - "internal/adapters/outbound/kafkacptschedule/**"
  - "internal/adapters/outbound/kafkapathcapacity/**"
  - "internal/adapters/inbound/kafka/**"
  - "internal/adapters/kafka/**"
  - "cmd/order-projector/**"
  - "apis/asyncapi*.yaml"
---
# Events: CloudEvents 1.0 is MANDATORY

Every Kafka message this service produces or consumes (integration
`warehouse.<ctx>.events` AND analytics `warehouse.<ctx>.analytics`) is a
CloudEvents 1.0 event in structured content mode. This is a hard fleet rule,
not a preference:

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  via `internal/adapters/kafka/cloudevents/` (the ONLY CloudEvents 1.0
  New/Decode helpers, ADR-0030); transport stays kafka-go.
- Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.
- Required attributes: `specversion=1.0`, `id` (UUID, stable across outbox
  redelivery), `source=/warehouse/order-management`, `type`, `subject` (aggregate id), `time`
  (occurred-at, UTC), `datacontenttype=application/json`,
  `dataschema=urn:warehouse:order-management:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
  for this service: `com.warehouse.wes.order-management.<entity>.<EventName>`. Breaking payload
  change => new `.v2` type + new dataschema version, never mutate.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
  `id`, and DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.

Full standard and the fleet's cross-service type catalogue: ADR-0030
(`docs/docs/adr/`).

For this service: every Order* event uses entity `order`
(`com.warehouse.wes.order-management.order.<EventName>`); `subject` and the
Kafka key are the order id. Consumed types (exact):
`com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed`,
`com.warehouse.wes.fulfillment-execution.package.PackageManifested`,
`com.warehouse.wes.process-path-management.processpath.ProcessPath{Created,Updated,Deactivated}`,
`com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged`,
`com.warehouse.wes.work-planning.workpool.PathCapacityChanged`, plus this
service's own analytics types (projector).
