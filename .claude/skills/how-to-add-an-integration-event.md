# How to add an integration event (publish and consume)

Use when asked to publish a new cross-context integration event, or
consume one from a sibling bounded context. This fleet's Kafka is ONE
broker platform-wide — every design decision below exists because that
shared-broker reality has already caused a real incident once
(wes-work-planning#67).

## Publishing a new integration event

### 1. Is it actually cross-service?

Not every domain event this service raises belongs on the wire. Check
`internal/adapters/outbound/kafka/publisher.go`'s doc comment — this repo
forwards only `OrderAllocated`/`OrderPartiallyAllocated`; every other
domain event (`OrderReceived`, `OrderLineBackordered`, `OrderCancelled`,
etc.) is a local/analytics concern, published only on the separate
`warehouse.order-management.analytics` topic for `cmd/order-projector`,
not broadcast as an integration fact. Before adding a new event to the
integration publisher, confirm a sibling context genuinely needs to react
to it — check `apis/asyncapi.yaml`'s intro section (it documents exactly
who is downstream: wes-work-planning consumes the integration topic) or
the equivalent ubiquitous-language doc for the other services.

### 2. Envelope: CloudEvents 1.0 (mandatory, ADR-0030)

Every message on `warehouse.order-management.events` (integration) and
`warehouse.order-management.analytics` (analytics) is a CloudEvents 1.0
event in structured content mode, built ONLY through
`internal/adapters/kafka/cloudevents.New` (never a hand-rolled struct),
with the `cloudevents.ContentTypeHeader()` Kafka header:

```json
{
  "specversion": "1.0",
  "id": "<uuid, minted once at encode time>",
  "source": "/warehouse/order-management",
  "type": "com.warehouse.wes.order-management.order.OrderAllocated",
  "subject": "<order id>",
  "time": "<occurred-at, RFC3339 UTC>",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:order-management:events:OrderAllocated:v1",
  "data": { /* the actual payload, business types only */ }
}
```

Analytics events use the SAME `type` with
`dataschema: urn:warehouse:order-management:analytics:<EventName>:v1`.
Both topics are keyed by `OrderId`. A breaking `data` change needs a new
`.v2` type and a new `dataschema` version — never mutate an existing one.
Add a golden exact-JSON test for the new type (see
`internal/adapters/outbound/kafka/cloudevents_golden_test.go`).

### 3. Implementation

Add the event struct to `internal/domain/order/` or `internal/domain/shared/`
(it should already exist as a domain event the aggregate raises —
publishing wires an EXISTING domain event onto Kafka, it doesn't invent a
new payload shape at the adapter layer; see `internal/domain/shared/events.go`).
In `internal/adapters/outbound/kafka/publisher.go`:

- Add the event's case to `encodeEvent` (it becomes a CloudEvent via `cloudevents.New`)
- The `data.lines[]` entry shape (`releasedLineData`) is FROZEN and
  shared verbatim with wes-work-planning's consumer — any change there
  needs both sides coordinated, not just this repo
- Use `Topic` (`warehouse.order-management.events`), never a sibling's
  topic constant

### 4. Contract + docs

- Add the message to `apis/asyncapi.yaml` under this service's channel,
  matching the entity-grouping convention already there
- This repo's `api-lint` CI job (Spectral) lints both `apis/openapi.yaml`
  and `apis/asyncapi.yaml` — run it locally if you have Spectral
  installed, or at minimum validate the YAML is well-formed before
  pushing
- `docs-api-drift` (CI) regenerates the REST reference from
  `apis/openapi.yaml`, not the async contract — this repo does not
  currently auto-generate an AsyncAPI HTML site the way some sibling
  repos do; check `docs/package.json`'s `scripts` before assuming a
  `gen-async-docs` command exists here

### 5. Test

Unit test the marshal shape against a fake `Writer` (see
`internal/adapters/outbound/kafka/publisher_test.go` — never a real
broker in a unit test). If a NEW consumer now needs a
`_integration_test.go` asserting real delivery, it MUST use
testcontainers (see the fitness test `TestKafkaIntegrationTestsUseTestcontainers`
in `internal/architecture/fitness_test.go` — a skip-gated `KAFKA_BROKERS`
test or a hardcoded `localhost:9092` fails CI).

## Consuming an integration event from a sibling context

### 1. Never import the sibling's Go packages

This service knows a sibling's topic name and payload shape ONLY — never
its Go types. See `internal/adapters/outbound/kafkacatalog/consumer.go`'s
own doc comment: this service has no business knowing anything else
about process-path-management beyond its topic name and the
envelope/payload shapes it hand-mirrors locally (`pathData`,
`eligibilityData` structs) — it never adds a Go module dependency on
that repo. `internal/architecture/architecture_test.go`'s hexagonal
dependency-rule fitness test would catch an accidental sibling-package
import at the adapter layer anyway.

### 2. Choose the right consumer-group pattern — this is the part that bites

Two DIFFERENT correct patterns exist. Picking the wrong one for your use
case is THE most common integration-event mistake in this fleet, and it
was learned from a real incident (wes-work-planning#67).

**Pattern A — long-lived, single-instance consumer group (a named
constant).** Use when exactly ONE instance of this consumer ever runs at
a time (e.g. this service's own `cmd/order-projector` analytics writer,
which is deliberately the sole consumer/writer of the analytics topic per
ADR-0006). The group id is a plain named constant, reused across
restarts — that's correct because Kafka's committed-offset resume
semantics are EXACTLY what you want: pick up where the single instance
left off.

**Pattern B — per-process-unique consumer group (a generated id).** Use
when this consumer rebuilds a complete read model from a topic's FULL
history on every start (an event-sourced local cache, not a work queue).
This repo has THREE separate, independent examples of this pattern, all
consuming from sibling-context topics:

- `internal/adapters/outbound/kafkacatalog/consumer.go` — process-path
  catalogue cache, consuming `warehouse.process-path-management.events`,
  filtering for `ProcessPathCreated`/`Updated`/`Deactivated`.
- `internal/adapters/outbound/kafkacptschedule/consumer.go` — CPT
  schedule cache, consuming the SAME topic (`warehouse.process-path-management.events`)
  as `kafkacatalog` but filtering for `CPTScheduleChanged` — two
  independent consumers on one topic, each ignoring event types outside
  its own contract, per `kafkacatalog`'s own doc comment.
- `internal/adapters/outbound/kafkapathcapacity/consumer.go` — path
  capacity cache, consuming `warehouse.work-planning.events` (ADR-0015),
  filtering for `PathCapacityChanged`.

Every one of the three uses the identical `uniqueConsumerGroup()` helper
(hostname + PID + nanosecond timestamp) — see `kafkacatalog/consumer.go`'s
`uniqueConsumerGroup()` for the canonical implementation; a new fourth
consumer of this shape should copy that function, not reinvent it. The
group id MUST be unique per process instance, NEVER a fixed shared
string. Consumer group offsets are shared infrastructure state: a
brand-new process joining a group an EARLIER instance already consumed
resumes from that instance's committed offset, so the new process gets
marked "ready" with an empty local cache having replayed nothing — a
silent correctness bug, not a crash.

**Never do this** (the actual incident): a fixed shared consumer group id
on a consumer meant to run as exactly one instance per environment. When
a local dev/test harness process joins the SAME broker's SAME group as a
live in-cluster Deployment, Kafka's rebalance protocol hands the
partition to only ONE of the two group members — the other silently
starves. Fix: make the group id env-configurable
(`KAFKA_CONSUMER_GROUP`/`<SERVICE>_CONSUMER_GROUP`) for Pattern A, or use
`uniqueConsumerGroup()` for Pattern B. This repo's own
`internal/architecture/fitness_test.go`'s
`TestKafkaConsumerGroupNeverHardcodedInline` fitness test enforces this
statically — an inline `GroupID: "literal"` fails CI.

### 3. Readiness gate, if this consumer backs a local cache

If the consumer replays a topic's full history to build a cache other
code depends on, expose a `Ready()`/`WaitReady(ctx)` gate the composition
root consults before serving traffic — see `kafkacatalog.Consumer.Ready`/
`WaitReady`, and `kafkacatalog.WaitReadyTimeout` (60s) for the bound
`cmd/order/main.go` applies before giving up on startup. The readiness
gate captures each partition's target ("last") offset BEFORE consuming
starts (`newTargetOffsets`) and flips ready only once every tracked
partition has been caught up to (`checkReady`) — this sidesteps the bug
class where a check that only re-evaluates on a NEW message arriving
deadlocks forever on an ordinary restart where a shared/already-caught-up
group never gets a new message to trigger it. All three consumers in
this repo share this exact design; copy it rather than re-deriving it.

## Verify before opening the PR

```bash
make check-all    # includes arch-test — will catch a sibling-package import
```

If this repo has testcontainers-based integration tests for the
consumer/publisher touched (e.g.
`internal/adapters/outbound/kafkacatalog/consumer_integration_test.go`,
or `kafkacatalog/two_consumers_integration_test.go` for the
"two independent consumers on one topic" scenario), run
`go test -tags=integration ./...` to prove the change against a real
broker, not just the unit-test fakes.
