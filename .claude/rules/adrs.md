---
paths:
  - "docs/docs/adr/**"
---
# Architecture Decision Records (0001-0020 summarized in order here; 0021-0033 summarized below; read the file in `docs/docs/adr/` before acting)

1. **0001 — Hexagonal (ports & adapters) architecture.** The dependency
   rule this whole repo enforces (`internal/architecture/` fitness test).
2. **0002 — HTTP consumer of inventory-storage and wes-work-planning, not
   shared code.** The original Customer/Supplier boundary decision — see
   `bounded-context-boundary.md`. Partially superseded by 0005 for the
   release leg only (allocation via inventory-storage is unchanged).
3. **0003 — Ship-complete by default and fail-closed allocation.** BR2/BR3
   in ADR form — see `domain-model.md`.
4. **0004 — The cancellation boundary is release.** BR6 in ADR form,
   including the documented "no clawback of released work" known gap.
5. **0005 — Choreographed release via Kafka, folded allocate-then-release,
   and pathId goes internal-only.** The big one: the `/allocate` and
   `/release` REST verbs were deleted, as were `ports.WorkReleaseClient` and
   the whole wes-work-planning HTTP outbound adapter; it replaces the synchronous
   wes-work-planning call with Kafka choreography, folds the whole saga into
   `ReceiveOrder`/`RetryAllocation`. **Read this before touching anything in
   the allocation/release path.**
6. **0006 — Analytical data product.** The Order Funnel & Allocation Health
   report, `cmd/order-projector`/`cmd/order-reports`, isolation enforced by
   `arch-test`.
7. **0007 — Adopt the fleet micro-frontend console (`order-mgmt-mfe`).**
   `web/`'s existence, CORS wiring, the `warehouse-console` BFF's
   `GET /console/orders/{id}/lifecycle` fan-out hop. NOTE: this repo has TWO
   things legitimately called "ADR-0002" — its own (item 2 above, about the
   inventory-storage/wes-work-planning HTTP boundary) and the fleet-wide MFE
   decision numbered independently in `warehouse-ops-agent`. Don't confuse
   them.
8. **0008 — FulfillmentClass, a demand-shape classifier, not a process-path
   name.** `Single`/`SameSKUMulti`/`MultiLineMulti`, derive-don't-store,
   additive on the frozen Kafka `lines[]` payload. See `domain-model.md`.
9. **0009 — Standard metrics convention across the fleet.** Closed a real
   gap: `order-management` had ZERO telemetry (no `telemetry.Setup`, no
   `otelchi` RED middleware) before this ADR. Now emits Go-runtime metrics,
   HTTP RED via `otelchi`, and one business counter `order.orders.received`.
10. **0010 — MCP as an inbound adapter, not a new service.** `cmd/mcp`,
    one read-only tool (`get_order`), no write tool — see
    `api-contracts.md`.
11. **0011 — Adopt the fleet REST identity (static bearer keys, read/
    read-write scopes).** **Superseded by 0012** — do not re-implement this
    without checking 0012 first.
12. **0012 — Remove the REST/MCP bearer auth layer.** Fleet-wide rollback:
    `internal/adapters/inbound/auth/` deleted entirely, every REST and MCP
    route reachable with no `Authorization` header. Re-adopting auth later
    means adopting the fleet's NEXT iteration of that decision, not
    resurrecting this deleted package verbatim.
13. **0013 — Process-path selection as a real domain policy, validated
    against a live catalogue.** `order.PathSelectionPolicy` (v1: one rule,
    `PICK`) plus `ports.ProcessPathCatalogue.IsActive` backed by a Kafka-fed
    `kafkacatalog` cache (`PATH_CATALOGUE_SOURCE=none|kafka`); unknown path
    -> synchronous 400 before anything persists.
14. **0014 — ACCEPTED: the promise is a CPT window derived from fulfillment
    capability.** Replaces `now + PROMISE_PATH_LEAD_TIMES` with a
    `PromisePolicy` over process-path-management's capability contract
    (their ADR 0010: `cycleTimeP95`, `eligibility`, site `CPTSchedule`)
    and wes-work-planning capacity; `LeadTimePolicy` stays as the tagged
    fallback (`basis=LeadTime`); per-shipment-group promising when
    `AllowPartialShipment`; `OrderRepromised` closes the loop. Implemented
    across ADRs 0015-0019 (`order.PromisePolicy` in `promise_policy.go` is
    the live policy in `cmd/order`) — read it before touching `promise.go`
    or `path_selection.go`.
15. **0015 — ACCEPTED: wes-work-planning's PathCapacityChanged wired as
    the real PathCapacity adapter.** Closes ADR-0014 step 3:
    `kafkapathcapacity.Consumer`, a third Kafka consumer (own
    per-process-unique group) on a NEW topic
    (`warehouse.work-planning.events`), replaces `UnknownPathCapacity`
    as the `PATH_CATALOGUE_SOURCE=kafka` default. `ports.PathCapacity`/
    `order.CapacitySource.Remaining` widened to accept `cutoffAt
    time.Time` alongside `cptId` — the caller (`PromisePolicy.
    linesFitWindow`) already has it in scope, so the cache is keyed on
    `(PathId, CutoffAt)` with an exact match, not `cptId` string
    matching. `UnknownPathCapacity` stays available for
    `PATH_CATALOGUE_SOURCE=none`/dev mode. Read it before touching
    `ports.PathCapacity`, `promise_policy.go`, or adding a fourth Kafka
    consumer.
16. **0016 — ACCEPTED: eligibility-driven process-path selection (ADR
    0014 step B, ROUTING ONLY).** `order.PathSelectionPolicy.Select`
    widened to `(sku, quantity, giftWrap, productAttributes,
    EligibilitySource) (PathId, bool)` and now actually evaluates
    `shared.DefaultPathId`'s declared `Eligibility` against the line
    (`MaxUnitsPerLine`, required/excluded product attributes, gift wrap
    folded in as the attribute string `"giftWrap"`). New
    `ports.ProductClassificationLookup` port +
    `internal/adapters/outbound/productclassification` (HTTP client +
    fail-open `PermissiveLookup`, `PRODUCT_CLASSIFICATION_MODE=http|
    permissive`) mirror wes-work-planning's own ADR-0009 pattern exactly
    — independently written, no cross-service Go import. An ineligible
    line is rejected with a new `shared.ErrLineIneligibleForResolvedPath`
    (422) before anything persists. Per-shipment-group promising — ADR
    0014's OTHER half of "step B" — is explicitly OUT OF SCOPE here and
    deferred to a future ADR/PR: this phase never touches `Order`'s
    promise fields, `PromisePolicy`'s single-`Promise`-per-order
    contract, or `SetPromise`. Honest v1 limitation, documented in the
    ADR: `ports.ProcessPathCatalogue` has no "list active paths" method,
    so this policy can only evaluate `shared.DefaultPathId` itself, not
    choose among multiple real paths — read the ADR before extending
    `path_selection.go` to a real multi-path decision.
17. **0017 — ACCEPTED: per-shipment-group promising (ADR 0014 step B,
    the second half).** `order.PromisePolicy` gains `PromiseGroups(now,
    o) ([]PromiseGroup, bool)`: for `AllowPartialShipment=false`, exactly
    one group covering every allocated line, computed via the UNCHANGED
    `Promise(now, o)` — byte-identical to pre-ADR-0017 behaviour. For
    `AllowPartialShipment=true`, each allocated line is evaluated
    independently and lines with an identical resulting `Promise`
    (same `Basis`/`CptId`/`CutoffAt`) are grouped together.
    `Order.SetPromiseGroups` stores the full `[]PromiseGroup` breakdown
    AND re-derives the legacy single-valued `promiseDate`/`promiseCptId`/
    `promiseBasis` as a "latest cutoff" projection (ADR 0014 §3's stated
    rule, extended here to cover CptId/Basis too — documented in the
    ADR). New additive Postgres table `order_promise_groups`
    (delete-then-reinsert on every save; `orders`' existing columns
    untouched); new additive `shared.ReleasedLine` pointer fields
    (`PromiseCptId`/`PromiseBasis`/`PromiseCutoffAt`) carrying per-line
    group attribution on the Kafka wire, `omitempty`. wes-work-planning
    needs zero changes (verified against its real consumer decode
    struct) — read the ADR before extending `promise_policy.go`,
    `order.go`'s promise fields, or the Postgres/Kafka promise wiring.
18. **0018 — ACCEPTED: RepromiseOrder consumer and OrderRepromised —
    closing ADR 0014's feedback loop.** The final piece of ADR 0014's
    entire rollout. New inbound Kafka consumer
    (`internal/adapters/inbound/kafka/repromise_consumer.go`) on
    fulfillment-execution's real `warehouse.fulfillment.events` topic
    (its own ADR 0025), reacting to `TaskCPTMissed`/`PackageManifested`
    only, under a STABLE shared consumer group
    (`order-management-repromise` — NOT per-process-unique, a different
    correctness shape than `kafkacatalog`/`kafkacptschedule`/
    `kafkapathcapacity`'s full-replay pattern). New
    `usecases.ParseWorkUnitID` reverses this repo's own frozen
    `WorkUnitID` formula to recover `(OrderId, LineNo)` from the wire's
    `order_ref`. New use case `RepromiseOrder` finds the line's current
    `PromiseGroup`, calls the EXISTING `PromisePolicy.PromiseGroups`
    again fresh, and — if the group's promise moved — saves the new
    breakdown and publishes the new `shared.OrderRepromised` event on
    `warehouse.order-management.events`. Idempotent on the Kafka
    message's id (CloudEvents `id` since ADR-0030) alone via a NEW OLTP-side port
    `ports.RepromiseProcessedEvents` (Postgres migration
    `0004_repromise_processed_events`) — a deliberate, documented
    simplification of ADR 0014 §5's stated `(orderId, sourceEventId)`
    key. No new promise-computation logic anywhere — read the ADR before
    touching `repromise_order.go`, `repromise_consumer.go`, or
    `ParseWorkUnitID`.
19. **0019 — ACCEPTED: promise KPIs on the Order Funnel data product,
    closing ADR 0014 §6 (order-management half).** Widens ADR 0006's
    report.Row with `PromiseBasisCapability`/`PromiseBasisLeadTime`
    (basis distribution), `OrdersRepromised` (NEW `repromise_rollup`
    table, hour-only grain — deliberately not path-dimensioned, since
    `OrderRepromised` carries no path), `OrdersSplitShipment`
    (`len(o.PromiseGroups()) > 1` at allocation time), and
    `PromiseToCutoffGapSeconds`/`PromiseToCutoffGapSamples` (sum+count
    mean, fulfillment-execution's `throughput_rollup` pattern).
    `ApplyOrderAllocated`/`ApplyOrderPartiallyAllocated` widened IN
    PLACE (new `basis`/`cutoffAt`/`splitShipment` params) rather than a
    second Apply* method, so the promise facts share the SAME eventId
    claim as the funnel counter. `AnalyticsPublisher.marshalData` gains
    the previously-missing `OrderRepromised` case plus
    `promise_basis`/`promise_cutoff_at`/`split_shipment` enrichment on
    `OrderAllocated`/`OrderPartiallyAllocated`. New MCP tool
    `get_promise_health` (`internal/adapters/inbound/mcp/
    promise_health.go`) — declares its OWN `PromiseHealthStore` port
    (never imports `internal/analytics/report` directly, per
    `TestMCPAdapterDependencyRule`/ADR-0008); `cmd/mcp` adapts the real
    `report.ReportStore` into it. On-time-to-CPT is explicitly OUT OF
    SCOPE here — fulfillment-execution's own companion analytics, a
    separate repo/PR. Read the ADR before touching `funnel.go`,
    `ports.go`, `postgres_projection.go`, the analytics Kafka
    consumer/publisher, or `promise_health.go`.
20. **0020 — ACCEPTED: network-originated demand — release-on-allocation,
    deadline feasibility, and the `Network` promise basis.** Companion to
    `network-fulfillment` ADR 0001 (the bounded context that speaks
    a major e-commerce retailer's Selling Partner API and owns the 24h acknowledgement clock);
    neither is meaningful alone. Three additive capabilities here: (a) an
    optional `releaseOnAllocation` intake flag, DEFAULT `true` so every
    existing caller is byte-identical — `false` stops the ADR-0005 folded
    saga after allocation (lines reach `Allocated`, `OrderAllocated` still
    publishes, nothing is released), plus a `ReleaseHeldOrder` use case
    reusing `allocation.go`'s release leg; (b)
    `PromisePolicy.FeasibleBy(now, o, deadline)`, the dual of `Promise` —
    same three ADR-0014 §2 conditions, returns the LATEST qualifying
    window at-or-before the deadline, and **never falls back to
    `LeadTimePolicy`** (cold cache/unknown cycle time ⇒ `false`, because
    "could not determine" and "can meet it" must not be the same answer);
    (c) `BasisNetwork`, a third `PromiseBasis` for a promise DICTATED by
    an external deadline, keeping ADR 0019's KPIs separable. Plus a 422
    invariant: `releaseOnAllocation=false` + `allowPartialShipment=true`
    is contradictory (ADR 0017 group promising gives several answers where
    a whole-order accept/reject can send one). Hold state is deliberately
    "allocated, not released" — NOT a new `Held` status — so ADR 0004's
    cancellation boundary stays intact and rejection cancels cleanly.
    a major e-commerce retailer's PO/ASIN/acknowledgement vocabulary and customer PII are
    explicitly OUT: they live in `network-fulfillment`, and `arch-go`
    cannot catch a vocabulary leak — that check is human. Known gap
    recorded: nothing here sweeps an orphaned hold, which sits on real
    inventory reservations. Shipped: `POST /orders/{id}/release`
    (`releaseHeldOrder`), `releaseOnAllocation`/`requiredShipBy` on
    `POST /orders`, `ErrOrderNotHeld` -> 409 `order-not-held`, migrations
    `0005_release_on_allocation`/`0006_required_ship_by`. The caller is
    `network-fulfillment` (its own order-management outbound adapter, in
    that repo: `POST /orders`, `POST /orders/{id}/release`, `DELETE /orders/{id}`).

**0026 — ACCEPTED: per-workload HorizontalPodAutoscaler and pgxpool
MaxConns/statement_timeout tuning (Phase 3 scalability).** An
`autoscaling/v2` HPA per independently-assessed Deployment: `api`
(min 1/max 4/70% CPU — stateless, `RepromiseConsumer`'s stable
shared Kafka group is scale-safe), `analytics-projector` (min
1/max **2**, not 4 — `kafka.AnalyticsConsumerGroup` is also a
stable shared group so N replicas share partitions safely, but
capped low since OrderId-keyed partitioning + idempotent upserts
don't need wide fan-out), `analytics-reports` (min 1/max 3 —
stateless reader, same treatment as `api`), `frontend` (min 1/max
3 — pure static assets). `mcp` gets **NO** `autoscaling.mcp` block
at all — deliberately excluded, not disabled: the MCP Go SDK's
`StreamableHTTPHandler` keeps in-memory session state keyed by
`Mcp-Session-Id` with no sticky routing in `mcp-service.yaml`, so
>1 replica can misroute a mid-session request. Every block defaults
`enabled: false`; each Deployment template guards its `replicas:`
field with `{{- if not .Values.autoscaling.<x>.enabled }}` so a
hardcoded replica count never fights an active HPA (verified via
`helm template` with every combination). Also sets explicit
`pgxpool.Config.MaxConns` (OLTP pool: 10, shared by `cmd/order`/
`cmd/mcp`; analytics writer: 5; analytics reader: 5) and
`statement_timeout` (OLTP: 5s, analytics writer: 10s, analytics
reader: 15s) via `AfterConnect`, sized against the shared Postgres
instance's REAL `max_connections=100` (confirmed live, an
unmodified Bitnami default — verified this service shares ONE
Postgres instance with up to 9 siblings, not a dedicated one).
Read the ADR before touching `charts/order-management/values.yaml`'s
`autoscaling:` block, any `*-deployment.yaml`/`hpa.yaml` template,
or `postgres/pool.go`/`analyticsstore/pool.go`.

**0027 — ACCEPTED: partition key (OrderId) on the integration
publisher's Kafka messages, closing the Phase 3 partition-scaleup
ordering gap.** `internal/adapters/outbound/kafka/publisher.go`'s
`Publisher` (forwards `OrderAllocated`/`OrderPartiallyAllocated`/
`OrderRepromised` to `warehouse.order-management.events`) never set
`kafkago.Message.Key`, which was accidentally safe at 1 partition
(total order) but breaks per-order event ordering once
warehouse-infra's PR #42 scaled every business topic to 8 partitions.
Fix: `encodeEnvelope` now returns the event's `shared.OrderId.String()`
as the key (mirroring `AnalyticsPublisher.marshalData`'s existing
choice) for both the direct-publish and outbox-`Encode` paths. Also
found and fixed a second, non-obvious bug: every writer in this
package used `&kafkago.LeastBytes{}`, a balancer that ignores
`Message.Key` entirely for partition routing (it balances purely by
cumulative byte volume) — so `AnalyticsPublisher`'s own pre-existing
`Key` was ALSO not actually achieving partition affinity against a
real broker until this ADR switched every writer's `Balancer` to
`&kafkago.Hash{}` (FNV-1a over `Key`). Verified via a real-Kafka
Testcontainers integration test (8-partition topic, 3 events for one
order + 1 for another) that failed under `LeastBytes` even with `Key`
populated, and passed only after the `Hash` balancer switch — a fake-
writer unit test alone would not have caught this. Read the ADR before
touching `publisher.go`'s `encodeEnvelope`/`Encode`/`Publish`,
`analytics_publisher.go`'s `NewAnalyticsPublisher`, or `relay_sink.go`'s
`NewRelaySink` — all three writer constructors must keep matching
`Balancer` choices.

Other ADR-adjacent facts worth knowing without opening every file:

- **0028 — ACCEPTED: send `Idempotency-Key` on `POST /reservations`,
  restoring the inventory-storage contract PR #98 changed.**
  Production-blocking bug found in Phase 4 live-cluster validation:
  inventory-storage's PR #98 required a caller-supplied `Idempotency-Key`
  on `POST /reservations` (mirroring this repo's own ADR 0023 verbatim),
  but `Client.Reserve` was never updated to send one — every real
  allocation attempt failed `400 idempotency-key-required` with zero
  fault injection, leaving every line `Pending` and tripping the circuit
  breaker permanently open. Fix: `ports.ReservationRequest` gains
  `LineNo`/`Attempt` (the latter is `Order.Version()`, reused from ADR
  0024, not a new counter); `Client.Reserve` derives
  `res-<orderId>-line-<lineNo>-att-<version>` and sends it as
  `Idempotency-Key` — stable across a retry of the same attempt,
  different for a genuinely new one. `RevokeReservation` needs no
  header: inventory-storage's `DELETE /reservations/{id}` route is
  confirmed not idempotency-gated. Read the ADR before touching
  `Client.Reserve`, `ports.ReservationRequest`, or `allocateLines`'
  `inventory.Reserve` call site.

- **0021 — ACCEPTED: multi-path attribute-driven routing, closing ADR-0013's
  original deferral and ADR-0016 §4's named limitation.** `ports.ProcessPathCatalogue`
  gains `ListActive() []shared.ActivePathCandidate`; `kafkacatalog.Consumer`
  implements it directly from its existing in-memory cache (no new wire
  decoding). `order.PathSelectionPolicy.Select` now enumerates every
  currently active path, filters to the ones whose declared `Eligibility`
  admits the line, and picks the shortest KNOWN `CycleTimeP95` among them
  (ADR-0014 §4's original rule); ties break on the lower `PathId` for
  determinism. A line ADR-0016 would have rejected outright (ineligible
  for `shared.DefaultPathId`, no other candidate reachable) now routes to
  a genuinely different active path when one exists and admits it. Nil
  catalogue or an empty `ListActive()` still fails OPEN to
  `shared.DefaultPathId`, exactly ADR-0013's original floor.

- **0030 — ACCEPTED: CloudEvents 1.0 as the mandatory event envelope
  (fleet standard).** Every Kafka message produced or consumed (integration
  AND analytics) is a CloudEvents 1.0 event, structured mode, built/parsed
  only via `internal/adapters/kafka/cloudevents` (sdk-go `event` package).
  `source=/warehouse/order-management`,
  `type=com.warehouse.wes.order-management.order.<EventName>`,
  `subject=<order id>`, `dataschema=urn:warehouse:order-management:<events|analytics>:<EventName>:v1`.
  Consumers dispatch on the FULL type, dedupe on `id`, DLQ/skip
  non-CloudEvents. **Supersedes** the flat envelope of ADR-0005, the
  analytics Envelope v1 (`schema_version`) of ADR-0006, and the flat
  dispatch shown in ADR-0015/0018. No dual-read, no toggle.

- **0031 — ACCEPTED: consume warehouse-planning's capacity plans into a
  local planned-capacity read model; annotate, never reject or re-promise.**
  `inbound/kafka.PlannedCapacityConsumer` (STABLE group from
  `PLANNED_CAPACITY_CONSUMER_GROUP`, no default; the env var is also the
  off switch) on `warehouse.warehouse-planning.events` upserts
  `order.PlannedCapacityWindow` rows (Postgres `planned_capacity_windows`,
  migration 0010; last-writer-wins per plan id via `Supersedes`) in ONE
  `UnitOfWork` with the CloudEvents-id claim; poison -> `<topic>.dlq`. A
  PUBLISHED shortage whose window overlaps `[now, promise cutoff)` (both
  half-open) at `PLANNED_CAPACITY_SITE_ID` (default `DEFAULT_SITE_ID`) adds
  an optional `capacityConstraint` to the order response, derived at read
  time — promise, status, allocation, reservations and events are
  untouched. `GET /planned-capacity?site=` reads the model. KNOWN GAP:
  orders carry no site, so the configured one is used. Read the ADR before
  touching `planned_capacity.go`, the consumer or `Server.orderResponse`.

- **0022 — ACCEPTED: transactional outbox.** Use cases no longer write to
  Kafka directly: with `DATABASE_URL` + `EVENT_PUBLISHER=kafka`,
  `postgres.OutboxPublisher` inserts one `outbox_events` row per
  (event x topic) inside the same `UnitOfWork` transaction as the `Order`
  save, and `postgres.OutboxRelay` (`OUTBOX_RELAY_INTERVAL`, default 1s)
  drains them to both topics with `FOR UPDATE SKIP LOCKED`.
- **0023 — ACCEPTED: transactional `Idempotency-Key` middleware on
  `POST /orders`.** Route-scoped `RequireIdempotencyKey` opens the outer
  transaction, inserts into `idempotency_keys`, joins `ReceiveOrder`'s
  `UnitOfWork` via `internal/pgtx`, and replays the stored response for a
  repeated key (422 `idempotency-key-reused` on a different body, 400
  `idempotency-key-required` when absent). Only active with Postgres.
- **0024 — ACCEPTED: optimistic concurrency.** `orders.version` (migration
  0009); `OrderRepo.Save` is `UPDATE ... WHERE id AND version`, a lost
  race surfaces as `ports.ErrConcurrentModification` (409
  `concurrent-modification`).
- **0025 — ACCEPTED: resilience.** sony/gobreaker per outbound dependency
  (inventory-storage reservations, product classification), jittered
  retry on the read-only classification GET only, `<topic>.dlq` for the
  re-promise consumer (also used by the planned-capacity consumer), and a
  readiness-flip-first graceful shutdown (`/readyz`,
  `SHUTDOWN_DRAIN_DELAY`).
- **0029 — ACCEPTED: migrations bypass PgBouncer.**
  `MIGRATIONS_DATABASE_URL` (defaults to `DATABASE_URL`) is a direct
  connection used only for golang-migrate's advisory lock.
- **0032 — ACCEPTED: housekeeping sweeper.** `postgres.Sweeper` in
  `cmd/order` deletes `idempotency_keys` older than `IDEMPOTENCY_KEY_TTL`
  (24h) and PUBLISHED `outbox_events` older than `OUTBOX_RETENTION` (7d)
  every `HOUSEKEEPING_INTERVAL` (1h; `0` disables), plus the
  `order.outbox.lag_seconds` gauge.
- **0033 — ACCEPTED: boot retry and Kafka writer tuning.**
  `internal/bootretry` retries every composition root's first
  Postgres/Kafka dial; synchronous kafka-go writers use a 10ms
  `BatchTimeout` and `RequireAll`; DLQ publishes retry while the
  auto-created topic elects a leader.

- Gateway API `HTTPRoute` chart template exists (`charts/order-management`
  `values.yaml` `gatewayApi:` block, `enabled: false` by default) —
  it is not itself the subject of a numbered ADR in this list; check the
  chart's own comments if you need its exact behavior.
