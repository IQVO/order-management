---
title: Sequence Diagrams
sidebar_label: Sequence Diagrams
sidebar_position: 9
description: "UML sequence diagrams for every command and query use case of order-management (REST, MCP and Kafka entry points), traced from the use-case function bodies on develop."
---

# Sequence Diagrams

One diagram per entry point, traced from the code on `develop`. Participant
names match the code: the inbound adapter, the application use case, the
`Order` aggregate, the repository (`ports.OrderRepo`), the outbox
(`ports.EventPublisher` = `postgres.OutboxPublisher` when
`DATABASE_URL` and `EVENT_PUBLISHER=kafka` are set) and the downstream
system. All diagrams assume the Postgres + Kafka configuration; with no
`DATABASE_URL` there is no idempotency middleware, no outbox and no
transaction, and with `EVENT_PUBLISHER=log` events are only logged.

The [use cases page](./use-cases.md) has the same flows in prose; the
[domain message flow](./domain-message-flow.md) shows them across contexts.

## 1. ReceiveOrder — `POST /orders`

```mermaid
sequenceDiagram
  autonumber
  actor Client
  participant MW as RequireIdempotencyKey
  participant H as http.Server
  participant UC as ReceiveOrder
  participant PCL as ProductClassificationLookup
  participant CAT as ProcessPathCatalogue
  participant O as Order
  participant R as OrderRepo
  participant OB as Outbox
  participant DB as Postgres

  Client->>MW: POST /orders with Idempotency-Key
  alt header missing
    MW-->>Client: 400 idempotency-key-required
  end
  MW->>DB: BEGIN then INSERT idempotency_keys ON CONFLICT DO NOTHING
  alt key already stored
    MW->>DB: ROLLBACK then SELECT stored response
    alt request hash differs
      MW-->>Client: 422 idempotency-key-reused
    else same request
      MW-->>Client: replay stored status, headers and body
    end
  end
  MW->>H: next handler with tx in context
  H->>UC: ExecuteWithDeadline(lines, allowPartialShipment, releaseOnAllocation, requiredShipBy)
  UC->>UC: order.ValidateIntakeIntent
  alt held and partial shipment
    UC-->>H: ErrHeldOrderMustBeShipComplete
    H-->>Client: 422 held-order-must-be-ship-complete
  end
  loop every requested line
    UC->>PCL: GetClassification(sku), fail-open
    UC->>UC: PathSelectionPolicy.Select(sku, qty, giftWrap, tags, catalogue)
    alt no eligible active path
      UC-->>H: ErrLineIneligibleForResolvedPath
      H-->>Client: 422 line-ineligible-for-resolved-path
    end
    UC->>O: NewOrderLine(lineNo, sku, qty, pathId, giftWrap)
    UC->>CAT: IsActive(pathId)
    alt path not active
      UC-->>H: ErrUnknownProcessPath
      H-->>Client: 400 unknown-process-path
    end
  end
  UC->>R: NextID
  UC->>O: order.New, then Hold and SetRequiredShipBy when asked
  UC->>DB: UnitOfWork.Execute joins the outer tx
  UC->>R: Save(order) version-guarded insert
  UC->>OB: Publish(OrderReceived) as analytics outbox row
  UC->>UC: allocateAndRelease, see diagram 2
  alt allocation pass failed
    UC->>R: FindByID to return what was saved
  end
  UC-->>H: order
  H->>H: orderResponse adds capacityConstraint when enabled
  H-->>MW: 201 Created with Location
  MW->>DB: UPDATE idempotency_keys SET response then COMMIT
  MW-->>Client: 201 Created
```

Source: `internal/adapters/inbound/http/idempotency.go`, `server.go`,
`internal/application/usecases/receive_order.go`. Omits: the
`OrderMetrics` accepted/rejected counter, JSON decoding errors
(`400 malformed-request-body`), `400 empty-sku` / `422
non-positive-quantity` from `NewOrderLine`, and the
`OrderReceived` outbox row on the integration topic (none — only the
analytics encoder accepts it). A hard failure in step 2 never fails the
request: the order was received and the response shows whatever was saved.

## 2. The allocation pass — `allocateAndRelease`

Shared by ReceiveOrder, RetryAllocation and ReleaseHeldOrder, inside one
`UnitOfWork` transaction.

```mermaid
sequenceDiagram
  autonumber
  participant UC as allocateAndRelease
  participant INV as inventory-storage
  participant O as Order
  participant PP as PromisePolicy
  participant R as OrderRepo
  participant OB as Outbox

  loop every line to allocate
    UC->>INV: POST /reservations, Idempotency-Key res-orderId-line-n-att-version
    alt 201 or 200
      UC->>O: Allocate or RetryAllocate(lineNo, reservationId)
      UC->>OB: Publish(OrderLineAllocated)
    else 409 insufficient stock
      UC->>O: MarkBackordered(lineNo)
      UC->>OB: Publish(OrderLineBackordered)
    else transport error, 5xx, open breaker
      alt some lines already allocated
        UC->>PP: PromiseGroups or FeasibleBy
        UC->>R: Save(order)
        UC->>OB: Publish(OrderAllocationPartiallyFailed)
      end
      Note over UC: pass aborts and the error is returned
    end
  end
  alt requiredShipBy set
    UC->>PP: FeasibleBy(now, order, deadline), no lead-time fallback
  else
    UC->>PP: PromiseGroups(now, order)
  end
  UC->>O: SetPromiseGroups(groups)
  alt held at intake
    UC->>R: Save(order)
    UC->>OB: Publish(OrderAllocated or OrderPartiallyAllocated) without lines
  else release on allocation
    loop every line allocated in an earlier pass
      UC->>INV: POST /reservations again to reconfirm
      alt reserved
        UC->>O: ReconfirmReservation(lineNo, id)
      else 409
        UC->>O: LoseReservation(lineNo)
        UC->>OB: Publish(OrderLineBackordered)
      end
    end
    alt a line was lost and the order is ship-complete
      UC->>R: Save(order)
      Note over UC: release blocked, outcome published by status
    else
      UC->>O: EnsureReleasable
      opt BR3 satisfied
        UC->>O: Release(lineNo) for every Allocated line
      end
      UC->>R: Save(order) with version guard
      UC->>OB: Publish(OrderAllocated or OrderPartiallyAllocated) with released lines
    end
  end
  Note over OB: integration and analytics rows, drained by the relay, diagram 9
```

Source: `internal/application/usecases/allocation.go`
(`allocateLines`, `salvageAllocationFailure`, `reconfirmBeforeRelease`,
`releaseAllocatedLines`, `publishOrderAllocationOutcome`),
`internal/adapters/outbound/inventorystorage/client.go`. Omits: the circuit
breaker wrapping the inventory client (ADR 0025), and the rule that the
outcome event is skipped when the order ends `Backordered` or the pass
changed nothing.

## 3. RetryAllocation — `POST /orders/{id}/retry-allocation`

```mermaid
sequenceDiagram
  autonumber
  actor Operator
  participant H as http.Server
  participant UC as RetryAllocation
  participant R as OrderRepo
  participant O as Order
  participant AP as allocateAndRelease

  Operator->>H: POST /orders/{id}/retry-allocation
  H->>UC: Execute(id)
  UC->>R: FindByID(id)
  alt not found
    H-->>Operator: 404 order-not-found
  end
  UC->>O: LinesWithStatus(Backordered)
  alt none backordered
    H-->>Operator: 409 no-backordered-lines
  end
  UC->>AP: backordered lines, retry=true, releaseOnAllocation of the order
  alt inventory-storage failed
    AP-->>UC: ErrDownstreamUnavailable
    H-->>Operator: 503 downstream-unavailable
  else concurrent writer won
    AP-->>UC: ErrConcurrentModification
    H-->>Operator: 409 concurrent-modification
  end
  UC-->>H: order
  H-->>Operator: 200 OK with order
```

Source: `internal/application/usecases/retry_allocation.go`,
`internal/adapters/inbound/http/errors.go`. Omits: the permissive inventory
client (`503 downstream-not-configured`). Unlike ReceiveOrder, a hard
failure here is returned to the caller.

## 4. ReleaseHeldOrder — `POST /orders/{id}/release`

```mermaid
sequenceDiagram
  autonumber
  actor NF as network-fulfillment
  participant H as http.Server
  participant UC as ReleaseHeldOrder
  participant R as OrderRepo
  participant O as Order
  participant AP as allocateAndRelease

  NF->>H: POST /orders/{id}/release
  H->>UC: Execute(id)
  UC->>R: FindByID(id)
  alt not found
    H-->>NF: 404 order-not-found
  end
  UC->>O: ReleaseOnAllocation
  alt order was never held
    H-->>NF: 409 order-not-held
  end
  UC->>O: LinesWithStatus(Allocated)
  alt nothing allocated, e.g. already released
    UC-->>H: order unchanged
    H-->>NF: 200 OK
  end
  UC->>AP: no lines to allocate, releaseOnAllocation=true
  Note over AP: reconfirm, EnsureReleasable, Release, Save, publish
  alt inventory-storage failed during reconfirm
    H-->>NF: 503 downstream-unavailable
  end
  UC-->>H: order
  H-->>NF: 200 OK with order
```

Source: `internal/application/usecases/release_held_order.go`,
`allocation.go`. Omits: the BR3 branch — a held ship-complete order that
lost a reservation releases nothing and still answers `200`.

## 5. CancelOrder — `DELETE /orders/{id}`

```mermaid
sequenceDiagram
  autonumber
  actor Caller
  participant H as http.Server
  participant UC as CancelOrder
  participant R as OrderRepo
  participant O as Order
  participant INV as inventory-storage
  participant OB as Outbox

  Caller->>H: DELETE /orders/{id}
  H->>UC: Execute(id)
  UC->>R: FindByID(id)
  alt not found
    H-->>Caller: 404 order-not-found
  end
  UC->>O: EnsureCancellable
  alt any line Released, BR6
    H-->>Caller: 409 order-already-released
  end
  loop every Allocated line reservation
    UC->>INV: DELETE /reservations/{reservationId}
    alt unexpected status or transport error
      H-->>Caller: 503 downstream-unavailable
    end
  end
  UC->>O: Cancel
  UC->>R: Save(order) in UnitOfWork, version-guarded
  UC->>OB: Publish(OrderCancelled)
  alt version conflict
    H-->>Caller: 409 concurrent-modification
  end
  H-->>Caller: 204 No Content
```

Source: `internal/application/usecases/cancel_order.go`,
`internal/adapters/outbound/inventorystorage/client.go` (`204`, `200` and
`404` count as revoked). Omits: a revoke that fails part-way leaves the
earlier revocations in place while the order stays uncancelled — the
caller retries.

## 6. GetOrder — `GET /orders/{id}` and MCP `get_order`

```mermaid
sequenceDiagram
  autonumber
  actor Client
  actor Agent as MCP host
  participant H as http.Server
  participant M as mcp server
  participant UC as GetOrder
  participant R as OrderRepo
  participant CC as OrderCapacityConstraints
  participant PCR as PlannedCapacityRepo

  Client->>H: GET /orders/{id}
  H->>UC: Execute(id)
  UC->>R: FindByID(id)
  alt not found
    H-->>Client: 404 order-not-found
  end
  UC-->>H: order
  opt planned capacity enabled
    H->>CC: For(order)
    CC->>PCR: ListByLocation(site, now)
    CC-->>H: windows overlapping now to promise cutoff
  end
  H-->>Client: 200 OK, status derived, capacityConstraint when constrained
  Agent->>M: tools/call get_order with orderId
  M->>UC: Execute(orderId)
  UC->>R: FindByID(orderId)
  M-->>Agent: structured order or tool error
```

Source: `internal/application/usecases/get_order.go`,
`planned_capacity.go` (`OrderCapacityConstraints`),
`internal/adapters/inbound/http/server.go` (`orderResponse`),
`internal/adapters/inbound/mcp/tools.go`. Omits: `get_promise_health`,
which reads the analytics report store through the MCP adapter's own
`PromiseHealthStore` port, and the reports binary's
`GET /reports/funnel`. A failed capacity lookup only drops the annotation.

## 7. RepromiseOrder — Kafka `warehouse.fulfillment.events`

```mermaid
sequenceDiagram
  autonumber
  participant FE as fulfillment-execution
  participant K as RepromiseConsumer
  participant UC as RepromiseOrder
  participant PE as RepromiseProcessedEvents
  participant R as OrderRepo
  participant PP as PromisePolicy
  participant O as Order
  participant OB as Outbox

  FE-)K: TaskCPTMissed or PackageManifested CloudEvent
  K->>K: cloudevents.Decode
  alt not a CloudEvent
    K->>K: publish raw message to warehouse.fulfillment.events.dlq then commit
  end
  K->>K: ParseWorkUnitID(order_ref)
  alt not orderId-line-n
    K->>K: log, commit, skip
  end
  K->>UC: Execute(sourceEventId, orderId, lineNo, reason), up to 3 attempts
  UC->>PE: MarkProcessed(ce id) in UnitOfWork
  alt already processed
    UC-->>K: nil, skip
  end
  UC->>R: FindByID(orderId)
  UC->>PP: PromiseGroups, or FeasibleBy when requiredShipBy is set
  alt promise of the line's group unchanged
    UC-->>K: nil
  else promise moved
    UC->>O: SetPromiseGroups(fresh)
    UC->>R: Save(order) version-guarded
    UC->>OB: Publish(OrderRepromised)
  end
  alt still failing after 3 attempts
    K->>K: publish to warehouse.fulfillment.events.dlq
  end
  K->>K: CommitMessages
```

Source: `internal/adapters/inbound/kafka/repromise_consumer.go`,
`internal/application/usecases/repromise_order.go`. Omits: OpenTelemetry
spans, the DLQ topic-create retry (ADR 0033), and the skip branches for an
unknown order or a line outside every promise group (logged, no event).

## 8. ApplyPlannedCapacity — Kafka `warehouse.warehouse-planning.events`

```mermaid
sequenceDiagram
  autonumber
  participant WPL as warehouse-planning
  participant K as PlannedCapacityConsumer
  participant UC as ApplyPlannedCapacity
  participant PE as PlannedCapacityProcessedEvents
  participant PCR as PlannedCapacityRepo

  WPL-)K: CapacityPlanCreated, Published or ShortageDetected CloudEvent
  K->>K: cloudevents.Decode
  alt not a CloudEvent
    K->>K: dead-letter to warehouse.warehouse-planning.events.dlq
  end
  K->>K: planningRequestFrom, DRAFT for Created, PUBLISHED otherwise
  alt other type, e.g. BottleneckDetected
    K->>K: commit and ignore
  else invalid payload or subject differs from plan_id
    K->>K: dead-letter
  end
  K->>UC: Execute(eventId, window), bounded retry
  UC->>UC: window.Validate
  UC->>PE: MarkProcessed(ce id) in UnitOfWork
  alt duplicate
    UC-->>K: nil
  end
  UC->>PCR: Upsert(window), last writer wins by event time
  K->>K: CommitMessages
```

Source: `internal/adapters/inbound/kafka/planned_capacity_consumer.go`,
`internal/application/usecases/planned_capacity.go`,
`internal/adapters/outbound/postgres/planned_capacity_repo.go`. Omits:
`PlannedCapacityWindow.Supersedes`, which also refuses a DRAFT that would
overwrite a PUBLISHED plan.

## 9. Outbox relay — Postgres to Kafka

```mermaid
sequenceDiagram
  autonumber
  participant RL as OutboxRelay
  participant DB as Postgres
  participant RS as RelaySink
  participant K as Kafka
  participant WP as wes-work-planning
  participant PJ as order-projector

  loop every OUTBOX_RELAY_INTERVAL, default 1s
    RL->>DB: BEGIN, SELECT unpublished outbox_events FOR UPDATE SKIP LOCKED
    loop each claimed row
      RL->>RS: Send(topic, key=OrderId, CloudEvents value, headers)
      RS->>K: WriteMessages, hash balancer, RequireAll
      alt send failed
        RL->>DB: UPDATE attempts and last_error, COMMIT, retry next tick
      else sent
        RL->>DB: UPDATE published_at
      end
    end
    RL->>DB: COMMIT
  end
  K-)WP: warehouse.order-management.events, OrderAllocated and OrderPartiallyAllocated
  K-)PJ: warehouse.order-management.analytics, every raised event
```

Source: `internal/adapters/outbound/postgres/outbox_relay.go`,
`outbox_publisher.go`, `internal/adapters/outbound/kafka/relay_sink.go`,
`writer_config.go`. Omits: the housekeeping sweeper that later deletes
published rows (ADR 0032) and the `order.outbox.lag_seconds` gauge.
`OrderRepromised` is also written to the integration topic; no consumer of
it is known from this repository.
