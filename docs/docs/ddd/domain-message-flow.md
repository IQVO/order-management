---
title: Domain Message Flow
sidebar_label: Domain Message Flow
sidebar_position: 5
description: "ddd-crew Domain Message Flow Modelling: four business scenarios across contexts, every arrow a real REST route, MCP tool or Kafka CloudEvents type, prefixed cmd, evt or qry."
---

# Domain Message Flow

Follows
[ddd-crew Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling).
Participants are bounded contexts, actors and external systems; every
arrow is a real message, prefixed `cmd:` (command), `evt:` (event) or
`qry:` (query). Kafka arrows use the event name; the full CloudEvents type
is `com.warehouse.<tier>.<context>.<entity>.<EventName>` — `wms` for
product-master, `wes` for every other context on this page — see
[Domain Events](./domain-events.md).

## 1. Customer order to released work

```mermaid
sequenceDiagram
  autonumber
  actor Customer as Customer channel
  participant PM as product-master
  participant OM as order-management
  participant INV as inventory-storage
  participant WP as wes-work-planning
  participant PJ as order-projector

  PM-)OM: evt: ProductClassified (kept in a local copy, ADR 0036)
  Customer->>OM: cmd: POST /orders
  Note over OM: reads the SKU's tags from the local copy for path selection
  loop every line
    OM->>INV: cmd: POST /reservations with lineNo (ADR 0037)
    INV-->>OM: 201 reserved, or 409 insufficient stock
  end
  OM-->>Customer: 201 order with status and promiseDate
  OM-)WP: evt: OrderAllocated with released lines
  OM-)PJ: evt: OrderReceived, OrderLineAllocated, OrderAllocated
  Note over WP: rebuilds work unit id orderId-line-n
```

Source: `internal/application/usecases/receive_order.go`, `allocation.go`,
`internal/adapters/outbound/inventorystorage/client.go`,
`inbound/kafka/product_classification_consumer.go`,
`outbound/productclassificationcopy/`, `outbound/kafka/publisher.go`.
Omits: the backordered branch (`OrderLineBackordered`, then
`OrderPartiallyAllocated` for a partial-shipment order, nothing to
wes-work-planning for a ship-complete one), the
`OrderAllocationPartiallyFailed` branch, and the outbox hop.

## 2. Network order held, then committed or rejected

```mermaid
sequenceDiagram
  autonumber
  actor Net as External retail network
  participant NF as network-fulfillment
  participant OM as order-management
  participant INV as inventory-storage
  participant WP as wes-work-planning

  Net->>NF: purchase order
  NF->>OM: cmd: POST /orders held, ship-complete, requiredShipBy
  OM->>INV: cmd: POST /reservations per line, with lineNo (ADR 0037)
  OM-)WP: evt: OrderAllocated with no lines, held
  OM-->>NF: 201 order, promiseDate absent if the deadline cannot be met
  alt network commits
    NF->>OM: cmd: POST /orders/id/release
    OM->>INV: cmd: POST /reservations again to reconfirm
    OM-)WP: evt: OrderAllocated with released lines
    OM-->>NF: 200 order Released
  else network rejects
    NF->>OM: cmd: DELETE /orders/id
    OM->>INV: cmd: DELETE /reservations/id per line
    OM-->>NF: 204
  end
```

Source: ADR 0020, `internal/application/usecases/release_held_order.go`,
`cancel_order.go`, `allocation.go` (`publishOrderAllocationOutcome`).
Omits: network-fulfillment's own translation (PO, acknowledgement) and the
BR3 branch where a reconfirm loses a reservation and nothing is released.
The held-order `OrderAllocated` carries an empty `lines[]`, so
wes-work-planning has nothing to schedule until the release.

## 3. Missed CPT re-promises the order

```mermaid
sequenceDiagram
  autonumber
  participant FE as fulfillment-execution
  participant OM as order-management
  participant PPM as process-path-management
  participant WP as wes-work-planning
  participant PJ as order-projector

  PPM-)OM: evt: CPTScheduleChanged, ProcessPathUpdated
  WP-)OM: evt: PathCapacityChanged
  Note over OM: local read models feed PromisePolicy
  FE-)OM: evt: TaskCPTMissed, order_ref orderId-line-n
  OM->>OM: recompute that line's shipment-group promise
  alt promise moved
    OM-)PJ: evt: OrderRepromised
    Note over OM: also on warehouse.order-management.events, no known consumer
  else unchanged
    Note over OM: no event
  end
  FE-)OM: evt: PackageManifested
```

Source: `internal/adapters/inbound/kafka/repromise_consumer.go`,
`internal/application/usecases/repromise_order.go`,
`internal/adapters/outbound/{kafkacatalog,kafkacptschedule,kafkapathcapacity}`.
Omits: idempotency on the CloudEvents id and the DLQ after three failed
attempts.

## 4. Planned shortage annotates an order

```mermaid
sequenceDiagram
  autonumber
  participant WPL as warehouse-planning
  participant OM as order-management
  actor Console as Console or MCP host

  WPL-)OM: evt: CapacityPlanCreated
  WPL-)OM: evt: CapacityPlanPublished
  WPL-)OM: evt: CapacityShortageDetected
  Note over OM: planned_capacity_windows read model, opt-in
  Console->>OM: qry: GET /orders/id
  OM-->>Console: order with capacityConstraint when the promise overlaps a published shortage
  Console->>OM: qry: GET /planned-capacity?site=SIM1
  OM-->>Console: windows for the site
  Console->>OM: qry: MCP get_order
```

Source: `internal/adapters/inbound/kafka/planned_capacity_consumer.go`,
`internal/application/usecases/planned_capacity.go`,
`internal/domain/order/planned_capacity.go`,
`internal/adapters/inbound/http/server.go`. Omits:
`BottleneckDetected` (ignored), DRAFT vs PUBLISHED precedence, and the MCP
response (it does not carry the capacity annotation). Planned capacity
never moves a promise (ADR 0031).
