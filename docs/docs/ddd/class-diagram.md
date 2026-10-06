---
title: UML Class Diagrams
sidebar_label: Class Diagrams
sidebar_position: 7
description: "UML class diagrams of internal/domain (Order aggregate, promise and routing policies, planned capacity, domain events) plus the hexagonal ports and adapters, drawn from the code on develop."
---

# UML Class Diagrams

Drawn from `internal/domain/**` and `internal/application/ports` on
`develop`. Field and method names are the real Go identifiers (lower-case
fields are unexported). Go pointers and slices are shown as plain types
(`Time`, `OrderLine[]`); only state-changing methods and the key queries
are listed. Stereotypes: `<<AggregateRoot>>`, `<<Entity>>`,
`<<ValueObject>>`, `<<Enumeration>>`, `<<DomainEvent>>`, `<<Repository>>`
(persistence ports) and `<<Port>>` (other outbound ports).

## 1. The Order aggregate

```mermaid
classDiagram
  direction LR
  class Order {
    <<AggregateRoot>>
    -id OrderId
    -lines OrderLine[]
    -allowPartialShipment bool
    -promiseDate Time
    -promiseCptId string
    -promiseBasis PromiseBasis
    -promiseGroups PromiseGroup[]
    -heldAtIntake bool
    -requiredShipBy Time
    -version int
    +Status() Status
    +FulfillmentClass() FulfillmentClass
    +ReleaseOnAllocation() bool
    +Hold()
    +SetRequiredShipBy(t Time)
    +Allocate(lineNo int, reservationID string) error
    +RetryAllocate(lineNo int, reservationID string) error
    +MarkBackordered(lineNo int) error
    +ReconfirmReservation(lineNo int, reservationID string) error
    +LoseReservation(lineNo int) error
    +EnsureReleasable() error
    +Release(lineNo int) error
    +EnsureCancellable() error
    +Cancel() error
    +SetPromise(p Promise)
    +SetPromiseGroups(groups PromiseGroup[])
    +SetVersion(v int)
  }
  class OrderLine {
    <<Entity>>
    -lineNo int
    -sku SKU
    -quantity int
    -pathID PathId
    -giftWrap bool
    -status LineStatus
    -reservationID string
  }
  class LineStatus {
    <<Enumeration>>
    Pending
    Allocated
    Backordered
    Released
    Cancelled
  }
  class Status {
    <<Enumeration>>
    Received
    Allocated
    PartiallyAllocated
    Backordered
    Released
    PartiallyReleased
    Cancelled
  }
  class FulfillmentClass {
    <<Enumeration>>
    SINGLE
    SAME_SKU_MULTI
    MULTI_LINE_MULTI
  }
  class PromiseGroup {
    <<ValueObject>>
    +LineNos int[]
    +Promise Promise
  }
  class Promise {
    <<ValueObject>>
    +CptId string
    +CutoffAt Time
    +Basis PromiseBasis
  }
  class PromiseBasis {
    <<Enumeration>>
    Capability
    LeadTime
    Network
  }
  class OrderId {
    <<ValueObject>>
  }
  class SKU {
    <<ValueObject>>
  }
  class PathId {
    <<ValueObject>>
    DefaultPathId = pick
  }

  Order "1" *-- "1..*" OrderLine : lines
  Order "1" *-- "0..*" PromiseGroup : promiseGroups
  PromiseGroup *-- Promise
  Promise --> PromiseBasis
  Order --> OrderId : id
  OrderLine --> SKU
  OrderLine --> PathId
  OrderLine --> LineStatus
  Order ..> Status : derives
  Order ..> FulfillmentClass : derives
```

Source: `internal/domain/order/order.go`, `order_line.go`, `status.go`,
`fulfillment_class.go`, `promise_basis.go`, `promise_group.go`,
`internal/domain/shared/order_id.go`, `sku.go`, `path_id.go`.
Omits: constructors (`New`, `NewOrderLine`, `Rehydrate*`), read-only
getters, and `Order.version`'s persistence-only role (ADR 0024).

## 2. Policies, routing and planned capacity

```mermaid
classDiagram
  direction LR
  class PromisePolicy {
    <<ValueObject>>
    +Schedule ScheduleSource
    +Capability CapabilitySource
    +Capacity CapacitySource
    +Fallback LeadTimePolicy
    +SiteId string
    +Horizon int
    +Promise(now Time, o Order) Promise
    +FeasibleBy(now Time, o Order, deadline Time) Promise
    +PromiseGroups(now Time, o Order) PromiseGroup[]
  }
  class LeadTimePolicy {
    <<ValueObject>>
    +Default Duration
    +PerPath map
    +LeadTimeFor(pathID PathId) Duration
    +PromiseDate(now Time, o Order) Time
  }
  class CPTWindow {
    <<ValueObject>>
    +CptId string
    +CutoffAt Time
    +EligiblePathIds string[]
  }
  class ScheduleSource {
    <<interface>>
    +NextCutoffs(siteId string, from Time, n int) CPTWindow[]
  }
  class CapabilitySource {
    <<interface>>
    +CycleTimeP95(pathID PathId) Duration
  }
  class CapacitySource {
    <<interface>>
    +Remaining(pathID PathId, cptId string, cutoffAt Time) int
  }
  class PathSelectionPolicy {
    <<ValueObject>>
    +Select(sku SKU, quantity int, giftWrap bool, attrs string[], catalogue EligibilitySource) PathId
  }
  class EligibilitySource {
    <<interface>>
    +Eligibility(pathID PathId) Eligibility
    +ListActive() ActivePathCandidate[]
  }
  class ActivePathCandidate {
    <<ValueObject>>
    +PathId PathId
    +CycleTimeP95 Duration
    +CycleTimeKnown bool
    +Eligibility Eligibility
  }
  class Eligibility {
    <<ValueObject>>
    -maxUnitsPerLine int
    -requiredProductAttributes string[]
    -excludedProductAttributes string[]
    -nonSortable bool
  }
  class PlannedCapacityWindow {
    <<ValueObject>>
    +PlanID string
    +WarehouseID string
    +Location string
    +PathID string
    +Start Time
    +End Time
    +AssignedDemand float64
    +CapacityOverWindow float64
    +Shortage float64
    +BottleneckStep string
    +Status PlannedCapacityStatus
    +AsOf Time
    +Validate() error
    +Overlaps(from Time, until Time) bool
    +Constraining() bool
    +Supersedes(prev PlannedCapacityWindow) bool
  }
  class PlannedCapacityStatus {
    <<Enumeration>>
    DRAFT
    PUBLISHED
  }
  class Catalogue {
    <<ValueObject>>
    -defs PathDefinition[]
    +Lookup(id string) PathDefinition
  }
  class PathDefinition {
    <<ValueObject>>
    +Id string
    +MatchPrefix string
    +CycleTimeP95 Duration
    +CycleTimeKnown bool
    +Eligibility Eligibility
    +DestinationLocationRole string
  }

  PromisePolicy --> ScheduleSource
  PromisePolicy --> CapabilitySource
  PromisePolicy --> CapacitySource
  PromisePolicy *-- LeadTimePolicy : Fallback
  ScheduleSource ..> CPTWindow
  PathSelectionPolicy ..> EligibilitySource
  EligibilitySource ..> ActivePathCandidate
  ActivePathCandidate *-- Eligibility
  PlannedCapacityWindow --> PlannedCapacityStatus
  Catalogue *-- PathDefinition
  PathDefinition *-- Eligibility
```

Source: `internal/domain/order/promise_policy.go`, `promise.go`,
`path_selection.go`, `planned_capacity.go`,
`internal/domain/processpath/catalogue.go`,
`internal/domain/shared/eligibility.go`, `active_path_candidate.go`.
Omits: unexported helpers (`linesFitWindow`, `fallback`,
`promiseForLines`, `lineEligible`) and the free function
`order.CapacityConstraints(o, now, site, windows)`.
`ports.ProcessPathCatalogue`, `ports.CPTScheduleCache` and
`ports.PathCapacity` structurally satisfy `CapabilitySource`/
`EligibilitySource`, `ScheduleSource` and `CapacitySource`.

## 3. Domain events

```mermaid
classDiagram
  direction TB
  class DomainEvent {
    <<interface>>
    +EventName() string
    +OccurredAt() Time
  }
  class OrderReceived {
    <<DomainEvent>>
    +OrderID OrderId
    +LineCount int
  }
  class OrderLineAllocated {
    <<DomainEvent>>
    +OrderID OrderId
    +LineNo int
    +SKU SKU
    +Quantity int
    +ReservationID string
  }
  class OrderLineBackordered {
    <<DomainEvent>>
    +OrderID OrderId
    +LineNo int
    +SKU SKU
    +Quantity int
  }
  class OrderAllocated {
    <<DomainEvent>>
    +OrderID OrderId
    +PromiseDate Time
    +PromiseCptId string
    +PromiseBasis string
    +Lines ReleasedLine[]
  }
  class OrderPartiallyAllocated {
    <<DomainEvent>>
    +OrderID OrderId
    +AllocatedLines int
    +BackorderedLines int
    +PromiseDate Time
    +PromiseCptId string
    +PromiseBasis string
    +Lines ReleasedLine[]
  }
  class ReleasedLine {
    <<ValueObject>>
    +LineNo int
    +SKU SKU
    +PathID PathId
    +GiftWrap bool
    +FulfillmentClass string
    +PromiseCptId string
    +PromiseBasis string
    +PromiseCutoffAt Time
  }
  class OrderAllocationPartiallyFailed {
    <<DomainEvent>>
    +OrderID OrderId
    +AllocatedLines int
    +RemainingLines int
    +Cause string
  }
  class OrderCancelled {
    <<DomainEvent>>
    +OrderID OrderId
    +RevokedReservations int
  }
  class OrderRepromised {
    <<DomainEvent>>
    +OrderID OrderId
    +CptIdOld string
    +CptIdNew string
    +Reason string
  }
  class OrderLineReleased {
    <<DomainEvent>>
    +OrderID OrderId
    +LineNo int
    +PathID PathId
    +WorkUnitID string
  }
  class OrderReleased {
    <<DomainEvent>>
    +OrderID OrderId
  }

  DomainEvent <|.. OrderReceived
  DomainEvent <|.. OrderLineAllocated
  DomainEvent <|.. OrderLineBackordered
  DomainEvent <|.. OrderAllocated
  DomainEvent <|.. OrderPartiallyAllocated
  DomainEvent <|.. OrderAllocationPartiallyFailed
  DomainEvent <|.. OrderCancelled
  DomainEvent <|.. OrderRepromised
  DomainEvent <|.. OrderLineReleased
  DomainEvent <|.. OrderReleased
  OrderAllocated *-- ReleasedLine
  OrderPartiallyAllocated *-- ReleasedLine
```

Source: `internal/domain/shared/events.go`. Omits: the embedded `base`
struct (`eventName`, `occurredAt`) that implements `DomainEvent` for every
event. `OrderLineReleased` and `OrderReleased` are declared but no use case
raises them today.

## 4. Application ports

```mermaid
classDiagram
  direction LR
  class OrderRepo {
    <<Repository>>
    +Save(ctx, o Order) error
    +FindByID(ctx, id OrderId) Order
    +NextID(ctx) OrderId
  }
  class PlannedCapacityRepo {
    <<Repository>>
    +Upsert(ctx, w PlannedCapacityWindow) bool
    +ListByLocation(ctx, location string, endingAfter Time) PlannedCapacityWindow[]
  }
  class RepromiseProcessedEvents {
    <<Repository>>
    +MarkProcessed(ctx, eventId string) bool
  }
  class PlannedCapacityProcessedEvents {
    <<Repository>>
    +MarkProcessed(ctx, eventId string) bool
  }
  class UnitOfWork {
    <<Port>>
    +Execute(ctx, fn) error
  }
  class EventPublisher {
    <<Port>>
    +Publish(ctx, event DomainEvent) error
  }
  class InventoryReservationClient {
    <<Port>>
    +Reserve(ctx, req ReservationRequest) ReservationResult
    +RevokeReservation(ctx, reservationID string) error
  }
  class ProductClassificationLookup {
    <<Port>>
    +GetClassification(ctx, sku string) ProductClassification
  }
  class ProcessPathCatalogue {
    <<Port>>
    +IsActive(pathId PathId) bool
    +CycleTimeP95(pathId PathId) Duration
    +Eligibility(pathId PathId) Eligibility
    +ListActive() ActivePathCandidate[]
  }
  class CPTScheduleCache {
    <<Port>>
    +NextCutoffs(siteId string, from Time, n int) CPTWindow[]
  }
  class PathCapacity {
    <<Port>>
    +Remaining(pathId PathId, cptId string, cutoffAt Time) int
  }
  class Clock {
    <<Port>>
    +Now() Time
  }
  class OrderMetrics {
    <<Port>>
    +OrderAccepted(ctx)
    +OrderRejected(ctx)
  }
  class ReservationRequest {
    <<ValueObject>>
    +SKU SKU
    +Quantity int
    +DemandRef OrderId
    +LineNo int
    +Attempt int
  }
  InventoryReservationClient ..> ReservationRequest
```

Source: `internal/application/ports/ports.go`. Omits: the sentinel errors
(`ErrInsufficientStock`, `ErrDownstreamNotConfigured`,
`ErrDownstreamUnavailable`, `ErrConcurrentModification`) and the
`ProductClassification`/`ReservationResult` result structs.

## 5. Hexagonal view: inbound and outbound adapters

```mermaid
flowchart LR
  subgraph IN["Inbound adapters"]
    HTTP["inbound/http<br/>chi router, RequireIdempotencyKey"]
    MCP["inbound/mcp<br/>get_order, get_promise_health"]
    KREP["inbound/kafka<br/>RepromiseConsumer"]
    KPC["inbound/kafka<br/>PlannedCapacityConsumer"]
    KAN["inbound/kafka<br/>AnalyticsConsumer"]
  end
  subgraph APP["Application"]
    UC["usecases<br/>ReceiveOrder, RetryAllocation, ReleaseHeldOrder,<br/>CancelOrder, GetOrder, RepromiseOrder,<br/>ApplyPlannedCapacity, GetPlannedCapacity,<br/>OrderCapacityConstraints"]
    PORTS["ports"]
  end
  subgraph DOM["Domain"]
    D["order, processpath, shared"]
  end
  subgraph OUT["Outbound adapters"]
    PG["postgres<br/>OrderRepo, UnitOfWork, OutboxPublisher,<br/>OutboxRelay, Sweeper, PlannedCapacityRepo"]
    MEM["memory<br/>in-memory repos, SystemClock"]
    INV["inventorystorage<br/>Client, BreakerClient, PermissiveClient"]
    CLS["productclassification<br/>Client, BreakerClient, PermissiveLookup"]
    CACHE["kafkacatalog, kafkacptschedule,<br/>kafkapathcapacity, pathcapacity"]
    KPUB["kafka<br/>Publisher, AnalyticsPublisher, RelaySink"]
    LOG["events<br/>LogPublisher"]
    TEL["telemetry<br/>OrderMetrics"]
    ANS["analyticsstore<br/>projection, report"]
  end
  REP["analytics/report"]

  HTTP --> UC
  MCP --> UC
  KREP --> UC
  KPC --> UC
  KAN --> REP
  UC --> PORTS
  UC --> D
  PORTS --> D
  PG -. implements .-> PORTS
  MEM -. implements .-> PORTS
  INV -. implements .-> PORTS
  CLS -. implements .-> PORTS
  CACHE -. implements .-> PORTS
  KPUB -. implements .-> PORTS
  LOG -. implements .-> PORTS
  TEL -. implements .-> PORTS
  ANS -. implements .-> REP
```

Source: `cmd/order/main.go`, `cmd/order/wiring.go`,
`cmd/order/planned_capacity.go`, `cmd/mcp/main.go`,
`cmd/order-projector/main.go`, `internal/adapters/**`. Omits:
`cmd/order-reports` (reads `analytics/report` through `analyticsstore` and
serves `inbound/http.ReportsHandlers`), and the `cloudevents` helper every
Kafka adapter shares. The MCP adapter reaches the report store through its
own `PromiseHealthStore` port, adapted in `cmd/mcp`.
