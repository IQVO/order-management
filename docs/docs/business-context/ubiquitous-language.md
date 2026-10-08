---
title: Ubiquitous Language
sidebar_label: Ubiquitous Language
description: The exact vocabulary of the Order Management bounded context, with definitions and where each term lives in code.
---

# Ubiquitous Language

These are the terms this bounded context uses, with the definitions it
uses them with. Every term maps to a Go identifier on `develop`. They are
not interchangeable with the same English words used in inventory-storage,
wes-work-planning or fulfillment-execution — see
[the last table](#words-that-mean-something-different-elsewhere).

## Core terms

| Term | Definition | Code |
| --- | --- | --- |
| **Order** | The aggregate root: one customer's demand, its lines, its delivery promise and its hold/deadline intent. | `order.Order` |
| **Order line** | One requested SKU and quantity within an order, with its resolved process path, gift-wrap flag, line status and (once allocated) reservation id. | `order.OrderLine` |
| **Order status** | Derived on every read from line statuses, never stored: `Received`, `Allocated`, `PartiallyAllocated`, `Backordered`, `Released`, `PartiallyReleased`, `Cancelled`. | `order.Status`, `Order.Status()` |
| **Line status** | `Pending`, `Allocated`, `Backordered`, `Released`, `Cancelled`. | `order.LineStatus` |
| **Allocation** | Reserving stock for one line through inventory-storage's `POST /reservations`. No local `Reservation` model — only the id is kept. | `usecases.allocateLines` |
| **Backordered** | A line inventory-storage answered `409` for (insufficient usable stock). A business fact, unlike a transport or 5xx error, which fails the pass instead (BR2, fail closed). | `order.LineStatusBackordered`, `ports.ErrInsufficientStock` |
| **Retry allocation** | The only path from `Backordered` back to `Allocated`. | `usecases.RetryAllocation`, `Order.RetryAllocate` |
| **Reconfirm** | Re-reserving a line allocated in an earlier pass right before it is released, so a lapsed reservation is caught. | `Order.ReconfirmReservation`, `reconfirmAllocatedLines` |
| **Lost reservation** | A reconfirm that `409`s: the line moves `Allocated` → `Backordered`. | `Order.LoseReservation` |
| **Release** | Marking allocated lines `Released` (a pure domain transition) once BR3 allows, and announcing them on `OrderAllocated`/`OrderPartiallyAllocated`. Not a call to wes-work-planning (ADR 0005). | `Order.Release`, `releaseAllocatedLines` |
| **Ship-complete** | An order with `allowPartialShipment=false` (the default): nothing is released while any line is unallocated (BR3). | `Order.EnsureReleasable`, `ErrShipCompleteBlocked` |
| **Partial shipment** | `allowPartialShipment=true`: allocated lines release even if others are backordered, possibly in several shipment groups. | `Order.AllowPartialShipment()` |
| **Cancellation boundary** | An order can be cancelled until any line is released (BR6). | `Order.EnsureCancellable`, `ErrOrderAlreadyReleased` |
| **Held order** | Received with `releaseOnAllocation=false`: it allocates, then waits for `POST /orders/{id}/release` or a cancel. Must be ship-complete. Not a status. | `Order.Hold`, `Order.ReleaseOnAllocation()`, `usecases.ReleaseHeldOrder` |
| **Required ship-by** | A caller's deadline; the promise may only use a CPT window at or before it. | `Order.SetRequiredShipBy`, `PromisePolicy.FeasibleBy` |

## Promise terms

| Term | Definition | Code |
| --- | --- | --- |
| **CPT** (critical pull time) | A site's carrier cutoff: work must be done by then to leave on that departure. | `order.CPTWindow` |
| **Promise** | The CPT window (or lead-time date) an order, or a shipment group, is promised to leave by. | `order.Promise` |
| **Promise basis** | Which policy produced a promise: `Capability` (catalogue cycle time, CPT schedule, path capacity), `LeadTime` (per-path fallback), or `Network` (a window chosen to meet `requiredShipBy`). | `order.PromiseBasis` |
| **Promise group** / **shipment group** | The lines that ship together under one promise; one per order when ship-complete, possibly several with partial shipment. | `order.PromiseGroup` |
| **Promise date** | On the order, the latest cutoff across its promise groups. | `Order.PromiseDate()` |
| **Re-promise** | Recomputing a line's group promise after fulfillment-execution reports `TaskCPTMissed` or `PackageManifested`; raises `OrderRepromised` if it moved. | `usecases.RepromiseOrder` |
| **Fulfillment class** | Demand-shape classifier `SINGLE`, `SAME_SKU_MULTI`, `MULTI_LINE_MULTI`, derived from line count and quantity, never stored (ADR 0008). | `order.FulfillmentClass` |

## Routing terms

| Term | Definition | Code |
| --- | --- | --- |
| **Process path** | The building workflow a line's work is dispatched to (`pick`, ...). Owned by process-path-management; cached here. | `shared.PathId`, `processpath.PathDefinition` |
| **Default path** | `pick`, used when no catalogue is wired or nothing better is eligible. | `shared.DefaultPathId` |
| **Eligibility** | A path's admission rules: max units per line, required and excluded product attributes, non-sortable. | `shared.Eligibility` |
| **Path selection** | Choosing, per line, the eligible active path with the shortest known `CycleTimeP95`, ties to the lower `PathId` (ADR 0021). Never caller-supplied. | `order.PathSelectionPolicy` |
| **Product classification** | A SKU's handling tags (Hazmat, Fragile, TemperatureSensitive, Oversized, HighValue), owned by **product-master** and read here from a local copy of its `ProductClassified` events (ADR 0036). Used only as a routing hint (fail-open: an unknown SKU has no tags). | `ports.ProductClassificationLookup`, `product_classification_copy` |
| **Work unit id** | `{orderId}-line-{lineNo}`, derived identically here and in wes-work-planning; never transmitted. | `usecases.WorkUnitID`, `ParseWorkUnitID` |

## Planned capacity terms (ADR 0031)

| Term | Definition | Code |
| --- | --- | --- |
| **Planned capacity window** | A local copy of one warehouse-planning capacity plan: location, path, time window, demand, capacity, shortage, `DRAFT`/`PUBLISHED`. | `order.PlannedCapacityWindow` |
| **Capacity constraint** | A published shortage window the order's promise overlaps; shown on the order response, never changes the promise. | `order.CapacityConstraints`, `capacityConstraint` |

## Infrastructure terms the domain relies on

| Term | Definition | Code |
| --- | --- | --- |
| **Idempotency key** | Required header on `POST /orders` with Postgres; replays return the stored response (ADR 0023). | `RequireIdempotencyKey`, table `idempotency_keys` |
| **Outbox** | Events written in the same transaction as the order and relayed to Kafka (ADR 0022). | `postgres.OutboxPublisher`, `OutboxRelay` |
| **Version** | Optimistic-concurrency counter on `orders`; a stale save is `ErrConcurrentModification` (ADR 0024). | `Order.Version()` |

## Words that mean something different elsewhere

| Word | Here (Order Management) | Elsewhere |
| --- | --- | --- |
| **Reservation** | *Not modelled* — only a `reservationId` reference is held | inventory-storage: the aggregate itself, with `ACTIVE`/`CONFIRMED`/`REVOKED`/`EXPIRED` |
| **Release** | Marking allocated lines `Released` and announcing them on Kafka | wes-work-planning: accepting and scheduling a `WorkUnit` |
| **Status** | Order-level, always derived from line statuses | inventory-storage's `Reservation.Status` is a different state machine on a different aggregate |
| **Capacity** | Path capacity (`PathCapacityChanged`, remaining units per cutoff) feeds the promise; planned capacity (warehouse-planning) only annotates | warehouse-planning: the plan itself, with demand and bottleneck steps |
| **Order reference** | This context's real `OrderId` | `demandRef` on inventory-storage's `Reservation`; `order_ref` on fulfillment-execution's tasks (a work-unit id, not a bare order id) |

Do not share a DTO or type across those boundaries. Translate at the
anti-corruption layer instead — see
[ADR 0002](/docs/adr/0002-http-consumer-of-inventory-and-wes-not-shared-code).
The full state diagrams are on the
[Aggregate Design Canvas](/docs/ddd/aggregate-design-canvas).
