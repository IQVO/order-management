# Derived from: docs/docs/ddd/use-cases.md §13 (ConfirmShipment's second
# leg: "The same consumer routes PackageMissedLoad (source_order_id, a
# bare order id) to RepromiseOrder.ExecuteForMissedLoad, the order-wide
# variant of use case 6: every promise group of the order is recomputed,
# and each group whose promise moved publishes one OrderRepromised with
# reason PackageMissedLoad"), docs/docs/ddd/domain-events.md (the
# PackageMissedLoad row: "`cutoff` (logged, not used as the new
# promise)"; "`PackageMissedLoad` goes through the existing
# repromise_processed_events gate") and ADR 0051.
# The missed-load fact is fed through the SAME use case the consumer
# calls, exactly as the per-line fulfillment steps do. The scenario clock
# starts at 2026-09-07T09:00:00Z.
Feature: A missed load re-promises the whole order
  When outbound-shipping reports a package that missed its load, every
  promise group of that order is recomputed from current capability, and
  each group whose promise moved is announced. The event id is claimed on
  the same gate the per-line signals use, and every "nothing to re-promise"
  outcome is a silent no-op.

  Scenario: A missed load re-promises every group of the order and announces each move
    Given the Order Management service is running
    And the deployment projects demand to the static site "SIM1" including re-promises
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1800 | 2026-09-07T18:00:00Z |
      | sp1-2100 | 2026-09-07T21:00:00Z |
    And path "pick" has capacity for 5 units before CPT "sp1-1200"
    And path "pick" has capacity for 20 units before CPT "sp1-1800"
    And path "pick" has capacity for 50 units before CPT "sp1-2100"
    When an order allowing partial shipment is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 2        | false    |
      | SKU-TOY-0042  | 8        | false    |
    Then the request is accepted with status 201
    And the last "OrderAllocated" event released line 1 under promise CPT "sp1-1200"
    And the last "OrderAllocated" event released line 2 under promise CPT "sp1-1800"
    When the clock advances 580 minutes
    And outbound-shipping reports a missed load "ml-1" for the order with cutoff "2026-09-07T18:00:00Z"
    Then exactly 2 "OrderRepromised" events were published
    And an "OrderRepromised" event moved the promise from CPT "sp1-1200" to "sp1-2100" because of "PackageMissedLoad"
    And an "OrderRepromised" event moved the promise from CPT "sp1-1800" to "sp1-2100" because of "PackageMissedLoad"
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T21:00:00Z"
    And an ACTIVE SiteSkuDemandChanged event for line 1 is due at "2026-09-07T21:00:00Z"
    And an ACTIVE SiteSkuDemandChanged event for line 2 is due at "2026-09-07T21:00:00Z"

  Scenario: The missed load's cutoff is telemetry only and never becomes the promise
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And path "pick" has capacity for 5 units before CPT "sp1-1200"
    And path "pick" has capacity for 50 units before CPT "sp1-1500"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the response field "promiseDate" is "2026-09-07T12:00:00Z"
    When the clock advances 210 minutes
    And outbound-shipping reports a missed load "ml-cut" for the order with cutoff "2026-09-07T13:37:00Z"
    Then exactly 1 "OrderRepromised" event was published
    And an "OrderRepromised" event moved the promise from CPT "sp1-1200" to "sp1-1500" because of "PackageMissedLoad"
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T15:00:00Z"

  Scenario: A redelivered missed-load event id changes nothing
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And path "pick" has capacity for 5 units before CPT "sp1-1200"
    When an order is placed for SKUs "SKU-BOOK-0001"
    When the clock advances 210 minutes
    And outbound-shipping reports a missed load "ml-once" for the order with cutoff "2026-09-07T12:00:00Z"
    Then exactly 1 "OrderRepromised" event was published
    And the published events are remembered
    When outbound-shipping reports a missed load "ml-once" for the order with cutoff "2026-09-07T12:00:00Z"
    Then no new "OrderRepromised" event was published

  Scenario: One event id gate spans the per-line and missed-load re-promise paths
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
      | sp1-1800 | 2026-09-07T18:00:00Z |
    And path "pick" has capacity for 5 units before CPT "sp1-1200"
    When an order is placed for SKUs "SKU-BOOK-0001"
    When the clock advances 210 minutes
    And outbound-shipping reports a missed load "gate-1" for the order with cutoff "2026-09-07T12:00:00Z"
    Then exactly 1 "OrderRepromised" event was published
    And the published events are remembered
    When the clock advances 190 minutes
    And fulfillment-execution reports TaskCPTMissed event "gate-1" for line 1 of the order
    Then no new "OrderRepromised" event was published
    When fulfillment-execution reports TaskCPTMissed event "gate-2" for line 1 of the order
    Then exactly 2 "OrderRepromised" events were published
    And the last "OrderRepromised" event moved the promise from CPT "sp1-1500" to "sp1-1800" because of "TaskCPTMissed"

  Scenario: A missed load for an unknown order is ignored
    Given the Order Management service is running
    When outbound-shipping reports a missed load "ml-unknown" for order "ord-does-not-exist" with cutoff "2026-09-07T12:00:00Z"
    Then no "OrderRepromised" event was published

  Scenario: A missed load for an order that holds no promise groups is ignored
    Given the Order Management service is running
    And inventory-storage reports insufficient usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the order status is "Backordered"
    When outbound-shipping reports a missed load "ml-bare" for the order with cutoff "2026-09-07T12:00:00Z"
    Then no "OrderRepromised" event was published

  Scenario: A missed load for a cancelled order is ignored
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    When an order is placed on hold for SKUs "SKU-BOOK-0001"
    Then the order status is "Allocated"
    When the order is cancelled
    Then the request is accepted with status 204
    When outbound-shipping reports a missed load "ml-cancelled" for the order with cutoff "2026-09-07T15:00:00Z"
    Then no "OrderRepromised" event was published

  Scenario: A missed load whose recomputed promise did not move raises nothing
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the response field "promiseDate" is "2026-09-07T15:00:00Z"
    When the clock advances 10 minutes
    And outbound-shipping reports a missed load "ml-still" for the order with cutoff "2026-09-07T15:00:00Z"
    Then no "OrderRepromised" event was published
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T15:00:00Z"

  Scenario: A missed load for a deadline the order can no longer meet keeps its promise
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
    And path "pick" has capacity for 50 units before CPT "sp1-1200"
    When an order is placed for SKUs "SKU-BOOK-0001" with required ship-by "2026-09-07T12:00:00Z"
    Then the response field "promiseDate" is "2026-09-07T12:00:00Z"
    When the clock advances 130 minutes
    And outbound-shipping reports a missed load "ml-late" for the order with cutoff "2026-09-07T12:00:00Z"
    Then no "OrderRepromised" event was published
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T12:00:00Z"
