# event_driven_rules.feature — scenarios derived from the Order Management
# documentation for the facts that arrive from other contexts over Kafka
# (use-cases 6, 10, 11, 12 and 13; domain-events; aggregate-design-canvas;
# troubleshooting). The consumers' use cases are driven directly, exactly as
# the planned-capacity features drive ApplyPlannedCapacity: there is no HTTP
# write path for these facts.
# The scenario clock is 2026-09-07T09:00:00Z; the path "pick" has a 60 minute
# P95 cycle time wherever a CPT schedule is declared.
@bdd
Feature: Facts consumed from fulfillment-execution, wes-work-planning and outbound-shipping

  Background:
    Given the Order Management service is running

  # --- RepromiseOrder (use case 6, ADR 0018) --------------------------------

  Scenario Outline: A <reason> signal moves the promise to the next window and announces OrderRepromised
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the clock advances 210 minutes
    When fulfillment-execution reports <reason> event "evt-m1" for line 1 of the order
    Then exactly 1 "OrderRepromised" event was published
    And the last "OrderRepromised" event moved the promise from CPT "sp1-1200" to "sp1-1500" because of "<reason>"
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T15:00:00Z"

    Examples:
      | reason           |
      | TaskCPTMissed    |
      | PackageManifested |

  Scenario: A redelivered event id is evaluated once and a new id for an already moved promise raises nothing
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the clock advances 210 minutes
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    And fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then exactly 1 "OrderRepromised" event was published
    When fulfillment-execution reports PackageManifested event "evt-m2" for line 1 of the order
    Then exactly 1 "OrderRepromised" event was published

  Scenario: A lead-time promise that slipped is re-promised without any CPT identity
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the clock advances 210 minutes
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then the last "OrderRepromised" event moved the promise from CPT "" to "" because of "TaskCPTMissed"
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-08T12:30:00Z"

  Scenario: A promise that moves to another departure at the same instant is still a re-promise
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId        | cutoffAt             |
      | sp1-1500-std | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId        | cutoffAt             |
      | sp1-1500-exp | 2026-09-07T15:00:00Z |
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then the last "OrderRepromised" event moved the promise from CPT "sp1-1500-std" to "sp1-1500-exp" because of "TaskCPTMissed"

  Scenario: A signal that leaves the promise where it was raises nothing and changes nothing
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001"
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then no "OrderRepromised" event was published
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T12:00:00Z"

  Scenario: A signal for an unknown order is ignored
    When fulfillment-execution reports TaskCPTMissed event "evt-x" for line 1 of order "ord-does-not-exist"
    Then no "OrderRepromised" event was published

  Scenario: A signal for a line that holds no promise is ignored
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    And the clock advances 210 minutes
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 2 of the order
    Then no "OrderRepromised" event was published
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T12:00:00Z"

  Scenario Outline: An order_ref that does not parse as a work unit id is skipped
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the clock advances 210 minutes
    When fulfillment-execution reports TaskCPTMissed event "evt-bad" for order_ref "<ref>"
    Then no "OrderRepromised" event was published

    Examples:
      | ref              |
      | {order}          |
      | {order}-line-0   |
      | {order}-line-x   |
      | -line-1          |

  Scenario: A cancelled order is not re-promised
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    And the order is cancelled
    And the clock advances 210 minutes
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then no "OrderRepromised" event was published

  Scenario: A deadline order is re-promised to the latest window within its deadline
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001" with required ship-by "2026-09-07T15:00:00Z"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1430 | 2026-09-07T14:30:00Z |
      | sp1-1800 | 2026-09-07T18:00:00Z |
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then the last "OrderRepromised" event moved the promise from CPT "sp1-1500" to "sp1-1430" because of "TaskCPTMissed"
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T14:30:00Z"

  Scenario: A deadline order keeps its promise when no window within the deadline is left
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001" with required ship-by "2026-09-07T15:00:00Z"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1800 | 2026-09-07T18:00:00Z |
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then no "OrderRepromised" event was published
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T15:00:00Z"

  Scenario: A re-promise re-emits the line's demand with the fresh cutoff
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001" at fulfillment site "WH2"
    And the clock advances 210 minutes
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then exactly 2 "SiteSkuDemandChanged" events were published
    And the last "SiteSkuDemandChanged" event is for site "WH2" with assignment version "order-site-v1" and state "ACTIVE"
    And the last "SiteSkuDemandChanged" event is for line 1 due at "2026-09-07T15:00:00Z"

  # --- Release confirmation (use cases 11 and 12, ADR 0043) -----------------

  Scenario: A release opens a PENDING confirmation for every line it releases
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    When an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    Then the confirmation of line 1 is "PENDING"
    And the confirmation of line 2 is "PENDING"

  Scenario: Only the lines released in a pass get a confirmation
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-0042"
    And inventory-storage reports insufficient usable stock for SKU "SKU-GAME-0007"
    And an order allowing partial shipment is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042,SKU-GAME-0007"
    Then the confirmation of line 1 is "PENDING"
    And there is no confirmation for line 2
    And there is no confirmation for line 3
    Given inventory-storage has usable stock for SKU "SKU-TOY-0042"
    When the order is retried for allocation
    Then the confirmation of line 2 is "PENDING"
    And there is no confirmation for line 3

  Scenario: A held order opens its confirmations only when it is released
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    Then there is no confirmation for line 1
    When the order is released
    Then the confirmation of line 1 is "PENDING"

  Scenario: A ship-complete order blocked by BR3 opens no confirmation
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    When an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    Then the order status is "Backordered"
    And there is no confirmation for line 1

  Scenario: WorkUnitCreated confirms a PENDING line and a second event for it changes nothing
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    When wes-work-planning confirms line 1 of the order in event "evt-w1"
    Then the confirmation outcome is "confirmed"
    And the confirmation of line 1 is "CONFIRMED"
    When wes-work-planning confirms line 1 of the order in event "evt-w1"
    Then the confirmation outcome is "duplicate"
    When wes-work-planning confirms line 1 of the order in event "evt-w2"
    Then the confirmation outcome is "duplicate"
    And the confirmation of line 1 is "CONFIRMED"

  Scenario Outline: A WorkUnitCreated that names no line this service released is ignored and counted
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    When wes-work-planning publishes WorkUnitCreated "evt-w1" for reference "<reference>" and line <line>
    Then the confirmation outcome is "<outcome>"
    And the confirmation of line 1 is "PENDING"

    Examples:
      | reference | line | outcome              |
      |           | 1    | ignored_no_reference |
      | {order}   | 0    | ignored_no_reference |
      | TRF-0042  | 1    | ignored_unknown_line |
      | {order}   | 9    | ignored_unknown_line |

  Scenario: The sweep flags a line that stayed PENDING past the timeout once and raises one fact per line
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    And the clock advances 16 minutes
    When the housekeeping sweep runs with a confirmation timeout of 15 minutes
    Then the sweep flagged 2 lines
    And the confirmation of line 1 is "UNCONFIRMED"
    And the confirmation of line 2 is "UNCONFIRMED"
    And exactly 2 "OrderReleaseUnconfirmed" events were published
    And the last "OrderReleaseUnconfirmed" event is for line 2 with work unit id "{order}-line-2"
    When the housekeeping sweep runs with a confirmation timeout of 15 minutes
    Then the sweep flagged 0 lines
    And exactly 2 "OrderReleaseUnconfirmed" events were published

  Scenario Outline: The sweep flags a line only once the 15 minute timeout has strictly passed (<minutes> minutes after release)
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the clock advances <minutes> minutes
    When the housekeeping sweep runs with a confirmation timeout of 15 minutes
    Then the sweep flagged <flagged> lines

    Examples:
      | minutes | flagged |
      | 14      | 0       |
      | 15      | 0       |
      | 16      | 1       |

  Scenario: A flagged line is only a fact and leaves the order alone
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the reservation requests are remembered
    And the clock advances 60 minutes
    When the housekeeping sweep runs with a confirmation timeout of 15 minutes
    Then the sweep flagged 1 line
    And no new reservation request was made
    And no "OrderCancelled" event was published
    And exactly 1 "OrderAllocated" event was published
    When the order is fetched
    Then the order status is "Released"
    And line 1 status is "Released"

  Scenario: A confirmed line is never flagged
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And wes-work-planning confirms line 1 of the order in event "evt-w1"
    And the clock advances 60 minutes
    When the housekeeping sweep runs with a confirmation timeout of 15 minutes
    Then the sweep flagged 0 lines
    And the confirmation of line 1 is "CONFIRMED"

  Scenario: A late confirmation clears the flag but does not retract the fact already raised
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the clock advances 16 minutes
    And the housekeeping sweep runs with a confirmation timeout of 15 minutes
    When wes-work-planning confirms line 1 of the order in event "evt-w1"
    Then the confirmation outcome is "late_confirmed"
    And the confirmation of line 1 is "CONFIRMED"
    And the confirmation of line 1 keeps the time it was flagged
    And exactly 1 "OrderReleaseUnconfirmed" event was published

  Scenario: A confirmation timeout of zero switches the sweep off
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the clock advances 60 minutes
    When the housekeeping sweep runs with a confirmation timeout of 0 minutes
    Then the sweep flagged 0 lines
    And the confirmation of line 1 is "PENDING"

  # --- ConfirmShipment (use case 13, ADR 0051) ---------------------------------

  Scenario: A confirmed load ships every released line and raises OrderShipped with the load
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    And the order is called "first"
    When outbound-shipping confirms load "LOAD-7" in event "evt-s1" for orders "first"
    Then the shipment outcome for "first" is "shipped"
    And exactly 2 "OrderLineShipped" events were published
    And exactly 1 "OrderShipped" event was published
    And the last "OrderShipped" event carries load "LOAD-7"
    And the last "OrderLineShipped" event carries load "LOAD-7"
    And no "OrderPartiallyShipped" event was published
    When the order is fetched
    Then the order status is "Shipped"
    And line 1 status is "Shipped"
    And line 2 status is "Shipped"

  Scenario: A partial shipment ships what was released and the order reads PartiallyShipped
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order allowing partial shipment is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    And the order is called "first"
    When outbound-shipping confirms load "LOAD-7" in event "evt-s1" for orders "first"
    Then the shipment outcome for "first" is "shipped"
    And exactly 1 "OrderLineShipped" event was published
    And exactly 1 "OrderPartiallyShipped" event was published
    And the last "OrderPartiallyShipped" event carries load "LOAD-7"
    And no "OrderShipped" event was published
    When the order is fetched
    Then the order status is "PartiallyShipped"
    And line 1 status is "Shipped"
    And line 2 status is "Backordered"

  Scenario: A partially shipped order ships its remaining line on a later load
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-0042"
    And an order allowing partial shipment is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    And the order is called "first"
    And outbound-shipping confirms load "LOAD-7" in event "evt-s1" for orders "first"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And the order is retried for allocation
    When outbound-shipping confirms load "LOAD-8" in event "evt-s2" for orders "first"
    Then the shipment outcome for "first" is "shipped"
    And exactly 2 "OrderLineShipped" events were published
    And exactly 1 "OrderShipped" event was published
    And the last "OrderShipped" event carries load "LOAD-8"
    When the order is fetched
    Then the order status is "Shipped"

  Scenario: A redelivered ShipmentConfirmed changes nothing and a new id for a shipped order is a no-op
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the order is called "first"
    And outbound-shipping confirms load "LOAD-7" in event "evt-s1" for orders "first"
    And the published events are remembered
    When outbound-shipping confirms load "LOAD-7" in event "evt-s1" for orders "first"
    Then the shipment outcome for "first" is "duplicate"
    When outbound-shipping confirms load "LOAD-7" in event "evt-s2" for orders "first"
    Then the shipment outcome for "first" is "no_change"
    And no new "OrderLineShipped" event was published
    And no new "OrderShipped" event was published

  Scenario: One event names several orders and each is handled on its own merits
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the order is called "first"
    And an order is placed for SKUs "SKU-TOY-9999"
    And the order is called "second"
    When outbound-shipping confirms load "LOAD-9" in event "evt-s1" for orders "first,second,ord-ghost"
    Then the shipment outcome for "first" is "shipped"
    And the shipment outcome for "second" is "ignored_not_shippable"
    And the shipment outcome for "ord-ghost" is "ignored_unknown_order"
    And exactly 1 "OrderShipped" event was published
    When the order is fetched
    Then the order status is "Backordered"

  Scenario: A cancelled order is not shippable
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    And the order is called "first"
    And the order is cancelled
    When outbound-shipping confirms load "LOAD-7" in event "evt-s1" for orders "first"
    Then the shipment outcome for "first" is "ignored_not_shippable"
    And no "OrderLineShipped" event was published
    When the order is fetched
    Then the order status is "Cancelled"

  Scenario: An event that names no orders is ignored
    When outbound-shipping confirms load "LOAD-7" in event "evt-s1" naming no orders
    Then the shipment outcome for "no orders" is "ignored_no_orders"
    And no "OrderLineShipped" event was published

  # Known divergence: BR6 / the line state diagram make Shipped terminal (a
  # shipped line is past Released, and only Pending, Backordered and Allocated
  # lines can be Cancelled) but EnsureCancellable only looks for a Released
  # line, so DELETE /orders/{id} on a shipped order answers 204 and rewrites
  # its lines to Cancelled. Written as the specified behaviour.
  @known-bug
  Scenario: A shipped order can no longer be cancelled
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the order is called "first"
    And outbound-shipping confirms load "LOAD-7" in event "evt-s1" for orders "first"
    When the order is cancelled
    Then the request is rejected with status 409
    And the problem detail type is "order-already-released"
    When the order is fetched
    Then the order status is "Shipped"
