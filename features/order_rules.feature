# order_rules.feature — scenarios derived from the Order Management documentation
# (warehouse-docs docs/contexts/order-management: business-context, use-cases,
# aggregate-design-canvas, domain-events, troubleshooting; the repo's own
# docs/docs/ddd copies). Each scenario names ONE documented rule.
# The scenario clock is 2026-09-07T09:00:00Z; without a CPT schedule the
# promise is the 24h lead-time fallback, 2026-09-08T09:00:00Z.
@bdd
Feature: The order rules of the Order Management documentation

  Background:
    Given the Order Management service is running

  # --- ReceiveOrder: intake ------------------------------------------------

  Scenario Outline: A quantity of <quantity> is accepted at intake and reserved as stated
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed with lines:
      | sku           | quantity   | giftWrap |
      | SKU-BOOK-0001 | <quantity> | false    |
    Then the request is accepted with status 201
    And line 1 is for SKU "SKU-BOOK-0001" with quantity <quantity> and gift wrap false
    And inventory-storage received 1 reservation request
    And reservation request 1 asked for <quantity> of SKU "SKU-BOOK-0001" for line 1 of the order

    Examples:
      | quantity |
      | 1        |
      | 2        |
      | 500      |

  Scenario: A held order that also allows partial shipment is refused before anything is persisted
    When an order is placed on hold allowing partial shipment for SKUs "SKU-BOOK-0001"
    Then the request is rejected with status 422
    And the problem detail type is "held-order-must-be-ship-complete"
    And no "OrderReceived" event was published
    And inventory-storage received 0 reservation requests

  Scenario Outline: One invalid line refuses the whole order and nothing is persisted
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed with lines:
      | sku           | quantity   | giftWrap |
      | SKU-BOOK-0001 | 1          | false    |
      | <sku>         | <quantity> | false    |
    Then the request is rejected with status <status>
    And the problem detail type is "<slug>"
    And no "OrderReceived" event was published
    And inventory-storage received 0 reservation requests

    Examples:
      | sku           | quantity | status | slug                  |
      |               | 1        | 400    | empty-sku             |
      | SKU-TOY-0042  | 0        | 422    | non-positive-quantity |
      | SKU-TOY-0042  | -1       | 422    | non-positive-quantity |

  Scenario: An order that states no flags is ship-complete and released on allocation
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the response flag "allowPartialShipment" is false
    And the response flag "releaseOnAllocation" is true

  Scenario: The hold flag is never cleared, not even by the release it was waiting for
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    Then the response flag "releaseOnAllocation" is false
    When the order is released
    Then the request is accepted with status 200
    And the order status is "Released"
    And the response flag "releaseOnAllocation" is false

  Scenario: Lines are numbered from 1 in request order and keep their quantity and gift wrap
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    When an order is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 2        | true     |
      | SKU-TOY-0042  | 5        | false    |
    Then line 1 is for SKU "SKU-BOOK-0001" with quantity 2 and gift wrap true
    And line 2 is for SKU "SKU-TOY-0042" with quantity 5 and gift wrap false

  Scenario: Allocation reserves each line once with the order id as demand reference
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And inventory-storage has usable stock for SKU "SKU-GAME-0007"
    When an order is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 2        | false    |
      | SKU-TOY-0042  | 1        | false    |
      | SKU-GAME-0007 | 4        | false    |
    Then inventory-storage received 3 reservation requests
    And reservation request 1 asked for 2 of SKU "SKU-BOOK-0001" for line 1 of the order
    And reservation request 2 asked for 1 of SKU "SKU-TOY-0042" for line 2 of the order
    And reservation request 3 asked for 4 of SKU "SKU-GAME-0007" for line 3 of the order

  # --- Process path selection (ADR 0021 / ADR 0042) ------------------------

  Scenario: A line goes to the CUSTOMER_ORDER path with the shortest known cycle time
    Given process-path-management declares the paths:
      | pathId       | cycleMinutes | intake                      | maxUnits | requires | excludes |
      | pick         | 45           | CUSTOMER_ORDER              | -        | -        | -        |
      | pick-express | 20           | CUSTOMER_ORDER              | -        | -        | -        |
      | pack         | 5            | STOCK_TRANSFER              | -        | -        | -        |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And line 1 is on process path "pick-express"

  Scenario: Paths with the same cycle time tie-break on the lower path id
    Given process-path-management declares the paths:
      | pathId   | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick-b   | 30           | CUSTOMER_ORDER | -        | -        | -        |
      | pick-a   | 30           | CUSTOMER_ORDER | -        | -        | -        |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then line 1 is on process path "pick-a"

  Scenario: A path with an unknown cycle time loses to one with a known cycle time
    Given process-path-management declares the paths:
      | pathId   | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick-a   | unknown      | CUSTOMER_ORDER | -        | -        | -        |
      | pick-z   | 90           | CUSTOMER_ORDER | -        | -        | -        |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then line 1 is on process path "pick-z"

  Scenario: With no path declaring CUSTOMER_ORDER intake the line falls back to pick
    Given process-path-management declares the paths:
      | pathId | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick   | 45           | SLOT_RELOCATION | -       | -        | -        |
      | pack   | 5            | STOCK_TRANSFER  | -       | -        | -        |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And line 1 is on process path "pick"

  Scenario: When the pick fallback is not in the catalogue the order is refused as an unknown process path
    Given process-path-management declares the paths:
      | pathId | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pack   | 5            | STOCK_TRANSFER | -        | -        | -        |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is rejected with status 400
    And the problem detail type is "unknown-process-path"
    And no "OrderReceived" event was published

  Scenario Outline: A line of <quantity> units goes to the path whose unit cap admits it
    Given process-path-management declares the paths:
      | pathId       | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick         | 45           | CUSTOMER_ORDER | -        | -        | -        |
      | pick-singles | 10           | CUSTOMER_ORDER | 2        | -        | -        |
    When an order is placed with lines:
      | sku           | quantity   | giftWrap |
      | SKU-BOOK-0001 | <quantity> | false    |
    Then the request is accepted with status 201
    And line 1 is on process path "<path>"

    Examples:
      | quantity | path         |
      | 2        | pick-singles |
      | 3        | pick         |

  Scenario: A line that no CUSTOMER_ORDER path admits is refused as ineligible
    Given process-path-management declares the paths:
      | pathId       | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick-singles | 10           | CUSTOMER_ORDER | 2        | -        | -        |
    When an order is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 3        | false    |
    Then the request is rejected with status 422
    And the problem detail type is "line-ineligible-for-resolved-path"
    And no "OrderReceived" event was published

  Scenario: A hazmat SKU is kept off the path that excludes hazmat and an unclassified SKU fails open
    Given process-path-management declares the paths:
      | pathId    | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick      | 45           | CUSTOMER_ORDER | -        | -        | -        |
      | pick-fast | 10           | CUSTOMER_ORDER | -        | -        | Hazmat   |
    And product-master has classified SKU "SKU-CHEM-0007" as "Hazmat" at version 1 in event "evt-c1"
    When an order is placed for SKUs "SKU-CHEM-0007,SKU-BOOK-0001"
    Then the request is accepted with status 201
    And line 1 is on process path "pick"
    And line 2 is on process path "pick-fast"

  Scenario: Gift wrap counts as a product attribute against a path's required attributes
    Given process-path-management declares the paths:
      | pathId    | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick      | 45           | CUSTOMER_ORDER | -        | -        | -        |
      | pick-gift | 10           | CUSTOMER_ORDER | -        | giftWrap | -        |
    When an order is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 1        | true     |
      | SKU-TOY-0042  | 1        | false    |
    Then line 1 is on process path "pick-gift"
    And line 2 is on process path "pick"

  # --- Product classification local copy (ADR 0036) ------------------------

  Scenario: A newer classification version replaces the stored one and an older one never does
    Given process-path-management declares the paths:
      | pathId    | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick      | 45           | CUSTOMER_ORDER | -        | -        | -        |
      | pick-fast | 10           | CUSTOMER_ORDER | -        | -        | Hazmat   |
    And product-master has classified SKU "SKU-CHEM-0007" as "Hazmat" at version 2 in event "evt-v2"
    When product-master publishes event "evt-v1" classifying SKU "SKU-CHEM-0007" as "-" at version 1
    Then the classification is accepted
    When an order is placed for SKUs "SKU-CHEM-0007"
    Then line 1 is on process path "pick"

  Scenario: A redelivered classification event id is applied only once
    Given process-path-management declares the paths:
      | pathId    | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick      | 45           | CUSTOMER_ORDER | -        | -        | -        |
      | pick-fast | 10           | CUSTOMER_ORDER | -        | -        | Hazmat   |
    And product-master has classified SKU "SKU-CHEM-0007" as "-" at version 1 in event "evt-1"
    When product-master publishes event "evt-1" classifying SKU "SKU-CHEM-0007" as "Hazmat" at version 2
    And an order is placed for SKUs "SKU-CHEM-0007"
    Then line 1 is on process path "pick-fast"
    When product-master publishes event "evt-2" classifying SKU "SKU-CHEM-0007" as "Hazmat" at version 2
    And an order is placed for SKUs "SKU-CHEM-0007"
    Then line 1 is on process path "pick"

  Scenario Outline: A classification that can never be applied is rejected and changes nothing
    Given process-path-management declares the paths:
      | pathId    | cycleMinutes | intake         | maxUnits | requires | excludes |
      | pick      | 45           | CUSTOMER_ORDER | -        | -        | -        |
      | pick-fast | 10           | CUSTOMER_ORDER | -        | -        | Hazmat   |
    When product-master publishes event "<event>" classifying SKU "<sku>" as "Hazmat" at version <version>
    Then the classification is rejected as invalid
    When an order is placed for SKUs "SKU-CHEM-0007"
    Then line 1 is on process path "pick-fast"

    Examples:
      | event  | sku           | version |
      |        | SKU-CHEM-0007 | 1       |
      | evt-9  |               | 1       |
      | evt-9  | SKU-CHEM-0007 | 0       |

  # --- Allocation: BR2 fail closed, BR3 ship-complete ------------------------

  Scenario: A hard failure before any line is allocated raises no partial-failure event
    Given inventory-storage is unreachable
    When an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    Then the request is accepted with status 201
    And the order status is "Received"
    And exactly 1 "OrderReceived" event was published
    And exactly 0 "OrderAllocationPartiallyFailed" events were published
    And exactly 0 "OrderLineBackordered" events were published
    And inventory-storage received 1 reservation request

  Scenario: Intake answers 201 with Pending lines when inventory-storage is not configured
    Given inventory-storage is not configured
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order status is "Received"
    And line 1 status is "Pending"
    And exactly 1 "OrderReceived" event was published

  Scenario: A retry while inventory-storage is not configured answers 503 downstream-not-configured
    Given inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed for SKUs "SKU-TOY-9999"
    And inventory-storage is not configured
    When the order is retried for allocation
    Then the request is rejected with status 503
    And the problem detail type is "downstream-not-configured"

  Scenario: A Pending line cannot be re-allocated by a retry, only cancelled and re-submitted
    Given inventory-storage is unreachable
    And an order is placed for SKUs "SKU-BOOK-0001"
    When the order is retried for allocation
    Then the request is rejected with status 409
    And the problem detail type is "no-backordered-lines"
    When the order is cancelled
    Then the request is accepted with status 204
    And exactly 0 reservations were revoked
    When the order is fetched
    Then the order status is "Cancelled"
    And line 1 status is "Cancelled"

  Scenario: Retrying an unknown order yields a not-found problem
    When the order "ord-does-not-exist" is retried for allocation
    Then the request is rejected with status 404
    And the problem detail type is "order-not-found"

  Scenario: Every 409 raises its own OrderLineBackordered and no order-level allocation event
    Given inventory-storage reports insufficient usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-0042"
    And inventory-storage reports insufficient usable stock for SKU "SKU-GAME-0007"
    When an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042,SKU-GAME-0007"
    Then the order status is "Backordered"
    And exactly 1 "OrderReceived" event was published
    And exactly 3 "OrderLineBackordered" events were published
    And no "OrderAllocated" event was published
    And no "OrderPartiallyAllocated" event was published

  Scenario: A clean two-line ship-complete order raises N plus 2 events and its release facts
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    When an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    Then exactly 1 "OrderReceived" event was published
    And exactly 2 "OrderLineAllocated" events were published
    And exactly 1 "OrderAllocated" event was published
    And exactly 0 "OrderLineBackordered" events were published
    And exactly 2 "OrderLineReleased" events were published
    And exactly 1 "OrderReleased" event was published

  Scenario: A hard failure on a retry keeps the line the retry did allocate
    Given inventory-storage reports insufficient usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-0042"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage is unreachable for SKU "SKU-TOY-0042"
    When the order is retried for allocation
    Then the request is rejected with status 503
    And the problem detail type is "downstream-unavailable"
    And exactly 1 "OrderAllocationPartiallyFailed" event was published
    And the last "OrderAllocationPartiallyFailed" event reports 1 allocated lines
    When the order is fetched
    Then the order status is "Backordered"
    And line 1 status is "Allocated"
    And line 2 status is "Backordered"
    And no "OrderReleased" event was published

  Scenario: A reservation that lapsed before the retry sends its line back to Backordered and blocks the release
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    And the reservation for SKU "SKU-BOOK-0001" has lapsed upstream
    And inventory-storage has usable stock for SKU "SKU-TOY-9999"
    When the order is retried for allocation
    Then the request is accepted with status 200
    And the order status is "Backordered"
    And line 1 status is "Backordered"
    And line 2 status is "Allocated"
    And exactly 2 "OrderLineBackordered" events were published
    And no "OrderLineReleased" event was published
    And no "OrderAllocated" event was published

  Scenario: A still-valid reservation is reconfirmed before release without raising a second allocation fact
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    And inventory-storage has usable stock for SKU "SKU-TOY-9999"
    When the order is retried for allocation
    Then the order status is "Released"
    And inventory-storage received 4 reservation requests
    And exactly 2 "OrderLineAllocated" events were published
    And the last "OrderAllocated" event released exactly the lines "1,2"
    And exactly 2 "OrderLineReleased" events were published
    And exactly 1 "OrderReleased" event was published

  Scenario: A partial shipment retry releases only the line it cleared and announces it once
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-0042"
    And inventory-storage reports insufficient usable stock for SKU "SKU-GAME-0007"
    And an order allowing partial shipment is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042,SKU-GAME-0007"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    When the order is retried for allocation
    Then the request is accepted with status 200
    And the order status is "PartiallyReleased"
    And line 1 status is "Released"
    And line 2 status is "Released"
    And line 3 status is "Backordered"
    And the last "OrderPartiallyAllocated" event released exactly the lines "2"
    And exactly 2 "OrderLineReleased" events were published
    And exactly 3 "OrderLineBackordered" events were published
    And no "OrderReleased" event was published

  # --- ReleaseHeldOrder (ADR 0020) ------------------------------------------

  Scenario: A held order's allocation announces no released lines and still carries its promise
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed on hold for SKUs "SKU-BOOK-0001"
    Then the order status is "Allocated"
    And the response field "promiseDate" is "2026-09-08T09:00:00Z"
    And the last "OrderAllocated" event released exactly the lines ""
    And no "OrderLineReleased" event was published

  Scenario: BR3 holds a ship-complete held order that has a backordered line, with 200 and no problem
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed on hold for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    When the order is released
    Then the request is accepted with status 200
    And the order status is "Backordered"
    And line 1 status is "Allocated"
    And line 2 status is "Backordered"
    And no "OrderLineReleased" event was published
    And no "OrderReleased" event was published

  Scenario: A release that cannot reconfirm its reservations answers 503 and releases nothing
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    And inventory-storage is unreachable for SKU "SKU-BOOK-0001"
    When the order is released
    Then the request is rejected with status 503
    And the problem detail type is "downstream-unavailable"
    When the order is fetched
    Then the order status is "Allocated"
    And line 1 status is "Allocated"
    And no "OrderLineReleased" event was published

  Scenario: Releasing a held order twice neither reconfirms nor announces anything the second time
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    And the order is released
    And the published events are remembered
    And the reservation requests are remembered
    When the order is released
    Then the request is accepted with status 200
    And no new reservation request was made
    And no new "OrderAllocated" event was published
    And no new "OrderLineReleased" event was published

  # --- CancelOrder (BR6) ------------------------------------------------------

  Scenario: Cancelling an order whose lines are all backordered revokes nothing
    Given inventory-storage reports insufficient usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-0042"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    When the order is cancelled
    Then the request is accepted with status 204
    And exactly 0 reservations were revoked
    And the last "OrderCancelled" event revoked 0 reservations
    When the order is fetched
    Then the order status is "Cancelled"

  Scenario: A cancellation that fails half way keeps the first revocation and can be retried to completion
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And an order is placed on hold for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    And inventory-storage cannot revoke the reservation for SKU "SKU-TOY-0042"
    When the order is cancelled
    Then the request is rejected with status 503
    And the problem detail type is "downstream-unavailable"
    And exactly 1 reservation was revoked
    And no "OrderCancelled" event was published
    When the order is fetched
    Then the order status is "Allocated"
    Given inventory-storage can revoke reservations again
    When the order is cancelled
    Then the request is accepted with status 204
    And exactly 2 reservations were revoked
    And the last "OrderCancelled" event revoked 2 reservations

  Scenario: A rejected cancellation of a released order leaves the order and inventory untouched
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the reservation requests are remembered
    When the order is cancelled
    Then the request is rejected with status 409
    And the problem detail type is "order-already-released"
    And exactly 0 reservations were revoked
    And no new reservation request was made
    When the order is fetched
    Then the order status is "Released"

  Scenario: A cancelled order has nothing left to retry
    Given inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed for SKUs "SKU-TOY-9999"
    And the order is cancelled
    And the reservation requests are remembered
    When the order is retried for allocation
    Then the request is rejected with status 409
    And the problem detail type is "no-backordered-lines"
    And no new reservation request was made

  # --- Demand projection (ADR 0035 / ADR 0044) --------------------------------

  Scenario: A retry projects demand only for the line it newly allocated
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-0042"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042" at fulfillment site "WH2"
    Then exactly 1 "SiteSkuDemandChanged" event was published
    And the last "SiteSkuDemandChanged" event is for line 1 due at "2026-09-08T09:00:00Z"
    Given inventory-storage has usable stock for SKU "SKU-TOY-0042"
    When the order is retried for allocation
    Then exactly 2 "SiteSkuDemandChanged" events were published
    And the last "SiteSkuDemandChanged" event is for line 2 due at "2026-09-08T09:00:00Z"

  # --- Promise (ADR 0014 / 0017 / 0020) ---------------------------------------

  Scenario: The promise is the earliest window the line can make and a window at exactly now plus the cycle time qualifies
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-0930 | 2026-09-07T09:30:00Z |
      | sp1-1000 | 2026-09-07T10:00:00Z |
      | sp1-1200 | 2026-09-07T12:00:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the response field "promiseDate" is "2026-09-07T10:00:00Z"
    And the last "OrderAllocated" event has promise basis "Capability"
    And the last "OrderAllocated" event has promise CPT "sp1-1000"

  Scenario: When no window can be made the promise falls back to lead time
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-0930 | 2026-09-07T09:30:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the response field "promiseDate" is "2026-09-08T09:00:00Z"
    And the last "OrderAllocated" event has promise basis "LeadTime"
    And the last "OrderAllocated" event has no promise CPT

  Scenario: A ship-complete order has one promise and its most constrained line governs it
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And path "pick" has capacity for 5 units before CPT "sp1-1200"
    When an order is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 2        | false    |
      | SKU-TOY-0042  | 8        | false    |
    Then the response field "promiseDate" is "2026-09-07T15:00:00Z"
    And the last "OrderAllocated" event released line 1 under promise CPT "sp1-1500"
    And the last "OrderAllocated" event released line 2 under promise CPT "sp1-1500"

  Scenario: A partial shipment promises each line to the window it can make and the order to the latest
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And path "pick" has capacity for 5 units before CPT "sp1-1200"
    When an order allowing partial shipment is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 2        | false    |
      | SKU-TOY-0042  | 8        | false    |
    Then the response field "promiseDate" is "2026-09-07T15:00:00Z"
    And the last "OrderAllocated" event released line 1 under promise CPT "sp1-1200"
    And the last "OrderAllocated" event released line 2 under promise CPT "sp1-1500"

  Scenario Outline: A window with <capacity> units left <outcome> a line of 8 units
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And path "pick" has capacity for <capacity> units before CPT "sp1-1200"
    When an order is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 8        | false    |
    Then the last "OrderAllocated" event has promise CPT "<cpt>"

    Examples:
      | capacity | outcome | cpt      |
      | 8        | admits  | sp1-1200 |
      | 7        | refuses | sp1-1500 |

  Scenario: A deadline cannot be met by a window the cycle time makes unreachable
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-0930 | 2026-09-07T09:30:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001" with required ship-by "2026-09-07T09:45:00Z"
    Then the request is accepted with status 201
    And line 1 has a reservation
    And the order has no promise date

  Scenario: A deadline with no CPT schedule known is never answered with a lead-time guess
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001" with required ship-by "2026-09-09T09:00:00Z"
    Then the request is accepted with status 201
    And the order status is "Released"
    And the order has no promise date

  Scenario: The deadline promise skips the latest window when it lacks capacity
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And path "pick" has capacity for 8 units before CPT "sp1-1200"
    And path "pick" has capacity for 3 units before CPT "sp1-1500"
    When an order of 8 units of SKU "SKU-BOOK-0001" is placed with required ship-by "2026-09-07T15:00:00Z"
    Then the response field "promiseDate" is "2026-09-07T12:00:00Z"
    And the last "OrderAllocated" event has promise basis "Network"
    And the last "OrderAllocated" event has promise CPT "sp1-1200"

  # --- Fulfillment class and work unit id ---------------------------------------

  Scenario Outline: A single line of <quantity> unit(s) is announced as <class>
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed with lines:
      | sku           | quantity   | giftWrap |
      | SKU-BOOK-0001 | <quantity> | false    |
    Then the last "OrderAllocated" event released line 1 on path "pick" with fulfillment class "<class>" and gift wrap false

    Examples:
      | quantity | class          |
      | 1        | SINGLE         |
      | 3        | SAME_SKU_MULTI |

  Scenario: Two lines of the same SKU are still a multi-line order
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 1        | false    |
      | SKU-BOOK-0001 | 1        | false    |
    Then the last "OrderAllocated" event released line 1 on path "pick" with fulfillment class "MULTI_LINE_MULTI" and gift wrap false

  Scenario: The work unit id of a released line is the order id, "-line-" and the line number
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    When an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    Then the OrderLineReleased event for line 1 carries the work unit id "{order}-line-1"
    And the OrderLineReleased event for line 2 carries the work unit id "{order}-line-2"

  # --- Planned capacity (ADR 0031 / ADR 0047) -------------------------------------

  Scenario: An order carrying its own site is annotated by that site's shortage, not the default site's
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    And warehouse-planning has published a shortage of 900 for site "WH2" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    When an order is placed for SKUs "SKU-BOOK-0001" at fulfillment site "WH2"
    Then the request is accepted with status 201
    And the order is capacity-constrained at site "WH2" by plan "plan-bdd-2"
    And the order is not constrained by plan "plan-bdd-1"

  Scenario Outline: A plan window that can never be valid is refused and nothing is stored
    When warehouse-planning event "evt-bad" for plan "plan-bad" is refused as invalid:
      | status   | shortage   | site | from   | to   | asOf                 |
      | <status> | <shortage> | SIM1 | <from> | <to> | 2026-09-07T08:00:00Z |
    Then no plan is listed for site "SIM1"

    Examples:
      | status    | shortage | from                 | to                   |
      | PUBLISHED | 100      | 2026-09-07T14:00:00Z | 2026-09-07T14:00:00Z |
      | PUBLISHED | 100      | 2026-09-07T20:00:00Z | 2026-09-07T14:00:00Z |
      | PUBLISHED | -5       | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z |
      | ARCHIVED  | 100      | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z |

  # --- Readiness (use-cases REST summary) ---------------------------------------

  Scenario: Readiness flips to 503 not_ready once shutdown starts while liveness stays ok
    When the service is probed for readiness
    Then the request is accepted with status 200
    And the response field "status" is "ready"
    Given the service begins shutting down
    When the service is probed for readiness
    Then the request is rejected with status 503
    And the response field "status" is "not_ready"
    When the service is probed for liveness
    Then the request is accepted with status 200
    And the response field "status" is "ok"
