# Derived from: apis/openapi.yaml paths./orders/{id}/release (releaseHeldOrder)
# and paths./orders/{id}/retry-allocation, and .claude/rules/adrs.md entry 20
# (ADR 0020): a held order allocates and stops; a later retry-allocation must
# not release work the caller never committed to; release goes through "the
# same BR3-gated flow" and fires "the same OrderAllocated/OrderReleased
# events with the same per-line payload"; reservations that lapsed upstream
# are re-confirmed before release (.claude/rules/domain-model.md, OrderLine
# invariants: Order.LoseReservation).
@bdd
Feature: Held orders — interaction with retry, expiry and events
  The hold set at intake survives a backorder and a retry, a release
  re-confirms stock before committing work to the floor, and the release
  announces the released lines to wes-work-planning.

  Scenario: Retrying a held backordered order allocates but does not release
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed on hold for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    And inventory-storage has usable stock for SKU "SKU-TOY-9999"
    When the order is retried for allocation
    Then the request is accepted with status 200
    And the order status is "Allocated"
    And line 1 status is "Allocated"
    And line 2 status is "Allocated"
    And no "OrderReleased" event was published
    And no "OrderLineReleased" event was published
    When the order is released
    Then the request is accepted with status 200
    And the order status is "Released"

  Scenario: A release whose reservation lapsed re-backorders the line instead of releasing
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-BOOK-0001"
    When the order is released
    Then the request is accepted with status 200
    And the order status is "Backordered"
    And line 1 status is "Backordered"
    And no "OrderLineReleased" event was published
    And no "OrderReleased" event was published

  Scenario: Releasing a held order announces the released lines
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And an order is placed on hold for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    When the order is released
    Then the request is accepted with status 200
    And exactly 2 "OrderAllocated" events were published
    And the last "OrderAllocated" event released exactly the lines "1,2"
    And exactly 2 "OrderLineReleased" events were published
    And exactly 1 "OrderReleased" event was published
