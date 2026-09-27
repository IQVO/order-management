# Held orders (ADR 0020), pinned to apis/openapi.yaml
# (components.schemas.ReceiveOrderRequest.releaseOnAllocation and
# paths./orders/{id}/release — operationId releaseHeldOrder) and
# .claude/rules/adrs.md entry 20: an optional intake flag holds the
# ADR-0005 folded saga after allocation, a later explicit release
# commits the order to the floor, that release is idempotent for a
# held order, an order that was never held answers 409, and holding
# together with allowPartialShipment is contradictory (422).
Feature: Held orders — release on demand
  A caller that must decide whether to commit before any work reaches
  the floor holds the order at intake: reservations genuinely exist and
  a promise is attached, but nothing releases until the caller says so.

  Scenario: A held order allocates without releasing
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed on hold for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order status is "Allocated"
    And line 1 status is "Allocated"
    And the order has a promise date
    And an "OrderAllocated" event was published
    And no "OrderReleased" event was published

  Scenario: Releasing a held order commits it to the floor
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    When the order is released
    Then the request is accepted with status 200
    And the order status is "Released"
    And line 1 status is "Released"

  Scenario: Releasing an already-released held order is idempotent
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    And the order is released
    And the published events are remembered
    When the order is released
    Then the request is accepted with status 200
    And the order status is "Released"
    And no new "OrderReleased" event was published

  Scenario: Releasing an order that was never held is rejected
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    When the order is released
    Then the request is rejected with status 409
    And the problem detail title is "Order was not held at intake and has nothing to release on demand"

  Scenario: A held order must be ship-complete
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed on hold allowing partial shipment for SKUs "SKU-BOOK-0001"
    Then the request is rejected with status 422
