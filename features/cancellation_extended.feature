# Derived from: apis/openapi.yaml paths./orders/{id}.delete (cancelOrder) and
# .claude/rules/domain-model.md, BR6: cancellation is legal only while no
# line has reached Released; "the boundary is checked BEFORE any reservation
# is revoked, so a rejected cancellation leaves inventory-storage untouched";
# a legal cancellation revokes every allocated line's reservation and raises
# OrderCancelled (domain-events table).
@bdd
Feature: Cancellation boundary — corners beyond the basic BR6 scenarios
  A partially released order is as un-cancellable as a fully released one,
  and a held order (allocated, never released) is cancellable like any other
  pre-release order.

  Scenario: A partially released order is rejected without touching inventory
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order allowing partial shipment is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    When the order is cancelled
    Then the request is rejected with status 409
    And the problem detail title is "Order already has released lines and can no longer be cancelled"
    And exactly 0 reservations were revoked
    And no "OrderCancelled" event was published
    When the order is fetched
    Then the order status is "PartiallyReleased"

  Scenario: Cancelling a held order gives back every reservation
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And an order is placed on hold for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    When the order is cancelled
    Then the request is accepted with status 204
    And the reservation for SKU "SKU-BOOK-0001" was revoked
    And the reservation for SKU "SKU-TOY-0042" was revoked
    And exactly 2 reservations were revoked
    And the last "OrderCancelled" event revoked 2 reservations
    When the order is fetched
    Then the order status is "Cancelled"
