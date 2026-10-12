# Derived from: apis/openapi.yaml
#   - components.responses.OrderNotFound on paths./orders/{id}.delete (cancelOrder)
#     and paths./orders/{id}/release.post (releaseHeldOrder)
#   - components.responses.DownstreamUnavailable on
#     paths./orders/{id}/retry-allocation.post and paths./orders/{id}.delete
#     ("never papered over with a 2xx"; a failed revoke "fails the whole
#     cancellation and leaves the order untouched, so retrying is safe")
#   - apis problem types: internal/adapters/inbound/http/errors.go
#     (order-not-found, downstream-unavailable)
@bdd
Feature: Error paths of the order operations
  Not-found answers for the operations that act on a caller-supplied id, and
  the fail-closed 503 when inventory-storage cannot be relied on.

  Scenario: Cancelling an unknown order yields a not-found problem
    Given the Order Management service is running
    When the order "ord-does-not-exist" is cancelled
    Then the request is rejected with status 404
    And the problem detail title is "Order not found"

  Scenario: Releasing an unknown order yields a not-found problem
    Given the Order Management service is running
    When the order "ord-does-not-exist" is released
    Then the request is rejected with status 404
    And the problem detail title is "Order not found"

  Scenario: An explicit retry reports a failing supplier as unavailable
    Given the Order Management service is running
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed for SKUs "SKU-TOY-9999"
    And inventory-storage answers every reservation with a server error
    When the order is retried for allocation
    Then the request is rejected with status 503
    And the problem detail type is "downstream-unavailable"
    And the problem detail title is "A downstream service is unavailable or answered unexpectedly"

  Scenario: A failed revoke leaves the order untouched so cancelling can be retried
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    And inventory-storage can no longer revoke reservations
    When the order is cancelled
    Then the request is rejected with status 503
    And the problem detail type is "downstream-unavailable"
    And no "OrderCancelled" event was published
    When the order is fetched
    Then the request is accepted with status 200
    And the order status is "Backordered"
    And line 1 status is "Allocated"
