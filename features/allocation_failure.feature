Feature: Fail-closed allocation
  A 409 from inventory-storage is a BUSINESS FACT (Backordered). A
  transport failure or 5xx is NOT: nothing is silently marked backordered.
  The implicit allocation attempt at intake is best-effort — the order was
  genuinely received — while an explicit retry propagates the hard failure.

  Scenario: An unreachable supplier leaves the order Received, never backordered
    Given the Order Management service is running
    And inventory-storage is unreachable
    When an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    Then the request is accepted with status 201
    And the order status is "Received"
    And line 1 status is "Pending"
    And line 2 status is "Pending"

  Scenario: An explicit retry surfaces the hard failure
    Given the Order Management service is running
    And inventory-storage reports insufficient usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    And inventory-storage is unreachable
    When the order is retried for allocation
    Then the request is rejected with status 503
    And the problem detail type is "downstream-unavailable"
