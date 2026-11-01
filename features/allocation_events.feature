# Derived from: .claude/rules/domain-model.md — "Domain events" table
# (OrderAllocated / OrderPartiallyAllocated carry an enriched lines[] with the
# lines released in that pass; OrderLineBackordered per backordered line;
# OrderAllocationPartiallyFailed on a hard mid-allocation failure; "A fully
# backordered order emits no order-level event"), the FulfillmentClass
# definition (ADR 0008), BR2/BR3, and apis/openapi.yaml
# paths./orders/{id}/allocation-retries ("A line still short of stock stays
# Backordered (no error)").
@bdd
Feature: Allocation outcomes and the events that announce them
  What the allocation-then-release pass publishes for each outcome, asserted
  on the event payloads wes-work-planning relies on.

  Scenario: OrderAllocated lists every released line with its path, class and gift wrap
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    When an order is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 2        | true     |
      | SKU-TOY-0042  | 1        | false    |
    Then the request is accepted with status 201
    And the order status is "Released"
    And the last "OrderAllocated" event released exactly the lines "1,2"
    And the last "OrderAllocated" event released line 1 on path "pick" with fulfillment class "MULTI_LINE_MULTI" and gift wrap true
    And the last "OrderAllocated" event released line 2 on path "pick" with fulfillment class "MULTI_LINE_MULTI" and gift wrap false
    And the last "OrderAllocated" event has promise basis "LeadTime"

  Scenario: A partial shipment announces only the lines it released
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    When an order allowing partial shipment is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    Then the request is accepted with status 201
    And the last "OrderPartiallyAllocated" event reports 1 allocated and 1 backordered lines
    And the last "OrderPartiallyAllocated" event released exactly the lines "1"
    And exactly 1 "OrderLineReleased" event was published
    And no "OrderReleased" event was published
    And no "OrderAllocated" event was published

  Scenario: A fully backordered order has no promise and no order-level allocation event
    Given the Order Management service is running
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    When an order is placed for SKUs "SKU-TOY-9999"
    Then the request is accepted with status 201
    And the order status is "Backordered"
    And the order has no promise date
    And exactly 1 "OrderLineBackordered" event was published
    And no "OrderLineAllocated" event was published
    And no "OrderAllocated" event was published
    And no "OrderPartiallyAllocated" event was published

  Scenario: A hard failure mid-allocation keeps the reservation already made
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage is unreachable for SKU "SKU-TOY-0042"
    When an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    Then the request is accepted with status 201
    And the order status is "PartiallyAllocated"
    And line 1 status is "Allocated"
    And line 2 status is "Pending"
    And exactly 1 "OrderAllocationPartiallyFailed" event was published
    And the last "OrderAllocationPartiallyFailed" event reports 1 allocated and 1 remaining lines
    And no "OrderAllocated" event was published

  Scenario: A retry for a line that is still short of stock leaves it backordered without error
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    When the order is retried for allocation
    Then the request is accepted with status 200
    And the order status is "Backordered"
    And line 1 status is "Allocated"
    And line 2 status is "Backordered"
