# Domain events, pinned to .claude/rules/domain-model.md — the
# "Domain events" table (OrderReceived is raised when ReceiveOrder
# accepts a new order; OrderCancelled when CancelOrder succeeds) and
# "Use cases" (ReceiveOrder publishes OrderReceived unconditionally,
# even when the folded best-effort allocation pass cannot run), plus
# "Status" (order-level status is always derived from line statuses,
# with Cancelled reachable only from a pre-release state).
Feature: Domain events
  Past-tense facts on the event publisher, asserted through the same
  HTTP surface the other features drive.

  Scenario: OrderReceived is published even when allocation cannot run
    Given the Order Management service is running
    And inventory-storage is unreachable
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order status is "Received"
    And an "OrderReceived" event was published

  Scenario: A cancelled order reads back as Cancelled
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage reports insufficient usable stock for SKU "SKU-TOY-9999"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-9999"
    When the order is cancelled
    Then the request is accepted with status 204
    And an "OrderCancelled" event was published
    When the order is fetched
    Then the request is accepted with status 200
    And the order status is "Cancelled"
    And line 1 status is "Cancelled"
