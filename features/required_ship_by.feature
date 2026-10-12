# Derived from: apis/openapi.yaml
#   - components.schemas.ReceiveOrderRequest.requiredShipBy
#     (the promise is CONSTRAINED to the LATEST CPT window at or before the
#     deadline; when it cannot be met the order is still created and
#     allocated but comes back with NO promiseDate; no lead-time fallback)
#   - .claude/rules/domain-model.md, "Promise" (PromisePolicy.FeasibleBy,
#     PromiseBasis=Network, ADR-0020)
# The scenario clock is 2026-09-07T09:00:00Z; the path "pick" has a 60 minute
# P95 cycle time and every window below is eligible for it.
@bdd
Feature: Required ship-by — a fill-or-kill promise
  An externally dictated deadline turns the promise from "the earliest we can
  manage" into "the latest departure that still meets the deadline", and
  refuses to guess when no departure does.

  Scenario: The promise is the latest window at or before the deadline
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
      | sp1-1800 | 2026-09-07T18:00:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001" with required ship-by "2026-09-07T15:00:00Z"
    Then the request is accepted with status 201
    And the response field "requiredShipBy" is "2026-09-07T15:00:00Z"
    And the response field "promiseDate" is "2026-09-07T15:00:00Z"
    And the last "OrderAllocated" event has promise basis "Network"

  Scenario: A deadline no window can meet yields an allocated order with no promise
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001" with required ship-by "2026-09-07T09:30:00Z"
    Then the request is accepted with status 201
    And line 1 has a reservation
    And the order has no promise date
