# REST-contract edge paths, each pinned to apis/openapi.yaml:
#   - intake validation problems .... components.responses.BadRequest,
#                                   paths./orders.post.responses."422",
#                                   ReceiveOrderRequest.lines (minItems 1)
#   - unknown-order reads ........... components.responses.OrderNotFound
#   - liveness probe ................ paths./healthz.get (getHealthz)
#   - Location header ............... paths./orders.post.responses."201"
#   - read-only pathId .............. components.schemas.OrderLine.pathId
#                                     (.claude/rules/domain-model.md,
#                                     "pathId is internal-only (ADR-0005)")
#   - no-backordered-lines guard .... paths./orders/{id}/retry-allocation.post
#                                     responses."409"
Feature: REST contract edge paths
  Documented corners of the REST surface the lifecycle features do not
  exercise: RFC 7807 problems for invalid intake, unknown-order reads,
  the liveness probe, the created-order Location header, the read-only
  process path, and retry-allocation's caller-mistake guard.

  Scenario: An empty SKU is rejected at intake
    Given the Order Management service is running
    When an order is placed with an empty SKU
    Then the request is rejected with status 400
    And the problem detail title is "SKU must not be empty"

  Scenario: A zero quantity is rejected at intake
    Given the Order Management service is running
    When an order is placed with quantity 0
    Then the request is rejected with status 422
    And the problem detail title is "Quantity must be greater than zero"

  Scenario: An order with no lines is rejected at intake
    Given the Order Management service is running
    When an order is placed with no lines
    Then the request is rejected with status 400
    And the problem detail title is "An order must have at least one line"

  Scenario: Reading an unknown order yields a not-found problem
    Given the Order Management service is running
    When the order "ord-does-not-exist" is fetched
    Then the request is rejected with status 404
    And the problem detail title is "Order not found"

  Scenario: The liveness probe answers ok without checking suppliers
    Given the Order Management service is running
    And inventory-storage is unreachable
    When the service is probed for liveness
    Then the request is accepted with status 200
    And the response field "status" is "ok"

  Scenario: A created order advertises its Location
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the Location header points to the created order

  Scenario: Every line carries the internal default process path
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And line 1 is on process path "pick"

  Scenario: Retrying an order with nothing backordered is a caller mistake
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    When the order is retried for allocation
    Then the request is rejected with status 409
    And the problem detail title is "Order has no backordered lines to retry"
