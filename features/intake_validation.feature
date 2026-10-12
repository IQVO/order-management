# Derived from: apis/openapi.yaml
#   - components.schemas.ReceiveOrderLineRequest.quantity (minimum 1)
#   - components.responses.BadRequest (a syntactically valid body with an
#     explicit null on a non-nullable field is rejected, not coerced)
#   - components.schemas.OrderLine.pathId and the description of
#     paths./orders.post ("pathId is visible on every line in the response,
#     but not settable on the request"; .claude/rules/domain-model.md,
#     "pathId is internal-only (ADR-0005)")
@bdd
Feature: Intake validation boundaries
  The edges of POST /orders that the existing contract scenarios do not
  reach: negative quantities, malformed or schema-violating JSON, and a
  caller trying to choose its own process path.

  Scenario: A negative quantity is rejected at intake
    Given the Order Management service is running
    When an order is placed with quantity -3
    Then the request is rejected with status 422
    And the problem detail title is "Quantity must be greater than zero"
    And the problem detail type is "non-positive-quantity"

  Scenario: A body that is not valid JSON is rejected
    Given the Order Management service is running
    When an order is posted with the body:
      """
      {"lines": [
      """
    Then the request is rejected with status 400
    And the problem detail title is "The request body is not valid JSON"
    And the problem detail type is "malformed-request-body"

  Scenario: An explicit null giftWrap is rejected rather than coerced to false
    Given the Order Management service is running
    When an order is posted with the body:
      """
      {"lines": [{"sku": "SKU-BOOK-0001", "quantity": 1, "giftWrap": null}]}
      """
    Then the request is rejected with status 400
    And the problem detail title is "The request body is not valid JSON"

  Scenario: A caller cannot choose the process path of a line
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order choosing process path "express" is placed for SKU "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And line 1 is on process path "pick"
