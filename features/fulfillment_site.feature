# Derived from: apis/openapi.yaml
#   - components.schemas.ReceiveOrderRequest.fulfillmentSiteId and
#     components.schemas.Order.fulfillmentSiteId (optional, caller-assigned,
#     immutable; ^[A-Z0-9]+$)
#   - paths./orders.post "400" (problem type invalid-fulfillment-site-id)
# and apis/asyncapi.yaml SiteSkuDemandChanged.assignment_version
# (order-site-v1 | static-site-v1); docs/docs/adr/0044 amends ADR 0035 §4.
# The scenario clock is 2026-09-07T09:00:00Z.
@bdd
Feature: Caller-assigned fulfillment site on the order
  A caller (customer-ordering or network-fulfillment) may name the site an
  order is to be fulfilled from. This service only carries it: demand for
  the order's lines is projected to THAT site, and an order without one
  keeps projecting to the deployment's static site exactly as before.

  Scenario: The order's site wins over the static demand site
    Given the Order Management service is running
    And the deployment projects demand to the static site "SIM1"
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001" at fulfillment site "WH2"
    Then the request is accepted with status 201
    And the response field "fulfillmentSiteId" is "WH2"
    And the last "SiteSkuDemandChanged" event is for site "WH2" with assignment version "order-site-v1" and state "ACTIVE"

  Scenario: An order without a site falls back to the static demand site
    Given the Order Management service is running
    And the deployment projects demand to the static site "SIM1"
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the response has no field "fulfillmentSiteId"
    And the last "SiteSkuDemandChanged" event is for site "SIM1" with assignment version "static-site-v1" and state "ACTIVE"

  Scenario: An order without a site and no static site projects no demand
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the response has no field "fulfillmentSiteId"
    And no "SiteSkuDemandChanged" event was published

  Scenario: A site works with no static site configured
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001" at fulfillment site "WH2"
    Then the request is accepted with status 201
    And the last "SiteSkuDemandChanged" event is for site "WH2" with assignment version "order-site-v1" and state "ACTIVE"

  Scenario: A network-originated hold keeps its site through release
    Given the Order Management service is running
    And the deployment projects demand to the static site "SIM1"
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    When a network-originated hold is placed for SKUs "SKU-BOOK-0001" at fulfillment site "DC7" with required ship-by "2026-09-07T15:00:00Z"
    Then the request is accepted with status 201
    And the response field "fulfillmentSiteId" is "DC7"
    And the order status is "Allocated"
    And the last "SiteSkuDemandChanged" event is for site "DC7" with assignment version "order-site-v1" and state "ACTIVE"
    When the order is released
    Then the request is accepted with status 200
    And the response field "fulfillmentSiteId" is "DC7"

  Scenario: Cancelling removes the demand at the same site
    Given the Order Management service is running
    And the deployment projects demand to the static site "SIM1"
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And a network-originated hold is placed for SKUs "SKU-BOOK-0001" at fulfillment site "WH2" with required ship-by "2026-09-07T15:00:00Z"
    When the order is cancelled
    Then the request is accepted with status 204
    And the last "SiteSkuDemandChanged" event is for site "WH2" with assignment version "order-site-v1" and state "REMOVED"

  Scenario Outline: A malformed fulfillment site is rejected and nothing is persisted
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001" at fulfillment site "<site>"
    Then the request is rejected with status 400
    And the problem detail type is "invalid-fulfillment-site-id"
    And no "OrderReceived" event was published

    Examples:
      | site  |
      |       |
      | wh1   |
      | Wh1   |
      | WH-1  |
      | WH 1  |
