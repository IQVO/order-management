# Planned capacity (ADR 0031), pinned to apis/openapi.yaml
# (components.schemas.Order.capacityConstraint and
# paths./planned-capacity — operationId getPlannedCapacity) and
# apis/asyncapi.yaml (the consumed warehouse-planning events).
#
# warehouse-planning publishes CapacityPlan events; order-management mirrors
# them into a local read model and ANNOTATES an order whose promise window
# overlaps a PUBLISHED shortage window at its site. It only annotates: the
# order is still accepted, allocated and released exactly as before, and its
# promise date is not moved (fill-or-kill, ADR 0017, is untouched).
#
# The scenario clock is 2026-09-07T09:00:00Z and the promise is 24h out, so
# every order below is promised for 2026-09-08T09:00:00Z; its work window is
# therefore [2026-09-07T09:00:00Z, 2026-09-08T09:00:00Z).
Feature: Planned capacity — a published shortage annotates the orders it overlaps
  An operator-published plan that says "this site cannot process the demand
  assigned to this window" must be visible on the orders promised across that
  window, without changing what the service accepts or promises.

  Scenario: A published shortage overlapping the promise annotates the order
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is capacity-constrained by plan "plan-bdd-1"

  Scenario: The annotation does not reject the order or move its promise
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order status is "Released"
    And line 1 status is "Released"
    And the response field "promiseDate" is "2026-09-08T09:00:00Z"
    And an "OrderAllocated" event was published
    And no "OrderRepromised" event was published

  Scenario: A later read of the order shows the same annotation
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    And an order is placed for SKUs "SKU-BOOK-0001"
    When the order is fetched
    Then the request is accepted with status 200
    And the order is capacity-constrained by plan "plan-bdd-1"

  Scenario: A shortage that begins exactly when the promise is due does not annotate
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-08T09:00:00Z" to "2026-09-08T17:00:00Z"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is not capacity-constrained

  Scenario: A shortage that begins one second before the promise is due annotates
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-08T08:59:59Z" to "2026-09-08T17:00:00Z"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is capacity-constrained by plan "plan-bdd-1"

  Scenario: A shortage at another site does not annotate
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has published a shortage of 4000 for site "SIM2" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is not capacity-constrained

  Scenario: A draft plan is not a published statement and does not annotate
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has created a draft plan with a shortage of 4000 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is not capacity-constrained

  Scenario: With no planning events the order is exactly as before
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is not capacity-constrained
    And the order has a promise date

  Scenario: The read model lists a site's planned windows with their status
    Given the Order Management service is running
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    And warehouse-planning has created a draft plan with a shortage of 17 for site "SIM1" from "2026-09-09T08:00:00Z" to "2026-09-09T16:00:00Z"
    And warehouse-planning has published a shortage of 90 for site "SIM2" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    When planned capacity is requested for site "SIM1"
    Then the request is accepted with status 200
    And the planned capacity lists plan "plan-bdd-1" as "PUBLISHED" with a shortage of 4000
    And the planned capacity lists plan "plan-bdd-2" as "DRAFT" with a shortage of 17
    And the planned capacity lists no plan "plan-bdd-3"

  Scenario: Planned capacity needs a site
    Given the Order Management service is running
    When planned capacity is requested without a site
    Then the request is rejected with status 400
