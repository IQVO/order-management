# A superseded capacity plan stops constraining orders (ADR 0052), pinned to
# apis/asyncapi.yaml (the consumed CapacityPlanSuperseded) and
# apis/openapi.yaml (PlannedCapacityWindow.status SUPERSEDED, supersededBy).
#
# warehouse-planning (its ADR 0024) moves an older PUBLISHED plan of the
# same window to SUPERSEDED when a newer one is published, and says so with
# CapacityPlanSuperseded. order-management keeps the row as SUPERSEDED: it
# never constrains an order again, and nothing that arrives later revives it.
#
# The scenario clock is 2026-09-07T09:00:00Z and the promise is 24h out, so
# every order below works across [2026-09-07T09:00:00Z, 2026-09-08T09:00:00Z).
Feature: Planned capacity — a superseded plan stops constraining orders
  Only the newest published plan of a window may annotate an order; the plan
  it replaced is still visible, as SUPERSEDED, on the planned-capacity read.

  Scenario: The superseded plan no longer annotates the order; the newer plan does
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    And warehouse-planning has published a shortage of 1200 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    And warehouse-planning has superseded plan "plan-bdd-1" by plan "plan-bdd-2" for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is capacity-constrained by plan "plan-bdd-2"
    And the order is not capacity-constrained by plan "plan-bdd-1"

  Scenario: A superseded plan stays superseded when its Published arrives late
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning has superseded plan "plan-bdd-1" by plan "plan-bdd-9" for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is not capacity-constrained

  Scenario: The read model lists the superseded plan with its successor and its facts
    Given the Order Management service is running
    And warehouse-planning has published a shortage of 4000 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    And warehouse-planning has published a shortage of 1200 for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    And warehouse-planning has superseded plan "plan-bdd-1" by plan "plan-bdd-2" for site "SIM1" from "2026-09-07T14:00:00Z" to "2026-09-07T20:00:00Z"
    When planned capacity is requested for site "SIM1"
    Then the request is accepted with status 200
    And the planned capacity lists plan "plan-bdd-1" as superseded by plan "plan-bdd-2" with a shortage of 4000
    And the planned capacity lists plan "plan-bdd-2" as "PUBLISHED" with a shortage of 1200
