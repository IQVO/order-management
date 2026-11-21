# Derived from: AGENTS.md and .claude/rules/domain-model.md "Planned
# capacity" (ADR 0053: "a plan matches by its site_id when it has one,
# else by its location, so a plan for another site is not applied to this
# order just because a location string matches"; "a write without a
# site_id never erases a stored one"), docs/docs/ddd/use-cases.md §8 ("a
# DRAFT never overwrites a PUBLISHED plan") and the domain rules in
# internal/domain/order/planned_capacity.go (Supersedes: "a later (or
# equal) occurred-at wins"). Events are fed through ApplyPlannedCapacity,
# the use case the Kafka consumer drives; the read is REST-only.
# The scenario clock is 2026-09-07T09:00:00Z and the promise is 24h out,
# so every order below works across [2026-09-07T09:00:00Z, 2026-09-08T09:00:00Z).
Feature: Planned capacity — site identity and statement ordering
  A capacity plan belongs to an order's site by its own site id when it
  carries one, else by its location; nothing that arrives later un-says a
  published statement.

  Scenario: A plan carrying another site's id does not constrain an order whose site matches only its location
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning event "evt-s1" states plan "plan-S" carrying site id "WH2":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 4000     | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is not capacity-constrained
    # The read model's `site` parameter IS the planning location (openapi:
    # getPlannedCapacity.site), so the plan is listed there — only the
    # order annotation (ADR 0053 MatchesSite) refuses the location match.
    When planned capacity is requested for site "SIM1"
    Then the planned capacity lists plan "plan-S" as "PUBLISHED" with a shortage of 4000

  Scenario: A plan whose site id matches the order's site constrains it whatever its location says
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning event "evt-s2" states plan "plan-S" carrying site id "SIM1":
      | status    | shortage | site      | from                 | to                   | asOf                 |
      | PUBLISHED | 4000     | WHATEVER  | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is capacity-constrained at site "SIM1" by plan "plan-S"

  Scenario: A later statement of a plan without a site id keeps matching the stored site
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning event "evt-s3" states plan "plan-S" carrying site id "SIM1":
      | status    | shortage | site    | from                 | to                   | asOf                 |
      | PUBLISHED | 4000     | LEGACY  | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    And warehouse-planning event "evt-s4" states plan "plan-S":
      | status    | shortage | site      | from                 | to                   | asOf                 |
      | PUBLISHED | 900      | WHATEVER  | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T09:00:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is capacity-constrained at site "SIM1" by plan "plan-S"
    # The later statement applied (its new location now answers the read)
    # and the stored site id still carries the SIM1 match above.
    When planned capacity is requested for site "WHATEVER"
    Then the planned capacity lists plan "plan-S" as "PUBLISHED" with a shortage of 900

  Scenario: A draft that arrives after publication never un-publishes the plan
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning event "evt-p1" states plan "plan-D":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 4000     | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    And warehouse-planning event "evt-p2" states plan "plan-D":
      | status | shortage | site | from                 | to                   | asOf                 |
      | DRAFT  | 900      | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T09:00:00Z |
    When an order is placed for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is capacity-constrained by plan "plan-D"
    When planned capacity is requested for site "SIM1"
    Then the planned capacity lists plan "plan-D" as "PUBLISHED" with a shortage of 4000

  Scenario: A re-statement of a plan at the same event time applies
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning event "evt-e1" states plan "plan-E":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 4000     | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    And warehouse-planning event "evt-e2" states plan "plan-E":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 700      | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    When planned capacity is requested for site "SIM1"
    Then the request is accepted with status 200
    And the planned capacity lists plan "plan-E" as "PUBLISHED" with a shortage of 700
