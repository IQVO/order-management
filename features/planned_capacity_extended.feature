# Derived from: apis/openapi.yaml paths./planned-capacity (getPlannedCapacity:
# windows "ordered by start", `from` "only windows whose end is after it are
# listed", 400 for a `from` that is not RFC 3339), components.schemas.
# CapacityConstraint (a cancelled order carries no annotation; derived at
# read time), and .claude/rules/domain-model.md "Planned capacity"
# (ADR 0031: fed only by warehouse-planning CloudEvents, last-writer-wins per
# plan id, idempotent on the CloudEvents id). Events are fed through
# ApplyPlannedCapacity, the use case the Kafka consumer drives.
# The scenario clock is 2026-09-07T09:00:00Z and the promise is 24h out.
@bdd
Feature: Planned capacity — read model ordering, filtering and event handling
  How the local planned-capacity read model orders and filters what it
  serves, and how it treats out-of-order and redelivered planning events.

  Scenario: A cancelled order no longer carries the capacity annotation
    Given the Order Management service is running
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And warehouse-planning event "evt-1" states plan "plan-A":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 4000     | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    When an order is placed on hold for SKUs "SKU-BOOK-0001"
    Then the request is accepted with status 201
    And the order is capacity-constrained by plan "plan-A"
    When the order is cancelled
    Then the request is accepted with status 204
    When the order is fetched
    Then the order status is "Cancelled"
    And the order is not capacity-constrained

  Scenario: Windows are listed by start and those ended before from are omitted
    Given the Order Management service is running
    And warehouse-planning event "evt-1" states plan "plan-late":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 10       | SIM1 | 2026-09-09T08:00:00Z | 2026-09-09T16:00:00Z | 2026-09-07T08:00:00Z |
    And warehouse-planning event "evt-2" states plan "plan-early":
      | status | shortage | site | from                 | to                   | asOf                 |
      | DRAFT  | 20       | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    And warehouse-planning event "evt-3" states plan "plan-ended":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 30       | SIM1 | 2026-09-07T06:00:00Z | 2026-09-07T12:00:00Z | 2026-09-07T08:00:00Z |
    And warehouse-planning event "evt-4" states plan "plan-edge":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 40       | SIM1 | 2026-09-07T10:00:00Z | 2026-09-07T12:00:01Z | 2026-09-07T08:00:00Z |
    When planned capacity is requested for site "SIM1" from "2026-09-07T12:00:00Z"
    Then the request is accepted with status 200
    And the planned capacity lists plans in order "plan-edge,plan-early,plan-late"

  Scenario: A from parameter that is not a timestamp is rejected
    Given the Order Management service is running
    When planned capacity is requested for site "SIM1" from "yesterday"
    Then the request is rejected with status 400
    And the problem detail type is "invalid-query-parameter"

  Scenario: An older event never overwrites a newer statement of the same plan
    Given the Order Management service is running
    And warehouse-planning event "evt-new" states plan "plan-A":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 500      | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:30:00Z |
    And warehouse-planning event "evt-old" states plan "plan-A":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 4000     | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    When planned capacity is requested for site "SIM1"
    Then the request is accepted with status 200
    And the planned capacity lists plan "plan-A" as "PUBLISHED" with a shortage of 500

  Scenario: A redelivered event id is applied only once
    Given the Order Management service is running
    And warehouse-planning event "evt-1" states plan "plan-A":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 100      | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:00:00Z |
    And warehouse-planning event "evt-1" states plan "plan-A":
      | status    | shortage | site | from                 | to                   | asOf                 |
      | PUBLISHED | 999      | SIM1 | 2026-09-07T14:00:00Z | 2026-09-07T20:00:00Z | 2026-09-07T08:30:00Z |
    When planned capacity is requested for site "SIM1"
    Then the request is accepted with status 200
    And the planned capacity lists plan "plan-A" as "PUBLISHED" with a shortage of 100
