# mutation_closeout.feature: pins the rules whose mutants SURVIVED the
# documentation-derived scenarios (re-proof on the merged code). Each scenario
# names the one rule a surviving mutant had been free to break; the production
# comparison it pins is named in the PR table. The scenario clock is
# 2026-09-07T09:00:00Z; path "pick" has a 60 minute cycle time.
@bdd
Feature: Rules the documentation-derived scenarios left unpinned

  Background:
    Given the Order Management service is running

  # --- RepromiseOrder: idempotent on the Kafka event_id (use case 6, ADR 0018) -------

  Scenario: An event id already seen is never evaluated again, even when the promise would move now
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001"
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then no "OrderRepromised" event was published
    When the clock advances 210 minutes
    And fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then no "OrderRepromised" event was published
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T12:00:00Z"
    When fulfillment-execution reports TaskCPTMissed event "evt-m2" for line 1 of the order
    Then exactly 1 "OrderRepromised" event was published

  Scenario Outline: An order_ref that does not parse claims no event id, so the same id still applies to a valid one
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the clock advances 210 minutes
    When fulfillment-execution reports TaskCPTMissed event "evt-bad" for order_ref "<ref>"
    Then no "OrderRepromised" event was published
    When fulfillment-execution reports TaskCPTMissed event "evt-bad" for line 1 of the order
    Then exactly 1 "OrderRepromised" event was published

    Examples:
      | ref                |
      | {order}            |
      | {order}-line-0     |
      | {order}-line--1    |
      | {order}-line-      |
      | {order}-line-x     |
      | -line-1            |

  Scenario: A work unit id is split at its LAST line marker, so a parseable ref claims its event id
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed for SKUs "SKU-BOOK-0001"
    And the clock advances 210 minutes
    When fulfillment-execution reports TaskCPTMissed event "evt-two" for order_ref "ord-line-5-line-1"
    Then no "OrderRepromised" event was published
    When fulfillment-execution reports TaskCPTMissed event "evt-two" for line 1 of the order
    Then no "OrderRepromised" event was published

  Scenario: A signal for a line backordered after its promise was set is ignored
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed on hold for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    And inventory-storage reports insufficient usable stock for SKU "SKU-BOOK-0001"
    And the order is released
    And the clock advances 210 minutes
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then no "OrderRepromised" event was published
    When the order is fetched
    Then line 1 status is "Backordered"
    And line 2 status is "Allocated"

  Scenario: A held deadline order, whose lines are still Allocated, is re-promised within its deadline
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed on hold for SKUs "SKU-BOOK-0001" with required ship-by "2026-09-07T15:00:00Z"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1430 | 2026-09-07T14:30:00Z |
      | sp1-1800 | 2026-09-07T18:00:00Z |
    When fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then the last "OrderRepromised" event moved the promise from CPT "sp1-1500" to "sp1-1430" because of "TaskCPTMissed"

  Scenario: When one promise group moves only the demand of that group is re-emitted
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
      | sp1-1800 | 2026-09-07T18:00:00Z |
    And path "pick" has capacity for 5 units before CPT "sp1-1200"
    And path "pick" has capacity for 5 units before CPT "sp1-1500"
    And an order allowing partial shipment at fulfillment site "WH2" is placed with lines:
      | sku           | quantity | giftWrap |
      | SKU-BOOK-0001 | 2        | false    |
      | SKU-TOY-0042  | 8        | false    |
    Then the ACTIVE SiteSkuDemandChanged event for line 1 is due at "2026-09-07T12:00:00Z"
    And the ACTIVE SiteSkuDemandChanged event for line 2 is due at "2026-09-07T18:00:00Z"
    When the clock advances 210 minutes
    And fulfillment-execution reports TaskCPTMissed event "evt-m1" for line 1 of the order
    Then exactly 3 "SiteSkuDemandChanged" events were published
    And the last "SiteSkuDemandChanged" event is for line 1 due at "2026-09-07T15:00:00Z"

  # --- ConfirmRelease: the CloudEvents id is the idempotency key (use case 11, ADR 0043) ---

  Scenario: One CloudEvents id confirms one line and is a duplicate for any other
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042"
    When wes-work-planning confirms line 1 of the order in event "evt-w1"
    Then the confirmation outcome is "confirmed"
    When wes-work-planning confirms line 2 of the order in event "evt-w1"
    Then the confirmation outcome is "duplicate"
    And the confirmation of line 2 is "PENDING"
    When wes-work-planning confirms line 2 of the order in event "evt-w2"
    Then the confirmation outcome is "confirmed"
    And the confirmation of line 2 is "CONFIRMED"

  Scenario: A WorkUnitCreated without a CloudEvents id can never be applied
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And an order is placed for SKUs "SKU-BOOK-0001"
    When wes-work-planning publishes a WorkUnitCreated without a CloudEvents id for line 1 of the order
    Then the confirmation is refused as invalid
    And the confirmation of line 1 is "PENDING"

  Scenario: A backlog larger than one batch is flagged in full within a single sweep pass
    Given the release confirmation loop is on
    And inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And inventory-storage has usable stock for SKU "SKU-TOY-0042"
    And inventory-storage has usable stock for SKU "SKU-GAME-0007"
    And an order is placed for SKUs "SKU-BOOK-0001,SKU-TOY-0042,SKU-GAME-0007"
    And the clock advances 16 minutes
    When the housekeeping sweep runs with a confirmation timeout of 15 minutes in batches of 2 lines
    Then the sweep flagged 3 lines
    And exactly 3 "OrderReleaseUnconfirmed" events were published

  # --- ReleaseHeldOrder: a second release is a pure no-op (ADR 0020 section 1) -----------

  Scenario: Releasing a held order again later changes nothing, not even its promise
    Given inventory-storage has usable stock for SKU "SKU-BOOK-0001"
    And the site has CPT windows on path "pick" with a cycle time of 60 minutes:
      | cptId    | cutoffAt             |
      | sp1-1200 | 2026-09-07T12:00:00Z |
      | sp1-1500 | 2026-09-07T15:00:00Z |
    And an order is placed on hold for SKUs "SKU-BOOK-0001"
    And the order is released
    And the published events are remembered
    And the reservation requests are remembered
    And the clock advances 210 minutes
    When the order is released
    Then the request is accepted with status 200
    And the order status is "Released"
    And no new "OrderAllocated" event was published
    And no new "OrderReleased" event was published
    And no new reservation request was made
    When the order is fetched
    Then the response field "promiseDate" is "2026-09-07T12:00:00Z"
