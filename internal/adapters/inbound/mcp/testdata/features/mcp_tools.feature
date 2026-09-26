# MCP behavioral evals for the order-management tool surface.
#
# Each scenario drives tools/call over the REAL Streamable HTTP handler
# with a connected SDK client — exactly the call a model host makes — and
# pins the structured result. Arguments are deliberately model-realistic:
# extra keys, wrong types, unknown ids, missing required bounds.
#
# This context exposes NO write tool over MCP (tools.go's Deps doc
# comment: CancelOrder, ReceiveOrder, Allocate and RetryAllocate are
# order-lifecycle commands with real business invariants, not decisions
# an MCP-calling agent should make on this context's behalf), so the
# write-tool happy/error/side-effect coverage requirement is structurally
# N/A — every scenario below is a read, and the read-only surface itself
# is pinned by the E1 golden registry.
#
# Derived from the tool contracts documented in:
#   - internal/adapters/inbound/mcp/tools.go and promise_health.go (tool
#     descriptions and semantics: get_order reuses the GetOrder use case
#     behind GET /orders/{id}; get_promise_health aggregates the Order
#     Funnel & Allocation Health data product's promise KPIs)
#   - docs/docs/mcp/governance-charter.md (§2 tool curation: intent-level
#     tools; this server's deliberately empty resource/prompt surfaces)
#   - mapping.go (the DTO contract: reservationId omitted, not "", when a
#     line is not allocated)
#   - apis/openapi.yaml GET /orders/{id} — the same read model the
#     get_order tool serves.

Feature: MCP tool behavioral evals
  The order-management MCP tools expose this bounded context to AI
  agents: one order's current state by id, and the promise-health KPIs of
  the Order Funnel & Allocation Health data product. An agent relying on
  them must get the same semantics the REST API guarantees, through the
  schema-decoded argument path a model host actually uses.

  Background:
    Given the MCP server is running with the canonical eval state (order ORD-1 with one allocated and one backordered line, one hour of promise-health analytics)

  Scenario: get_order returns the order's current state, line by line
    When I call the tool "get_order" with argument "orderId" = "ORD-1"
    Then the tool call succeeds
    And the structured result field "id" is "ORD-1"
    And the structured result field "status" is "PartiallyAllocated"
    And the structured result field "allowPartialShipment" is true
    And the structured result line 1 field "lineNo" is 1
    And the structured result line 1 field "sku" is "SKU-1"
    And the structured result line 1 field "quantity" is 2
    And the structured result line 1 field "pathId" is "pick"
    And the structured result line 1 field "giftWrap" is false
    And the structured result line 1 field "status" is "Allocated"
    And the structured result line 1 field "reservationId" is "RES-1"
    And the structured result line 2 field "sku" is "SKU-2"
    And the structured result line 2 field "giftWrap" is true
    And the structured result line 2 field "status" is "Backordered"

  Scenario: An unallocated line omits reservationId rather than sending an empty string
    mapping.go's contract: "not allocated" must be unambiguous to a
    calling model, so the field is absent — not "" and not null.
    When I call the tool "get_order" with argument "orderId" = "ORD-1"
    Then the tool call succeeds
    And the structured result line 2 omits "reservationId"

  Scenario: get_order is side-effect free — reading twice changes nothing
    This context exposes no write tool; the strongest side-effect pin
    available is idempotence of the read itself.
    When I call the tool "get_order" with argument "orderId" = "ORD-1"
    Then the tool call succeeds
    When I call the tool "get_order" with argument "orderId" = "ORD-1"
    Then the tool call succeeds
    And the structured result field "status" is "PartiallyAllocated"
    And the structured result line 1 field "reservationId" is "RES-1"

  Scenario: An unknown order id is a clean tool error
    When I call the tool "get_order" with argument "orderId" = "ORD-NOPE"
    Then the tool call reports a problem mentioning "order not found"

  Scenario: An empty order id is a clean tool error
    When I call the tool "get_order" with argument "orderId" = ""
    Then the tool call reports a problem mentioning "order id"

  Scenario: A wrong-typed argument is rejected without coercion
    When I call the tool "get_order" with argument "orderId" = 42
    Then the tool call does not succeed silently

  Scenario: Model chatter in the arguments is rejected, not ignored
    The typed tool schemas are strict (additionalProperties: false, the
    SDK default): a host forwarding stray model-generated keys gets a
    clean schema validation error rather than a silent ignore.
    When I call the tool "get_order" with arguments
      | orderId       | ORD-1                      |
      | model_chatter | what's the state of it?    |
      | step          | 2                          |
    Then the tool call reports a problem mentioning "model_chatter"

  Scenario: get_promise_health aggregates the window's promise KPIs
    Seeded at 09:00Z: one Capability-basis allocation with a 4h cutoff
    and a split shipment, one LeadTime-basis allocation, one fleet-wide
    re-promise.
    When I call the tool "get_promise_health" with arguments
      | from | 2026-09-14T08:00:00Z |
      | to   | 2026-09-14T10:00:00Z |
    Then the tool call succeeds
    And the structured result field "promiseBasisCapability" is 1
    And the structured result field "promiseBasisLeadTime" is 1
    And the structured result field "ordersAllocatedTotal" is 2
    And the structured result field "ordersSplitShipment" is 1
    And the structured result field "splitShipmentRate" is 0.5
    And the structured result field "ordersRepromised" is 1
    And the structured result field "repromiseRate" is 0.5
    And the structured result field "promiseToCutoffGapSeconds" is 14400

  Scenario: Filtering to a path zeroes the fleet-wide re-promise KPI
    OrdersRepromised carries no path dimension (promise_health.go: the
    re-promise row is fleet-wide, pathId ""), so an exact-match path
    filter excludes it — documented on the tool description, pinned here
    so it is a visible contract rather than a surprise.
    When I call the tool "get_promise_health" with arguments
      | from   | 2026-09-14T08:00:00Z |
      | to     | 2026-09-14T10:00:00Z |
      | pathId | pick                 |
    Then the tool call succeeds
    And the structured result field "ordersAllocatedTotal" is 2
    And the structured result field "ordersRepromised" is 0
    And the structured result field "repromiseRate" is 0.0

  Scenario: An empty window still reports zeros, not an error
    When I call the tool "get_promise_health" with arguments
      | from | 2026-10-01T00:00:00Z |
      | to   | 2026-10-01T01:00:00Z |
    Then the tool call succeeds
    And the structured result field "ordersAllocatedTotal" is 0
    And the structured result field "splitShipmentRate" is 0.0

  Scenario: A non-RFC3339 window bound is a clean tool error
    When I call the tool "get_promise_health" with arguments
      | from | yesterday-ish          |
      | to   | 2026-09-14T10:00:00Z |
    Then the tool call reports a problem mentioning "RFC3339"

  Scenario: A wrong-typed window bound is rejected without coercion
    When I call the tool "get_promise_health" with argument "from" = 42
    Then the tool call does not succeed silently

  Scenario: Omitting the required window bounds is rejected
    When I call the tool "get_promise_health" with no arguments
    Then the tool call does not succeed silently
