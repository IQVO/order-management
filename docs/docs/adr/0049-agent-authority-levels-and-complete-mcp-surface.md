---
id: 0049-agent-authority-levels-and-complete-mcp-surface
slug: /adr/0049-agent-authority-levels-and-complete-mcp-surface
title: "0049. Agent authority levels and complete MCP surface"
sidebar_label: "49. Agent authority and complete MCP surface"
sidebar_position: 49
description: "ADR 0049 — every MCP tool and A2A skill of this context declares authority L1 (it is read-only), the MCP handler goes stateless so it also answers the 2026-07-28 revision, and the server gains its first resource (the order read model at warehouse://order-management/orders/{orderId}) and first prompt (the investigate_order SOP). Amends ADR 0010."
---

# 0049. Agent authority levels and complete MCP surface

## Status

Accepted (2026-11-02). Applies the fleet decision "Agentic Standard v2"
(`docs/architecture/agentic-standard.md` and the fleet rule
`agents/fleet/agent-authority.md` in IQVO/warehouse-docs) to this context.
Extends [ADR 0010](./0010-mcp-inbound-adapter.md) (MCP) and
[ADR 0046](./0046-a2a-inbound-adapter.md) (A2A); changes nothing in
[ADR 0012](./0012-remove-rest-mcp-bearer-auth.md) (no auth).

## Context

Three gaps, all on the agent surfaces:

1. **Nothing declared how much an agent may do with a tool.** The only
   signal was the MCP `ReadOnlyHint` annotation and the A2A `read` tag —
   *whether* a call changes state, not *what level of authority* it
   represents.
2. **The Streamable HTTP handler ran with its default (stateful)
   options**, which advertises only the 2025-11-25 revision and older, so a
   client speaking the 2026-07-28 per-request handshake (`server/discover`)
   was refused with `-32022`.
3. **The charter parked this server at tools-only**: "deliberately no
   resources, resource templates, or prompts". A host mounting us got two
   tools and nothing loadable without a tool call.

## Decision

### Authority levels, declared where the tool is registered

Authority levels are the strings `L1`..`L4`. This context is read-only on
both agent surfaces, so every declaration is `L1`:

| Surface | Name | Level | Why |
|---|---|---|---|
| MCP | `get_order` | `L1` | reads facts of this context's own aggregate |
| MCP | `get_promise_health` | `L1` | reads the promise-health data product (a factual read, not a recommendation) |
| A2A | `order_status` | `L1` | reads; same level as the tool it fronts |
| A2A | `promise_health` | `L1` | reads; same level as the tool it fronts |

- **MCP** declares it in the tool's `Meta` under `com.warehouse/authority`
  (returned verbatim in `tools/list` `_meta`), through the fleet's pinned
  helper shape: `Meta: authorityMeta(authorityL1)` with the consts
  `authorityMetaKey`, `authorityL1..4` and `func authorityMeta(level string)
  mcp.Meta` in `tools.go`. No tool here is `L2` (nothing recommends) or
  `L3` (nothing writes); those constants stay pinned because the fleet's
  declaration shape requires them, and `TestAuthorityConstantsPinTheFleetContract`
  keeps them honest.
- **A2A** declares it as the skill tag `authority:L1` (read skills keep
  their `read` tag).
- **`L4` is not granted here** and never can be silently: no ADR in this
  context lets an agent act without a human-gated step, and the static
  sensor requires an `//authority:L4 <adr>` comment naming the granting ADR.

Three sensors enforce it: the runtime `TestEveryAgentToolDeclaresAuthority`
in both adapter packages, the static go/ast sensor copied verbatim from the
fleet template (`internal/architecture/mcp_authority_fitness_test.go`), and
the authority column in `testdata/tool_registry.golden` — a level can now
only change through a reviewed golden edit. The level is a label for
callers and reviewers; **it is not access control** (the surface stays
unauthenticated, like REST, per ADR 0012).

### Stateless MCP

`mcp.NewStreamableHTTPHandler` now receives
`&mcp.StreamableHTTPOptions{Stateless: true}`. No `Mcp-Session-Id` is
issued and the server answers `server/discover` for the 2026-07-28
revision, while a legacy `initialize` (for example `2025-06-18`) still
answers 200, so existing session clients keep working. Nothing in this
adapter relied on cross-call session state; a tool that ever needs it takes
an explicit, server-minted handle as an argument. `stateless_test.go` pins
all three wire facts with raw POSTs (no SDK client), and reverting the
handler to nil options was proven to fail the first two probes.

### A complete surface: one resource, one prompt

- **Resource** `warehouse://order-management/orders/{orderId}` — the order
  read model this context owns, scoped to one order, backed by the same
  `GetOrder` use case as `get_order`, JSON text, bounded by construction.
  Reading a missing order is an error, not an empty document.
- **Prompt** `investigate_order` — the operator SOP: read the order (tool
  or resource), interpret order/line statuses, call `get_promise_health`
  for the window when a promise slipped, escalate policy calls (expedite,
  split, substitute) to a human. It names tools but never calls them, and
  performs no writes — trivially, since this surface has none.

The charter's tools-only stance is amended in `docs/docs/mcp/governance-charter.md`
(§ E2 and "Status in this repository" now pin the one-resource/one-prompt
surface instead of the empty lists).

Resources and prompts are `L1` by nature and do not need the tool `Meta`.

## Consequences

- A reviewer reads an agent's reach off the registry, and a new tool cannot
  ship without declaring a level (three sensors fail otherwise).
- The A2A card carries `authority:L1` tags; card consumers that ignore
  unknown tags are unaffected.
- Legacy session-based SDK clients are unaffected (initialize still
  negotiates their revision); serving 2026-07-28 does not mean the fleet's
  clients speak it yet.
- Facts still travel as CloudEvents on Kafka; MCP still carries tools and
  A2A still carries delegation. Nothing here adds a producer, a transport
  or a write.
- The write tools this context deliberately does NOT expose (see
  [ADR 0010](./0010-mcp-inbound-adapter.md)) remain unexposed; this ADR
  grants no new authority to act.
