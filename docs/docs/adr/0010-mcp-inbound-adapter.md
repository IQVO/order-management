---
id: 0010-mcp-inbound-adapter
title: 10. Model Context Protocol as an inbound adapter, not a new service
sidebar_label: 10. MCP inbound adapter
sidebar_position: 10
description: "Expose this bounded context to the AI ecosystem via an MCP server built as a second driving adapter over the existing GetOrder read use case -- Streamable HTTP, official Go SDK, static bearer-key auth, one curated read tool. This context exposes no write tool: ReceiveOrder, CancelOrder, Allocate, and RetryAllocate are all order-lifecycle commands with real business invariants, none a decision an MCP-calling agent should make."
---

# 10. Model Context Protocol as an inbound adapter, not a new service

## Status

**Accepted — auth section superseded by [ADR 0012](./0012-remove-rest-mcp-bearer-auth.md);** a second read-only tool, `get_promise_health`, was added by [ADR 0019](./0019-promise-kpis-on-order-funnel.md). The architecture-fitness suite this record deferred ("arch tests do not exist yet" in its original framing) now exists — see `internal/architecture`'s arch-go suite. The reference implementation and pilot for this pattern across
the estate is `fulfillment-execution` (its own ADR-0008); this record is
`order-management` adopting that same decision, adapted to a context whose
read surface is a single lookup-by-id.

## Context

The platform is being connected to the AI ecosystem (Claude, Cursor, ChatGPT,
agent frameworks) via the **Model Context Protocol (MCP)**: a client discovers
a server's *tools* (model-callable functions), *resources* (read-only
context), and *prompts* (reusable templates), then an LLM decides which to
call.

The forces, specific to this context:

- **The read surface is genuinely thin.** `GetOrder` (looked up by
  `OrderId`) is the *only* read use case this context has — `OrderRepo`
  itself exposes no listing or query capability beyond `FindByID`. An
  agent asking "what is the state of this order" needs exactly the answer
  `GetOrder` already gives the HTTP client; there is no second read
  question this context can answer today.
- **Every write use case here has a real business invariant behind it.**
  `ReceiveOrder` validates lines and mints an id; `CancelOrder` enforces
  `EnsureCancellable` (BR6: no cancellation once any line has Released);
  `Allocate`/`RetryAllocate` enforce the line-status state machine
  (`Backordered -> Allocated` ONLY via `RetryAllocate`). None of these is a
  transport wrapper around a trivial setter — each is a domain decision
  this fleet's own operators or choreographed Kafka consumers make, not
  something an MCP-calling agent should trigger directly.
- **The domain must not learn about MCP.** ADR-0001's dependency rule is
  load-bearing: domain depends on nothing, application depends on domain,
  adapters depend inward. A protocol whose shape is set by an external LLM
  ecosystem is precisely the kind of concern that must stay in an adapter.
- **MCP has an idiomatic Go path now.** The official **MCP Go SDK**
  (`github.com/modelcontextprotocol/go-sdk`) is a Tier-1 SDK, version-pinned
  in `go.mod` exactly like the other six contexts already do it.
- **This is an internal, non-user-facing deployment.** Per the fleet's MCP
  governance charter (`docs/docs/mcp/governance-charter.md`), a static bearer
  token is appropriate for this internal use; full OAuth 2.1 is deferred
  until a server faces real end users.

## Decision

**We will expose this bounded context to the AI ecosystem through an MCP
server built as a second driving adapter over the existing `GetOrder` read
use case -- leaving the domain and application layers untouched -- and,
because every write use case here is a real order-lifecycle command with a
domain invariant behind it, we will register exactly one READ tool and no
resource, no prompt, and no write tool.**

### The adapter, mirroring the HTTP one

A new `internal/adapters/inbound/mcp/` sits beside
`internal/adapters/inbound/http/` and `internal/adapters/inbound/kafka/`:

```
internal/adapters/inbound/mcp/
  server.go      MCP Server wiring (Go SDK), capability registration
  tools.go       the get_order tool handler -> calls the GetOrder use case
  mapping.go     the tool's output DTO, kept separate from the HTTP
                 adapter's own DTO even though the JSON shapes match today
  auth.go        bearer-key auth middleware (interface; OAuth-ready seam)
```

It depends inward on `application` exactly as the HTTP adapter does. No MCP
type appears in `internal/domain/**` or `internal/application/**`. The tool
handler calls the **same** `GetOrder` use case struct the HTTP handler
calls — never a parallel code path, never the domain directly.

### A separate `cmd/mcp` binary

The MCP server ships as its own composition root, `cmd/mcp/main.go`, reusing
the same `OrderRepo` selection (in-memory vs Postgres) as `cmd/order`. Four
deployables now exist from one module: the HTTP + Kafka-choreography service
(`cmd/order`), the MCP server (`cmd/mcp`), and the analytics writer/reader
pair (`cmd/order-projector`/`cmd/order-reports`, ADR-0006) -- each isolating
its own blast radius.

### Streamable HTTP only

The single supported transport is **Streamable HTTP**, matching every
sibling context.

### One curated READ tool -- not a resource, not a prompt

- `get_order` (read) -- one order's current state by id: status,
  allow-partial-shipment flag, promise date, and every line's
  SKU/quantity/process-path/gift-wrap/status/reservation-id.

Unlike the other MCP adopters in this fleet, **no resource is registered**:
a resource template here would just be the same `OrderId`-keyed lookup
behind a second, redundant surface — this context has no larger scoped
read model (like `facility-layout`'s site layout or `labor-performance`'s
scorecard) that a resource would meaningfully expose beyond what the tool
already returns. **No prompt is registered** either: there is no multi-step
navigation workflow here worth an operational SOP — the entire surface is
one question with one answer.

### No write tool -- but the scope seam is kept

Because every write use case here (`ReceiveOrder`, `CancelOrder`,
`Allocate`, `RetryAllocate`) is a genuine order-lifecycle command with a
domain invariant behind it, **no write tool is registered.** The
read/read-write `Scope` plumbing in `auth.go` is nonetheless kept identical
to the pilot: two key classes, the `scopeAllows` gate, and the
scope-parameterised tool wrapper all exist, and the one registered tool
requires `ScopeRead`. This keeps the pattern uniform across the seven
contexts and means that if a legitimate write use case is ever designed for
MCP here, exposing it is a single `ScopeReadWrite` registration with no
auth rework.

### Static bearer-key auth, behind an OAuth-ready seam

`auth.go` validates a per-client API key (from a Kubernetes Secret) on every
request; missing or invalid key returns `401` with a `WWW-Authenticate`
challenge; the key is never logged. The middleware is an **interface**, so an
OAuth 2.1 resource-server implementation can drop in later without touching
any tool handler.

### Reuse the existing observability

The adapter is instrumented with the same OpenTelemetry setup as the HTTP
boundary: a span per tool call (tool name, required scope, outcome). MCP
calls appear in traces next to HTTP and choreographed Kafka activity.

## Consequences

### Easier

- **The domain and application layers do not change at all.** MCP is purely
  additive; the dependency rule (ADR-0001) is preserved. This repo has no
  arch-go fitness test suite today (unlike the newer bounded contexts in
  this fleet), so there is nothing to extend here -- a gap that predates
  this ADR and is unrelated to it.
- **One read surface, two protocols.** HTTP and MCP call the same
  `GetOrder` use case, so behaviour is identical regardless of caller.
- **Nothing here can be mutated by an agent.** No write tool is registered,
  so the entire class of "an autonomous agent changed the wrong thing" risk
  does not exist for this server.
- **A genuinely honest minimal surface.** Rather than padding the tool
  count or inventing a resource/prompt for the sake of parity with other
  contexts' richer surfaces, this adapter exposes exactly what this
  context's read side actually supports today: one question, one answer.
- **It stays in Go, in one quality gate.** Unit-tested (17 tests: auth (8),
  scope gating (4), the one tool's read-use-case wrapping (3, including an
  order with mixed allocated/backordered lines seeded via the domain's own
  `Rehydrate` constructor), governance-charter checks (4), and full
  transport-level tests over a real in-process Streamable HTTP server (3)),
  linted, and CI-gated like every other package.

### Harder

- **A second deployable to run and secure.** `cmd/mcp` is another binary and
  image. Today it is built and run only by the `e2e-tests` black-box
  harness (matching the other six contexts' own current state) -- it is
  **not yet deployed to the live `warehouse` kind cluster** by
  `warehouse-infra`'s Terraform, and **not yet wired as a client inside
  `warehouse-ops-agent`** (T5): no existing T5 use case
  (`console_reports`, `dailybrief`, `flow_balance_advisory`,
  `order_lifecycle`, `stranded_reservation`) currently reaches this
  context via MCP rather than its existing REST client, and the charter's
  own "tools map to a real decision" rule means adding a redundant client
  would be premature. Both the live-cluster deployment and any additional
  T5 wiring are deliberately deferred as a fast-follow, for this context
  and for the other six equally (none of the seven fleet MCP servers is
  deployed live today).
- **Auth is deliberately minimal.** A static bearer key is appropriate for an
  internal, non-user-facing server, but does **not** cover user-facing,
  multi-tenant use.
- **The MCP spec is a moving target.** The SDK must stay pinned and
  revisited; deprecated features (`roots`/`sampling`) must be avoided.
- **A thin surface today does not mean it stays thin.** If this context's
  read side ever grows (e.g. a listing/query capability on `OrderRepo`),
  the tool-curation discipline from the charter applies just as much here
  as anywhere else in the fleet -- add tools around real agent decisions,
  never one per new endpoint.
- **LLM-chosen arguments are untrusted input.** The tool handler validates
  its input via `shared.NewOrderId`'s strict validation (reject an empty
  id, never default) -- stricter than what the HTTP DTO layer assumes,
  since the caller here is a model, not our own code.
