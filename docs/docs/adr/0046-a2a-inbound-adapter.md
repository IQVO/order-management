---
id: 0046-a2a-inbound-adapter
slug: /adr/0046-a2a-inbound-adapter
title: "0046. A2A inbound adapter (third driving adapter, read-only)"
sidebar_label: "46. A2A inbound adapter"
sidebar_position: 46
description: "ADR 0046 — order-management is an Agent2Agent (A2A) agent through a third driving adapter over the same GetOrder use case and analytics report store as REST and MCP. Two curated goal-shaped READ skills, order_status and promise_health, in a message-only agent served by cmd/a2a on port 8093. There is no write skill because the MCP surface offers no write (ADR 0010): placing an order stays the one public command POST /orders (ADR 0005). A2A carries delegation only; facts still leave as CloudEvents on Kafka from the OLTP binary."
---

# 0046. A2A inbound adapter (third driving adapter, read-only)

## Status

Accepted (2026-10-29). Mirrors the reference rollout in
[workforce-management ADR-0039](https://iqvo.github.io/workforce-management/docs/adr/0039-a2a-inbound-adapter)
and [wes-work-planning ADR-0046](https://iqvo.github.io/wes-work-planning/docs/adr/0046-a2a-inbound-adapter);
follows [ADR-0010](./0010-mcp-inbound-adapter.md) (the MCP adapter this one is
modelled on), [ADR-0012](./0012-remove-rest-mcp-bearer-auth.md) (no
authentication on REST or MCP) and
[ADR-0030](./0030-cloudevents-mandatory-event-envelope.md) (CloudEvents on every
Kafka message).

## Context

The fleet is adopting the Agent2Agent protocol so that a bounded context can
**delegate** a goal to another context's agent. This context already exposes its
use cases over REST and, read-only, over MCP. It is the system of record for
customer orders, so what an agent may ask of it needs more care than elsewhere.

Three transports now carry three different things and must not overlap:

| Transport | Carries |
|---|---|
| Kafka + CloudEvents | **facts** between contexts ([ADR-0030](./0030-cloudevents-mandatory-event-envelope.md)) |
| MCP | an agent's **tools** |
| A2A | **delegation** of a goal to another context's agent |

## Decision

Add `internal/adapters/inbound/a2a`, a third driving adapter built on the
official `a2aproject/a2a-go/v2` SDK (pinned `v2.7.0`) and served by a new
`cmd/a2a` binary: JSON-RPC 2.0 over HTTP, port **8093**, the Agent Card at
`/.well-known/agent-card.json`, the endpoint mounted at `/` and `/a2a`,
`GET /healthz` for the probes.

### Read-only: the write gate

An A2A skill may change state only if this context's MCP surface already offers
that write. [ADR-0010](./0010-mcp-inbound-adapter.md) deliberately offers none:
`ReceiveOrder`, `CancelOrder`, `RetryAllocation` and `ReleaseHeldOrder` are
order-lifecycle commands with real business invariants, and
[ADR-0005](./0005-choreographed-release-via-kafka.md) makes `POST /orders` the
one public command. Delegation must never widen what the context exposes, so
**this agent has no write skill** and no order-placing path:

- `Deps` carries exactly the `GetOrder` read use case and the analytics read
  port. A fitness test (`TestA2AAgentIsReadOnly`) fails if any other field
  appears or if the adapter references a write use case.
- `cmd/a2a` imports no Kafka, outbox, in-memory or inventory-storage package and
  mentions no write use case or publisher variable (sensor
  `TestRuntimeIsReadOnlyAndPostgresOnly`).
- Every write verb a caller might guess (`place_order`, `cancel_order`, …) is
  answered as an `unknown-skill` problem, and the card says it is read-only.

Adding a write skill later needs a new ADR that first widens the MCP surface.

### Two curated skills, not one per endpoint

Derived from the existing MCP tools (`get_order`, `get_promise_health`), under
goal-shaped names:

| Skill | Kind | What it does |
|---|---|---|
| `order_status` | read | one order by `orderId`: status, allow-partial-shipment, promise date and every line (SKU, quantity, path, gift-wrap, status, `reservationId` only once allocated). Runs the same `GetOrder` use case as REST and MCP. |
| `promise_health` | read | promise-health KPIs for `[from, to)` (RFC3339) and an optional exact `pathId`: promise-basis distribution (capability / lead-time / network), re-promise rate, split-shipment rate, promise-to-cutoff gap. Reads the analytics data product ([ADR-0006](./0006-analytical-data-product.md)), which is eventually consistent. |

Same curation discipline as [ADR-0010](./0010-mcp-inbound-adapter.md). The
re-promise count carries no path dimension, so a `pathId` filter zeroes it; the
skill description says so, exactly as the MCP tool does.

### Message-only, answered in-band

Both skills are request/response reads, so the agent is message-only (no Task
lifecycle). Domain rejections and malformed requests are **answered** as RFC
7807-shaped `problem`s inside the message envelope, never raised as JSON-RPC
transport errors, with the **same classification and problem types as the REST
adapter** (`statusFor` / `problemCatalog`): `404 order-not-found`, `400
empty-order-id`, `500 internal-error`; adapter-level `400`s are
`malformed-skill-request`, `unknown-skill` and `invalid-argument`. The
`409`/`422`/`503` classes of the REST mapping belong to write commands and to the
inventory-storage hop; a read skill cannot reach them, so they are not copied
(dead, untestable branches). A `500` carries a fixed sentence and the cause is
logged, so a delegating agent never receives raw database text.

### Strict input

Unknown argument names are **rejected**, not ignored (a misspelled `pathIds`
must not silently drop the filter), and trailing data after the JSON document is
refused. Go's `encoding/json` matches names case-insensitively, so `pathid` still
reaches `pathId`. This context has no deprecated alias key on these surfaces, so
there is nothing to hide from the card or from error text.

### Postgres only, no degraded mode

Unlike `cmd/mcp`, `cmd/a2a` has no in-memory fallback and no empty-analytics
fallback: `DATABASE_URL`, `ANALYTICS_DATABASE_URL` and `A2A_AGENT_URL` (an
absolute http(s) URL, advertised on the card) are mandatory and a missing one is
a boot error naming it, raised before telemetry, any connection and the
listener. An empty analytics store would answer all-zero KPIs as if they were
facts, which is wrong for an answer another agent will act on. A listener that
cannot bind ends the process non-zero instead of leaving a pod that serves
nothing. Everything else is `cmd/mcp`'s composition: same read use case, same
boot-retried migrations on the direct connection
([ADR-0029](./0029-migrations-direct-postgres-connection.md)).

### Facts still travel on Kafka

A2A never becomes a data-sync channel. This agent writes nothing, so it has no
outbox rows to leave; the OLTP binary keeps owning the outbox relay
([ADR-0022](./0022-transactional-outbox.md)). The chart therefore renders no
`KAFKA_BROKERS` or `EVENT_PUBLISHER` for it. There is also **no outbound A2A
client** here (a fitness test fails on any import of the SDK's `a2aclient`):
this context reaches siblings only through the HTTP and Kafka contracts of
`bounded-context-boundary.md`.

### Unauthenticated, like REST and MCP

The card declares no security schemes: this surface is unauthenticated by the
same fleet-wide decision as REST and MCP
([ADR-0012](./0012-remove-rest-mcp-bearer-auth.md)). Schemes are added to the
card, not a redesign, if that decision is ever reversed.

## Consequences

- A fifth binary (`/app/a2a`) in the same image and an `a2a.*` chart block
  (`<release>-a2a`, default **off**), exposed through Kong at
  `/api/a2a/order-management` rewritten to `/` so the card stays reachable under
  the same prefix.
- Enabling it requires `database.url|existingSecret` **and**
  `analytics.enabled=true`; the render fails with a named reason otherwise. The
  agent reads the analytics Secret's read-only `ANALYTICS_READER_DATABASE_URL`
  key, the same DSN `cmd/order-reports` uses.
- `cmd/mcp`'s chart never renders `ANALYTICS_DATABASE_URL`, so a deployed
  `get_promise_health` answers zeros (documented in the troubleshooting guide);
  `promise_health` does not inherit that gap. Fixing the MCP chart is a separate
  change.
- The ~40-line KPI aggregation is duplicated from the MCP adapter: nothing may
  depend on the MCP adapter, and routing it through `internal/application` would
  push `internal/analytics/report` into a layer that is restricted to the domain.
  Both copies are pinned to the same numbers by their own table tests.
- Port 8093 is the fleet's A2A port (8090 is MCP, 8091 the projector admin
  port, 8092 the reports API).
