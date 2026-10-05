---
id: 0001-hexagonal-ports-and-adapters
slug: /adr/0001-hexagonal-ports-and-adapters
title: 0001. Hexagonal (ports & adapters) architecture
sidebar_label: 0001. Hexagonal architecture
description: ADR 0001 — adopt ports & adapters with a strict inward-only dependency rule.
---

# 0001. Hexagonal (ports & adapters) architecture

## Status

Accepted. Established with the initial implementation of this bounded context.

## Context

Order Management is a **Generic/Supporting subdomain**, not a Core one: the
value here is not a clever algorithm, it is that the order lifecycle is
*correct and legible* — that a line cannot be allocated twice, that a
ship-complete order does not leak half-released work into the warehouse, and
that a cancellation either revokes every reservation or changes nothing. The
rules are simple to state and easy to get quietly wrong.

Several forces push against keeping them clean:

- **This context is defined by its calls to other services.** Almost every
  interesting operation here — allocate, release, cancel — is a conversation
  with `inventory-storage` or `wes-work-planning`. Written naively, an HTTP
  client type ends up inside the business logic and the rules become
  untestable without a network.
- **The rules must be testable without infrastructure.** "A backordered line
  returns to Allocated only via RetryAllocation" and "cancellation is illegal
  once any line is released" each deserve a dedicated failing-path test. That
  investment only pays if tests run in milliseconds and need no Postgres, no
  Supplier, and no fixtures.
- **The order-level status is derived, not stored.** That decision (see
  [ADR 0003](./0003-ship-complete-default-and-fail-closed-allocation.md)) only
  survives if the derivation lives in one place that nothing can route around
  — not in a SQL projection some later query forgets to apply.
- **The service must run without a database.** A developer poking at the API
  and the whole `httptest` suite need a fully functional service with no
  Postgres.
- **The rest of the platform already does this.** The five sibling services
  are all hexagonal. A sixth shaped differently would be a tax on every
  reader who moves between them.

## Decision

**We will structure the service as a hexagon (ports & adapters), with a strict
inward-only dependency rule:**

> **domain depends on nothing; application depends on domain; adapters depend
> on application/domain.**

Concretely:

- `internal/domain/` is **pure Go**. No `chi`, no `pgx`, no `net/http`, no
  struct tags for serialisation. The `Order` aggregate root and its
  `OrderLine` entities enforce their own invariants on every mutation, and
  `LeadTimePolicy` is a domain policy — the promise date is a business
  decision, not a formatting concern.
- `internal/application/ports/` declares the **outbound interfaces** the
  application needs: `OrderRepo`, `EventPublisher`, `Clock`,
  `InventoryReservationClient` — plus, since
  [ADR 0005](./0005-choreographed-release-via-kafka.md) deleted the
  synchronous release call, the ports that replaced `WorkReleaseClient`
  (`UnitOfWork` from [ADR 0022](./0022-transactional-outbox.md), the
  catalogue/capacity lookups of ADR 0014/0015/0016, and others; see
  `ports.go`). They are owned by the
  application and expressed in *this* context's types — never in a Supplier's
  types (see [ADR 0002](./0002-http-consumer-of-inventory-and-wes-not-shared-code.md)).
- `internal/application/usecases/` holds **one struct per use case**, with
  collaborators as plain fields. No use case imports an adapter package.
- `internal/adapters/` implements the ports: `inbound/http` (chi, DTOs, RFC
  7807 error mapping), `outbound/inventorystorage`,
  `outbound/postgres`, `outbound/memory`, `outbound/events` — plus, added
  by later ADRs, `outbound/kafka` (ADR 0005), `outbound/kafkacatalogue`/
  `kafkacptschedule`/`kafkapathcapacity` (ADR 0014/0015) and
  `outbound/productclassification` (ADR 0016). (`outbound/weswork` was
  deleted by [ADR 0005](./0005-choreographed-release-via-kafka.md).)
- `cmd/order/main.go` is the **only** composition root for the write-side
  service — the only file that knows both a port and its implementation.
  When this record was written it was also the only `cmd/` binary and the
  only env reader. The fleet has since grown four composition roots
  (`cmd/order`, `cmd/mcp`, `cmd/order-reports`, `cmd/order-projector`),
  each the sole env reader for its own process, with `cmd/order/main.go`'s
  `wiring.go`/`planned_capacity.go` companions and `internal/telemetry`'s
  `Setup` reading their own env seams — the same rule, applied per binary.

`Clock` is a port for the same reason the repository is: the promise date is a
*domain* output computed from "now", so time is injected rather than read from
`time.Now()` inside a policy. That is what makes promise-date assertions exact
instead of tolerance-based.

## Consequences

### Easier

- **Invariants are unit-testable in microseconds.** The domain has no I/O, so
  every failing path (`ErrLineAlreadyAllocated`, `ErrLineNotBackordered`,
  `ErrOrderAlreadyReleased`, `ErrShipCompleteBlocked`) is a table-driven test.
  This is what made 100% domain coverage affordable.
- **The Suppliers are fakes in every test.** `AllocateOrder`'s hardest
  behaviour — "409 is a business fact, everything else is not" — is asserted
  against a scripted `InventoryReservationClient`, with no HTTP anywhere. The
  real HTTP client is then tested separately against an `httptest` server for
  wire-shape fidelity.
- **Two storage backends, no domain change.** `memory` and `postgres`
  implement the same port. `go run ./cmd/order` with no `DATABASE_URL` starts
  a working service.
- **The wire contract and the model evolve independently.** DTOs live in the
  HTTP adapter; an `orderResponse` is not an `order.Order`. The derived
  `status` field is computed at serialisation time from the aggregate.
- **The deferred work is additive — and it happened that way.** Kafka
  publishing became a second `EventPublisher` (ADR 0005) and the
  capability-derived promise became `order.PromisePolicy` with
  `LeadTimePolicy` as its explicit fallback (ADR 0014) — neither touched
  a use case's shape.

### Harder

- **More files and more indirection.** Adding a field end-to-end touches the
  aggregate, the port, the two adapters and the DTO. For a CRUD service this
  would be over-engineering; here the mapping is the boundary that keeps a
  Supplier's wire shape out of the aggregate.
- **Mapping code is real work, and it is where wire bugs live.** Each outbound
  adapter converts between this context's types and the Supplier's JSON. That
  mapping is exactly what the `httptest`-server adapter tests exist to pin
  down.
- **Cross-service orchestration is an explicit use-case concern.** `CancelOrder`
  must revoke reservations *before* it mutates the aggregate, and
  `AllocateOrder` must persist partial progress *before* it returns a hard
  error. Nothing in the compiler enforces that ordering; the tests do.
- **The rule is easy to violate under deadline pressure.** Nothing stops a use
  case importing `net/http` — except the arch-go fitness tests that now
  exist (`internal/architecture`): when this record was written they were
  deferred and the rule was upheld by review alone; the suite has since
  been adopted (see `internal/architecture/architecture_test.go`,
  `fitness_test.go`, `events_fitness_test.go`), closing the gap the same
  way the sibling services did.
