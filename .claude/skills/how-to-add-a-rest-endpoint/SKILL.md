---
name: how-to-add-a-rest-endpoint
description: Add or change a REST endpoint in this service in the fleet's hexagonal order (domain invariant, use case, port, HTTP adapter, apis/openapi.yaml, generated docs, godog scenario). Use when touching internal/adapters/inbound/http, apis/openapi.yaml, or exposing a use case over HTTP.
---

# How to add a REST endpoint

Use when asked to add a new REST use case/endpoint to this service. Follow
this order — domain first, adapter last — never the reverse; writing the
HTTP handler before the domain invariant it enforces produces handlers
that validate nothing and use cases that get bypassed.

This walks the exact path `POST /orders/{id}/retry-allocation` took
(`internal/application/usecases/retry_allocation.go` +
`internal/adapters/inbound/http/server.go`'s `handleRetryAllocation`) as
the concrete worked example — read those two files alongside this guide.

## 1. Domain first: does an invariant already exist, or do you need one?

Check `internal/domain/order/` (and `internal/domain/shared/` for
cross-aggregate value objects) for the rule this endpoint enforces. A REST
endpoint should almost never contain business logic itself — it decodes a
request, calls a use case, encodes the result. `RetryAllocation` is a good
worked example of this discipline: the endpoint itself does nothing but
extract `{id}` and call the use case; the actual rule — "a Backordered
line may transition back to Allocated ONLY via RetryAllocation, no other
path" — lives in `Order.RetryAllocate` in `internal/domain/order/order.go`,
not in the HTTP handler. If the operation needs a new domain rule, add it
to the aggregate/value-object in `internal/domain/`, with its own
table-driven unit test, BEFORE touching the application or adapter layers.

## 2. Application: define the use case

Add a new file in `internal/application/usecases/` (one file per use
case, this repo's convention — not one giant `usecases.go`). Shape:

```go
package usecases

type <Verb><Noun>Result struct {
    // fields the caller needs back — domain types, not DTOs
}

// <Verb><Noun> — one sentence: what business capability this represents,
// and the domain rule it enforces (mirror RetryAllocation's doc comment,
// which states up front which transition it is the "single sanctioned
// route" for, and calls out how its error-propagation behaviour
// deliberately differs from ReceiveOrder's best-effort treatment).
type <Verb><Noun> struct {
    Orders    ports.OrderRepo               // driven ports only — never a concrete adapter
    Inventory ports.InventoryReservationClient // if it touches inventory-storage
    Events    ports.EventPublisher          // if this raises a domain event
    Clock     ports.Clock                   // if it needs "now" (never call time.Now() directly)
}

func (uc *<Verb><Noun>) Execute(ctx context.Context, /* domain-typed args */) (<Verb><Noun>Result, error) {
    // 1. load the aggregate via the port (Orders.FindByID)
    // 2. call the aggregate's own method to apply the rule (never inline
    //    the invariant here — that belongs in internal/domain/order/)
    // 3. persist via the port
    // 4. publish the domain event via Events, if any
    // 5. return the result
}
```

Add the port to `internal/application/ports/ports.go` if it doesn't exist
yet — ports are interfaces ONLY (`TestPortsAreCustomerOwned` in
`internal/architecture/architecture_test.go` enforces this; a struct or
function in the ports package fails CI).

Write the use case's unit test against the in-memory adapter
(`internal/adapters/outbound/memory/`) and the fake inventory-storage
client used across `internal/application/usecases/*_test.go` (see
`fakes_test.go`) — never a real HTTP/Postgres/Kafka call in a unit test.
Cover the success path AND the domain-rule failure path (for
`RetryAllocation`, see `retry_allocation_test.go`'s
`TestRetryAllocationClearsABackorderAndUnblocksShipComplete` next to
`TestRetryAllocationLeavesAStillShortLineBackordered`).

## 3. Adapter: wire the HTTP handler

In `internal/adapters/inbound/http/`:

1. `dto.go` — add the request/response DTO structs (JSON tags, this
   repo's naming convention). DTOs live ONLY in the adapter layer —
   domain types never carry JSON tags.
2. `server.go` — add the route (`r.Post("/path/{id}", s.handle<Name>)` in
   `NewRouter`) and the handler function:
   - decode + validate the request (`decodeJSON`), converting to domain
     value objects immediately (`shared.NewOrderId`, `shared.NewSKU`,
     etc. — see `orderIDParam`'s helper) — a bad value fails here as an
     RFC 7807 validation error, never reaches the use case
   - call the use case's `Execute`
   - map use-case errors to HTTP status via `writeError`/`statusFor` in
     `errors.go` (check the existing switch before adding a new error
     branch — `statusFor` and `problemFor` are meant to mirror each
     other one-for-one)
   - encode the domain result back to the response DTO
     (`toOrderResponse`) and `writeJSON`
3. Add the new use case field to the `Server` struct (`server.go`) and
   wire it in the composition root (`cmd/order/main.go`).

Write at least one httptest per endpoint: one success path, one error
path (validation failure AND/OR the domain-rule failure). See
`server_test.go`'s `TestPostRetryAllocation` for the shape — it exercises
the success case, "no backordered lines" (409), and "order not found"
(404) as three separate subtests against the same `chi` router.

## 4. Contract: update OpenAPI, then regenerate docs

Add the path to `apis/openapi.yaml` (request/response schemas, the RFC
7807 problem-detail response for each error case — see the existing
`/orders/{id}/retry-allocation` entry for the shape).

Regenerate the Docusaurus REST reference. Do it by hand: the
`docs-api-drift` CI job is meant to catch a skipped regen but currently
diffs the wrong path and always passes (see `ci-quality-gates.md`) — that
is how `releaseHeldOrder` shipped without a generated page. Clean first;
a plain `gen-api-docs` will not add a new operation to the committed
`sidebar.ts`:

```bash
cd docs
npm ci
npm run clean-api-docs order && npm run gen-api-docs order
npm run build          # onBrokenLinks: 'throw'
```

## 5. Behaviour: add a godog scenario

If this endpoint is user-facing behaviour (not purely internal
plumbing), add a `.feature` file under `features/` exercising it
end-to-end against the real HTTP server — see `features/ship_complete.feature`
or `features/allocation_failure.feature` for the exact shape this repo's
`bdd` CI job (`go test ./... -run TestFeatures`, driven by
`features_test.go`) expects (Given/When/Then over a real chi router, not
mocked).

## 6. Verify before opening the PR

```bash
make check       # fmt-check vet build lint test
make check-all    # + coverage (90% gate) + arch-test + bdd
```

`make coverage` gates `./internal/domain/...,./internal/application/...`
at 90% — a new use case with no test on its failure path is the most
common way to miss this gate.
