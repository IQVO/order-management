---
slug: /api-reference
title: API Reference
sidebar_label: Overview
description: REST conventions, the endpoint matrix, and the RFC 7807 error catalog.
---

# API Reference

This service exposes two contracts, kept as source-of-truth artefacts in
the repository and linted by Spectral (the `api-lint` CI job):

| Contract | File | Rendered here |
| --- | --- | --- |
| REST (synchronous) | `apis/openapi.yaml` — OpenAPI 3.0.3 | **[REST API](./rest/order-management-api.info.mdx)** — generated directly from the spec |
| Kafka (asynchronous) | `apis/asyncapi.yaml` — AsyncAPI 2.6.0 | Not rendered on this site; see [Domain Events](/docs/ddd/domain-events) |

The REST pages under **REST API** are *generated* from `apis/openapi.yaml`
by `docusaurus-plugin-openapi-docs` at build time. They are not
hand-transcribed, so they cannot drift from the spec the service ships.

## Endpoint matrix

`internal/adapters/inbound/http/server.go` registers 8 routes; 7 of them are
declared in `apis/openapi.yaml` (`GET /readyz` is not — see the note below
the table). There is no `/orders/{id}/allocate` any more: allocation and
release are folded into `POST /orders` and `retry-allocation`
([ADR 0005](/docs/adr/0005-choreographed-release-via-kafka)).

| Method | Path | Operation | Tag | Success | Errors |
| --- | --- | --- | --- | --- | --- |
| `GET` | `/healthz` | `getHealthz` | Health | `200` | — |
| `GET` | `/readyz` | *(not in spec)* | — | `200` `{"status":"ready"}` | `503` `{"status":"not_ready"}` once shutdown starts (ADR 0025) |
| `POST` | `/orders` | `receiveOrder` | Orders | `201` | `400` `422` `500` |
| `GET` | `/orders/{id}` | `getOrder` | Orders | `200` | `400` `404` `500` |
| `DELETE` | `/orders/{id}` | `cancelOrder` | Orders | `204` | `400` `404` `409` `503` `500` |
| `POST` | `/orders/{id}/release` | `releaseHeldOrder` | Allocation | `200` | `404` `409` `503` `500` |
| `POST` | `/orders/{id}/retry-allocation` | `retryAllocation` | Allocation | `200` | `400` `404` `409` `503` `500` |
| `GET` | `/planned-capacity` | `getPlannedCapacity` | PlannedCapacity | `200` | `400` `500` |

`GET /planned-capacity` is registered only when
`PLANNED_CAPACITY_CONSUMER_GROUP` is set
([ADR 0031](/docs/adr/0031-consume-warehouse-planning-capacity-plans)).
`POST /orders` is wrapped by the `Idempotency-Key` middleware
([ADR 0023](/docs/adr/0023-idempotency-key-middleware)) whenever the
service runs with `DATABASE_URL`: a missing key is `400
idempotency-key-required`, the same key with a different body is `422
idempotency-key-reused`, and a replay returns the stored response. The
header is not declared as a parameter in `apis/openapi.yaml`. A
version-guard conflict on any write (ADR 0024) is `409
concurrent-modification`.

## Status-code conventions

| Code | Used for | Example |
| --- | --- | --- |
| `200 OK` | A read, or a command whose result *is* the response body | `GET /orders/{id}`; `POST /orders/{id}/retry-allocation` returns the order after the pass |
| `201 Created` | A new addressable resource, with a `Location` header | `POST /orders` → `Location: /orders/{id}` |
| `204 No Content` | A state transition with nothing useful to return | `DELETE /orders/{id}` |
| `400 Bad Request` | Malformed or missing input | empty SKU, unparseable JSON |
| `404 Not Found` | The addressed resource does not exist | unknown order |
| `409 Conflict` | Well-formed and addressable, but conflicts with current state | order already released (cancel), ship-complete blocked or order not held (release), no backordered lines (retry) |
| `422 Unprocessable Entity` | Well-formed but semantically invalid *values* | quantity ≤ 0; a line ineligible for its resolved path; a held order with `allowPartialShipment: true` |
| `503 Service Unavailable` | A downstream Supplier could not be reached, answered ambiguously, or is wired in permissive (no-op) mode | any non-409 failure from inventory-storage during retry/release/cancel |

The `400` / `422` split is the one worth internalising: `400` means "I
could not understand the request," `422` means "I understood it perfectly
and it is not a legal thing to ask for." The `409` / `503` split matters
just as much here: `409` means this context's own state disagrees with
the request; `503` means a Supplier could not be trusted to answer,
never papered over with a fabricated success (BR2).

## Errors: RFC 7807 Problem Details

Every error response uses `application/problem+json`, the same shape the
other services in this platform emit:

```json
{
  "type": "https://errors.order-management.warehouse-systems.dev/order-already-released",
  "title": "Order already has released lines and can no longer be cancelled",
  "status": 409,
  "detail": "order already has released lines and can no longer be cancelled",
  "instance": "/orders/ord-a1b2c3d4-0000-0000-0000-000000000001"
}
```

- `type` is a stable, unique URI per error **category**. It is an
  identifier — it does not have to resolve to a page.
- `title` is a fixed human string for the category.
- `detail` is the dynamic message from the underlying typed error.
- `instance` is the request path.

### Problem-type catalog

| `type` slug | Status | Raised by |
| --- | --- | --- |
| `order-not-found` | 404 | order id does not exist |
| `empty-order-id` | 400 | the path's order id is empty |
| `empty-sku` | 400 | a line's SKU is empty |
| `order-without-lines` | 400 | the order has no lines |
| `unknown-process-path` | 400 | the resolved path is not active in the process-path catalogue (ADR 0013) |
| `malformed-request-body` | 400 | the body is not valid JSON, or a field is explicitly `null` |
| `idempotency-key-required` | 400 | `POST /orders` without an `Idempotency-Key` header (ADR 0023) |
| `invalid-query-parameter` | 400 | `GET /planned-capacity` without `site`, or a non-RFC 3339 `from` |
| `non-positive-quantity` | 422 | a line's quantity is not greater than zero |
| `line-ineligible-for-resolved-path` | 422 | a line's product attributes are not eligible for any active path (ADR 0016/0021) |
| `held-order-must-be-ship-complete` | 422 | `releaseOnAllocation: false` combined with `allowPartialShipment: true` (ADR 0020) |
| `idempotency-key-reused` | 422 | the same `Idempotency-Key` sent with a different body (ADR 0023) |
| `order-already-released` | 409 | `CancelOrder` when any line is `Released` (BR6) |
| `ship-complete-blocked` | 409 | release of a ship-complete order with an unallocated line (BR3) |
| `no-backordered-lines` | 409 | `RetryAllocation` on an order with nothing backordered |
| `order-not-held` | 409 | `ReleaseHeldOrder` on an order that was not held at intake (ADR 0020) |
| `concurrent-modification` | 409 | the order's `version` changed between read and save (ADR 0024) |
| `downstream-not-configured` | 503 | a Supplier client is running in permissive (no-op) mode |
| `downstream-unavailable` | 503 | inventory-storage failed (transport error, timeout, open circuit breaker, unexpected status) on `POST /orders/{id}/retry-allocation`, `POST /orders/{id}/release` or `DELETE /orders/{id}` (ADR 0003, ADR 0025) |
| `internal-error` | 500 | anything unmapped |

The reports binary (`cmd/order-reports`) uses its own two types,
`invalid-report-query` (400) and `report-store-error` (500).

`problemFor` in `internal/adapters/inbound/http/errors.go` also maps
line-state guard errors (`order-line-not-found` 400;
`order-line-already-allocated`, `order-line-not-pending`,
`order-line-not-backordered`, `order-line-not-allocated`,
`no-allocated-lines`, `promise-date-not-set` 409) and
`insufficient-stock` (503); the current use cases catch or never return
most of these on their HTTP paths (the per-line `409` from inventory-storage
becomes a `Backordered` line, not an error).

The domain never knows about any of this. It returns typed errors; the
inbound adapter is the only layer that translates them.

## DTOs never leak domain types

Request and response bodies are adapter-local structs in
`internal/adapters/inbound/http/dto.go`. An `orderResponse` is not an
`order.Order`. That indirection is what lets the domain model evolve
without breaking the wire contract.

## Authentication

None. The spec declares no `security` scheme at all: this service's
REST and MCP surfaces are reachable with no `Authorization` header (see
[ADR 0012](../adr/0012-remove-rest-mcp-bearer-auth.md), which removed the
static-bearer layer ADR 0011 had adopted).
