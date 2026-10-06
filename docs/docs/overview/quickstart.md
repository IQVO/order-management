---
title: Quickstart
sidebar_label: Quickstart
description: Run the service locally and walk every endpoint with curl.
---

# Quickstart

## Run it

### Option A — in-memory, no database (fastest)

```bash
go run ./cmd/order
# {"level":"INFO","msg":"database url not configured; using in-memory adapters"}
# {"level":"INFO","msg":"http server listening","addr":":8080"}
```

With no `DATABASE_URL`, the service starts on the in-memory adapters and is
fully functional.

### Option B — with Postgres

```bash
docker compose up -d postgres          # Postgres 16 on localhost:5434

export DATABASE_URL='postgres://order:***@localhost:5434/order?sslmode=disable'
go run ./cmd/order                     # migrations run automatically at startup
```

Migrations live in `migrations/` and are applied by `golang-migrate` on
boot; there is no separate migrate step to remember.

### Option C — wired to the real Suppliers

Both inventory-storage clients default to **permissive (no-op) mode**, so
tests and CI never reach the network. For reservations, permissive does
not mean fail-open: allocating real stock must never appear to succeed
against a no-op, so the implicit allocation pass fails with
`downstream-not-configured` (the order is still received; an explicit
`retry-allocation` returns 503). Only `http` mode is suitable for a real
integration test or deployment:

```bash
export INVENTORY_STORAGE_MODE=http
export INVENTORY_STORAGE_BASE_URL=http://localhost:8080
export PRODUCT_CLASSIFICATION_MODE=http     # optional, ADR 0016
export EVENT_PUBLISHER=kafka                # optional: publish release events
export PATH_CATALOGUE_SOURCE=kafka          # optional: capability-derived promise
export KAFKA_BROKERS=localhost:9092
export HTTP_ADDR=:8082
go run ./cmd/order
```

Without `PATH_CATALOGUE_SOURCE=kafka` the promise always comes from the
`LeadTimePolicy` fallback (`PROMISE_DEFAULT_LEAD_TIME`, default `48h`).

## Walk the API

**Health:**

```bash
curl -s localhost:8080/healthz
# {"status":"ok"}
```

**ReceiveOrder** — `allowPartialShipment` defaults to `false`
(ship-complete). There is no `pathId` in the request: the path is chosen
by `PathSelectionPolicy` (the eligible active path with the shortest known
cycle time, or the default `pick` when no catalogue is configured —
ADR 0013/0016/0021). Intake runs allocation and release in the same call
(ADR 0005), so the response already reflects that pass. With
`DATABASE_URL` set, `POST /orders` also requires an `Idempotency-Key`
header (ADR 0023; `400 idempotency-key-required` without it, `422
idempotency-key-reused` for the same key with a different body); the
in-memory Option A does not enforce it:

```bash
curl -s -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
        "allowPartialShipment": false,
        "lines": [
          {"sku": "SKU-1", "quantity": 2},
          {"sku": "SKU-2", "quantity": 1, "giftWrap": true}
        ]
      }'
# 201 Created, Location: /orders/ord-...
# {"id":"ord-...","status":"Released","allowPartialShipment":false,
#  "releaseOnAllocation":true,"promiseDate":"...",
#  "lines":[{"lineNo":1,"sku":"SKU-1","quantity":2,"pathId":"pick",
#            "giftWrap":false,"status":"Released","reservationId":"res-..."}, ...]}
```

A backordered line is a successful outcome reported in the body, not an
error. If the allocation pass hits a hard failure (e.g. permissive
inventory client), the order is still `201` in `Received` with pending
lines.

**GetOrder:**

```bash
ORDER_ID=ord-...   # from the response above
curl -s localhost:8080/orders/$ORDER_ID
```

**RetryAllocation** — re-attempts the backordered lines and only those,
releasing whatever becomes releasable:

```bash
curl -s -X POST localhost:8080/orders/$ORDER_ID/retry-allocation
```

**Held order + ReleaseHeldOrder** (ADR 0020) — `releaseOnAllocation:
false` allocates and stops; `requiredShipBy` constrains the promise to a
window that meets the deadline (no `promiseDate` in the response means the
deadline cannot be met):

```bash
curl -s -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"releaseOnAllocation": false,
       "requiredShipBy": "2026-10-01T18:00:00Z",
       "lines": [{"sku": "SKU-1", "quantity": 1}]}'

curl -s -X POST localhost:8080/orders/$ORDER_ID/release
# 200 OK — the allocated lines are released
# 409 order-not-held if the order was never held.
# A ship-complete order with a line that is no longer allocated (BR3)
# releases nothing and still answers 200 with the unchanged order.
```

**CancelOrder** — revokes every allocated line's reservation, then
cancels:

```bash
curl -s -i -X DELETE localhost:8080/orders/$ORDER_ID
# 204 No Content
```

Rejected by BR6 once any line has been released:

```bash
curl -s -X DELETE localhost:8080/orders/$ORDER_ID
# 409 Conflict, application/problem+json
# {"type":"https://errors.order-management.warehouse-systems.dev/order-already-released",
#  "title":"Order already has released lines and can no longer be cancelled",
#  "status":409,...}
```

## Tests

```bash
go build ./...
go vet ./...
go test ./...
go test ./... -race

# Coverage gate on domain + application
go test -race -coverprofile=coverage.out \
  -coverpkg=./internal/domain/...,./internal/application/... ./...
go tool cover -func=coverage.out
```

`make check` runs `fmt-check`, `vet`, `build`, `lint`, `test` in one pass —
the same feedback CI gives you post-push, available locally pre-commit. See
the [Quality gate section of the README](https://github.com/IQVO/order-management#quality-gate).
