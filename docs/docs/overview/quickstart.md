---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
description: Prerequisites, make targets, running cmd/order in memory, with Postgres and with the fleet Kafka broker, and the first curl calls with the responses a local run actually returned.
---

# Quickstart

## Prerequisites

| Tool | Version | Needed for |
| --- | --- | --- |
| Go | 1.26.9 (`go.mod`) | everything |
| Docker | any recent | local Postgres (`docker-compose.yml`) and every integration test (Testcontainers) |
| golangci-lint | v2.13.1 | `make lint`, `make check` |
| lefthook | 1.7.0 or later | optional git hooks (`lefthook install`) |
| Node.js | 20+ for `docs/`, 22 for `web/` | the docs site and the MFE |
| The kind cluster from warehouse-infra | | only to talk to the fleet Kafka broker and the sibling services |

## Make targets

```bash
make            # prints the list (help is the default target)
make build      # go build ./...
make test       # go test ./... -race
make check      # fmt-check vet build lint test
make check-all  # check + coverage (90 % gate) + arch-test + bdd
make integration  # Testcontainers suites, needs Docker
```

There is no `run` target: start a binary with `go run ./cmd/<name>`. The
full list, including `bdd`, `contract`, `mutation-fast` and `vuln`, is on
[Testing](/docs/development/testing).

## Option A — in memory, no database

```bash
go run ./cmd/order
```

With no `DATABASE_URL` the service uses in-memory repositories and the
log event publisher. It needs nothing else to start. The first lines of a
real run (trimmed):

```text
{"level":"INFO","msg":"telemetry configured","service_name":"order-management","service_version":"dev","environment":"local","otlp_endpoint":"localhost:4317"}
{"level":"INFO","msg":"database url not configured; using in-memory adapters"}
{"level":"WARN","msg":"inventory-storage client in permissive (no-op) mode; allocation will refuse to run",...}
{"level":"WARN","msg":"product classification lookup in permissive (fail-open) mode; ...",...}
{"level":"WARN","msg":"process-path catalogue source not configured; ReceiveOrder will skip path validation, ...",...}
{"level":"WARN","msg":"KAFKA_BROKERS not configured; RepromiseOrder consumer will not run, OrderRepromised will never fire",...}
{"level":"INFO","msg":"http server listening","addr":":8080"}
```

Every WARN line names an integration that is switched off and the
variable that switches it on. With no OTel collector running, telemetry is
dropped silently.

In this mode `POST /orders` does **not** need an `Idempotency-Key`, and
allocation never succeeds: the inventory client is permissive, so new
orders stay `Received` with `Pending` lines.

## Option B — with Postgres

```bash
docker compose up -d postgres     # postgres:16 on localhost:5434, user/password/db "order"

export DATABASE_URL='postgres://order:order@localhost:5434/order?sslmode=disable'
go run ./cmd/order                # applies migrations/ before listening
```

Migrations run at every boot; there is no separate step. With a database,
`POST /orders` requires an `Idempotency-Key` header and the housekeeping
sweeper runs hourly.

## Option C — with Kafka and the sibling services

The fleet runs **one** Kafka broker, inside the kind cluster, reachable
from the host at `localhost:9092`. Bring the cluster up with
warehouse-infra, then:

```bash
export KAFKA_BROKERS=localhost:9092
export EVENT_PUBLISHER=kafka                       # publish to warehouse.order-management.events and .analytics
export PATH_CATALOGUE_SOURCE=kafka                 # capability-derived promise; boot waits for 3 cache replays
export PRODUCT_CLASSIFICATION_MODE=kafka
export PRODUCT_CLASSIFICATION_CONSUMER_GROUP=om-local-$USER   # never reuse the cluster's group
export INVENTORY_STORAGE_MODE=http
export INVENTORY_STORAGE_BASE_URL=http://localhost:8000/api/inventory-storage
go run ./cmd/order
```

- Use your own consumer-group ids locally. Joining the cluster's
  `order-management-product-classification` or
  `order-management-planned-capacity` group would split partitions with
  the deployed pod.
- The re-promise consumer uses the fixed group
  `order-management-repromise`, the same as the deployed pod. A local
  process with `KAFKA_BROKERS` set therefore shares that group's
  partitions with the cluster while it runs.
- The fleet has no docker-compose broker: `docker-compose.kafka.yml` was
  removed from this repository (it bound `localhost:9092`, the same port
  as the kind cluster's broker). Use the cluster's broker.
- `INVENTORY_STORAGE_BASE_URL` above goes through Kong; any URL that
  reaches inventory-storage works.

### Seeding

This repository ships no seed data and no seed command: orders are
created through `POST /orders`. For allocation to succeed in `http` mode,
inventory-storage must hold usable stock for the SKUs you order; seed it
there.

## First calls

These are the responses of a local in-memory run (Option A, plus
`PLANNED_CAPACITY_CONSUMER_GROUP=docs-local` and
`PLANNED_CAPACITY_SITE_ID=SIM1` so that `/planned-capacity` exists),
captured on 2026-10-12.

```bash
curl -s localhost:8080/healthz
# {"status":"ok"}

curl -s localhost:8080/readyz
# {"status":"ready"}
```

**Receive an order.** `allowPartialShipment` defaults to `false`
(ship-complete) and `releaseOnAllocation` to `true`. There is no `pathId`
in the request: `PathSelectionPolicy` picks it (the default `pick` without
a catalogue). Add `-H "Idempotency-Key: $(uuidgen)"` when the service has
a database.

```bash
curl -s -i -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"allowPartialShipment":false,
       "lines":[{"sku":"SKU-1","quantity":2},
                {"sku":"SKU-2","quantity":1,"giftWrap":true}]}'
# HTTP/1.1 201 Created
# Location: /orders/ord-1
# {"id":"ord-1","status":"Received","allowPartialShipment":false,"releaseOnAllocation":true,
#  "lines":[{"lineNo":1,"sku":"SKU-1","quantity":2,"pathId":"pick","giftWrap":false,"status":"Pending"},
#           {"lineNo":2,"sku":"SKU-2","quantity":1,"pathId":"pick","giftWrap":true,"status":"Pending"}]}
```

In memory the ids are `ord-1`, `ord-2`, …. With inventory-storage in
`http` mode and stock available, the same call allocates and releases the
lines in the same request and answers with `"status":"Released"`, a
`promiseDate` and a `reservationId` per line.

**Read it back, and try the commands:**

```bash
curl -s localhost:8080/orders/ord-1
# 200, same body as above

curl -s -X POST localhost:8080/orders/ord-1/retry-allocation
# 409 {"type":"https://errors.order-management.warehouse-systems.dev/no-backordered-lines",
#      "title":"Order has no backordered lines to retry","status":409,...}

curl -s -X POST localhost:8080/orders/ord-1/release
# 409 {"type":".../order-not-held","title":"Order was not held at intake and has nothing to release on demand",...}

curl -s -i -X DELETE localhost:8080/orders/ord-1
# HTTP/1.1 204 No Content     (the order is now Cancelled; nothing was released)
```

**Validation errors** are RFC 7807 problems:

```bash
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' -d '{"lines":[]}'
# 400 .../order-without-lines

curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"lines":[{"sku":"SKU-1","quantity":0}]}'
# 422 .../non-positive-quantity

curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"releaseOnAllocation":false,"allowPartialShipment":true,"lines":[{"sku":"SKU-1","quantity":1}]}'
# 422 .../held-order-must-be-ship-complete

curl -s localhost:8080/orders/ord-does-not-exist
# 404 .../order-not-found
```

**Held order** (ADR 0020): `releaseOnAllocation: false` allocates and
stops; `requiredShipBy` limits the promise to windows that meet it.
`POST /orders/{id}/release` then releases it.

```bash
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"releaseOnAllocation":false,"requiredShipBy":"2026-10-20T18:00:00Z",
       "lines":[{"sku":"SKU-1","quantity":1}]}'
```

**Planned capacity** (only registered when
`PLANNED_CAPACITY_CONSUMER_GROUP` is set):

```bash
curl -s 'localhost:8080/planned-capacity?site=SIM1'
# {"site":"SIM1","windows":[]}
```

Every request writes one log line such as
`{"msg":"http request","method":"POST","route":"/orders","status":201,"bytes":284,"duration_ms":0,"request_id":"…"}`,
and with `EVENT_PUBLISHER=log` every domain event writes
`{"msg":"domain event published","event_name":"OrderReceived",…}`.

## The other binaries

```bash
# MCP server, Streamable HTTP on :8090 (see MCP tools)
go run ./cmd/mcp

# Analytics writer and reader: both need an analytical database
export ANALYTICS_DATABASE_URL='postgres://order:order@localhost:5434/order_analytics?sslmode=disable'
go run ./cmd/order-projector     # consumes warehouse.order-management.analytics, admin :8091
go run ./cmd/order-reports       # :8092
curl -s 'localhost:8092/reports/funnel?from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z'
curl -s localhost:8092/reports/funnel/freshness
```

The `order_analytics` database does not exist in the compose Postgres:
create it first (`createdb -h localhost -p 5434 -U order order_analytics`).
See [MCP tools](/docs/mcp/tools) and
[Order Funnel & Allocation Health](/docs/analytics/order-funnel-report).

## Next

- [Configuration](/docs/operations/configuration): every variable.
- [Testing](/docs/development/testing): every test layer and CI job.
- [Troubleshooting](/docs/operations/troubleshooting): what the WARN lines
  and problem types mean.
