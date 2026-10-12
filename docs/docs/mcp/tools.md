---
id: tools
title: MCP tools
sidebar_label: MCP tools
description: The order-management MCP server — transport, endpoint, and every tool with its input schema, output fields and read-only annotation, taken from a live tools/list.
---

# MCP tools

`cmd/mcp` is a separate binary and Deployment (ADR 0010) that exposes
order-management to AI agents over the Model Context Protocol. It is
**read-only**: no command of this context is offered as a tool, because
receiving, allocating, retrying and cancelling orders carry business
invariants that an agent should not decide on this context's behalf.
Since ADR 0012 there is no authentication.

| Property | Value |
| --- | --- |
| Transport | Streamable HTTP only (`github.com/modelcontextprotocol/go-sdk` v1.8.0) |
| Endpoint | every path under `MCP_ADDR` (default `:8090`): the handler is mounted on the chi pattern `/*`, which also matches `/`. In the cluster: `http://order-management-mcp.<ns>.svc.cluster.local:8090/` |
| Server info | name `order-management-mcp`, version `1.0.0` |
| Capabilities | `tools` (with `listChanged`), `logging`; no resources, no prompts |
| Sessions | in process memory, keyed by the `Mcp-Session-Id` response header of `initialize`; run one replica |
| Telemetry | one span `mcp.tool <name>` per call; `http.server.request.duration` and `http.server.active_requests` |
| Backing data | `get_order`: the OLTP database (`DATABASE_URL`, in-memory when unset). `get_promise_health`: the analytical database (`ANALYTICS_DATABASE_URL`, an empty store when unset) |

The schemas below are copied from a `tools/list` against a locally built
`cmd/mcp` on 2026-10-12. The go-sdk adds `"additionalProperties": false`
to every input, so an unknown argument is rejected.

## `get_order`

Annotations: `readOnlyHint: true`, `idempotentHint: false`.

> Return one order's current state by id: status, allow-partial-shipment
> flag, promise date, and every line's
> SKU/quantity/process-path/gift-wrap/status/reservation-id.

| Input | Type | Required | Description |
| --- | --- | --- | --- |
| `orderId` | string | yes | the id of the order to look up |

Output (structured content):

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | |
| `status` | string | derived order status (`Received`, `Allocated`, `PartiallyAllocated`, `Backordered`, `Released`, `PartiallyReleased`, `Cancelled`) |
| `allowPartialShipment` | boolean | |
| `promiseDate` | string, RFC 3339 | omitted when no promise is set |
| `lines[]` | array | `lineNo`, `sku`, `quantity`, `pathId`, `giftWrap`, `status`, `reservationId` (omitted when not allocated) |

Unlike `GET /orders/{id}`, the tool does not return `releaseOnAllocation`,
`requiredShipBy` or `capacityConstraint`.

Errors are tool results with `isError: true` and a text message: an
unknown id gives `order not found`, an empty id `order id must not be
empty`.

## `get_promise_health`

Annotations: `readOnlyHint: true`, `idempotentHint: false`.

> Return promise-health KPIs for a time window (and optional process-path
> filter) from the Order Funnel & Allocation Health data product.

| Input | Type | Required | Description |
| --- | --- | --- | --- |
| `from` | string | yes | inclusive start of the time window, RFC3339 |
| `to` | string | yes | exclusive end of the time window, RFC3339 |
| `pathId` | string | no | optional exact-match process path filter |

Output (structured content), summed over the hourly rows of
`/reports/funnel` for the window:

| Field | Type | Meaning |
| --- | --- | --- |
| `promiseBasisCapability`, `promiseBasisLeadTime`, `promiseBasisNetwork` | integer | allocations per promise basis |
| `ordersAllocatedTotal` | integer | the sum of the three bases |
| `ordersSplitShipment`, `splitShipmentRate` | integer, number | orders promised to more than one cutoff, and that count over `ordersAllocatedTotal` |
| `ordersRepromised`, `repromiseRate` | integer, number | re-promises, and that count over `ordersAllocatedTotal`. Re-promises carry no path, so a `pathId` filter makes both `0`. |
| `promiseToCutoffGapSeconds` | number | sample-weighted mean of (cutoff − allocation time) over capability promises |

On-time-to-CPT is not here: fulfillment-execution measures it. A `from`
or `to` that is not RFC 3339 returns a tool error such as `from must be
an RFC3339 timestamp: …`.

## A session by hand

```bash
# 1. initialize: note the Mcp-Session-Id response header
curl -s -D - http://localhost:8090/ \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'

SID=...   # value of Mcp-Session-Id

# 2. confirm initialization, then list and call tools
curl -s http://localhost:8090/ -H "Mcp-Session-Id: $SID" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}'

curl -s http://localhost:8090/ -H "Mcp-Session-Id: $SID" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_promise_health","arguments":{"from":"2026-10-01T00:00:00Z","to":"2026-10-02T00:00:00Z"}}}'
```

Responses arrive as server-sent events. Against a binary with no
`ANALYTICS_DATABASE_URL` the last call returned:

```text
event: message
data: {"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"{...}"}],"structuredContent":{"ordersAllocatedTotal":0,"ordersRepromised":0,"ordersSplitShipment":0,"promiseBasisCapability":0,"promiseBasisLeadTime":0,"promiseBasisNetwork":0,"promiseToCutoffGapSeconds":0,"repromiseRate":0,"splitShipmentRate":0}}}
```

See the [MCP Governance Charter](./governance-charter.md) for the rules
any new tool must follow, and [Testing](../development/testing.md#mcp-evals-internaladaptersinboundmcp)
for the evals that pin these schemas.
