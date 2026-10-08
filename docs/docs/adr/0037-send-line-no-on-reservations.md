---
id: 0037-send-line-no-on-reservations
slug: /adr/0037-send-line-no-on-reservations
title: "0037. Send the order line number (lineNo) on POST /reservations"
sidebar_label: "37. lineNo on POST /reservations"
sidebar_position: 37
description: "ADR 0037 — the inventory-storage client puts the order line number in the POST /reservations body as the optional integer lineNo (only when it is at least 1), so inventory-storage can store which line a reservation is for and confirm a pick per line (fleet decision 18, hop 1). Additive: the Idempotency-Key derivation (ADR 0028) is unchanged and OrderAllocated is untouched."
---

# 0037. Send the order line number (lineNo) on POST /reservations

## Status

Accepted — implemented by the change that introduces this record.

Extends [ADR 0028](./0028-inventory-storage-reservations-idempotency-key.md)
(the request now carries one more body field; the key derivation is
unchanged) and refines [ADR 0002](./0002-http-consumer-of-inventory-and-wes-not-shared-code.md)
(the local mirror of inventory-storage's request shape gains one optional
field).

## Context

When a pick task completes, inventory-storage turns the order's reservation
into a confirmed pick. Today it can only count picks per order
(`demandRef`): a reservation does not know which order line it was made
for, so confirmation waits for the order's last pick. The fleet decided
(decision 18, 2026-10-06) to carry the line number explicitly, as an
optional additive field, through every hop so each pick confirms its own
line's reservation:

1. **order-management → inventory-storage `POST /reservations` — `lineNo`**
   (this ADR);
2. inventory-storage stores a nullable `line_no` on `Reservation`;
3. wes-work-planning stores the `line_no` it already receives on
   `OrderAllocated`;
4. `WorkReleased` carries `line_no`;
5. fulfillment-execution stamps `source_line_no` on the `Task`;
6. `TaskCompleted` carries `line_no`;
7. inventory-storage confirms per line, falling back to the existing
   last-pick counting when the line is unknown.

This service already knows the line: `ports.ReservationRequest.LineNo` is
set from `OrderLine.LineNo()` at both call sites in `allocation.go`
(allocate/retry and reconfirm-before-release). Until now it only fed the
`Idempotency-Key` (`res-<order>-line-<n>-att-<m>`, ADR 0028) and never
reached the body.

## Decision

### 1. The body gains an optional `lineNo`

`POST /reservations` is sent as

```json
{ "sku": "SKU-1", "quantity": 3, "demandRef": "ord-7", "lineNo": 2 }
```

`lineNo` is an integer and is sent **only when the line number is at least
1**. For `0` or a negative value (unknown) the field is omitted rather than
sent as `0`, so absent keeps meaning "unknown" for every consumer. The
reconfirm call before release sends the same body.

### 2. Idempotency-Key unchanged

`idempotencyKeyFor` is not touched: the key remains
`res-<order>-line-<n>-att-<m>`, byte-identical to ADR 0028. It is a replay
contract with inventory-storage (a unit test pins the exact strings).

### 3. Rollout safety: sent unconditionally, no flag

inventory-storage's `POST /reservations` handler decodes the body with a
plain `json.NewDecoder(...).Decode` — no `DisallowUnknownFields`, no
request-schema validator in the chain — so an older inventory-storage
silently ignores `lineNo` and a newer one stores it. The change can
therefore be released in any order relative to inventory-storage's sibling
change, and no feature flag (`INVENTORY_RESERVE_SEND_LINE_NO` was
considered) is needed. Checked against inventory-storage `origin/develop`.

One narrow caveat, inherited from ADR 0028: inventory-storage's
Idempotency-Key middleware hashes the request body, and a reused key with a
different body is answered `422 idempotency-key-reused`. A call with the
**same** `(order, line, attempt)` key that was first sent *before* this
change is deployed and is retried *after* it (same aggregate version, no
intervening save) would therefore hit that `422`, which this client maps to
`ErrUnexpectedStatus` (a hard error, never a backorder). The window is
bounded by inventory-storage's `IDEMPOTENCY_KEY_TTL` (default 24h); a new
allocation pass bumps the order version and so uses a new key. Not worth a
flag, but worth knowing during the rollout.

### 4. Nothing else changes

- `OrderAllocated`, `OrderPartiallyAllocated` and every other event are
  untouched (they already carry `lines[].line_no`).
- `apis/openapi.yaml` and `apis/asyncapi.yaml` do not describe the outbound
  call's body (this service's own REST/Kafka surface is unchanged), so no
  spec or generated API reference changes.
- No database migration.

## Consequences

- inventory-storage can start storing `line_no` as soon as it ships its
  side; reservations created before that carry no line and are handled by
  its legacy fallback.
- A reservation made by this service now identifies its order line on the
  wire, which is the first hop of the per-line confirm-pick chain; the
  remaining hops live in the sibling repositories' records.
- The local request mirror gains one optional pointer field; the
  inventory-storage module is still not imported (ADR 0002).

## Alternatives considered

- **Gate the field behind `INVENTORY_RESERVE_SEND_LINE_NO` (default off).**
  Rejected: it is only needed when the receiver rejects unknown fields, and
  inventory-storage's handler does not. A flag would add a deploy step and a
  state where the chain silently does nothing.
- **Encode the line in `demandRef` (e.g. `<order>#<n>`).** Rejected:
  `demandRef` is the order identity that inventory-storage and the cancel
  path key on; overloading it would break `demand_ref = order_ref` lookups.
- **Always send `lineNo`, including `0`.** Rejected: `0` would be
  indistinguishable from a real value for a consumer that treats presence as
  "known".
