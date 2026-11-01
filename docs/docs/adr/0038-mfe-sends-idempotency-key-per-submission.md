---
id: 0038-mfe-sends-idempotency-key-per-submission
slug: /adr/0038-mfe-sends-idempotency-key-per-submission
title: "0038. The operator MFE sends an Idempotency-Key per order submission"
sidebar_label: "38. MFE Idempotency-Key per submission"
sidebar_position: 38
description: "ADR 0038 — order_mgmt_mfe (web/) sends Idempotency-Key on POST /orders: one crypto.randomUUID() per order submission, reused when the same submission is retried, replaced when the form is edited or the order is placed. CORS allows the header. Fleet gap-fix decision D8."
---

# 0038. The operator MFE sends an Idempotency-Key per order submission

## Status

> Amended by [ADR 0048](./0048-adopt-fleet-rest-api-guideline.md): key reuse is now `409`, and every command (not only `POST /orders`) carries a key.

Accepted — implemented by the change that introduces this record (fleet
gap-fix wave 2026-10-12, decision D8).

Builds on [ADR 0023](./0023-idempotency-key-middleware.md) (the server
requires the header on `POST /orders` whenever it runs with Postgres) and
[ADR 0007](./0007-adopt-fleet-micro-frontend-console.md) (the operator MFE).

## Context

ADR 0023 made `Idempotency-Key` mandatory on `POST /orders` when
`DATABASE_URL` is set: a request without it is answered
`400 idempotency-key-required`. The operator MFE (`web/src/api.ts`) never
sent the header, and the CORS allow-list (`corsMiddleware` in
`internal/adapters/inbound/http/server.go`) only allowed `Content-Type` and
`Authorization`. Every deployed instance runs with Postgres, so placing an
order from the console failed, and the browser would have blocked the header
at the preflight anyway.

The key also has to mean something. ADR 0023 caches every normal response,
4xx included, under `(key, body hash)`, and answers a reused key with a
different body with `422 idempotency-key-reused`. A key per HTTP call would
lose the retry protection the header exists for (a dropped response followed
by a click on "Place order" again would create a second order). One key per
page would turn every later order into a 422.

## Decision

1. **One key per submission.** The screen holds the key of the current
   submission. It is minted with `crypto.randomUUID()` (a v4 UUID) the first
   time "Place order" is clicked and sent as `Idempotency-Key`
   (`apiPost(path, body, { idempotencyKey })`).
2. **Reused on a retry of the same submission.** While no answer has arrived
   (the request failed at the network level, so the order may or may not
   exist), a second click sends the SAME key, so the service replays the
   first outcome instead of creating a second order.
3. **Replaced when the submission changes or the service answered.** Editing
   a line, adding or removing a line, toggling "Allow partial shipment", a
   successful placement, or any problem+json answer (4xx/5xx) drops the key;
   the next click mints a new one. An edited form is a different request, and
   reusing the key with it would be a 422. An answer is final and cached by
   ADR 0023 (5xx included), so resending its key would only replay it.
4. **CORS allows the header.** `Idempotency-Key` joins `Content-Type` and
   `Authorization` in `AllowedHeaders`. Kong's fleet CORS plugin in
   warehouse-infra already allows it.

Only `POST /orders` carries a key. `retry-allocation`, `release` and
`DELETE /orders/{id}` act on a caller-supplied id and the middleware does not
apply to them (ADR 0023).

## Consequences

- Order entry from the console works against a Postgres-backed service.
- A double click or a retry after a lost response cannot duplicate an order.
- A retry after an error ANSWER (say a `500`) is a new submission with a new
  key: it re-attempts the order instead of replaying the cached answer. The
  operator has already seen that answer before choosing to retry, and
  `POST /orders` answers `201` even when the implicit allocation fails (the
  lines stay `Pending`), so an error answer is the service rejecting that
  request, not a lost success.
- `crypto.randomUUID()` needs a secure context (HTTPS or `localhost`). The
  console is served from `localhost` or behind TLS; a plain-HTTP non-local
  origin would throw when the key is minted.
- Tests: `web/src/api.test.ts` (header sent, header absent without a key,
  UUID shape), three cases in `web/src/screens/OrderManagementScreen.test.tsx`
  (UUID sent, reused on retry and fresh after success, fresh after an edit),
  and `TestCORS_PreflightAllowsIdempotencyKey`
  (`internal/adapters/inbound/http/cors_test.go`).

## Alternatives considered

- **Make the header optional on the server.** Rejected: ADR 0023 requires it
  on purpose, because an optional header is forgotten on exactly the retry
  path where it matters.
- **A new key per HTTP call.** Rejected: it removes the retry protection.
- **Derive the key from a hash of the form.** Rejected: two genuinely
  separate identical orders would collide and the second would replay the
  first.
