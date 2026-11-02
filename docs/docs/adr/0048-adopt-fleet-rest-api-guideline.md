---
id: 0048-adopt-fleet-rest-api-guideline
slug: /adr/0048-adopt-fleet-rest-api-guideline
title: "0048. Adopt the fleet REST API guideline"
sidebar_label: "48. Fleet REST API guideline"
sidebar_position: 48
description: "ADR 0048 — order-management adopts the fleet REST API guideline: noun routes (/orders/{id}/releases, /orders/{id}/allocation-retries), every POST behind the Idempotency-Key middleware, key reuse is 409 not 422, Idempotency-Replayed on every replay. Amends ADR 0023 and ADR 0038."
---

# 0048. Adopt the fleet REST API guideline

## Status

Accepted (2026-11-01). Amends [ADR 0023](./0023-idempotency-key-middleware.md)
(reuse is now `409`, not `422`, and the middleware wraps every `POST`, not only
`POST /orders`) and [ADR 0038](./0038-mfe-sends-idempotency-key-per-submission.md)
(every command carries a key, not only `POST /orders`). Part of the fleet-wide
REST cutover: it ships and is deployed together with the other services'
cutover PRs, because the new routes and the key contract are breaking for every
client.

## Context

The 2026-10-30 portfolio assessment measured two spellings of the replay header,
key reuse answered `422` in most services and `409` in one, three command-URL
styles (`:verb`, verb sub-paths, sub-resources) and unpaginated lists. The fleet
standard removes each divergence and is enforced by a Spectral ruleset
(`.spectral.yaml` + `.spectral/functions/restGuideline.js`, rules R1-R8) in the
`api-lint` job with `--fail-severity=warn`.

Before this ADR only `POST /orders` was idempotent. `POST /orders/{id}/release`
and `POST /orders/{id}/retry-allocation` were protected only by the optimistic
`version` check (ADR 0024), which stops a double effect but answers the retry
with a confusing `409 concurrent-modification`.

## Decision

1. **Routes.** The two verb routes become transition resources, with the same
   status codes and bodies:
   `POST /orders/{id}/release` to `POST /orders/{id}/releases` and
   `POST /orders/{id}/retry-allocation` to `POST /orders/{id}/allocation-retries`.
   `operationId`s are unchanged. The old routes are removed, not aliased
   (`TestRetiredRoutesAreGone`).
2. **Idempotency on every POST.** `POST /orders`, `/releases` and
   `/allocation-retries` all sit behind the middleware, and each use case joins
   the unit of work, so the key row, the order change and the outbox rows commit
   together. The optimistic-concurrency check stays as a second line of defence.
   `DELETE /orders/{id}` is idempotent by method and takes no middleware.
3. **Key contract.** The key is global (looked up by key alone, compared by
   method + path + SHA-256 of the body). Same key and same request replays the
   stored answer with `Idempotency-Replayed: true`; same key and a different
   request is `409` problem `idempotency-key-reused`; a missing key is `400
   idempotency-key-required`; a key that is not 1-128 printable ASCII is `400
   idempotency-key-invalid`. `Idempotent-Replayed` is retired.
4. **Pagination.** `GET /planned-capacity` is a read model bounded by a window
   and carries `x-pagination-exempt: report`. `GET /orders/{id}` is a single
   resource.

## Consequences

- Breaking for clients: the two routes moved, reuse is `409`, and `release` and
  `retry-allocation` now require a key. network-fulfillment (which calls
  `POST /orders` and the release route) and e2e-tests change in their own PRs.
- A retry of a `release` after a timeout now replays the first answer instead of
  failing with `concurrent-modification`.
- Existing in-flight keys keep working: a key first used before the deploy is
  looked up by key alone, so it replays; only a retry whose body changed gets
  `409`.
