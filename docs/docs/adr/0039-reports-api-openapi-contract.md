---
id: 0039-reports-api-openapi-contract
slug: /adr/0039-reports-api-openapi-contract
title: "0039. The reports binary's routes are a contract: apis/openapi-reports.yaml"
sidebar_label: "39. Reports API contract"
sidebar_position: 39
description: "ADR 0039 — every route of cmd/order-reports (GET /healthz, /reports/funnel, /reports/funnel/freshness) is specified in apis/openapi-reports.yaml, linted by Spectral, rendered as a generated Reports API reference and pinned by Go contract tests against the real router. Fleet gap-fix decision D18 (the labor-performance convention)."
---

# 0039. The reports binary's routes are a contract: `apis/openapi-reports.yaml`

## Status

Accepted — implemented by the change that introduces this record (fleet
gap-fix wave 2026-10-12, decision D18).

Extends [ADR 0006](./0006-analytical-data-product.md) (the analytical data
product and its separate `cmd/order-reports` reader).

## Context

ADR 0006 split the Order Funnel & Allocation Health data product into a
writer (`cmd/order-projector`) and a read-only reader (`cmd/order-reports`,
default `:8092`). The reader serves three routes (`NewReportsRouter` in
`internal/adapters/inbound/http/reports_handler.go`): `GET /healthz`,
`GET /reports/funnel` and `GET /reports/funnel/freshness`. Other contexts
call them: the console's context-reports page and warehouse-ops-agent
(`ORDER_MANAGEMENT_REPORTS_REST_URL`).

None of these routes was in any spec. `apis/openapi.yaml` describes
`cmd/order` only. The shape lived in a hand-written page
(`docs/docs/analytics/order-funnel-report.md`) and in the handler's DTOs, so
nothing linted it, nothing rendered it and nothing failed when the handler
and the documentation disagreed. labor-performance already solved the same
problem with a second spec, `apis/openapi-reports.yaml`, and the fleet chose
that convention for every `*-reports` binary.

## Decision

1. **A second OpenAPI 3.0.3 file, `apis/openapi-reports.yaml`,** specifies
   every route of `cmd/order-reports`: operations `getReportsHealthz`,
   `getOrderFunnelReport` (query `from`, `to` required RFC 3339; `pathId`
   optional; `granularity` optional, only `hour`) and
   `getOrderFunnelReportFreshness`, the `FunnelRow` with all 17 fields the
   handler emits, `Freshness`, `Health` and the RFC 7807 `Problem` with the
   two reports problem types (`invalid-report-query` 400,
   `report-store-error` 500). Server `http://localhost:8092`. No security
   scheme (ADR 0012). It is a description of what already ships: no route,
   field or status changes.
2. **Linted** by the `api-lint` CI job with the same `.spectral.yaml` ruleset
   as the OLTP spec.
3. **Rendered** as a generated "Reports API Reference" in the docs site: a
   second `docusaurus-plugin-openapi-docs` config (`orderReports`, output
   `docs/docs/api-reference/rest-reports`), with `gen-api-docs:all` /
   `clean-api-docs:all` scripts. The `docs-api-drift` job regenerates both
   specs and fails on any difference.
4. **Contract-tested against the real router.** Schemathesis (the `contract`
   job) boots `cmd/order` in memory; `cmd/order-reports` refuses to start
   without an analytical database, so instead two Go tests in the `test`
   job read the spec:
   - `TestReportsContract_RoutesMatchSpec` walks the chi router and fails
     when a route is served but not specified, or specified but not served.
   - `TestReportsContract_ResponsesMatchSpec` drives every documented status
     of every operation with a stub store and fails on an undeclared status
     or media type, a missing required property or an undocumented property
     (rows included).

## Consequences

- A new reports route or field fails the build until the spec, and so the
  generated reference, describe it.
- Consumers (console, ops agent) have a published shape to code against.
- The spec does not prove value semantics (that a counter counts the right
  event); the analytics store's integration tests and the handler unit tests
  keep doing that.
- `gopkg.in/yaml.v3`, already in the module graph, becomes a direct
  (test-only) dependency.

## Alternatives considered

- **Add the routes to `apis/openapi.yaml`.** Rejected: that spec describes
  `cmd/order` on `:8080`, and Schemathesis would then call `/reports/*` on a
  binary that does not serve them.
- **Schemathesis against `cmd/order-reports` with a testcontainers
  Postgres.** Rejected for now: it needs the projector's migrations and a
  container in the `contract` job, for a read-only surface with three GET
  operations whose inputs the Go tests already cover.
