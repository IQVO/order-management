# Order Management

Generic/Supporting bounded context: order intake, per-line stock allocation,
a capability-derived delivery promise, and choreographed release — the
upstream Open Host Service for the `warehouse-systems` fleet. Owns the
**Order** aggregate (with its **OrderLine** entity) as a first-class concept
(previously an unowned string reinvented by three other services). Study project, not a production system,
not affiliated with any real-world company.

Design source of truth: `/Users/claudioed/warehouse-systems/.hermes/plans/2026-08-25_023800-order-management.md`
and the fleet DDD docs `/Users/claudioed/docs/amazon-fulfillment-ddd.md` and
`/Users/claudioed/warehouse-systems-ddd.md`. Honor their ubiquitous language
and strategic classifications.

## Commands

```bash
make check          # fmt-check + vet + build + lint + test — run after every change
make check-fast     # quick gate; run before saying "done"
make check-all      # check + coverage (90% gate) + arch-test + bdd
```

Other targets (integration, coverage, bdd, arch-test, mutation-fast, vuln),
docs-site regeneration and local run: `.claude/rules/ci-quality-gates.md` and
`.claude/rules/hexagonal-layout.md`.

## Hard rules (NON-NEGOTIABLE)

- **Hexagonal:** domain depends on nothing; application depends on domain;
  adapters depend on application/domain. No framework, HTTP or SQL type ever
  appears in the domain layer. Enforced by the arch-go fitness test
  (`make arch-test`). `internal/analytics/report` depends on nothing internal.
- **One public command (ADR-0005):** callers place an order via `POST /orders`
  alone; allocation + release are a folded, best-effort saga inside it. There is
  NO public `/allocate` verb and NO `WorkReleaseClient`. `POST /orders/{id}/release`
  exists only for orders received with `releaseOnAllocation=false` (ADR-0020).
  Read `.claude/rules/bounded-context-boundary.md` before touching the
  allocation/release path.
- **Bounded context:** MUST NOT import Go packages from `inventory-storage`,
  `wes-work-planning` or any sibling context, and never touch another
  service's DB — only their published HTTP/Kafka contracts. Release is Kafka
  choreography, never an HTTP call to wes-work-planning; the work-unit id
  `{orderID}-line-{lineNo}` is a frozen contract that must match byte-for-byte.
- **CloudEvents 1.0 is MANDATORY** for every Kafka message produced or
  consumed (integration `warehouse.<ctx>.events` and analytics
  `warehouse.<ctx>.analytics`), structured content mode:
  - No flat envelope, no dual-write/dual-read, no envelope toggle env var.
  - Build/validate/(un)marshal ONLY via `internal/adapters/kafka/cloudevents/`
    (sdk-go v2 `event`); transport stays kafka-go.
  - `type` = `com.warehouse.wes.order-management.<entity>.<EventName>`;
    `id` stable across outbox redelivery; `subject` = aggregate id (the order id).
    A breaking payload change => new `.v2` type + new dataschema, never mutate.
  - Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
    `id`, and DLQ/skip (never crash, never parse a legacy shape) anything
    failing validation.
  - Attribute list, header, consumed types: `.claude/rules/cloudevents-events.md`; ADR-0030.
- **Planned capacity is advisory (ADR-0031):** `warehouse-planning`'s CapacityPlan events feed a LOCAL
  read model (`order.PlannedCapacityWindow`, Postgres `planned_capacity_windows`; never a live call to
  that service). A PUBLISHED shortage overlapping an order's `[now, promise cutoff)` at
  `PLANNED_CAPACITY_SITE_ID` only ANNOTATES the order response (`capacityConstraint`, derived at read
  time): it MUST NOT move a promise, reject/hold an order, change `Status`, or touch allocation or
  reservations (ADR-0003/0017 fill-or-kill is unchanged). The consumer group id comes from
  `PLANNED_CAPACITY_CONSUMER_GROUP` (no default; unset = feature off). Read the ADR before touching
  `internal/domain/order/planned_capacity.go` or `inbound/kafka/planned_capacity_consumer.go`.
- **Tests:** never hit a real network in a unit test. Kafka `-tags=integration`
  tests use testcontainers, never a skip-gated `KAFKA_BROKERS` check. One
  httptest per REST endpoint (success + error path). Every named invariant in
  `domain-model.md` needs a dedicated failing-path test.
- **Gates:** coverage 90% on `internal/domain` + `internal/application`; do not
  let a change regress below the mutation thresholds pinned in `.gremlins.yaml`
  (efficacy 89 / mutant-coverage 83). Details: `.claude/rules/testing-standards.md`.
- **Docs:** regenerate the Docusaurus API reference after ANY
  `apis/openapi.yaml` change (procedure in `ci-quality-gates.md`; the
  `docs-api-drift` CI check fails on any drift).
- **`web/`** is a separate Vite/React MFE remote, NOT part of the Go module and
  never part of `make check`/`check-all`; it talks only to this service's own
  REST API. See `.claude/rules/frontend-mfe.md`.

## Rule files (read BEFORE editing the matching area; OpenCode/Codex don't auto-load)

`domain-model.md` (invariants, events, promise/hold) · `adrs.md` (ADR index) ·
`deferred-and-known-gaps.md` (intentionally unbuilt) · `hexagonal-layout.md`
(ports/adapters notes, local run) · `cloudevents-events.md` · `testing-standards.md`
· `bounded-context-boundary.md` (always loaded).

## Git workflow

GitFlow: `feature/*` branches off `develop`, PR into `develop`
(`gh pr create --base develop`); `develop` promotes to `main` for release.
Do not merge your own PR — leave it open for independent review. Do not
force-push over history once pushed.

Fleet-wide rules: `.claude/rules/fleet/*.md` (canonical in IQVO/warehouse-docs `agents/fleet/`; never hand-edit).

<!-- harness:scoped-rules:start (generated by tools/migrate_v3.py in warehouse-harness-template; do not hand-edit) -->
## Scoped rules and harness

Claude Code loads each rule below automatically when you touch the matching paths. OpenCode and Codex do NOT: read the rule BEFORE editing matching files.

| When touching | Read |
|---|---|
| `docs/docs/adr/**` | `.claude/rules/adrs.md` |
| `internal/adapters/inbound/http/**`, `apis/openapi*.yaml`, `apis/openapi/**` | `.claude/rules/api-contracts.md` |
| `.github/**`, `Makefile`, `.gremlins.yaml` ... | `.claude/rules/ci-quality-gates.md` |
| `internal/adapters/outbound/kafka/**`, `internal/adapters/outbound/kafkacatalog/**`, `internal/adapters/outbound/kafkacptschedule/**` ... | `.claude/rules/cloudevents-events.md` |
| `internal/domain/**`, `internal/application/**`, `README.md` ... | `.claude/rules/deferred-and-known-gaps.md` |
| `internal/domain/**`, `internal/application/**`, `features/**` | `.claude/rules/domain-model.md` |
| `web/**` | `.claude/rules/frontend-mfe.md` |
| `internal/**`, `cmd/**`, `migrations/**` ... | `.claude/rules/hexagonal-layout.md` |
| `**/*_test.go`, `features/**`, `internal/architecture/**` ... | `.claude/rules/testing-standards.md` |

Hooks (`scripts/harness/hook.py`, wired for Claude Code, Codex and OpenCode) block pushes to develop/main, `--no-verify`, bare `rm -rf`, and edits to generated files, and feed gofmt/vet findings back after each edit. Before saying "done" run `make check-fast`; the full gate is `make check-all`. `HARNESS_OFF=1` disables the hooks when debugging the harness itself.
<!-- harness:scoped-rules:end -->
