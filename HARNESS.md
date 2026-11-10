# HARNESS.md — order-management (warehouse-harness-template v4)

This file describes every guide and sensor this repo actually runs: what it
does, what it costs, and when in the change lifecycle it runs, so an agent
working here can answer "what harness do I have, and why" without
archaeology. It follows product-master's HARNESS.md; every row below is taken
from this repo's `.github/workflows/ci.yml`, `Makefile`, `lefthook.yml` and
`internal/architecture/`. When you add, rename or remove a CI job or Makefile
target, update the matching row in the same PR.

Reference: Birgitta Böckeler, "Harness engineering for coding agent users"
(martinfowler.com, 02 Apr 2026). Model: **guides** (feedforward) + **sensors**
(feedback), each either **computational** or **inferential**, spread across
three regulation categories: **maintainability**, **architecture fitness**,
**behaviour**.

Binaries guarded here: `cmd/a2a`, `cmd/mcp`, `cmd/order`, `cmd/order-projector`, `cmd/order-reports`.

---

## Guides (feedforward)

| File | Category | Purpose |
|---|---|---|
| `CLAUDE.md` (`AGENTS.md` is a symlink to it) | computational | Entry point every agent session reads first: identity, hard rules, commands, scoped-rules table. Carries `harness-template: v4`. |
| `.claude/rules/adrs.md` | computational | Architecture Decision Records (0001-0020 summarized in order here; 0021-0037 summarized below; read the file in `docs/docs/adr/` before acting) (path-scoped). |
| `.claude/rules/api-contracts.md` | computational | API contracts (path-scoped). |
| `.claude/rules/bounded-context-boundary.md` | computational | Bounded-context boundary (NON-NEGOTIABLE) (always loaded). |
| `.claude/rules/ci-quality-gates.md` | computational | CI / quality gates (path-scoped). |
| `.claude/rules/cloudevents-events.md` | computational | Events: CloudEvents 1.0 is MANDATORY (path-scoped). |
| `.claude/rules/deferred-and-known-gaps.md` | computational | Deferred and known gaps (document them, don't skip silently) (path-scoped). |
| `.claude/rules/domain-model.md` | computational | Domain model (path-scoped). |
| `.claude/rules/frontend-mfe.md` | computational | Frontend micro-frontend remote (`web/`) (path-scoped). |
| `.claude/rules/hexagonal-layout.md` | computational | Hexagonal layout, binaries and local run (path-scoped). |
| `.claude/rules/testing-standards.md` | computational | Code standards / testing (path-scoped). |
| `.claude/rules/fleet/*.md` | computational | Fleet-wide rules (a2a, agent-authority, cloudevents, context-boundaries, gitflow-and-ci, idempotency-and-outbox, kafka-testing-and-consumers, no-auth-and-mcp, readme-standard); canonical in IQVO/warehouse-docs `agents/fleet/`, never hand-edited here. |
| `internal/adapters/kafka/cloudevents/` | computational | The ONLY CloudEvents 1.0 envelope builder/decoder (sdk-go v2 `event`); `TestCloudEventsOnly` keeps it the only path. |
| `.claude/skills/<name>/SKILL.md` (8 skills; `.agents/skills` symlink for Codex) | inferential | How-tos for recurring task shapes (how-to-add-a-frontend-remote, how-to-add-a-rest-endpoint, how-to-add-an-integration-event, how-to-test, how-to-write-an-adr) plus the three review skills. |
| `.claude/agents/{code-review,architecture-review,domain-review}.md` | inferential | Review subagents: review never shares context with the turn that produced the diff. |
| `.claude/memory/MEMORY.md` | inferential | Durable repo-specific heuristics that are not incidents; grown by hand, never template-overwritten. |
| ADRs (`docs/docs/adr/`, 51 records) | computational | Point-in-time decisions with rationale: the source of truth for "why". |
| `INCIDENTS.md` | computational | Fleet incident log: every escaped failure, the guard that now catches it, or the open gap. |

## Sensors (feedback) — lifecycle position

CI (`.github/workflows/ci.yml`) runs on push and pull request to `develop` and
`main`, on a weekly schedule (cron `0 6 * * 1`, UTC) and on manual dispatch. A
job with no condition runs on every one of those events.

| Sensor | Category | Type | Lifecycle | Blocking? | Cost |
|---|---|---|---|---|---|
| `lefthook` pre-commit (`make fmt-check`, `make vet`, `make lint`) | maintainability | computational | pre-commit | yes (local; `lefthook install` once per clone) | seconds |
| `lefthook` pre-push (`make check` = fmt-check vet build lint test) | maintainability | computational | pre-push | yes (local) | ~1 min |
| Stop hook: `make check-fast` (fmt-check + vet + arch-test + tests of changed packages) | maintainability | computational | end of every agent turn | yes (agent loop) | seconds |
| `make check-all` (check + coverage + arch-test + bdd) | maintainability / behaviour / architecture fitness | computational | before pushing (by hand) | yes (local) | minutes |
| `lint` (golangci-lint, `.golangci.yml`) | maintainability | computational | PR + push (`develop`, `main`) | yes | seconds |
| `guide-lint` (`scripts/harness/guide_lint.py` + `repo_lint.py` + the harness scripts' own tests) | maintainability | computational | PR + push (`develop`, `main`) | yes | seconds |
| `complexity` (gocyclo/gocognit/cyclop/funlen/nestif via golangci-lint; gocyclo >10 report is informational) | maintainability | computational | PR + push (`develop`, `main`) | yes | seconds |
| `file-length` (revive, 300 code lines per production file; fails only on files the PR adds or touches) | maintainability | computational | PR + push (`develop`, `main`) | yes | seconds |
| `test` (build + vet + gofmt + `make coverage`: `go test -race`, 90% gate) | maintainability | computational | PR + push (`develop`, `main`) | yes | minutes |
| `bdd` (godog/Gherkin, `go test ./... -run TestFeatures`) | behaviour | computational | PR + push (`develop`, `main`) | yes | seconds |
| `contract` (Schemathesis over `apis/openapi.yaml`, `scripts/contract-test.sh`, pinned via `.github/requirements/schemathesis.txt`) | behaviour | computational | PR + push (`develop`, `main`) | yes | minutes |
| `evals-tests` (MCP evals: E1 schemas, E2 wire conformance, E3 behavioural, `internal/adapters/inbound/mcp`) | behaviour | computational | PR + push (`develop`, `main`) | yes | seconds |
| `integration` (`go test -tags=integration ./... -race`; testcontainers, every suite boots its own Postgres/Kafka) | behaviour | computational | PR + push (`develop`, `main`) | yes | minutes |
| `arch-test` (`go test ./internal/architecture/...`: arch-go + 13 fitness test files, see below) | architecture fitness | computational | PR + push (`develop`, `main`) | yes | seconds |
| `mutation-fast` (gremlins on `./internal/domain/order`; efficacy > 97%, mutant coverage > 99%) | maintainability | computational | PR + push (`develop`, `main`) | yes | ~1-3 min |
| `vuln` (govulncheck, deps + stdlib) | maintainability | computational | PR + push (`develop`, `main`) | yes | seconds |
| `api-lint` (Spectral, `--fail-severity=warn`, on `apis/asyncapi.yaml`, `apis/openapi-reports.yaml`, `apis/openapi.yaml`) | maintainability | computational | PR + push (`develop`, `main`) | yes | seconds |
| `docs-api-drift` (in `docs/`: `npm run gen-api-docs:all`, fails if the committed API reference differs) | maintainability | computational | PR + push (`develop`, `main`) | yes | seconds |
| `chart-tests` (`charts/order-management/tests/render_test.sh`, `charts/order-management/tests/test_service_selectors.py`, `charts/order-management/tests/test_kong_config.py`) | maintainability | computational | PR + push (`develop`, `main`) | yes | seconds |
| `helm-lint` (`ct lint` on `charts/order-management`) | maintainability | computational | PR targeting `main` | yes | seconds |
| `web` (`web/`: eslint, `tsc -b`, vitest, vite build; builds warehouse-ui-kit first) | maintainability | computational | PR + push (`develop`, `main`) | yes | minutes |
| `trivy-scan` (image scan, CRITICAL/HIGH with a fix fails; SARIF to code scanning) | maintainability | computational | PR targeting `main` | yes | minutes |
| `mutation` (gremlins, full `./internal/domain`) | maintainability | computational | weekly schedule + manual dispatch | no PR gate; a red run opens/refreshes a `harness:red` issue (`scripts/harness/red_issue.py`) | tens of minutes |
| `drift` (deadcode, `go mod tidy -diff`, knip on `web/`, `scripts/coverage-quality.py`) | maintainability | computational | weekly schedule + manual dispatch | **no — advisory** (findings go to the step summary); a red run opens a `harness:red` issue | minutes |
| `CodeQL` (`.github/workflows/codeql.yml`) | maintainability | computational | PR + push (`develop`, `main`; docs-only changes skipped), weekly schedule | yes | minutes |
| `Scorecard` (`.github/workflows/scorecard.yml`) | maintainability | computational | push to `develop`, weekly schedule | no | minutes |
| `docs` build (`.github/workflows/docs.yml`) | maintainability | computational | PR touching `docs/**` or `apis/**` (build only); push to `main` publishes GitHub Pages | yes (PR build) | minutes |
| AI review (`.github/workflows/ai-review.yml`) | architecture fitness / behaviour | **inferential** | PR into `develop` touching domain, application, `apis/` or `charts/` | no — advisory (no-ops without `ANTHROPIC_API_KEY`) | API spend |
| `/code-review`, `/architecture-review`, `/domain-review` (`.claude/agents/*.md`) | architecture fitness / behaviour | **inferential** | invoked by hand, isolated subagent context | no | LLM cost per invocation |

Not sensors, but in the same workflow: `docker-publish` (push to `main`, needs
every blocking Go job; builds, signs with cosign and attests an SBOM for
`ghcr.io/iqvo/order-management`) and `release` (tags the image, packages the Helm chart,
creates the GitHub release).

**New sensors default to advisory for ~2 weeks, then auto-promote to
blocking** (user decision, Phase 5 of the harness-coverage-expansion plan)
unless the sensor is inherently non-deterministic (inferential/LLM-based),
in which case it stays advisory until its precision is proven.

## Fitness functions and the incidents that motivated them

`make arch-test` (CI job `arch-test`) runs every test in
`internal/architecture/`. Each rule corresponds to a real, already-lived
incident or an accepted ADR, not a hypothetical:

- **TestA2AAdapterDependencyRule** (`a2a_fitness_test.go`) — the A2A inbound adapter is additive, like MCP: application + domain only, nothing depends on it.
- **TestNoOutboundA2AClient** (`a2a_fitness_test.go`) — ADR-0046: this context only receives A2A delegations; no outbound A2A client.
- **TestA2AAgentIsReadOnly** (`a2a_fitness_test.go`) — the A2A write gate: its deps carry no write use case.
  - self-tests in `a2a_fitness_test.go` (prove the sensors above can fail): `TestA2AClientSensorFailsOnBadFixtures`, `TestA2AReadOnlySensorFailsOnBadFixtures`.
- **TestHexagonalDependencyRule** (`architecture_test.go`) — the ports-and-adapters dependency direction (arch-go): domain depends on nothing, application only on domain, adapters never on each other, only `cmd` wires every layer. Never assume it holds without running it: order-management once shipped this file without wiring it into CI.
- **TestAnalyticsIsolation** (`architecture_test.go`) — ADR-0006: `internal/analytics` is an additive read side that never depends on the OLTP layers.
- **TestPortsAreCustomerOwned** (`architecture_test.go`) — ADR-0002: `internal/application/ports` expresses cross-context calls in this context's own types, never importing an adapter.
- **TestEventCatalogueMatchesContract** (`catalogue_fitness_test.go`) — every CloudEvents type `apis/asyncapi.yaml` declares for this service must appear in a CloudEvents ADR (the fleet type catalogue). Template-managed.
  - self-tests in `catalogue_fitness_test.go` (prove the sensors above can fail): `TestEventCatalogueDetector`.
- **TestCloudEventsOnly** (`events_fitness_test.go`) — CloudEvents 1.0 is the ONLY envelope: no `EVENT_ENVELOPE_MODE` toggle anywhere, and a Kafka-touching module must build envelopes with the CloudEvents SDK (template-managed).
- **TestReplayConsumersSetCommitInterval** (`events_fitness_test.go`) — a kafka-go reader on a process-unique (full-replay) group must set `CommitInterval`, else boot replay commits after every message (order-management CrashLoopBackOff). Template-managed.
- **TestMCPAdapterDependencyRule** (`fitness_test.go`) — ADR-0008 (fleet): the MCP inbound adapter is additive. It may depend only on application + domain, and nothing may depend on it.
- **TestNoAuthMiddlewareReintroduced** (`fitness_test.go`) — encodes the fleet-wide 2026-09-11 static-bearer-auth revert. An agent "helpfully" re-adding auth middleware to REST or MCP fails CI instead of shipping unreviewed.
- **TestKafkaConsumerGroupNeverHardcodedInline** (`fitness_test.go`) — wes-work-planning's hardcoded consumer group id let a locally-run e2e-tests process join the SAME group as the live Deployment on the shared broker and starve it via rebalances (wes-work-planning#67). Group ids come from config.
- **TestKafkaIntegrationTestsUseTestcontainers** (`fitness_test.go`) — the CI `integration` job provisions no service containers. A Kafka `-tags=integration` test that gates on `KAFKA_BROKERS` + `t.Skip` or hardcodes `localhost:9092` silently proves nothing; testcontainers is the only accepted variant.
- **TestPostgresIntegrationTestsUseTestcontainers** (`fitness_test.go`) — order-management's `OrderRepo` integration test gated on `DATABASE_URL` + `t.Skip` and was silently skipped in CI (the same shape existed in 10 more repos). A Postgres integration test must boot `testcontainers-go/modules/postgres`.
- **TestDomainHasNoSerializationTags** (`fitness_test.go`) — no `json:"`/`db:"` struct tags under `internal/domain/**`: wire and row shapes belong to adapter DTOs.
  - self-tests in `fitness_test.go` (prove the sensors above can fail): `TestPostgresIntegrationDetector`, `TestDomainTagSensorFailsOnBadFixtures`.
- **TestEveryIntegrationPackageRunsInCI** (`integration_coverage_fitness_test.go`) — CI escape 2026-10-11: a hand-kept list of integration packages silently skipped the MCP and use-case suites. Every package with a `//go:build integration` test must be reached by an integration `go test` line in `ci.yml`.
  - self-tests in `integration_coverage_fitness_test.go` (prove the sensors above can fail): `TestIntegrationCoverageSensorFailsOnBadFixtures`.
- **TestDeadLetterCallsGuardCancellation** (`kafka_dlq_shutdown_fitness_test.go`) — on shutdown a cancelled context made a handler fail, the retry gave up and a good message was dead-lettered and committed past (network-fulfillment#89 + eight repos). Every dead-letter send under `internal/adapters/inbound/kafka` checks `ctx.Err()`/`context.Canceled` itself or carries `//dlq:deterministic <reason>`.
  - self-tests in `kafka_dlq_shutdown_fitness_test.go` (prove the sensors above can fail): `TestDeadLetterShutdownSensorBites`.
- **TestKafkaWritersSetRequiredAcks** (`kafka_writer_acks_fitness_test.go`) — every production kafka-go `Writer{...}` sets `RequiredAcks` (kafka-go defaults to acks=0, fire-and-forget).
  - self-tests in `kafka_writer_acks_fitness_test.go` (prove the sensors above can fail): `TestKafkaWriterAcksSensorBites`.
- **TestKafkaWriterTopicNotSetTwice** (`kafka_writer_topic_fitness_test.go`) — kafka-go rejects a write whose `Message.Topic` is set when the `Writer` has a `Topic` too, so dead letters never landed in two repos (gap E6).
  - self-tests in `kafka_writer_topic_fitness_test.go` (prove the sensors above can fail): `TestKafkaWriterTopicSensorBites`.
- **TestEveryMCPToolDeclaresAuthority** (`mcp_authority_fitness_test.go`) — agent authority (fleet rule `agent-authority`, ADR 49): every `mcp.Tool` declares its level in `_meta` `com.warehouse/authority`.
- **TestMCPWriteToolsAreAtLeastL3** (`mcp_authority_fitness_test.go`) — a tool that is not provably read-only is L3, or L4 with an ADR comment.
- **TestA2ASkillsDeclareAuthority** (`mcp_authority_fitness_test.go`) — every `a2a.AgentSkill` carries `authority:L<n>`; write skills are L3/L4.
- **TestMCPServerOffersResourceAndPrompt** (`mcp_authority_fitness_test.go`) — AS-3: the MCP server offers at least one resource and one prompt, not tools alone.
  - self-tests in `mcp_authority_fitness_test.go` (prove the sensors above can fail): `TestMCPAuthoritySensorBites`, `TestMCPWriteLevelSensorBites`, `TestA2AAuthoritySensorBites`, `TestMCPSurfaceSensorBites`, `TestAgentAdapterSkipRules`.
- **TestOptionalMCPInputsCarryOmitempty** (`mcp_optional_inputs_fitness_test.go`) — report tools described filters as optional while their input structs had no `omitempty`, so the MCP SDK published them as REQUIRED (gap D16). An "optional" field must carry `omitempty`.
  - self-tests in `mcp_optional_inputs_fitness_test.go` (prove the sensors above can fail): `TestOptionalMCPInputSensorFailsOnBadFixtures`.
- **TestOutboxRelayCallsSetMaxAttempts** (`outbox_relay_fitness_test.go`) — inventory-storage's relay had no attempt cap, so one unsendable row blocked the relay forever (gap D12). Every outbox relay built by a composition root passes `WithMaxAttempts(...)` (env `OUTBOX_MAX_ATTEMPTS`).
  - self-tests in `outbox_relay_fitness_test.go` (prove the sensors above can fail): `TestOutboxRelayCapSensorBites`.

## Mutation testing: what it catches that coverage doesn't

`go test -cover` measures whether a line EXECUTED during a test run.
gremlins measures whether a test would actually FAIL if that line's logic
were subtly wrong (each mutant is one such subtle change: `>=` to `>`, a
negated boolean, a swapped operand). 100% line coverage with a surviving
mutant means a test exercises the code path but asserts nothing about its
actual behaviour on that path — the "green tests that assert nothing"
failure mode the `drift` job's coverage-quality check is built to catch.

In this repo: thresholds live in `.gremlins.yaml` (efficacy
97, mutant coverage 99); the blocking
`mutation-fast` job runs `gremlins unleash ./internal/domain/order`; the weekly `mutation`
job runs the whole `./internal/domain`. Locally: `make mutation-fast`, `make mutation`.

**gremlins fails when the measured value is `<=` the configured threshold**
(not `<`) — see `.gremlins.yaml`'s own comment. Always measure first, then
set the threshold strictly below what you measured; never copy a sibling
repo's numbers.

**Pitfall: a tagless `switch { case ...: ... }` can under-report mutator
coverage versus the equivalent `if / if / return` chain**, even with
identical test coverage (warehouse-ops-agent PR #58: 89.91% as a switch,
97.17–97.25% as an if-chain, zero test changes). If a pure classification
function's mutator coverage comes in surprisingly low despite full branch
coverage, try the if-chain rewrite before assuming tests are missing a case.

## Credential handling in this harness

Never embed a real password in a `scheme://user:PASSWORD@host` URL anywhere
in committed files, including local-dev-only test fixtures. Some agent
tooling treats that literal shape as a credential leak and will silently
strip/redact it on write AND read-back — a write that "succeeds" but reads
back unchanged is that guard firing, not a bug to retry around. Use a
password-less URL plus a separate `PGPASSWORD` env var, or your driver's own
password-injection API (`pgx.ParseConfig` + setting `.Password`).

## Agent runtime harness: one logic, three runtimes

The guides LOAD and the sensors run INSIDE the agent loop, for Claude Code,
OpenCode and Codex.

| Piece | File(s) | Cost | Lifecycle position |
|---|---|---|---|
| Skills that actually load | `.claude/skills/<name>/SKILL.md` (frontmatter `name`+`description`); `.agents/skills` symlink for Codex; OpenCode scans `.claude/skills` natively | ~50 tokens each until used | on task match |
| Review subagents | `.claude/agents/{code-review,architecture-review,domain-review}.md`, invoked via `/code-review`, `/architecture-review`, `/domain-review` | LLM | on demand, pre-merge |
| Path-scoped rules | `.claude/rules/*.md` with `paths:`; `CLAUDE.md` carries a generated pointer table for runtimes without scoping | tokens only when matching files are touched | on edit |
| Guard + feedback hooks | `scripts/harness/hook.py` (`pre` / `post` / `stop`) + adapters `.claude/settings.json`, `.codex/hooks.json`, `.opencode/plugins/harness.ts` | ms to seconds | every tool call; end of turn |
| Fast gate | `make check-fast` (fmt-check + vet + arch-test + tests of changed packages) | seconds | Stop hook, before "done" |
| Guide freshness | `scripts/harness/guide_lint.py` / `make guide-lint` (CI job `guide-lint`) | CPU, ms | PR |
| Red-sensor loop | `scripts/harness/red_issue.py` steps in the scheduled `mutation` and `drift` jobs -> `harness:red` issue | none | weekly |
| Fleet-rule drift | `scripts/harness/fleet_drift.py` (fails if `.claude/rules/fleet/*.md` differ from the canonical IQVO/warehouse-docs copy; needs `gh`) | gh calls | on demand / template sync |
| Advisory AI review | `.github/workflows/ai-review.yml` (needs `ANTHROPIC_API_KEY`; no-ops without it) | API spend | PR into develop |

Rules live ONLY in `hook.py`; the three adapters translate events and cannot
drift. The hooks block pushes to develop/main, `--no-verify`, force-push,
`rm -rf` on non-scratch paths, destructive git, and edits to generated files;
they feed gofmt/vet findings back after each edit. Every message states
WHAT / WHY / FIX. `HARNESS_PROTECT_THRESHOLDS=1` (fix-agent mode) additionally
forbids editing gates (`.gremlins.yaml`, `.golangci.yml`,
`internal/architecture/**`, CI). `HARNESS_OFF=1` disables the lot. The hook
tests run in CI (`scripts/harness/test_hook.py`, in the `guide-lint` job).

Runtime notes (verified against real runs, 2026-10-03):
- Claude Code: hooks run in any trusted workspace; project `permissions.allow`
  is ignored until the user accepts the workspace trust dialog once.
- Codex: project hooks run only after the user trusts them once (`/hooks`), or
  per run with `--dangerously-bypass-hook-trust` (+ `--enable hooks`, project
  trusted). Codex may write files through shell heredocs rather than
  `apply_patch`, so the PostToolUse gofmt feedback can be skipped; the Stop
  gate (`make check-fast`) and lefthook are the backstop.
- OpenCode 2.x: the plugin uses `ctx.permission.hook("evaluate")` (deny) and
  `ctx.tool.hook`; the 1.x `server()` hooks are exported from the same file.
  The idle -> re-prompt stop gate is best-effort.

The managed files (`scripts/harness/*`, the hook adapters, the review
skills/agents, the generated scoped-rules block in `CLAUDE.md`) are owned by
IQVO/warehouse-harness-template and re-applied by its migrate_v3 / migrate_v4
tools (the weekly template sync); fix them there, not here. The fleet audit
and health reports (harness-audit, harness-health) also run from the
template repo, not from this one.

## Config sensors (`repo_lint`) and the incident protocol

`scripts/harness/repo_lint.py` (stdlib only, runs in the `guide-lint` CI job)
checks the configuration mistakes that reached CI in practice: **R1** workflow
`working-directory`/`cache-dependency-path` that do not exist (checkout-path
aware), **R2** `needs:` naming jobs that do not exist, **R3** Dependabot
entries that cannot work (missing directory or manifest; an npm tree with a
`file:` dependency outside the repo), **R4** a stale GitHub owner in
registry/docs/chart URLs, **R5** unsubstituted `{{PLACEHOLDER}}`s.
`# repo-lint: ignore` suppresses one line (say why). Tests:
`scripts/harness/test_repo_lint.py`; two cases are the real incidents and must
keep failing the lint.

**Incident protocol.** Every failure that was not caught before CI ends with:
the fix, a sensor or guide that would have caught it, that sensor rolled out
to the whole fleet, and a row in [INCIDENTS.md](INCIDENTS.md) (the fleet-wide
log, identical in every repo). A row with no guard is an open gap and says so.
A check whose message needed a human to explain it is not done: sensor output
is a prompt, so it must state WHAT, WHY and FIX.

## Versioning

This repo is at `harness-template: v4` (marker in `CLAUDE.md`). v4 = the
Harness² gap-closing release: review skills as isolated subagents
(`.claude/agents/*`) and `.claude/memory/MEMORY.md`; builds on v3 (agent
runtime harness: loadable skills, hooks for Claude Code / Codex / OpenCode,
guide-lint, red-issue loop) and v2 (CloudEvents 1.0 mandatory). The fleet
audit reads the marker to tell which repos are behind the template.
