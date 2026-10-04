---
paths:
  - ".github/**"
  - "Makefile"
  - ".gremlins.yaml"
  - ".golangci.yml"
  - "lefthook.yml"
  - "docs/**"
---

# CI / quality gates

## GitHub Actions workflows (`.github/workflows/`)

### `ci.yml` — triggers on push/PR to `main`, `develop` (+ weekly schedule for `mutation`)

- **`lint`** — golangci-lint v2.13.1, `--timeout=5m`.
- **`test`** — build, vet, gofmt check, `go test ./... -race
  -coverprofile=coverage.out -coverpkg=./internal/domain/...,./internal/application/...`,
  then a 90% coverage gate on that coverprofile.
- **`bdd`** — `go test ./... -run TestFeatures -v` (godog/Gherkin acceptance
  suite, `features/*.feature`).
- **`integration`** — `go build/vet -tags=integration ./...` then
  `go test -tags=integration ./internal/adapters/outbound/kafka` (+
  `kafkacatalog`, `kafkacptschedule`, `kafkapathcapacity`,
  `./internal/adapters/inbound/kafka` and, since ADR-0031,
  `./internal/adapters/outbound/postgres`) against a **Testcontainers** Kafka
  broker (and, for the planned-capacity tests, a Testcontainers Postgres) (no external Kafka service in this workflow — do not write a
  `KAFKA_BROKERS`-skip-gated test, it silently no-ops in CI). NOTE: the
  job provisions no external Postgres either: the Postgres tests that own a
  Testcontainers Postgres (the `outbound/postgres` outbox and
  planned-capacity tests) run, but any `DATABASE_URL`/
  `ANALYTICS_DATABASE_URL`-skip-gated test (`outbound/analyticsstore`, and
  any such test left in `outbound/postgres`) is compiled/vetted and skips.
- **`mutation-fast`** — blocking gremlins subset over `internal/domain/order`
  only, every push/PR. Thresholds in `.gremlins.yaml` (gremlins fails when
  the measured value is `<=` the threshold, so the threshold sits strictly
  below what's currently achieved — re-measure and ratchet, don't just raise
  the number).
- **`mutation`** — exhaustive gremlins run over `internal/domain`, weekly
  schedule + `workflow_dispatch` only (slow: up to 90 min).
- **`vuln`** — `govulncheck ./...`.
- **`api-lint`** — Spectral lint on `apis/openapi.yaml` and
  `apis/asyncapi.yaml`.
- **`arch-test`** — `go test ./internal/architecture/... -v` (arch-go
  fitness tests: hexagonal dependency rule, analytics isolation, ports
  customer-owned).
- **`docs-api-drift`** — `npm run clean-api-docs order && npm run
  gen-api-docs order` in `docs/`, then `git diff --exit-code -- docs/docs/
  api-reference/rest`. **Known defect:** that step runs with
  `working-directory: docs`, so the pathspec resolves to
  `docs/docs/docs/api-reference/rest` (nonexistent) and the check always <!-- guide-lint: ignore -->
  passes — it let ADR-0020's `releaseHeldOrder` endpoint land with no
  generated page. The working pathspec from `docs/` is
  `docs/api-reference/rest`. <!-- guide-lint: ignore --> Until the workflow is fixed, run the
  procedure below by hand after any `apis/openapi.yaml` change.
- **`web`** — builds `warehouse-ui-kit` (checked out at `develop`), then
  `npm ci`, `npm run lint`, `npx tsc -b`, `npm test`, `npm run build` in
  `web/`.
- **`drift`** — advisory, `schedule`/`workflow_dispatch` only, never blocks
  a PR: `deadcode`, `go mod tidy -diff`, `knip` over `web/`, and
  `scripts/coverage-quality.py` (high-coverage functions with a surviving
  gremlins mutant) written to the job summary.
- **`helm-lint`** — `ct lint --charts charts/order-management` — runs ONLY
  for a `pull_request` with `base_ref == 'main'` (i.e. the develop→main
  release PR), never on develop pushes/PRs.
- **`trivy-scan`** — builds the image (no push), blocks on CRITICAL/HIGH
  with a known fix — same `base_ref == 'main'` gating as `helm-lint`.
  Always uploads SARIF regardless of pass/fail.
- **`docker-publish`** — push-to-`main` only, needs `[lint, test]`. Builds,
  pushes to GHCR, cosign-signs keylessly, generates + attests an SPDX SBOM.
- **`release`** — push-to-`main` only, needs `[docker-publish]`. Auto-
  increments `vX.Y.Z` (patch), tags the already-published image, packages
  and pushes the Helm chart to `oci://ghcr.io/claudioed`, creates the git
  tag + GitHub Release.

`bdd`/`mutation-fast`/`mutation`/`vuln`/`api-lint`/`arch-test`/
`docs-api-drift` were added in PR #34 (fleet-parity quality sensors),
silently dropped as collateral damage in PR #35's "harden security" pass
(only `mutation`/`bdd` were documented as deliberately deferred there — the
rest had no such rationale, and `.gremlins.yaml`/`features/*.feature`/
`internal/architecture/` were never deleted), then restored to this list on
2026-09-13. Verify job names directly in `.github/workflows/ci.yml` before
relying on this summary if it's been a while — job composition has moved
between commits before.

### `docs.yml` — Docusaurus site build + GitHub Pages deploy

- **Trigger: `push: branches: [main]`, paths `["docs/**",
  ".github/workflows/docs.yml"]`, plus `workflow_dispatch`.** It does
  **NOT** trigger on `develop` pushes or on PRs at all — only a push to
  `main` (i.e. after a release PR merges) that touches `docs/**` deploys
  the live site at `https://claudioed.github.io/order-management/`.
- Job: `npm ci && npm run build` in `docs/`, uploads the built `build/`
  directory (gitignored) as a Pages artifact, then deploys via `actions/deploy-pages`.
- **This means a `docs/docs/api-reference/**` regeneration PR merged into
  `develop` alone does NOT redeploy the live site** — it only takes effect
  once `develop` promotes to `main` per the fleet's GitFlow release process.

### `codeql.yml` / `scorecard.yml`

Standard security scanning, `codeql.yml` on push/PR to `main`+`develop`
plus a weekly schedule; `scorecard.yml` on push to `main` plus weekly.

## OpenAPI docs-drift check & regeneration procedure

The Docusaurus API-reference pages (`docs/docs/api-reference/rest/*.api.mdx`)
are **generated artifacts**, not hand-edited files — each embeds a
compressed, base64-encoded copy of the relevant OpenAPI operation object in
its `api:` frontmatter field. They can go stale silently: a commit that
edits `apis/openapi.yaml` but forgets to re-run generation leaves committed
`.mdx` files whose embedded payload no longer matches the spec (verified
concretely: PR removing `security: []`/`securitySchemes.bearerAuth` from
`apis/openapi.yaml` did NOT regenerate the 5 `.api.mdx` files, leaving a
stale `"security": []` embedded in each — caught only by diffing the
decoded embedded payload, not by any file-timestamp or line-count check).

**To check for drift, don't just diff a fresh `gen-api-docs` run against
committed files** — a plain re-run without first cleaning can look
identical even when stale, because the generator sometimes preserves
untouched fields. The reliable check is:

```bash
cd docs
npm ci
npm run clean-api-docs order   # removes every generated file first
npm run gen-api-docs order     # regenerates from apis/openapi.yaml
cd ..
git status --short docs/docs/api-reference   # non-empty => real drift existed
```

If drift is found, regenerate (the steps above already did it), then:

```bash
cd docs && npm run build   # onBrokenLinks / onBrokenAnchors are both 'throw' — must pass clean
cd ..
git add docs/docs/api-reference
git commit -m 'docs: regenerate OpenAPI reference pages from apis/openapi.yaml'
git push -u origin <branch>
gh pr create --base develop --head <branch> --title '...' --body-file <file>
```

Never merge the PR yourself.

## AsyncAPI narrative-docs staleness

This repo does **not** run the AsyncAPI static-site generator (that is
reserved for the fleet aggregator repo per the fleet's docs convention).
Instead, `apis/asyncapi.yaml`'s channels/events are documented **narratively**
in plain markdown under `docs/docs/ddd/domain-events.md`,
`docs/docs/analytics/order-funnel-report.md`, and the relevant ADRs
(0005, 0006, 0008, 0018, 0019). When editing `apis/asyncapi.yaml` (new channel, renamed
event, changed payload shape), grep those narrative docs for the old
name(s) and update them by hand — there is no automated generation or CI
gate to catch this drift, unlike the OpenAPI case above.

## Docs site and golangci-lint commands

```bash
# Docusaurus — regenerate after ANY apis/openapi.yaml change
cd docs && npm ci
npm run clean-api-docs order && npm run gen-api-docs order   # -> docs/docs/api-reference/rest/
npm run typecheck && npm run build   # onBrokenLinks / onBrokenAnchors are both 'throw'

# golangci-lint (CI-pinned version)
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1
```

Other make targets: `make integration` (Kafka adapters against a
Testcontainers broker), `make coverage` (go test -race -coverprofile + the 90%
gate), `make bdd` (godog suite), `make mutation-fast` (gremlins subset on
`internal/domain/order`, thresholds in `.gremlins.yaml`), `make vuln`
(govulncheck).
