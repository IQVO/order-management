---
name: how-to-test
description: Write or review tests and diagnose a failing coverage, mutation, bdd or integration CI job: the four test layers, the 90% gate, gremlins threshold semantics, the testcontainers rule. Use when adding tests, killing a surviving mutant, or fixing a red check.
---

# How to test

Use when writing or reviewing tests in this repo, or diagnosing a failing
`coverage`/`mutation-fast`/`bdd`/`integration` CI job. This fleet's quality
bar is layered — passing `go test` is necessary but is the WEAKEST signal
of the four; mutation testing exists specifically because green tests can
assert nothing.

## The four layers, in order of what they actually prove

1. **Unit tests** (`go test ./...`) — prove the code runs without
   panicking and returns SOMETHING. Table-driven, in-memory adapters only
   (`internal/adapters/outbound/memory/`) and the fake inventory-storage
   client (`internal/application/usecases/fakes_test.go`), never a real
   network/DB call.
2. **Coverage** (`make coverage`, 90% gate on
   `./internal/domain/...,./internal/application/...` — see
   `COVERAGE_THRESHOLD := 90` in the `Makefile`) — proves lines executed.
   Proves nothing about whether the test asserted the right thing.
3. **Mutation testing** (`make mutation-fast`, gremlins) — proves the
   tests actually ASSERT, not merely execute. A mutant is a deliberately
   broken version of the code (`<` -> `<=`, `+` -> `-`, etc.); if the test
   suite still passes against the mutant, it "survived" (LIVED) — meaning
   no test would catch that exact bug in production. This is the sensor
   most worth understanding deeply; the pitfalls below are all about it.
4. **BDD / behaviour** (`make bdd`, godog) — proves the use case works
   end-to-end through the real HTTP surface (`features_test.go` drives
   `features/*.feature` against a real chi router with in-memory
   adapters), not through a mocked port.

## Mutation testing: `<=` fails, not `>=`

This repo's `.gremlins.yaml` sets `efficacy`/`mutant-coverage` as a floor
gremlins fails on if the MEASURED score is `<=` the threshold. As of the
current baseline (re-measured 2026-09-13 after ADR-0014's
`promise_policy.go`/`promise_basis.go` additions shifted the mutant
population from 53 to 67 mutants on `internal/domain/order`): 67 mutants,
killed 50 / lived 6 / not covered 11, efficacy 89.29%, mutant-coverage
83.58% — the gate is pinned at `efficacy: 89` / `mutant-coverage: 83`,
strictly below the measured value so a 100%-clean run doesn't accidentally
fail the "<=" comparison. When you deliberately lower coverage of a
package (rare, but happens when removing dead code), you may need to
lower the threshold in the SAME PR with a dated comment explaining why —
never silently; a future reader needs to know the drop was intentional,
not a regression that slipped through. Read the dated comment at the top
of `.gremlins.yaml` itself — it documents exactly why the 6 lived + 11
uncovered mutants (boundary conditionals in `promise.go`'s lead-time
fallbacks and `promise_policy.go`'s basis-selection branches) are tracked
as follow-up rather than chased immediately.

## Three real pitfalls that have each cost a real CI failure in this fleet

### 1. Zero/origin-value fixtures hide arithmetic mutants

A test built around zero-valued operands (e.g. a duration of `0` or a
quantity of `0`) makes `a - b` and `a + b` produce the same result, so a
mutant flipping `-` to `+` survives even though coverage looks complete.
Any new value object with real arithmetic (e.g. a new promise-window
calculation in `internal/domain/order/promise.go` or `promise_policy.go`)
needs fixture values where EVERY operand and every per-field delta is
distinct and non-zero, and the test must assert the exact expected value,
not just "no error".

### 2. Boundary guards need the boundary value itself

A test for `if quantity <= 0 { return err }` that only tries `-1`
(clearly invalid) and `5` (clearly valid) never exercises `0` — so a
`CONDITIONALS_BOUNDARY` mutant rewriting `<=` to `<` survives silently.
Every `< 0`/`> 0`/`<= 0` guard needs an explicit test for the boundary
value itself (e.g. `shared.NewQuantity(0)` must return
`ErrNonPositiveQuantity`, not silently accept it).

### 3. Tie-break / near-equivalent mutants: know when NOT to chase them

A comparison choosing between two otherwise-equal candidates (e.g. a
`<`/`<=` inside `PromisePolicy`'s per-shipment-group window selection,
`internal/domain/order/promise_policy.go`) can have a `<` -> `<=` mutant
that is undetectable by ANY test whose CPT windows/cycle times are all
distinct — the mutation only diverges on an exact tie. Do NOT force an
artificial tied-value fixture just to kill this; that pins an arbitrary,
currently-unspecified tie-break order as if it were a real invariant,
which is worse than an accepted near-equivalent survivor. This is exactly
the class of survivor `.gremlins.yaml`'s dated comment tracks as
follow-up rather than force-fixed. The same applies to a boundary guard
whose boundary is structurally unreachable — add a test for the
reachable edge case, but don't chase the mutant on the unreachable side.

## Diagnosing a `mutation-fast` CI failure: diff against develop, don't chase every LIVED line

```bash
gremlins unleash ./internal/domain/order          # on your branch
git stash && git checkout origin/develop -- . && gremlins unleash ./internal/domain/order   # baseline
```

Only entries NEW on your branch are your regression. This repo's own
`.gremlins.yaml` documents its currently-accepted survivor set (the 6
lived + 11 not-covered mutants noted above) — confirming the survivor SET
is unchanged from `origin/develop`, not just that the percentage cleared
the `.gremlins.yaml` gate, is the real proof a fix didn't just get lucky
on the threshold. This repo has no separate `MUTATION.md` triage file
yet; the survivor rationale lives directly in `.gremlins.yaml`'s header
comment — keep it updated there if you touch the threshold.

## Kafka/Postgres integration tests: testcontainers, never a skip-gate

A `-tags=integration` test touching Kafka or Postgres MUST start its own
container via `testcontainers-go`. Never gate on `os.Getenv("KAFKA_BROKERS")`
+ `t.Skip(...)`, and never hardcode `localhost:9092`. This repo's CI
`integration` job (`.github/workflows/ci.yml`) provisions NO service
containers at all (no Kafka, no Postgres) — a skip-gated test silently
skips in CI and proves nothing there (this is exactly what happens today to
the older `DATABASE_URL`-gated `outbound/postgres` and
`ANALYTICS_DATABASE_URL`-gated `outbound/analyticsstore` suites), while testcontainers actually exercises the assertions on
the runner. `internal/architecture/fitness_test.go`'s
`TestKafkaIntegrationTestsUseTestcontainers` enforces this statically.
See `internal/adapters/outbound/kafkacatalog/consumer_integration_test.go`
for the working recipe (unique topic per test, `createTopic` + poll for
the partition leader before the first read/write), and
`kafkacatalog/two_consumers_integration_test.go` for the pattern when TWO
independent consumers need to be proven correct on the SAME topic in one
test.

## Verify before opening the PR

```bash
make check-all   # check + coverage + arch-test + bdd (the full local gate)
```

`check-all` does not run `mutation-fast`/`vuln`/`integration` locally —
run them explicitly too when your change touches domain arithmetic,
Kafka adapters, or dependencies (`make mutation-fast`, `make vuln`,
`make integration`) — CI runs all of them even when the local gate
doesn't, so a PR can pass your local check and still go red in CI
otherwise.
