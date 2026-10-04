---
paths:
  - "**/*_test.go"
  - "features/**"
  - "internal/architecture/**"
  - ".gremlins.yaml"
  - ".golangci.yml"
---
# Code standards / testing

- gofmt/go vet clean; every package has a doc comment.
- `.golangci.yml` copied verbatim from `inventory-storage`'s own config.
- Table-driven tests: domain + application layers use the in-memory adapter
  and fake HTTP clients — **never hit a real network in a unit test.**
  Kafka-touching `-tags=integration` tests use **testcontainers**, never a
  skip-gated `KAFKA_BROKERS` env check (CI's `integration` job provisions no
  external broker; testcontainers is the only variant that actually runs).
- One httptest per REST endpoint: at least one success and one error path.
- Every named invariant in `.claude/rules/domain-model.md` needs a dedicated
  failing-path test.
- Coverage gate: **90%** on `./internal/domain/...,./internal/application/...`.
- Mutation gate (gremlins, `mutation-fast` on every push/PR, full `mutation`
  weekly): thresholds efficacy 89 / mutant-coverage 83 on
  `internal/domain/order` (measured 89.29% / 83.58%) — see `.gremlins.yaml`.
  Do not let a change silently regress below the pinned threshold.
- godog/BDD acceptance suite: `features/*.feature`, run via
  `go test ./... -run TestFeatures` (CI job `bdd`).
- `make arch-test` runs the arch-go hexagonal/analytics-isolation/ports
  fitness tests (`internal/architecture/architecture_test.go`, CI job
  `arch-test`).
