# Makefile — the local quality gate for order-management.
#
# Every target below mirrors a sensor in .github/workflows/ci.yml, so the same
# feedback CI gives you post-push is available locally, pre-commit. This is the
# "keep quality left" idea: shift the sensors left so an agent (or a human) can
# self-correct before code leaves the machine.
#
# See CLAUDE.md -> "Local quality gate".
#
# Restored to fleet parity 2026-09-13: bdd/arch-test/mutation/mutation-fast/
# vuln targets were shipped in PR #34 then silently dropped from this file
# in PR #35's "harden security" pass, even though the backing files
# (.gremlins.yaml, features/*.feature, internal/architecture/) were never
# removed. check-all now matches the rest of the fleet's shape (check +
# coverage + arch-test + bdd) instead of stopping at coverage alone.

GO                 ?= go
GOLANGCI_LINT      ?= golangci-lint
GOLANGCI_VERSION   := v2.13.1
GREMLINS_VERSION   := v0.6.0

COVERAGE_OUT       := coverage.out
COVERAGE_PKGS      := ./internal/domain/...,./internal/application/...
COVERAGE_THRESHOLD := 90

.DEFAULT_GOAL := help

.PHONY: help build vet fmt fmt-check lint test integration coverage bdd arch-test mutation-fast mutation vuln contract check check-all

help:
	@echo "order-management — local quality gate (targets mirror .github/workflows/ci.yml)"
	@echo ""
	@echo "  help          Print this list of targets (default target)"
	@echo "  build         go build ./..."
	@echo "  vet           go vet ./..."
	@echo "  fmt           gofmt -w . — format the tree in place"
	@echo "  fmt-check     Fail if gofmt -l . is non-empty (the CI-style check)"
	@echo "  lint          golangci-lint run ./... (pinned $(GOLANGCI_VERSION) in CI)"
	@echo "  test          go test ./... -race — unit + httptest, no DB needed"
	@echo "  integration   Run Kafka integration tests in an isolated Testcontainers broker"
	@echo "  coverage      CI coverage command + the $(COVERAGE_THRESHOLD)% gate"
	@echo "  bdd           godog/Gherkin acceptance tests (features/*.feature)"
	@echo "  arch-test     Architecture fitness tests (internal/architecture/)"
	@echo "  mutation-fast Fast blocking mutation subset (thresholds in .gremlins.yaml)"
	@echo "  mutation      Exhaustive mutation run over the whole domain layer (slow)"
	@echo "  vuln          Known CVEs in the dependency graph and the Go stdlib"
	@echo "  contract      scripts/contract-test.sh — Schemathesis vs apis/openapi.yaml"
	@echo ""
	@echo "  check       FAST bundle: fmt-check vet build lint test"
	@echo "  check-all   check + coverage + arch-test + bdd — run this before pushing"

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

fmt-check:
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "gofmt: the following files are not formatted:"; \
		echo "$$files" | sed 's/^/  /'; \
		echo "run 'make fmt' to fix them"; \
		exit 1; \
	fi; \
	echo "gofmt: clean"

lint:
	@if ! command -v $(GOLANGCI_LINT) >/dev/null 2>&1; then \
		echo "golangci-lint is not installed (or not on PATH)."; \
		echo "Install the exact version CI pins:"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)"; \
		exit 1; \
	fi
	$(GOLANGCI_LINT) run ./...

test:
	$(GO) test ./... -race

integration:
	$(GO) build -tags=integration ./...
	$(GO) vet -tags=integration ./...
	$(GO) test -tags=integration ./internal/adapters/outbound/kafka
	$(GO) test -tags=integration ./internal/adapters/outbound/kafkacatalog
	$(GO) test -tags=integration ./internal/adapters/outbound/kafkacptschedule
	$(GO) test -tags=integration ./internal/adapters/outbound/kafkapathcapacity
	$(GO) test -tags=integration ./internal/adapters/inbound/kafka
	$(GO) test -tags=integration ./internal/adapters/outbound/postgres

coverage:
	$(GO) test ./... -race -coverprofile=$(COVERAGE_OUT) -coverpkg=$(COVERAGE_PKGS)
	@COVERAGE=$$($(GO) tool cover -func=$(COVERAGE_OUT) | awk '/^total:/ {print $$3}' | tr -d '%'); \
	echo "Coverage: $${COVERAGE}% (gate: $(COVERAGE_THRESHOLD)%)"; \
	if awk -v c="$$COVERAGE" -v t="$(COVERAGE_THRESHOLD)" 'BEGIN { exit !(c < t) }'; then \
		echo "coverage $${COVERAGE}% is below the $(COVERAGE_THRESHOLD)% gate"; \
		exit 1; \
	fi

bdd:
	$(GO) test ./... -run TestFeatures -v

arch-test:
	$(GO) test ./internal/architecture/... -v

mutation-fast:
	@if ! command -v gremlins >/dev/null 2>&1; then \
		echo "gremlins is not installed."; \
		echo "install the version CI pins with:"; \
		echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION)"; \
		exit 1; \
	fi
	gremlins unleash ./internal/domain/order

mutation:
	@if ! command -v gremlins >/dev/null 2>&1; then \
		echo "gremlins is not installed."; \
		echo "install the version CI pins with:"; \
		echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION)"; \
		exit 1; \
	fi
	gremlins unleash ./internal/domain --workers 1 --timeout-coefficient 30

vuln:
	@if ! command -v govulncheck >/dev/null 2>&1; then \
		echo "govulncheck is not installed."; \
		echo "install it with:"; \
		echo "  go install golang.org/x/vuln/cmd/govulncheck@latest"; \
		exit 1; \
	fi
	govulncheck ./...

# Property-based contract tests against apis/openapi.yaml — mirrors the
# `contract` CI job. Not part of check/check-all (needs Python tooling
# on PATH, and a free loopback port).
contract:
	./scripts/contract-test.sh

# The fast self-correction loop: run this after every change, before committing.
check: fmt-check vet build lint test

# The fuller gate a human runs before pushing.
check-all: check coverage arch-test bdd

# --- agent harness (harness-template v3) -----------------------------------
.PHONY: check-fast guide-lint harness-test
# Fast local gate used by the agent Stop hook: format, vet, fitness tests, and the tests of
# the packages changed vs HEAD. The full gate stays `make check` / `make check-all`.
check-fast: fmt-check vet arch-test
	@pkgs="$$(python3 scripts/harness/hook.py changed-pkgs)"; \
	if [ -n "$$pkgs" ]; then go test $$pkgs; else echo "check-fast: no changed Go packages"; fi

guide-lint: ## lint agent guides: skills load, references resolve, context budget
	python3 scripts/harness/guide_lint.py

harness-test: ## unit-test the agent hooks (pre/post/stop)
	python3 scripts/harness/test_hook.py
