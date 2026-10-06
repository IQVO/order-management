// Package architecture holds fitness tests (the Go analogue of ArchUnit,
// via github.com/arch-go/arch-go) that enforce the hexagonal/ports-and-adapters
// dependency rule described in the project's CLAUDE.md: dependencies point
// inward only, and inbound/outbound adapters never depend on each other.
package architecture

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// TestMCPAdapterDependencyRule encodes ADR-0008: the MCP inbound adapter is
// additive, never load-bearing for the OLTP composition root. It may depend
// only on the application and domain layers (never on outbound adapters,
// never on cmd), and — the direction that actually matters for keeping it
// additive — nothing else in this codebase may depend on it. If some other
// package started importing internal/adapters/inbound/mcp, the MCP surface
// would have silently become a dependency other code relies on rather than
// a pure inbound entrypoint cmd/mcp wires up and nothing else touches.
func TestMCPAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("mcp adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.adapters.inbound.mcp.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{
							"**.internal.adapters.inbound.mcp.**",
							"**.internal.application.**",
							"**.internal.domain.**",
						},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the mcp adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.internal.adapters.inbound.mcp.**"},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})
}

// assertArchGoPasses is a shared helper for the two-return-shape arch-go
// result (DependenciesRuleResult here; the file's other tests use their own
// assertPass with the same semantics — kept separate to avoid touching
// existing tests' helper functions).
func assertArchGoPasses(t *testing.T, result *archgo.Result) {
	t.Helper()

	if result.Pass {
		return
	}

	if result.DependenciesRuleResult != nil {
		for _, r := range result.DependenciesRuleResult.Results {
			if r.Passes {
				continue
			}
			for _, v := range r.Verifications {
				if v.Passes {
					continue
				}
				for _, d := range v.Details {
					t.Errorf("%s: %s", v.Package, d)
				}
			}
		}
	}

	t.FailNow()
}

// ---------------------------------------------------------------------
// Source-scanning fitness tests below. arch-go's DSL only expresses import
// graph shape; these three invariants are about literal content (a string,
// a missing import) so they're enforced the same way
// internal/architecture/zerowrite does it in warehouse-ops-agent: walk the
// real .go files under internal/, skip generated/_test.go where noted, and
// fail with the exact file:line that violates the rule.
// ---------------------------------------------------------------------

// goFilesUnder returns every non-test .go file under root (relative to the
// module root), or every .go file including tests when includeTests is true.
func goFilesUnder(t *testing.T, root string, includeTests bool) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if !includeTests && strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return files
}

// authPatternRE matches the literal shapes a reintroduced bearer/JWT/API-key
// auth middleware would contain in source. Deliberately narrow (case
// sensitive on "Bearer " and "Authorization" header checks, exact import
// paths for known JWT libraries) so it doesn't false-positive on the
// existing CORS AllowedHeaders: []string{"Authorization"} allowlist entry
// or on the historical/comment-only mentions of the fleet's auth revert —
// this test scans non-comment, non-test Go source only, and every current
// hit in this codebase is exactly those two known-safe shapes, verified
// clean before this test was added.
var authPatternRE = regexp.MustCompile(`"Bearer |golang-jwt/jwt|dgrijalva/jwt-go|lestrrat-go/jwx`)

// TestNoAuthMiddlewareReintroduced encodes the fleet-wide 2026-09-11 REST +
// MCP static-bearer-auth revert: every endpoint is deliberately
// unauthenticated pending a fresh auth-model decision. An agent
// "helpfully" re-adding a bearer/JWT middleware to internal/adapters/inbound
// should fail CI, not ship silently. This does not flag the CORS
// AllowedHeaders allowlist (which legitimately lists "Authorization" as a
// header name a browser may send, not a check this service performs) or
// doc comments that reference the revert in prose.
func TestNoAuthMiddlewareReintroduced(t *testing.T) {
	for _, path := range goFilesUnder(t, "../adapters/inbound", false) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lineNo := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if authPatternRE.MatchString(line) {
				t.Errorf("%s:%d: matches a reintroduced-auth pattern: %q — the fleet-wide static-bearer auth layer was deliberately reverted 2026-09-11 (unauthenticated pending a fresh auth-model decision); if this is intentional, update this test alongside the ADR documenting the new decision", path, lineNo, trimmed)
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}
}

// groupIDLiteralRE matches a kafka-go ReaderConfig's GroupID field being
// assigned a bare double-quoted string literal, e.g. `GroupID: "my-group"`.
// A symbol reference (`GroupID: AnalyticsConsumerGroup`, `GroupID: groupID,
// `GroupID: uniqueConsumerGroup()`) never matches this.
var groupIDLiteralRE = regexp.MustCompile(`GroupID:\s*"[^"]+"`)

// TestKafkaConsumerGroupNeverHardcodedInline encodes a real, already-lived
// incident: wes-work-planning's OLTP consumer group was a hardcoded literal
// (`"wes-work-planning"`), which made a locally-run e2e-tests process join
// the SAME consumer group as the live in-cluster Deployment on the shared
// fleet Kafka broker — Kafka's rebalance protocol then handed the single
// partition to only one of the two group members, silently starving
// whichever process lost the race (fixed in wes-work-planning#67 by making
// the group id env-configurable). This test doesn't ban long-lived named
// consumer groups outright (the analytics projector's
// AnalyticsConsumerGroup constant is a deliberate, correct exception — see
// its own doc comment: only one instance of that consumer ever runs, so
// there's nothing to collide with) — it bans the shape that caused the
// incident: a GroupID assigned directly from an inline string literal
// rather than through a named symbol (const, var, or function call) that
// a reviewer can trace back to its definition and reasoning.
func TestKafkaConsumerGroupNeverHardcodedInline(t *testing.T) {
	for _, path := range goFilesUnder(t, "..", false) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lineNo := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			if groupIDLiteralRE.MatchString(line) {
				t.Errorf("%s:%d: GroupID assigned an inline string literal: %q — use a named const/var or a function call (see internal/adapters/outbound/kafkacatalog's uniqueConsumerGroup() for the per-process-unique pattern, or the AnalyticsConsumerGroup const for the single-instance pattern) so the group id's lifetime/uniqueness reasoning is traceable and a locally-run process can never silently join a live cluster's group", path, lineNo, strings.TrimSpace(line))
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}
}

// TestKafkaIntegrationTestsUseTestcontainers encodes the fleet-wide rule
// (corrected 2026-09-06): a Kafka-touching `-tags=integration` test MUST
// start its own broker via testcontainers-go/modules/kafka, never gate on
// os.Getenv("KAFKA_BROKERS") + t.Skip, and never hardcode localhost:9092.
// This fleet's CI `integration` job provisions Postgres only — a
// skip-gated Kafka test silently skips in CI and proves nothing there,
// while testcontainers is the only variant that actually exercises the
// Kafka assertions on a runner. Scans every _integration_test.go file that
// imports segmentio/kafka-go (this fleet's Kafka client) or references
// GroupID/kafka.Reader/kafka.Writer, and requires it to also import
// testcontainers-go/modules/kafka.
func TestKafkaIntegrationTestsUseTestcontainers(t *testing.T) {
	for _, path := range goFilesUnder(t, "..", true) {
		if !strings.HasSuffix(path, "_integration_test.go") {
			continue
		}

		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		content := string(src)

		touchesKafka := strings.Contains(content, "segmentio/kafka-go") ||
			strings.Contains(content, "kafka.Reader") ||
			strings.Contains(content, "kafka.Writer") ||
			strings.Contains(content, "KAFKA_BROKERS")
		if !touchesKafka {
			continue
		}

		assertIntegrationFileUsesTestcontainers(t, path, content)
	}
}

// assertIntegrationFileUsesTestcontainers applies the fleet-wide
// testcontainers rule to one Kafka-touching _integration_test.go file:
// no os.Getenv("KAFKA_BROKERS") skip gate, no hardcoded localhost:9092,
// and a testcontainers-go/modules/kafka import.
func assertIntegrationFileUsesTestcontainers(t *testing.T, path, content string) {
	t.Helper()

	// Scan line-by-line so a comment that merely MENTIONS the banned
	// shapes (e.g. explaining that a real broker via testcontainers is
	// used specifically so the test needs no KAFKA_BROKERS/localhost:9092)
	// doesn't false-positive the same way a commented-out `t.Skip` line
	// shouldn't either.
	hasSkipGate := false
	hasHardcodedBroker := false
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		if strings.Contains(line, `os.Getenv("KAFKA_BROKERS")`) {
			hasSkipGate = true
		}
		if strings.Contains(line, "localhost:9092") {
			hasHardcodedBroker = true
		}
	}
	f.Close()
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}

	if hasSkipGate {
		t.Errorf("%s: gates on os.Getenv(\"KAFKA_BROKERS\") — this fleet's CI integration job provisions Postgres only, so a skip-gated Kafka test silently skips in CI and proves nothing there; start a real broker via testcontainers-go/modules/kafka instead", path)
	}
	if hasHardcodedBroker {
		t.Errorf("%s: hardcodes localhost:9092 — a fresh CI runner has no broker at that address; start one via testcontainers-go/modules/kafka instead", path)
	}
	if !strings.Contains(content, "testcontainers-go/modules/kafka") {
		t.Errorf("%s: touches Kafka but does not import github.com/testcontainers/testcontainers-go/modules/kafka — Kafka-touching integration tests in this fleet must start their own broker via testcontainers, never assume/skip on an external one", path)
	}
}

// postgresEnvGateRE matches reading the Postgres connection URL from the
// environment: the shape every env-gated Postgres integration test took
// (`url := os.Getenv("DATABASE_URL"); if url == "" { t.Skip(...) }`).
var postgresEnvGateRE = regexp.MustCompile(`os\.(Getenv|LookupEnv)\("(ANALYTICS_|MIGRATIONS_)?DATABASE_URL"\)`)

// postgresSkipRE matches a t.Skip/Skipf/SkipNow call whose message names a
// DB env var — a skip tied to a missing DATABASE_URL.
var postgresSkipRE = regexp.MustCompile(`t\.Skip(f|Now)?\(.*DATABASE_URL`)

// TestPostgresIntegrationTestsUseTestcontainers is the Postgres twin of
// TestKafkaIntegrationTestsUseTestcontainers (audit 2026-10-05, F1): a
// Postgres-touching `-tags=integration` test MUST boot its own Postgres via
// testcontainers-go/modules/postgres — never read DATABASE_URL /
// ANALYTICS_DATABASE_URL from the environment and t.Skip when it is unset.
// An env-gated test passes green when the variable is missing, so it proves
// nothing wherever the CI integration job provisions no database — which is
// exactly how the OrderRepo + OutboxPublisher tests were silently skipped in
// CI until this sensor was added.
//
// A file satisfies the import requirement itself or via a shared helper in a
// sibling _test.go file of the same package directory (outboxDB/newPool),
// which is how most of this repo's tests reach their container.
func TestPostgresIntegrationTestsUseTestcontainers(t *testing.T) {
	// internal/ and cmd/ (cmd/order has a Postgres integration test of its own).
	var paths []string
	paths = append(paths, goFilesUnder(t, "..", true)...)
	paths = append(paths, goFilesUnder(t, "../../cmd", true)...)
	for _, path := range paths {
		if !strings.HasSuffix(path, "_integration_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, v := range postgresIntegrationViolations(path, string(src), siblingImportsPostgresTestcontainers(t, path)) {
			t.Errorf("%s", archViolation("contents", "Postgres integration tests boot their own Postgres via testcontainers-go/modules/postgres", v+
				" FIX: start a throwaway container (tcpostgres.Run -> ConnectionString -> postgres.RunMigrations -> postgres.NewPool -> t.Cleanup(TerminateContainer)) or reuse the package's shared helper (outboxDB); never read DATABASE_URL or t.Skip."))
		}
	}
}

// siblingImportsPostgresTestcontainers reports whether another _test.go file
// in the same directory (other than path itself) imports the testcontainers
// Postgres module — i.e. a shared helper the file under test can call.
func siblingImportsPostgresTestcontainers(t *testing.T, path string) bool {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("read dir of %s: %v", path, err)
	}
	for _, e := range entries {
		name := e.Name()
		sibling := filepath.Join(filepath.Dir(path), name)
		if e.IsDir() || sibling == path || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(sibling)
		if err != nil {
			t.Fatalf("read %s: %v", sibling, err)
		}
		if importsPostgresTestcontainers(string(src)) {
			return true
		}
	}
	return false
}

func importsPostgresTestcontainers(content string) bool {
	return strings.Contains(content, "testcontainers-go/modules/postgres")
}

// integrationTestTouchesPostgres reports whether an integration test's source
// talks to Postgres: a driver/database import, or this repo's own pool and
// migration entry points.
func integrationTestTouchesPostgres(content string) bool {
	return strings.Contains(content, "jackc/pgx") ||
		strings.Contains(content, "database/sql") ||
		strings.Contains(content, "pgxpool") ||
		strings.Contains(content, "RunMigrations(") ||
		strings.Contains(content, ".NewPool(")
}

// postgresIntegrationViolations returns one message per breach of the
// Postgres-testcontainers rule in an integration test file. Comment lines
// are skipped so a header explaining "never an external DATABASE_URL" does
// not false-positive. siblingHelper says a same-package _test.go file
// imports the testcontainers Postgres module.
func postgresIntegrationViolations(path, content string, siblingHelper bool) []string {
	var out []string
	for i, line := range strings.Split(content, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		if postgresEnvGateRE.MatchString(line) {
			out = append(out, fmt.Sprintf("%s:%d reads the Postgres URL from the environment (%s).", path, i+1, strings.TrimSpace(line)))
		}
		if postgresSkipRE.MatchString(line) {
			out = append(out, fmt.Sprintf("%s:%d skips when a DB env var is unset (%s).", path, i+1, strings.TrimSpace(line)))
		}
	}
	if integrationTestTouchesPostgres(content) && !importsPostgresTestcontainers(content) && !siblingHelper {
		out = append(out, path+" touches Postgres but neither it nor a sibling _test.go helper imports github.com/testcontainers/testcontainers-go/modules/postgres.")
	}
	return out
}

// The detector itself, so a refactor cannot quietly turn the sensor into a
// test that always passes: each bad fixture must be flagged, each good one not.
func TestPostgresIntegrationDetector(t *testing.T) {
	const envGated = "package x\n\nimport \"testing\"\n\nfunc TestA(t *testing.T) {\n" +
		"	url := os.Getenv(\"DATABASE_URL\")\n	if url == \"\" {\n		t.Skip(\"DATABASE_URL not set\")\n	}\n}\n"
	const analyticsGated = "func f() string { return os.Getenv(\"ANALYTICS_DATABASE_URL\") }\n"
	const lookupGated = "u, ok := os.LookupEnv(\"DATABASE_URL\")\n"
	const skipOnly = "	t.Skipf(\"no DATABASE_URL in this env\")\n"
	const touchesNoContainer = "import \"github.com/jackc/pgx/v5/pgxpool\"\nvar _ *pgxpool.Pool\n"
	const touchesViaEntryPoint = "pool, _ := postgres.NewPool(ctx, url)\n"
	const withContainer = "import tcpostgres \"github.com/testcontainers/testcontainers-go/modules/postgres\"\n" +
		"import \"github.com/jackc/pgx/v5/pgxpool\"\nvar _ *pgxpool.Pool\n"
	const commentOnly = "// never an external DATABASE_URL: os.Getenv(\"DATABASE_URL\") is banned, never t.Skip(\"DATABASE_URL\")\n" + withContainer
	const noPostgres = "package x\n\nfunc TestPure(t *testing.T) {}\n"

	cases := []struct {
		name    string
		content string
		sibling bool
		want    int // number of violations; 0 means clean
	}{
		{"env gate plus skip is flagged twice", envGated, true, 2},
		{"ANALYTICS_DATABASE_URL read is flagged", analyticsGated, true, 1},
		{"LookupEnv of DATABASE_URL is flagged", lookupGated, true, 1},
		{"skip tied to DATABASE_URL is flagged", skipOnly, true, 1},
		{"pgx without testcontainers or helper is flagged", touchesNoContainer, false, 1},
		{"NewPool entry point without testcontainers or helper is flagged", touchesViaEntryPoint, false, 1},
		{"pgx with a sibling testcontainers helper is clean", touchesNoContainer, true, 0},
		{"pgx importing the testcontainers postgres module is clean", withContainer, false, 0},
		{"comment mentions of the banned shapes are ignored", commentOnly, false, 0},
		{"a file that never touches Postgres is clean", noPostgres, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := postgresIntegrationViolations("fixture_integration_test.go", tc.content, tc.sibling)
			if len(got) != tc.want {
				t.Fatalf("want %d violation(s), got %d: %v", tc.want, len(got), got)
			}
		})
	}
}
