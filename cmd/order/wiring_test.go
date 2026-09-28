package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestBuildRepoAdapters_RetriesTheDatabaseNotJustOnce is the test that
// would have caught the original defect this repo's boot path had: a
// helper that works in isolation (retry_test.go) but isn't actually wired
// into the real boot path.
//
// It drives buildRepoAdapters — the ACTUAL composition-root function
// cmd/order/main.go calls at startup — against an unreachable address and
// asserts on the ELAPSED time: a single attempt returns fast (a refused
// connection is near-instant), whereas the retry budget cannot be paid in
// less than the sum of its backoffs (1s + 2s + 4s + 8s = 15s before the
// 5th and final attempt).
func TestBuildRepoAdapters_RetriesTheDatabaseNotJustOnce(t *testing.T) {
	// Port 1 on loopback refuses immediately, so each attempt fails fast
	// and the only thing that can make this slow is the backoff itself.
	databaseURL := "postgres://u:p@127.0.0.1:1/order_management?sslmode=disable&connect_timeout=1"

	start := time.Now()
	_, _, _, _, _, _, err := buildRepoAdapters(context.Background(), databaseURL, databaseURL, migrationsDirForTest(t), "log", quietLogger())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("an unreachable database must fail boot, never fall back to the in-memory repo")
	}

	// The full budget is ~31s; one attempt is near-instant. Anything under
	// the first two backoffs (1s + 2s) means the retry was skipped.
	if elapsed < 3*time.Second {
		t.Fatalf("buildRepoAdapters gave up in %v — it is not retrying, so a single "+
			"first-dial reset would crash-loop the pod (err: %v)", elapsed, err)
	}
	if !strings.Contains(err.Error(), "attempts") {
		t.Fatalf("err = %v, want it to report how many attempts were made", err)
	}
}

// With no DATABASE_URL the in-memory repo is correct and must cost
// nothing: no dial, no backoff, no delay to a local run.
func TestBuildRepoAdapters_NoDatabaseURLUsesMemoryImmediately(t *testing.T) {
	start := time.Now()
	orders, _, pool, _, _, closeFn, err := buildRepoAdapters(context.Background(), "", "", migrationsDirForTest(t), "log", quietLogger())
	if err != nil {
		t.Fatalf("buildRepoAdapters: %v", err)
	}
	defer closeFn()

	if orders == nil {
		t.Fatal("no repository returned")
	}
	if pool != nil {
		t.Fatal("no DATABASE_URL must not open a pool")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the in-memory path took %v; it must not touch the network", elapsed)
	}
}

// TestWireProcessPathCatalogue_RetriesKafkaNotJustOnce mirrors the same
// discipline for the Kafka-sourced process-path catalogue wiring: each
// NewConsumer call's newTargetOffsets dials the broker directly before
// building the real reader, and that dial is subject to the exact same
// first-dial reset as Postgres. Driven against an address nothing listens
// on, asserting on elapsed time.
func TestWireProcessPathCatalogue_RetriesKafkaNotJustOnce(t *testing.T) {
	start := time.Now()
	_, _, _, cleanup, err := wireProcessPathCatalogue(context.Background(), "kafka", "127.0.0.1:1", quietLogger())
	elapsed := time.Since(start)
	if cleanup != nil {
		cleanup()
	}

	if err == nil {
		t.Fatal("an unreachable Kafka broker must fail boot, not silently proceed with no catalogue")
	}
	if elapsed < 3*time.Second {
		t.Fatalf("wireProcessPathCatalogue gave up in %v — it is not retrying the Kafka dial, "+
			"so a single first-dial reset would crash-loop the pod (err: %v)", elapsed, err)
	}
}

// With PATH_CATALOGUE_SOURCE unset ("none") this must cost nothing: no
// dial, no backoff, immediate return with the UnknownPathCapacity default.
func TestWireProcessPathCatalogue_NoneSourceIsImmediate(t *testing.T) {
	start := time.Now()
	catalogue, cptSchedule, capacity, cleanup, err := wireProcessPathCatalogue(context.Background(), "none", "", quietLogger())
	if cleanup != nil {
		cleanup()
	}
	if err != nil {
		t.Fatalf("wireProcessPathCatalogue: %v", err)
	}
	if catalogue != nil || cptSchedule != nil {
		t.Fatal("the none source must not wire a catalogue or schedule cache")
	}
	if capacity == nil {
		t.Fatal("the none source must still return the UnknownPathCapacity default")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the none-source path took %v; it must not touch the network", elapsed)
	}
}

// migrationsDirForTest resolves the repo's migrations directory, so the
// retry under test fails on the DIAL rather than on a missing directory
// (which would return before any retry and make the test vacuous).
func migrationsDirForTest(t *testing.T) string {
	t.Helper()
	// cmd/order -> repo root.
	dir := "../../migrations"
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("migrations directory not found at %s: %v", dir, err)
	}
	return dir
}

// TestMigrationsDatabaseURLFallback proves the fallback wiring
// cmd/order/main.go's run() applies before calling buildRepoAdapters:
// getenv("MIGRATIONS_DATABASE_URL", databaseURL) must return
// MIGRATIONS_DATABASE_URL's own value when it is set, and databaseURL
// itself (DATABASE_URL) when it is unset. This is the exact env-lookup
// line the fix for the PgBouncer/pg_advisory_lock incompatibility (ADR
// 0029-migrations-direct-postgres-connection.md) depends on: any
// environment that doesn't provision the split (local dev, CI
// integration tests, a cluster whose Terraform predates this fix) must
// keep working exactly as before, using DATABASE_URL for everything
// including migrations.
func TestMigrationsDatabaseURLFallback(t *testing.T) {
	const databaseURL = "postgres://u:p@pgbouncer.example:6432/order_management?sslmode=disable"

	t.Run("falls back to DATABASE_URL when MIGRATIONS_DATABASE_URL is unset", func(t *testing.T) {
		t.Setenv("MIGRATIONS_DATABASE_URL", "")
		os.Unsetenv("MIGRATIONS_DATABASE_URL")

		got := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
		if got != databaseURL {
			t.Fatalf("getenv fallback = %q, want the DATABASE_URL value %q", got, databaseURL)
		}
	})

	t.Run("uses MIGRATIONS_DATABASE_URL when set, not DATABASE_URL", func(t *testing.T) {
		const direct = "postgres://u:p@postgres-postgresql.example:5432/order_management?sslmode=disable"
		t.Setenv("MIGRATIONS_DATABASE_URL", direct)

		got := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
		if got != direct {
			t.Fatalf("getenv = %q, want the direct MIGRATIONS_DATABASE_URL value %q (must NOT silently keep using DATABASE_URL/PgBouncer)", got, direct)
		}
		if got == databaseURL {
			t.Fatal("MIGRATIONS_DATABASE_URL and DATABASE_URL collapsed to the same value — the whole point of this env var is that it differs")
		}
	})
}

// TestBuildRepoAdapters_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations
// proves buildRepoAdapters itself — not just the env-var read above —
// actually threads migrationsDatabaseURL into the migration step and
// databaseURL into the pgxpool, rather than the two ever being
// conflated. Gives DATABASE_URL an address nothing listens on (so
// opening the pgxpool, which happens AFTER migrations succeed, would
// hang/fail loudly if ever reached) and MIGRATIONS_DATABASE_URL a
// schemeless string that migrate.New rejects immediately with a
// distinctive parse error ("failed to parse scheme from database URL")
// — if buildRepoAdapters ignored migrationsDatabaseURL and ran
// migrations against databaseURL instead, this test would see a
// dial/"connection refused" error, not the parse error.
func TestBuildRepoAdapters_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	const (
		bogusMigrationsURL = "not-a-valid-connection-string"
		unreachableAppURL  = "postgres://u:p@127.0.0.1:1/order_management?sslmode=disable&connect_timeout=1"
	)

	_, _, _, _, _, _, err := buildRepoAdapters(context.Background(), unreachableAppURL, bogusMigrationsURL, migrationsDirForTest(t), "log", quietLogger())
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v — expected the bogus-URL parse error from migrate.New; a \"connection refused\"/dial error here would mean migrations ran against databaseURL/unreachableAppURL instead of migrationsDatabaseURL", err)
	}
}
