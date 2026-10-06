//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
)

// sharedURL is the connection string of the ONE Postgres container this
// package's integration tests share. TestMain boots it once, runs every
// migration once, and terminates it after the last test; each test gets
// its own pool plus a TRUNCATE of every data table (outboxDB), so tests
// stay independent without paying a container start per test.
var sharedURL string

func TestMain(m *testing.M) { os.Exit(runWithSharedPostgres(m)) }

func runWithSharedPostgres(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("order_management"),
		tcpostgres.WithUsername("order_management"),
		tcpostgres.WithPassword("order_management"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(container) }()

	sharedURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(sharedURL, "../../../../migrations"); err != nil {
		fmt.Fprintf(os.Stderr, "run migrations: %v\n", err)
		return 1
	}
	return m.Run()
}

// outboxDB returns a pool on the shared, fully-migrated database with every
// data table emptied (identities restarted), closed when the test ends.
// Tests must not run in parallel: they share one database.
func outboxDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, sharedURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	truncateAllTables(t, pool)
	return pool
}

// truncateAllTables empties every table except golang-migrate's own
// bookkeeping, discovered from the catalog so a new migration's tables
// are covered without touching this helper.
func truncateAllTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	rows, err := pool.Query(ctx, `SELECT tablename FROM pg_tables
		WHERE schemaname = current_schema() AND tablename <> 'schema_migrations'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var quoted []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			t.Fatalf("scan table name: %v", err)
		}
		quoted = append(quoted, pgx.Identifier{name}.Sanitize())
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if len(quoted) == 0 {
		t.Fatal("no tables found to truncate: migrations did not run?")
	}
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE "+strings.Join(quoted, ", ")+" RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}
