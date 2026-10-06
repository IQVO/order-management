//go:build integration

package http_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
)

// One Postgres container serves every Postgres-backed test in this package.
// It is started lazily on first use (this package also holds plain unit
// tests that must not pay for a container), migrated once, and terminated
// by TestMain after the last test. Each test gets its own pool and a
// TRUNCATE of every data table (idempotencyDB), so tests stay independent.
var (
	sharedPGOnce      sync.Once
	sharedPGURL       string
	sharedPGErr       error
	sharedPGContainer testcontainers.Container
)

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedPGContainer != nil {
		_ = testcontainers.TerminateContainer(sharedPGContainer)
	}
	os.Exit(code)
}

func sharedPostgresURL(t *testing.T) string {
	t.Helper()
	sharedPGOnce.Do(func() {
		ctx := context.Background()
		container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("order_management"),
			tcpostgres.WithUsername("order_management"),
			tcpostgres.WithPassword("order_management"),
			tcpostgres.BasicWaitStrategies(),
		)
		sharedPGContainer = container
		if err != nil {
			sharedPGErr = err
			return
		}
		if sharedPGURL, err = container.ConnectionString(ctx, "sslmode=disable"); err != nil {
			sharedPGErr = err
			return
		}
		sharedPGErr = postgres.RunMigrations(sharedPGURL, "../../../../migrations")
	})
	if sharedPGErr != nil {
		t.Fatalf("shared postgres: %v", sharedPGErr)
	}
	return sharedPGURL
}

// truncateAllTables empties every table except golang-migrate's own
// bookkeeping, discovered from the catalog so a new migration's tables are
// covered without touching this helper.
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
