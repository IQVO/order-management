// Package pgtest boots ONE real Postgres through testcontainers per test
// binary and hands every test its own database, cloned from a template that
// already has the OLTP migrations applied (or left empty, for callers that
// apply the separate analytics schema from migrations/analytics, whose own
// golang-migrate numbering must never meet the OLTP set's in one
// schema_migrations table). It never reads DATABASE_URL and never skips: a
// missing Docker is a test failure (fleet rule,
// TestPostgresIntegrationTestsUseTestcontainers in internal/architecture).
//
// The container is started through github.com/testcontainers/testcontainers-go/
// modules/postgres — this helper is the shared container a package's
// integration tests reach it through.
package pgtest

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
)

const (
	user         = "order_test"
	password     = "order_test"
	adminDB      = "postgres"
	templateName = "order_test_template"
)

var (
	once      sync.Once
	container *tcpostgres.PostgresContainer
	errStart  error
	seq       atomic.Int64
)

func start() {
	ctx := context.Background()
	container, errStart = tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase(adminDB),
		tcpostgres.WithUsername(user),
		tcpostgres.WithPassword(password),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if errStart != nil {
		return
	}
	errStart = buildTemplate(ctx)
}

// migrationsDir resolves the repository's OLTP migrations directory from THIS
// file's compile-time location, so the helper works no matter which test
// package's working directory the binary runs from.
func migrationsDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "migrations")
}

// buildTemplate creates the template database and applies the OLTP migrations
// to it twice: the binaries under test run migrations on every boot, and a
// second run must be a no-op (golang-migrate is idempotent) — proving that
// here keeps the fresh databases this package hands out compatible with the
// cmd composition roots' own boot-time migration step.
func buildTemplate(ctx context.Context) error {
	if err := adminExec(ctx, `CREATE DATABASE `+templateName); err != nil {
		return err
	}
	url, err := databaseURL(ctx, templateName)
	if err != nil {
		return err
	}
	for range 2 {
		if err := postgres.RunMigrations(url, migrationsDir()); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
	}
	return nil
}

func databaseURL(ctx context.Context, db string) (string, error) {
	base, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", err
	}
	cfg, err := pgx.ParseConfig(base)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, password, cfg.Host, cfg.Port, db), nil
}

func adminExec(ctx context.Context, sql string) error {
	url, err := databaseURL(ctx, adminDB)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, sql)
	return err
}

// URL returns the DSN of a FRESH database (OLTP migrations applied) on the
// shared container.
func URL(t *testing.T) string {
	t.Helper()
	once.Do(start)
	if errStart != nil {
		t.Fatalf("start postgres container: %v", errStart)
	}
	ctx := context.Background()
	name := fmt.Sprintf("t_%d", seq.Add(1))
	if err := adminExec(ctx, `CREATE DATABASE `+name+` TEMPLATE `+templateName); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	url, err := databaseURL(ctx, name)
	if err != nil {
		t.Fatalf("database url: %v", err)
	}
	return url
}

// EmptyURL returns the DSN of a fresh database WITHOUT any migration applied,
// for callers that apply a different migration set (the analytics schema)
// themselves.
func EmptyURL(t *testing.T) string {
	t.Helper()
	once.Do(start)
	if errStart != nil {
		t.Fatalf("start postgres container: %v", errStart)
	}
	ctx := context.Background()
	name := fmt.Sprintf("e_%d", seq.Add(1))
	if err := adminExec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	url, err := databaseURL(ctx, name)
	if err != nil {
		t.Fatalf("database url: %v", err)
	}
	return url
}

// Shutdown terminates the shared container; call it from TestMain.
func Shutdown() {
	if container != nil {
		_ = testcontainers.TerminateContainer(container)
	}
}
