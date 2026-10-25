//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5"
)

// scratchDatabaseURL creates an empty database on the shared container and
// returns its URL, so a migration test can walk versions without
// disturbing the fully-migrated shared one.
func scratchDatabaseURL(t *testing.T, name string) string {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, sharedURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	u, err := url.Parse(sharedURL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

func siteColumnExists(t *testing.T, conn *pgx.Conn) bool {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'orders' AND column_name = 'fulfillment_site_id'`).Scan(&n); err != nil {
		t.Fatalf("inspect columns: %v", err)
	}
	return n == 1
}

// Migration 0013 is additive both ways, WITH DATA: orders written before
// it read back NULL (no site named) after UP; rolling it back drops only
// the column and leaves every order row in place.
func TestMigration0013_FulfillmentSite_UpAndDownKeepExistingOrders(t *testing.T) {
	ctx := context.Background()
	dbURL := scratchDatabaseURL(t, fmt.Sprintf("mig0013_%d", time.Now().UnixNano()))
	m, err := migrate.New("file://../../../../migrations", dbURL)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Migrate(12); err != nil {
		t.Fatalf("migrate to 12: %v", err)
	}
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if siteColumnExists(t, conn) {
		t.Fatal("column exists before 0013")
	}
	if _, err := conn.Exec(ctx, `INSERT INTO orders (id, allow_partial_shipment) VALUES ('ord-before', true)`); err != nil {
		t.Fatalf("seed pre-0013 order: %v", err)
	}

	// UP: the pre-existing row reads NULL; a new row can carry a site.
	if err := m.Steps(1); err != nil {
		t.Fatalf("migrate up to 13: %v", err)
	}
	var before *string
	if err := conn.QueryRow(ctx, `SELECT fulfillment_site_id FROM orders WHERE id = 'ord-before'`).Scan(&before); err != nil || before != nil {
		t.Fatalf("pre-existing order site = %v, err %v; want NULL", before, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO orders (id, allow_partial_shipment, fulfillment_site_id) VALUES ('ord-after', true, 'WH2')`); err != nil {
		t.Fatalf("insert with site after 0013: %v", err)
	}

	// DOWN: the column (and its constraint) go; both orders survive.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("migrate down to 12: %v", err)
	}
	if siteColumnExists(t, conn) {
		t.Fatal("column still exists after the down migration")
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM orders WHERE id IN ('ord-before', 'ord-after')`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("orders after down = %d, err %v; want both rows kept", n, err)
	}

	// And UP again works on the rolled-back schema (re-runnable).
	if err := m.Steps(1); err != nil {
		t.Fatalf("migrate up again: %v", err)
	}
	if !siteColumnExists(t, conn) {
		t.Fatal("column missing after re-applying 0013")
	}
}
