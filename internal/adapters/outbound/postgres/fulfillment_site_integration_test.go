//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

func newOrderWithSite(t *testing.T, id shared.OrderId, site string) *order.Order {
	t.Helper()
	o, err := order.New(id, mustLines(t, id), true)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	if site != "" {
		s, err := shared.NewFulfillmentSiteID(site)
		if err != nil {
			t.Fatalf("NewFulfillmentSiteID: %v", err)
		}
		if err := o.AssignFulfillmentSite(s); err != nil {
			t.Fatalf("AssignFulfillmentSite: %v", err)
		}
	}
	return o
}

// storedSiteColumn reads the raw fulfillment_site_id column (nil = NULL).
func storedSiteColumn(t *testing.T, pool *pgxpool.Pool, id shared.OrderId) *string {
	t.Helper()
	var col *string
	if err := pool.QueryRow(context.Background(),
		`SELECT fulfillment_site_id FROM orders WHERE id = $1`, id.String()).Scan(&col); err != nil {
		t.Fatalf("read fulfillment_site_id: %v", err)
	}
	return col
}

func TestOrderRepo_FulfillmentSite_RoundTripsAndStaysNullWhenAbsent(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewOrderRepo(pool)
	ctx := context.Background()

	withSite := newOrderWithSite(t, "ord-site-1", "WH2")
	without := newOrderWithSite(t, "ord-site-2", "")
	for _, o := range []*order.Order{withSite, without} {
		if err := repo.Save(ctx, o); err != nil {
			t.Fatalf("Save %s: %v", o.ID(), err)
		}
	}

	if col := storedSiteColumn(t, pool, "ord-site-1"); col == nil || *col != "WH2" {
		t.Fatalf("stored column = %v, want WH2", col)
	}
	if col := storedSiteColumn(t, pool, "ord-site-2"); col != nil {
		t.Fatalf("stored column = %q, want NULL for an order without a site", *col)
	}
	assertRoundTrip(t, repo, "ord-site-1", func(o *order.Order) {
		if got := o.FulfillmentSiteID(); got == nil || *got != "WH2" {
			t.Errorf("FulfillmentSiteID() = %v, want WH2", got)
		}
	})
	assertRoundTrip(t, repo, "ord-site-2", func(o *order.Order) {
		if got := o.FulfillmentSiteID(); got != nil {
			t.Errorf("FulfillmentSiteID() = %q, want nil", *got)
		}
	})
}

// A later Save (allocation, release, re-promise, cancel all go through
// the version-guarded UPDATE) must never rewrite the site column, even
// if a stale or tampered in-memory aggregate disagrees.
func TestOrderRepo_FulfillmentSite_IsNeverRewrittenByLaterSaves(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewOrderRepo(pool)
	ctx := context.Background()

	o := newOrderWithSite(t, "ord-site-3", "WH2")
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := repo.FindByID(ctx, "ord-site-3")
	if err != nil || loaded == nil {
		t.Fatalf("FindByID: %v, %v", loaded, err)
	}
	if err := loaded.Allocate(1, "res-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := repo.Save(ctx, loaded); err != nil {
		t.Fatalf("Save (allocated): %v", err)
	}

	// A different aggregate for the same row carrying ANOTHER site and
	// the current version: the UPDATE must not touch the column.
	other := "WH9"
	site := shared.FulfillmentSiteID(other)
	tampered := order.Rehydrate(order.OrderSnapshot{
		ID: "ord-site-3", Lines: loaded.Lines(), AllowPartialShipment: true,
		FulfillmentSiteID: &site, Version: loaded.Version() + 1,
	})
	_ = repo.Save(ctx, tampered)

	if col := storedSiteColumn(t, pool, "ord-site-3"); col == nil || *col != "WH2" {
		t.Fatalf("stored column = %v after later saves, want WH2 untouched", col)
	}
}

func TestOrders_FulfillmentSiteColumn_RejectsMalformedValuesAtTheDatabase(t *testing.T) {
	pool := newPool(t)
	for _, bad := range []string{"", "wh1", "WH-1", "WH 1"} {
		_, err := pool.Exec(context.Background(),
			`INSERT INTO orders (id, allow_partial_shipment, fulfillment_site_id) VALUES ($1, true, $2)`, "ord-bad", bad)
		if err == nil {
			t.Fatalf("INSERT with fulfillment_site_id %q succeeded, want the CHECK constraint to reject it", bad)
		}
	}
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO orders (id, allow_partial_shipment, fulfillment_site_id) VALUES ('ord-ok', true, 'WH1'), ('ord-null', true, NULL)`); err != nil {
		t.Fatalf("valid / NULL site values must be accepted: %v", err)
	}
}
