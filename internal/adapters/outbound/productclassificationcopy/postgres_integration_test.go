//go:build integration

package productclassificationcopy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
)

// copyDB boots a throwaway Postgres (testcontainers only, never an external
// DATABASE_URL), runs every migration and returns a pool on it.
func copyDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("order_management"),
		tcpostgres.WithUsername("order_management"),
		tcpostgres.WithPassword("order_management"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := postgres.RunMigrations(dsn, "../../../../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// One container for the whole behaviour: found / not found / version guard
// / optional columns / transactional claim rollback, against the real table.
func TestPostgresStore_RealDatabase(t *testing.T) {
	ctx := context.Background()
	pool := copyDB(t)
	store := productclassificationcopy.NewPostgresStore(pool, nil)
	processed := productclassificationcopy.NewPostgresProcessedEvents(pool)

	t.Run("an unknown SKU is Known=false with a nil error", func(t *testing.T) {
		got, err := store.GetClassification(ctx, "SKU-NONE")
		if err != nil || got.Known || got.SKU != "SKU-NONE" {
			t.Fatalf("lookup = %+v, %v", got, err)
		}
	})

	t.Run("insert, then only a higher version replaces the row", func(t *testing.T) {
		steps := []struct {
			rec         ports.ProductClassificationRecord
			wantApplied bool
			wantTags    []string
		}{
			{ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"Fragile"}, Version: 2}, true, []string{"Fragile"}},
			{ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"Hazmat", "TemperatureSensitive"}, TemperatureClass: "Frozen", DotHazardClass: 3, Version: 3}, true, []string{"Hazmat", "TemperatureSensitive"}},
			{ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"HighValue"}, Version: 3}, false, []string{"Hazmat", "TemperatureSensitive"}},
			{ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"Oversized"}, Version: 1}, false, []string{"Hazmat", "TemperatureSensitive"}},
		}
		for i, st := range steps {
			applied, err := store.Upsert(ctx, st.rec)
			if err != nil || applied != st.wantApplied {
				t.Fatalf("step %d: Upsert = %v, %v; want applied=%v", i, applied, err, st.wantApplied)
			}
			got, err := store.GetClassification(ctx, "SKU-1")
			if err != nil || !got.Known || !equal(got.HandlingTags, st.wantTags) {
				t.Fatalf("step %d: lookup = %+v, %v; want %v", i, got, err, st.wantTags)
			}
		}
		var version int64
		var temperature *string
		var hazard *int16
		if err := pool.QueryRow(ctx, `SELECT version, temperature_class, dot_hazard_class FROM product_classification_copy WHERE sku = 'SKU-1'`).
			Scan(&version, &temperature, &hazard); err != nil {
			t.Fatal(err)
		}
		if version != 3 || temperature == nil || *temperature != "Frozen" || hazard == nil || *hazard != 3 {
			t.Fatalf("row = version %d temperature %v hazard %v", version, temperature, hazard)
		}
	})

	t.Run("unset optional fields are stored as NULL and nil tags as empty", func(t *testing.T) {
		if _, err := store.Upsert(ctx, ports.ProductClassificationRecord{SKU: "SKU-2", Version: 1}); err != nil {
			t.Fatal(err)
		}
		var temperature *string
		var hazard *int16
		var tags []string
		if err := pool.QueryRow(ctx, `SELECT handling_tags, temperature_class, dot_hazard_class FROM product_classification_copy WHERE sku = 'SKU-2'`).
			Scan(&tags, &temperature, &hazard); err != nil {
			t.Fatal(err)
		}
		if temperature != nil || hazard != nil || len(tags) != 0 {
			t.Fatalf("row = tags %v temperature %v hazard %v; want empty/NULL/NULL", tags, temperature, hazard)
		}
	})

	t.Run("the claim and the upsert roll back together", func(t *testing.T) {
		uow := postgres.NewUnitOfWork(pool)
		failing := &failAfterUpsert{inner: store}
		uc := &usecases.ApplyProductClassification{Copy: failing, Processed: processed, UnitOfWork: uow}
		req := usecases.ApplyProductClassificationRequest{EventID: "evt-rollback", Record: ports.ProductClassificationRecord{SKU: "SKU-3", HandlingTags: []string{"Hazmat"}, Version: 1}}
		if err := uc.Execute(ctx, req); !errors.Is(err, errInjected) {
			t.Fatalf("err = %v, want the injected failure", err)
		}
		if got, _ := store.GetClassification(ctx, "SKU-3"); got.Known {
			t.Fatal("the upsert must have rolled back")
		}
		var claims int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM product_classification_processed_events WHERE event_id = 'evt-rollback'`).Scan(&claims); err != nil {
			t.Fatal(err)
		}
		if claims != 0 {
			t.Fatal("the claim must have rolled back with the upsert")
		}

		// The redelivery succeeds once the failure is gone, exactly once.
		uc.Copy = store
		if err := uc.Execute(ctx, req); err != nil {
			t.Fatalf("redelivery: %v", err)
		}
		if err := uc.Execute(ctx, req); err != nil {
			t.Fatalf("duplicate: %v", err)
		}
		if got, _ := store.GetClassification(ctx, "SKU-3"); !got.Known || !got.HasTag("Hazmat") {
			t.Fatalf("lookup = %+v, want the applied classification", got)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM product_classification_processed_events WHERE event_id = 'evt-rollback'`).Scan(&claims); err != nil {
			t.Fatal(err)
		}
		if claims != 1 {
			t.Fatalf("claims = %d, want 1", claims)
		}
	})

	t.Run("an unreadable copy fails open", func(t *testing.T) {
		closed, err := pgxpool.New(ctx, pool.Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		closed.Close()
		got, err := productclassificationcopy.NewPostgresStore(closed, nil).GetClassification(ctx, "SKU-1")
		if err != nil || got.Known {
			t.Fatalf("lookup on a closed pool = %+v, %v; want Known=false, nil error", got, err)
		}
	})
}

var errInjected = errors.New("injected failure after the upsert")

// failAfterUpsert really writes, then fails, so the rollback is observable.
type failAfterUpsert struct {
	inner ports.ProductClassificationCopy
}

func (f *failAfterUpsert) Upsert(ctx context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	if _, err := f.inner.Upsert(ctx, rec); err != nil {
		return false, err
	}
	return false, errInjected
}
