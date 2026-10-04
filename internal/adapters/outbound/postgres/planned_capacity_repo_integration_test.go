//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/domain/order"
)

func pcRow(id, location string, status order.PlannedCapacityStatus, asOf time.Time, shortage float64) order.PlannedCapacityWindow {
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	return order.PlannedCapacityWindow{
		PlanID: id, WarehouseID: "WH-1", Location: location, PathID: "pick-rebin-pack",
		Start: start, End: start.Add(8 * time.Hour), AssignedDemand: 12000, CapacityOverWindow: 8000,
		Shortage: shortage, BottleneckStep: "REBIN", Status: status, AsOf: asOf,
	}
}

// Real Postgres (testcontainers): the SQL adapter applies the same
// last-writer-wins rule as the in-memory one, because both defer to
// order.PlannedCapacityWindow.Supersedes.
func TestPlannedCapacityRepo_LastWriterWinsAgainstRealPostgres(t *testing.T) {
	ctx := context.Background()
	repo := postgres.NewPlannedCapacityRepo(outboxDB(t))
	asOf := time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)

	apply := func(w order.PlannedCapacityWindow, want bool) {
		t.Helper()
		if got, err := repo.Upsert(ctx, w); err != nil || got != want {
			t.Fatalf("Upsert(%s, %s, as of %s) = %v, %v; want %v", w.PlanID, w.Status, w.AsOf, got, err, want)
		}
	}
	apply(pcRow("plan-a", "SIM1", order.PlannedCapacityDraft, asOf.Add(-time.Minute), 4000), true)
	apply(pcRow("plan-a", "SIM1", order.PlannedCapacityPublished, asOf, 4000), true)
	apply(pcRow("plan-a", "SIM1", order.PlannedCapacityPublished, asOf.Add(-time.Second), 1), false)  // stale
	apply(pcRow("plan-a", "SIM1", order.PlannedCapacityDraft, asOf.Add(time.Hour), 2), false)         // downgrade
	apply(pcRow("plan-a", "SIM1", order.PlannedCapacityPublished, asOf.Add(time.Second), 5200), true) // re-plan
	apply(pcRow("plan-b", "SIM2", order.PlannedCapacityPublished, asOf, 90), true)

	got, err := repo.ListByLocation(ctx, "SIM1", time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	if err != nil || len(got) != 1 {
		t.Fatalf("ListByLocation = %+v, %v; want only plan-a", got, err)
	}
	want := pcRow("plan-a", "SIM1", order.PlannedCapacityPublished, asOf.Add(time.Second), 5200)
	if got[0] != want {
		t.Fatalf("stored = %+v\nwant     %+v", got[0], want)
	}

	// endingAfter is exclusive (window ends 2026-10-05T16:00:00Z).
	end := want.End
	if rows, _ := repo.ListByLocation(ctx, "SIM1", end); len(rows) != 0 {
		t.Fatalf("a window ending exactly at endingAfter must be excluded, got %+v", rows)
	}
	if rows, _ := repo.ListByLocation(ctx, "SIM1", end.Add(-time.Second)); len(rows) != 1 {
		t.Fatalf("a window ending one second after endingAfter must be included, got %+v", rows)
	}
}

// The heart of the at-least-once guarantee, against a real database: the
// idempotency claim and the upsert share ONE transaction, so a failure after
// both rolls BOTH back, and the redelivery is processed (not skipped).
func TestPlannedCapacityTransaction_RollsBackTheClaimWithTheWork(t *testing.T) {
	ctx := context.Background()
	pool := outboxDB(t)
	repo := postgres.NewPlannedCapacityRepo(pool)
	processed := postgres.NewPlannedCapacityProcessedEventsRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	asOf := time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)
	boom := errors.New("simulated failure after the write")

	err := uow.Execute(ctx, func(ctx context.Context) error {
		if isNew, err := processed.MarkProcessed(ctx, "evt-3b2a"); err != nil || !isNew {
			t.Fatalf("claim inside the tx = %v, %v; want true", isNew, err)
		}
		if applied, err := repo.Upsert(ctx, pcRow("plan-a", "SIM1", order.PlannedCapacityPublished, asOf, 4000)); err != nil || !applied {
			t.Fatalf("upsert inside the tx = %v, %v; want true", applied, err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Execute = %v, want the injected failure", err)
	}

	var rows, claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM planned_capacity_windows`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM planned_capacity_processed_events`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if rows != 0 || claims != 0 {
		t.Fatalf("after rollback rows=%d claims=%d, want 0 and 0: nothing written, claim un-recorded", rows, claims)
	}

	// The redelivery of the same id is therefore processed.
	err = uow.Execute(ctx, func(ctx context.Context) error {
		if isNew, err := processed.MarkProcessed(ctx, "evt-3b2a"); err != nil || !isNew {
			t.Fatalf("redelivered claim = %v, %v; want true (it was rolled back)", isNew, err)
		}
		_, err := repo.Upsert(ctx, pcRow("plan-a", "SIM1", order.PlannedCapacityPublished, asOf, 4000))
		return err
	})
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if isNew, err := processed.MarkProcessed(ctx, "evt-3b2a"); err != nil || isNew {
		t.Fatalf("a committed claim must make the next delivery a no-op, got %v, %v", isNew, err)
	}
}
