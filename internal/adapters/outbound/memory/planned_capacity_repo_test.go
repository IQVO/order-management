package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/domain/order"
)

func pcw(id, location string, start time.Time, status order.PlannedCapacityStatus, asOf time.Time, shortage float64) order.PlannedCapacityWindow {
	return order.PlannedCapacityWindow{
		PlanID: id, WarehouseID: "WH-7", Location: location, Start: start, End: start.Add(8 * time.Hour),
		AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: shortage, Status: status, AsOf: asOf,
	}
}

func TestPlannedCapacityRepo_UpsertIsLastWriterWinsAndListFilters(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewPlannedCapacityRepo()
	t0 := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)

	apply := func(w order.PlannedCapacityWindow, want bool) {
		t.Helper()
		got, err := repo.Upsert(ctx, w)
		if err != nil || got != want {
			t.Fatalf("Upsert(%s) = %v, %v; want %v", w.PlanID, got, err, want)
		}
	}
	apply(pcw("plan-b", "SIM1", t0.Add(24*time.Hour), order.PlannedCapacityPublished, asOf, 400), true)
	apply(pcw("plan-a", "SIM1", t0, order.PlannedCapacityPublished, asOf, 4000), true)
	apply(pcw("plan-c", "SIM2", t0, order.PlannedCapacityPublished, asOf, 90), true)
	apply(pcw("plan-a", "SIM1", t0, order.PlannedCapacityPublished, asOf.Add(-time.Second), 1), false) // stale
	apply(pcw("plan-a", "SIM1", t0, order.PlannedCapacityDraft, asOf.Add(time.Hour), 2), false)        // downgrade
	apply(pcw("plan-a", "SIM1", t0, order.PlannedCapacityPublished, asOf.Add(time.Second), 4100), true)

	got, err := repo.ListByLocation(ctx, "SIM1", t0)
	if err != nil || len(got) != 2 {
		t.Fatalf("ListByLocation = %v, %v; want 2 SIM1 windows", got, err)
	}
	if got[0].PlanID != "plan-a" || got[0].Shortage != 4100 || got[1].PlanID != "plan-b" {
		t.Fatalf("got %+v; want plan-a (latest write, shortage 4100) then plan-b, in start order", got)
	}

	// endingAfter is exclusive: plan-a ends at t0+8h.
	if got, _ := repo.ListByLocation(ctx, "SIM1", t0.Add(8*time.Hour)); len(got) != 1 || got[0].PlanID != "plan-b" {
		t.Fatalf("a window ending exactly at endingAfter must be excluded, got %+v", got)
	}
	if got, _ := repo.ListByLocation(ctx, "SIM1", t0.Add(8*time.Hour-time.Second)); len(got) != 2 {
		t.Fatalf("a window ending one second after endingAfter must be included, got %+v", got)
	}
}

func TestPlannedCapacityRepo_ListOrdersTiesByPlanID(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewPlannedCapacityRepo()
	t0 := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)
	for _, id := range []string{"plan-z", "plan-m", "plan-a"} {
		if _, err := repo.Upsert(ctx, pcw(id, "SIM1", t0, order.PlannedCapacityPublished, asOf, 5)); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := repo.ListByLocation(ctx, "SIM1", t0)
	if len(got) != 3 || got[0].PlanID != "plan-a" || got[1].PlanID != "plan-m" || got[2].PlanID != "plan-z" {
		t.Fatalf("got %+v; want plan-a, plan-m, plan-z", got)
	}
}

func TestPlannedCapacityProcessedEventsRepo_MarkProcessed(t *testing.T) {
	repo := memory.NewPlannedCapacityProcessedEventsRepo()
	ctx := context.Background()
	if isNew, err := repo.MarkProcessed(ctx, "evt-3b2a"); err != nil || !isNew {
		t.Fatalf("first mark = %v, %v; want true", isNew, err)
	}
	if isNew, err := repo.MarkProcessed(ctx, "evt-3b2a"); err != nil || isNew {
		t.Fatalf("second mark = %v, %v; want false", isNew, err)
	}
	if isNew, _ := repo.MarkProcessed(ctx, "evt-9f1c"); !isNew {
		t.Fatal("a different id is new")
	}
}
