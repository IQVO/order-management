package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/domain/order"
)

// ADR 0052: a SUPERSEDED write keeps the known row's facts, is terminal,
// and over an unseen plan inserts a tombstone.

func supersededWrite(id, site string, start, asOf time.Time) order.PlannedCapacityWindow {
	return order.PlannedCapacityWindow{
		PlanID: id, Location: site, PathID: "pick-rebin-pack", Start: start, End: start.Add(8 * time.Hour),
		Status: order.PlannedCapacitySuperseded, SupersededBy: "plan-newer", AsOf: asOf,
	}
}

func TestPlannedCapacityRepo_SupersededKeepsTheRowAndIsTerminal(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewPlannedCapacityRepo()
	t0 := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)

	published := pcw("plan-a", "SIM1", t0, order.PlannedCapacityPublished, asOf, 4000)
	if applied, err := repo.Upsert(ctx, published); err != nil || !applied {
		t.Fatalf("published = %v, %v", applied, err)
	}
	// Older than the published row: SUPERSEDED still wins.
	if applied, err := repo.Upsert(ctx, supersededWrite("plan-a", "SIM1", t0, asOf.Add(-time.Minute))); err != nil || !applied {
		t.Fatalf("superseded = %v, %v; want applied regardless of time", applied, err)
	}
	// A redelivered Published (newer) and a second Superseded are no-ops.
	if applied, _ := repo.Upsert(ctx, pcw("plan-a", "SIM1", t0, order.PlannedCapacityPublished, asOf.Add(time.Hour), 9000)); applied {
		t.Fatal("a Published after Superseded must never revive the plan")
	}
	second := supersededWrite("plan-a", "SIM1", t0, asOf.Add(time.Hour))
	second.SupersededBy = "plan-third"
	if applied, _ := repo.Upsert(ctx, second); applied {
		t.Fatal("a second Superseded must not replace the first")
	}

	got, _ := repo.ListByLocation(ctx, "SIM1", t0)
	want := published
	want.Status, want.SupersededBy, want.AsOf = order.PlannedCapacitySuperseded, "plan-newer", asOf.Add(-time.Minute)
	if len(got) != 1 || got[0] != want {
		t.Fatalf("stored = %+v\nwant     %+v", got, want)
	}
	if got[0].Constraining() {
		t.Fatal("a superseded row must never constrain")
	}
}

func TestPlannedCapacityRepo_SupersededForAnUnseenPlanIsATombstone(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewPlannedCapacityRepo()
	t0 := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)

	tombstone := supersededWrite("plan-unseen", "SIM1", t0, asOf)
	if applied, err := repo.Upsert(ctx, tombstone); err != nil || !applied {
		t.Fatalf("tombstone = %v, %v", applied, err)
	}
	if applied, _ := repo.Upsert(ctx, pcw("plan-unseen", "SIM1", t0, order.PlannedCapacityPublished, asOf.Add(time.Hour), 4000)); applied {
		t.Fatal("a late Published must not replace the tombstone")
	}
	got, _ := repo.ListByLocation(ctx, "SIM1", t0)
	if len(got) != 1 || got[0] != tombstone || got[0].Constraining() {
		t.Fatalf("stored = %+v; want the non-constraining tombstone %+v", got, tombstone)
	}
}
