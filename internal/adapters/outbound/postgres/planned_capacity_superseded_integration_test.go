//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/domain/order"
)

// ADR 0052 against real Postgres (migration 0015): a SUPERSEDED write keeps
// the known row's facts and stores superseded_by; it is terminal; over an
// unseen plan it is a tombstone a late Published cannot replace.
func TestPlannedCapacityRepo_SupersededAgainstRealPostgres(t *testing.T) {
	ctx := context.Background()
	repo := postgres.NewPlannedCapacityRepo(outboxDB(t))
	asOf := time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)
	from := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)

	apply := func(w order.PlannedCapacityWindow, want bool) {
		t.Helper()
		if got, err := repo.Upsert(ctx, w); err != nil || got != want {
			t.Fatalf("Upsert(%s, %s) = %v, %v; want %v", w.PlanID, w.Status, got, err, want)
		}
	}
	superseded := func(id string, at time.Time) order.PlannedCapacityWindow {
		w := pcRow(id, "SIM1", order.PlannedCapacitySuperseded, at, 0)
		w.WarehouseID, w.AssignedDemand, w.CapacityOverWindow, w.BottleneckStep = "", 0, 0, ""
		w.SupersededBy = "plan-newer"
		return w
	}

	published := pcRow("plan-a", "SIM1", order.PlannedCapacityPublished, asOf, 4000)
	apply(published, true)
	apply(superseded("plan-a", asOf.Add(-time.Minute)), true)                                     // wins regardless of time
	apply(pcRow("plan-a", "SIM1", order.PlannedCapacityPublished, asOf.Add(time.Hour), 9), false) // never revived
	apply(superseded("plan-unseen", asOf), true)                                                  // tombstone
	apply(pcRow("plan-unseen", "SIM1", order.PlannedCapacityPublished, asOf.Add(time.Hour), 4000), false)

	got, err := repo.ListByLocation(ctx, "SIM1", from)
	if err != nil || len(got) != 2 {
		t.Fatalf("ListByLocation = %+v, %v; want plan-a and plan-unseen", got, err)
	}
	wantA := published
	wantA.Status, wantA.SupersededBy, wantA.AsOf = order.PlannedCapacitySuperseded, "plan-newer", asOf.Add(-time.Minute)
	if got[0] != wantA {
		t.Fatalf("plan-a = %+v\nwant     %+v", got[0], wantA)
	}
	if want := superseded("plan-unseen", asOf); got[1] != want {
		t.Fatalf("tombstone = %+v\nwant        %+v", got[1], want)
	}
	for _, w := range got {
		if w.Constraining() {
			t.Fatalf("%s is SUPERSEDED and must never constrain", w.PlanID)
		}
	}
}
