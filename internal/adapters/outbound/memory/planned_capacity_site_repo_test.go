package memory_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/domain/order"
)

// ADR 0031 amendment: the annotation lists by site (site_id when the plan
// has one, else location); GET /planned-capacity keeps listing by location.
func TestPlannedCapacityRepo_ListForSiteMatchesBySiteIDElseLocation(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewPlannedCapacityRepo()
	t0 := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)

	withSite := func(w order.PlannedCapacityWindow, siteID string) order.PlannedCapacityWindow {
		w.SiteID = siteID
		return w
	}
	for _, w := range []order.PlannedCapacityWindow{
		withSite(pcw("zone-plan", "PATH-ZONE-A", t0, order.PlannedCapacityPublished, asOf, 10), "SIM1"),
		withSite(pcw("elsewhere", "SIM1", t0, order.PlannedCapacityPublished, asOf, 20), "SIM2"),
		pcw("legacy", "SIM1", t0.Add(time.Hour), order.PlannedCapacityPublished, asOf, 30),
		pcw("other-legacy", "SIM2", t0, order.PlannedCapacityPublished, asOf, 40),
	} {
		if _, err := repo.Upsert(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	// The shortage that follows the Published carries no site_id.
	late := pcw("zone-plan", "PATH-ZONE-A", t0, order.PlannedCapacityPublished, asOf.Add(time.Second), 15)
	if applied, err := repo.Upsert(ctx, late); err != nil || !applied {
		t.Fatalf("Upsert shortage = %v, %v", applied, err)
	}

	ids := func(ws []order.PlannedCapacityWindow) string {
		var out []string
		for _, w := range ws {
			out = append(out, w.PlanID)
		}
		return strings.Join(out, ",")
	}

	got, err := repo.ListForSite(ctx, "SIM1", t0)
	if err != nil || ids(got) != "zone-plan,legacy" {
		t.Fatalf("ListForSite(SIM1) = %v, %v; want zone-plan (site_id kept through the shortage) then legacy", ids(got), err)
	}
	if got[0].Shortage != 15 || got[0].SiteID != "SIM1" {
		t.Fatalf("zone-plan = %+v; want the shortage write with the stored site_id", got[0])
	}
	for _, tc := range []struct {
		name   string
		list   func(string, time.Time) ([]order.PlannedCapacityWindow, error)
		key    string
		wanted string
	}{
		{"a zone is not a site", func(k string, at time.Time) ([]order.PlannedCapacityWindow, error) {
			return repo.ListForSite(ctx, k, at)
		}, "PATH-ZONE-A", ""},
		{"SIM2 by site", func(k string, at time.Time) ([]order.PlannedCapacityWindow, error) {
			return repo.ListForSite(ctx, k, at)
		}, "SIM2", "elsewhere,other-legacy"},
		// ListByLocation is unchanged: it filters on location alone.
		{"SIM1 by location", func(k string, at time.Time) ([]order.PlannedCapacityWindow, error) {
			return repo.ListByLocation(ctx, k, at)
		}, "SIM1", "elsewhere,legacy"},
		{"the zone by location", func(k string, at time.Time) ([]order.PlannedCapacityWindow, error) {
			return repo.ListByLocation(ctx, k, at)
		}, "PATH-ZONE-A", "zone-plan"},
	} {
		rows, err := tc.list(tc.key, t0)
		if err != nil || ids(rows) != tc.wanted {
			t.Errorf("%s: got %q, %v; want %q", tc.name, ids(rows), err, tc.wanted)
		}
	}
}
