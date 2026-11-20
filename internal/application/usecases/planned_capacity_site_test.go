package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// ADR 0031 amendment: the order annotation matches a plan to the order's
// site by the plan's site_id when it has one, else by its location.
func TestOrderCapacityConstraints_For_MatchesBySiteID(t *testing.T) {
	tests := []struct {
		name     string
		siteID   string
		location string
		want     int
	}{
		{"a zone location whose site_id is the order's site annotates", "SIM1", "PATH-ZONE-A", 1},
		{"another site_id does not annotate even when location equals the site", "SIM2", "SIM1", 0},
		{"a legacy plan without site_id annotates by its location", "", "SIM1", 1},
		{"a legacy plan at another location does not annotate", "", "SIM2", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newPCTxStore()
			w := pcWindow()
			w.SiteID, w.Location = tt.siteID, tt.location
			s.windows[w.PlanID] = w
			uc := &usecases.OrderCapacityConstraints{Windows: s, Clock: memory.NewFixedClock(pcNow), DefaultSiteID: "SIM1"}

			got, err := uc.For(context.Background(), pcPromisedOrder(t, pcEnd.Add(time.Hour)))
			if err != nil || len(got) != tt.want {
				t.Fatalf("For = %v, %v; want %d windows", got, err, tt.want)
			}
		})
	}
}

// The read model and the order annotation through the real in-memory
// repo: Published (site_id) then Shortage (none) still annotates.
func TestOrderCapacityConstraints_For_SiteIDSurvivesALaterWriteWithoutOne(t *testing.T) {
	repo := memory.NewPlannedCapacityRepo()
	ctx := context.Background()
	pub := pcWindow()
	pub.SiteID, pub.Location = "SIM1", "PATH-ZONE-A"
	shortage := pcWindow()
	shortage.Location, shortage.Shortage = "PATH-ZONE-A", 4500
	for _, w := range []order.PlannedCapacityWindow{pub, shortage} {
		if _, err := repo.Upsert(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	uc := &usecases.OrderCapacityConstraints{Windows: repo, Clock: memory.NewFixedClock(pcNow), DefaultSiteID: "SIM1"}
	got, err := uc.For(ctx, pcPromisedOrder(t, pcEnd.Add(time.Hour)))
	if err != nil || len(got) != 1 || got[0].Shortage != 4500 || got[0].SiteID != "SIM1" {
		t.Fatalf("For = %+v, %v; want the shortage write, still carrying site_id SIM1", got, err)
	}
}
